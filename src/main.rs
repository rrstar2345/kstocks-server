mod api;
mod market;
mod settings;
mod stats;
mod storage;
mod users;
mod utils;

use clap::{Parser, Subcommand};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use market::market_clock;
use market::market_clock::new_shared_session_state;
use market::{streamers, symbols};
use settings::{load_or_create_config, setup_app_folders};
use stats::new_shared_stats;
use storage::{ohlc as aggregation, retention};
use users::init_users_pool;
use utils::dashboard;

#[derive(Parser, Debug)]
#[command(name = "kstocks-server", about = "NSE market data collector: WSS -> SQLite")]
struct Cli {
    /// Disable the interactive terminal dashboard (useful when run as a
    /// scheduled/headless task, e.g. via systemd or cron).
    #[arg(long)]
    no_dashboard: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Admin-token management. Does not start the server/streamers — this
    /// is a one-shot mode that only touches `kstocks-users.db`. Running it
    /// requires shell access to the host, which is the intended access
    /// control: the running server/admin API never has a code path that
    /// can create or rotate its own admin token.
    Admin {
        #[command(subcommand)]
        action: AdminAction,
    },
}

#[derive(Subcommand, Debug)]
enum AdminAction {
    /// Generate a new admin token. Fails if one already exists — use
    /// `regenerate` to rotate.
    Generate,
    /// Rotate the admin token, invalidating the previous one.
    Regenerate,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let paths = setup_app_folders()?;
    let users_db_path = paths.db_dir.join("kstocks-users.db").to_string_lossy().to_string();

    // `admin` subcommand: one-shot token management, no server startup.
    if let Some(Command::Admin { action }) = &cli.command {
        // Minimal logging to stderr only; this mode doesn't run long enough
        // to need file logging, and shouldn't require the full app folder
        // logging setup below.
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
            .init();

        match action {
            AdminAction::Generate => {
                let pool = users::init_users_pool(&users_db_path).await?;
                if users::get_admin_token_hash(&pool).await?.is_some() {
                    eprintln!(
                        "An admin token already exists. Use `kstocks-server admin regenerate` to rotate it."
                    );
                    std::process::exit(1);
                }
                users::admin_cli::run_generate(&users_db_path).await?;
            }
            AdminAction::Regenerate => {
                users::admin_cli::run_generate(&users_db_path).await?;
            }
        }
        return Ok(());
    }

    // File logging always on; when the dashboard takes over the terminal we
    // still want logs captured somewhere, so write to a log file under the
    // app's logs directory instead of stdout.
    let log_path = paths.logs_dir.join("kstocks-server.log");
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_writer(std::sync::Mutex::new(log_file))
        .init();

    info!("Starting kstocks-server, logs at {}", log_path.display());

    let config = load_or_create_config(&paths)?;

    let pool = match storage::init_pool(&config.database).await {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to initialize database: {}", e);
            eprintln!("Failed to connect to SQLite database: {e}");
            eprintln!("Check `database.connection_string` in {}", paths.settings_file.display());
            return Err(e);
        }
    };
    info!("SQLite connected and schema verified");

    let stats = new_shared_stats();

    // Market-hours session state, driven by NSE's own IST clock so this
    // works correctly regardless of the server's own timezone/region.
    let session = new_shared_session_state();
    market_clock::refresh_if_stale(&config).await;
    {
        let mut s = stats.write().await;
        s.session_mode_label = session.mode().await.label().to_string();
    }

    // Background supervisor: periodically re-syncs the NSE time offset and
    // re-evaluates Active/Idle mode (trading window + inactivity timeout).
    let supervisor_config = config.clone();
    let supervisor_stats = stats.clone();
    let supervisor_session = session.clone();
    let supervisor_handle = tokio::spawn(async move {
        loop {
            market_clock::refresh_if_stale(&supervisor_config).await;
            let mode = supervisor_session
                .tick(supervisor_config.runtime.inactive_switch_after_secs)
                .await;
            {
                let mut s = supervisor_stats.write().await;
                s.session_mode_label = mode.label().to_string();
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        }
    });

    // Batched writers (one per tick type), each backed by an mpsc channel so
    // streamers never block on the DB.
    let (index_tx, index_writer_handle) =
        storage::start_index_tick_writer(pool.clone(), config.database.clone(), stats.clone());
    let (option_tx, option_writer_handle) =
        storage::start_option_tick_writer(pool.clone(), config.database.clone(), stats.clone());

    // Resolve the 5 F&O symbols + nearest expiry dynamically from NSE.
    info!("Resolving F&O symbols and nearest expiries...");
    let symbol_expiries = symbols::resolve_symbol_expiries(&config).await?;
    for se in &symbol_expiries {
        info!("Will stream options for {} / {}", se.symbol, se.expiry);
    }

    // 1 indices streamer.
    let indices_handle = tokio::spawn(streamers::indices::run(
        config.clone(),
        index_tx.clone(),
        stats.clone(),
        session.clone(),
    ));

    // Up to 5 option streamers (one per resolved F&O symbol).
    let mut option_handles = Vec::with_capacity(symbol_expiries.len());
    for se in symbol_expiries {
        let handle = tokio::spawn(streamers::options::run(
            config.clone(),
            se.symbol,
            se.expiry,
            option_tx.clone(),
            stats.clone(),
            session.clone(),
        ));
        option_handles.push(handle);
    }

    // Aggregation: 1-minute OHLC bars every `run_interval_secs`, plus a
    // once-daily 1m -> 1d rollup after market close.
    let agg_1m_handle = aggregation::spawn_1m_aggregation_loop(pool.clone(), config.aggregation.run_interval_secs);
    let agg_rollup_handle = aggregation::spawn_daily_rollup_loop(pool.clone());

    // Retention: once-daily purge (raw ticks + 1m tiers + expired options),
    // weekly VACUUM.
    let retention_handle = retention::spawn_retention_loop(pool.clone(), config.retention.clone());

    // Separate users DB (registrations, approval status, admin token hash).
    // Opened here (not just in the `admin` subcommand) so the running
    // server can serve /register, /validate, and /admin/* itself.
    let users_pool = match init_users_pool(&users_db_path).await {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to initialize users database: {}", e);
            eprintln!("Failed to connect to users SQLite database: {e}");
            return Err(e);
        }
    };
    info!("Users database connected and schema verified");

    // Read-only HTTP OHLC API, on its own SqlitePool.
    let api_handle =
        api::spawn_api_server(config.clone(), stats.clone(), session.clone(), users_pool);

    if cli.no_dashboard {
        info!("Running headless (--no-dashboard). Ctrl-C to stop.");
        tokio::signal::ctrl_c().await?;
        info!("Ctrl-C received, shutting down.");
    } else {
        // Dashboard owns the terminal until the user quits; streaming keeps
        // running in the background tasks regardless.
        dashboard::run(stats.clone()).await?;
        info!("Dashboard closed by user. Streaming continues in background; Ctrl-C to fully stop.");
        tokio::signal::ctrl_c().await?;
    }

    // Drop senders so the writers flush any remaining buffered rows and exit.
    drop(index_tx);
    drop(option_tx);
    indices_handle.abort();
    for h in option_handles {
        h.abort();
    }
    supervisor_handle.abort();
    agg_1m_handle.abort();
    agg_rollup_handle.abort();
    retention_handle.abort();
    api_handle.abort();
    let _ = index_writer_handle.await;
    let _ = option_writer_handle.await;

    info!("kstocks-server shut down cleanly.");
    Ok(())
}