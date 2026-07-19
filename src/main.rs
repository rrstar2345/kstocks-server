mod dashboard;
mod db;
mod http;
mod settings;
mod stats;
mod streamers;
mod symbols;

use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use settings::{load_or_create_config, setup_app_folders};
use stats::new_shared_stats;

#[derive(Parser, Debug)]
#[command(name = "kstocks-server", about = "NSE market data collector: WSS -> QuestDB")]
struct Cli {
    /// Disable the interactive terminal dashboard (useful when run as a
    /// scheduled/headless task, e.g. via systemd or cron).
    #[arg(long)]
    no_dashboard: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let paths = setup_app_folders()?;

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

    let config = load_or_create_config(&paths.settings_file)?;

    let pool = match db::init_pool(&config.database).await {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to initialize database: {}", e);
            eprintln!("Failed to connect to QuestDB: {e}");
            eprintln!("Check `database.connection_string` in {}", paths.settings_file.display());
            eprintln!("Make sure QuestDB is running and reachable on its Postgres-wire port (default 8812).");
            return Err(e);
        }
    };
    info!("QuestDB connected and schema verified");

    let stats = new_shared_stats();

    // Batched writers (one per tick type), each backed by an mpsc channel so
    // streamers never block on the DB.
    let (index_tx, index_writer_handle) =
        db::start_index_tick_writer(pool.clone(), config.database.clone(), stats.clone());
    let (option_tx, option_writer_handle) =
        db::start_option_tick_writer(pool.clone(), config.database.clone(), stats.clone());

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
        ));
        option_handles.push(handle);
    }

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
    let _ = index_writer_handle.await;
    let _ = option_writer_handle.await;

    info!("kstocks-server shut down cleanly.");
    Ok(())
}