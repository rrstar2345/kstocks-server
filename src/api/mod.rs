//! Read-only HTTP API serving already-aggregated OHLC bars from
//! `index_ohlc_1m` / `index_ohlc_1d` / `option_ohlc_1m`. Never touches raw
//! ticks and never represents the in-progress current candle — that's the
//! desktop app's job once it's live on NSE's WSS. No push/streaming
//! endpoint (explicit non-goal).
//!
//! Uses its own `SqlitePool` (separate from the ingest writers, same DB
//! file) with a busy_timeout pragma, so slow reads never contend with the
//! ingest writer under WAL mode.

mod admin;
mod auth_middleware;
mod client_auth;
mod health;
mod index_ohlc;
mod option_ohlc;
mod register;
mod validate;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use serde::Serialize;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};

use crate::market::market_clock::SharedSessionState;
use crate::settings::{AppConfig, DatabaseConfig};
use crate::stats::SharedStats;

pub(crate) struct ApiState {
    pub(crate) pool: SqlitePool,
    #[allow(dead_code)]
    pub(crate) stats: SharedStats,
    pub(crate) session: SharedSessionState,
    /// Separate `kstocks-users.db` pool: client registrations, approval
    /// status, and the admin token hash. Kept apart from `pool` (market
    /// data) so the two subsystems never contend or share a failure mode.
    pub(crate) users_pool: SqlitePool,
}

/// Open a dedicated read pool for the API. Same DB file as the ingest
/// writers, but its own connection pool with a busy_timeout so contention
/// under WAL mode never blocks either side for long.
async fn init_api_pool(db_config: &DatabaseConfig) -> anyhow::Result<SqlitePool> {
    let connect_options = SqliteConnectOptions::new()
        .filename(&db_config.connection_string)
        .create_if_missing(false)
        .busy_timeout(std::time::Duration::from_secs(5));

    let pool = SqlitePool::connect_with(connect_options).await?;
    Ok(pool)
}

/// Spawn the read-only HTTP API on `config.api.port`. Runs for the
/// lifetime of the process.
pub fn spawn_api_server(
    config: AppConfig,
    stats: SharedStats,
    session: SharedSessionState,
    users_pool: SqlitePool,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let pool = match init_api_pool(&config.database).await {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to open API read pool: {}", e);
                return;
            }
        };

        let state = Arc::new(ApiState { pool, stats, session, users_pool });

        // `/ohlc/*` requires an approved client key.
        let ohlc_routes = Router::new()
            .route("/ohlc/index", get(index_ohlc::get_index_ohlc))
            .route("/ohlc/option", get(option_ohlc::get_option_ohlc))
            .route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware::require_approved_client,
            ));

        // `/admin/*` requires the separate admin token.
        let admin_routes = admin::admin_router().route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            admin::require_admin_token,
        ));

        let app = Router::new()
            .merge(ohlc_routes)
            .merge(admin_routes)
            .route("/health", get(health::get_health))
            .route("/register", axum::routing::post(register::post_register))
            .route("/validate", get(validate::get_validate))
            .with_state(state);

        let addr = format!("0.0.0.0:{}", config.api.port);
        info!("Starting read-only HTTP API on {}", addr);

        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind API listener on {}: {}", addr, e);
                return;
            }
        };

        // `into_make_service_with_connect_info` is required for the
        // `ConnectInfo<SocketAddr>` extractor used by `/register`'s
        // rate limiting.
        if let Err(e) =
            axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await
        {
            error!("API server error: {}", e);
        }
    })
}

// ============================================================================
// SHARED ERROR TYPE
// ============================================================================

#[derive(Debug, Serialize)]
pub(crate) struct ApiErrorBody {
    pub(crate) error: String,
}

pub(crate) fn bad_request(msg: impl Into<String>) -> axum::response::Response {
    (StatusCode::BAD_REQUEST, Json(ApiErrorBody { error: msg.into() })).into_response()
}

// ============================================================================
// RANGE / INTERVAL RESOLUTION (shared by index_ohlc and option_ohlc)
// ============================================================================

/// Source tier + SQL-friendly bucketing width for one validated
/// range/interval combination.
pub(crate) struct Resolved {
    pub(crate) table: &'static str,
    /// Duration of a single output bucket, in seconds. If this equals the
    /// source tier's native bucket size, no on-read aggregation is needed.
    pub(crate) bucket_secs: i64,
    /// Number of days of history to look back for the given range.
    pub(crate) lookback_days: i64,
}

pub(crate) fn interval_to_secs(interval: &str) -> Option<i64> {
    Some(match interval {
        "1m" => 60,
        "3m" => 180,
        "5m" => 300,
        "15m" => 900,
        "30m" => 1800,
        "1h" => 3600,
        "2h" => 7200,
        "4h" => 14400,
        "1d" => 86400,
        "1w" => 7 * 86400,
        "1mo" => 30 * 86400,
        _ => return None,
    })
}