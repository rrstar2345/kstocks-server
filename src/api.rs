//! Read-only HTTP API serving already-aggregated OHLC bars from
//! `index_ohlc_1m` / `index_ohlc_1d` / `option_ohlc_1m`. Never touches raw
//! ticks and never represents the in-progress current candle — that's the
//! desktop app's job once it's live on NSE's WSS. No push/streaming
//! endpoint (explicit non-goal).
//!
//! Uses its own `SqlitePool` (separate from the ingest writers, same DB
//! file) with a busy_timeout pragma, so slow reads never contend with the
//! ingest writer under WAL mode.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info};

use crate::market_clock::{self, SessionMode};
use crate::settings::{AppConfig, DatabaseConfig};
use crate::stats::SharedStats;

#[derive(Clone)]
struct ApiState {
    pool: SqlitePool,
    #[allow(dead_code)]
    stats: SharedStats,
    session: crate::market_clock::SharedSessionState,
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
    session: crate::market_clock::SharedSessionState,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let pool = match init_api_pool(&config.database).await {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to open API read pool: {}", e);
                return;
            }
        };

        let state = Arc::new(ApiState { pool, stats, session });

        let app = Router::new()
            .route("/ohlc/index", get(get_index_ohlc))
            .route("/ohlc/option", get(get_option_ohlc))
            .route("/health", get(get_health))
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

        if let Err(e) = axum::serve(listener, app).await {
            error!("API server error: {}", e);
        }
    })
}

// ============================================================================
// SHARED TYPES
// ============================================================================

#[derive(Debug, Serialize)]
struct Bar {
    bucket_start: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

#[derive(Debug, Serialize)]
struct OptionBar {
    bucket_start: String,
    ce_open: Option<f64>,
    ce_high: Option<f64>,
    ce_low: Option<f64>,
    ce_close: Option<f64>,
    ce_volume: Option<i64>,
    ce_oi_close: Option<f64>,
    pe_open: Option<f64>,
    pe_high: Option<f64>,
    pe_low: Option<f64>,
    pe_close: Option<f64>,
    pe_volume: Option<i64>,
    pe_oi_close: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ApiErrorBody {
    error: String,
}

fn bad_request(msg: impl Into<String>) -> axum::response::Response {
    (StatusCode::BAD_REQUEST, Json(ApiErrorBody { error: msg.into() })).into_response()
}

// ============================================================================
// RANGE / INTERVAL VALIDATION
// ============================================================================

/// Source tier + SQL-friendly bucketing width for one validated
/// range/interval combination.
struct Resolved {
    table: &'static str,
    /// Duration of a single output bucket, in seconds. If this equals the
    /// source tier's native bucket size, no on-read aggregation is needed.
    bucket_secs: i64,
    /// Number of days of history to look back for the given range.
    lookback_days: i64,
}

fn interval_to_secs(interval: &str) -> Option<i64> {
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

fn resolve_index_params(range: &str, interval: &str) -> Result<Resolved, String> {
    let valid: &[&str] = match range {
        "1d" => &["1m", "3m", "5m", "15m", "30m"],
        "3d" => &["15m", "30m", "1h"],
        "5d" => &["30m", "1h", "2h"],
        "7d" => &["1h", "2h", "4h"],
        "14d" => &["2h", "4h"],
        "1mo" => &["4h", "1d"],
        "3mo" => &["1d", "1w"],
        "6mo" => &["1d", "1w"],
        "1y" => &["1w", "1mo"],
        _ => return Err(format!("invalid range '{}'", range)),
    };

    if !valid.contains(&interval) {
        return Err(format!("invalid interval '{}' for range '{}'; valid: {:?}", interval, range, valid));
    }

    let lookback_days = match range {
        "1d" => 1,
        "3d" => 3,
        "5d" => 5,
        "7d" => 7,
        "14d" => 14,
        "1mo" => 31,
        "3mo" => 93,
        "6mo" => 186,
        "1y" => 366,
        _ => unreachable!(),
    };

    // Source table: index_ohlc_1m for everything except the daily/weekly/
    // monthly buckets on 1mo/3mo/6mo/1y ranges, which come from
    // index_ohlc_1d. On the "1mo" range specifically, "4h" still comes
    // from index_ohlc_1m while "1d" comes from index_ohlc_1d.
    let table = match (range, interval) {
        ("1mo", "1d") => "index_ohlc_1d",
        ("3mo", _) | ("6mo", _) | ("1y", _) => "index_ohlc_1d",
        _ => "index_ohlc_1m",
    };

    let bucket_secs = interval_to_secs(interval).ok_or_else(|| format!("invalid interval '{}'", interval))?;

    Ok(Resolved { table, bucket_secs, lookback_days })
}

fn resolve_option_params(range: &str, interval: &str) -> Result<Resolved, String> {
    let valid: &[&str] = match range {
        "1d" => &["1m", "5m", "15m"],
        "3d" => &["15m", "30m", "1h"],
        "5d" => &["30m", "1h", "2h"],
        "7d" => &["1h", "2h", "4h"],
        "14d" => &["2h", "4h"],
        _ => return Err(format!("invalid range '{}'", range)),
    };

    if !valid.contains(&interval) {
        return Err(format!("invalid interval '{}' for range '{}'; valid: {:?}", interval, range, valid));
    }

    let lookback_days = match range {
        "1d" => 1,
        "3d" => 3,
        "5d" => 5,
        "7d" => 7,
        "14d" => 14,
        _ => unreachable!(),
    };

    let bucket_secs = interval_to_secs(interval).ok_or_else(|| format!("invalid interval '{}'", interval))?;

    Ok(Resolved { table: "option_ohlc_1m", bucket_secs, lookback_days })
}

// ============================================================================
// GET /ohlc/index
// ============================================================================

#[derive(Debug, Deserialize)]
struct IndexOhlcQuery {
    name: String,
    range: String,
    interval: String,
}

async fn get_index_ohlc(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<IndexOhlcQuery>,
) -> axum::response::Response {
    let resolved = match resolve_index_params(&q.range, &q.interval) {
        Ok(r) => r,
        Err(msg) => return bad_request(msg),
    };

    let since = chrono::Utc::now() - chrono::Duration::days(resolved.lookback_days);

    // Proper OHLC aggregation (first open, max high, min low, last close)
    // per bucket_group needs "first"/"last" semantics that plain GROUP BY
    // can't express directly, so window functions pick the first/last row
    // per group before the final aggregation.
    let windowed_sql = format!(
        r#"
        WITH grouped AS (
            SELECT
                bucket_start,
                (CAST(strftime('%s', bucket_start) AS INTEGER) / {bucket_secs}) * {bucket_secs} AS bucket_group,
                open, high, low, close,
                ROW_NUMBER() OVER (PARTITION BY (CAST(strftime('%s', bucket_start) AS INTEGER) / {bucket_secs}) ORDER BY bucket_start ASC) AS rn_first,
                ROW_NUMBER() OVER (PARTITION BY (CAST(strftime('%s', bucket_start) AS INTEGER) / {bucket_secs}) ORDER BY bucket_start DESC) AS rn_last
            FROM {table}
            WHERE index_name = ? AND bucket_start >= ?
        )
        SELECT
            bucket_group,
            MAX(CASE WHEN rn_first = 1 THEN open END) AS open,
            MAX(high) AS high,
            MIN(low) AS low,
            MAX(CASE WHEN rn_last = 1 THEN close END) AS close
        FROM grouped
        GROUP BY bucket_group
        ORDER BY bucket_group ASC
        "#,
        bucket_secs = resolved.bucket_secs,
        table = resolved.table,
    );

    // Safe: `windowed_sql` only interpolates our own trusted constants
    // (bucket_secs, a fixed table name from `resolved.table`) — never
    // request-supplied strings. All actual request values (name, since)
    // are passed as bind parameters below.
    let rows = match sqlx::query(sqlx::AssertSqlSafe(windowed_sql))
        .bind(&q.name)
        .bind(since.to_rfc3339())
        .fetch_all(&state.pool)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("get_index_ohlc query failed: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiErrorBody { error: "query failed".into() }))
                .into_response();
        }
    };

    let mut bars = Vec::with_capacity(rows.len());
    for row in rows {
        let bucket_group: i64 = match row.try_get("bucket_group") {
            Ok(v) => v,
            Err(_) => continue,
        };
        let open: Option<f64> = row.try_get("open").ok();
        let high: Option<f64> = row.try_get("high").ok();
        let low: Option<f64> = row.try_get("low").ok();
        let close: Option<f64> = row.try_get("close").ok();

        // Gap rule: only emit buckets that actually have data.
        if let (Some(open), Some(high), Some(low), Some(close)) = (open, high, low, close) {
            let bucket_start = chrono::DateTime::<chrono::Utc>::from_timestamp(bucket_group, 0)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default();
            bars.push(Bar { bucket_start, open, high, low, close });
        }
    }

    Json(bars).into_response()
}

// ============================================================================
// GET /ohlc/option
// ============================================================================

#[derive(Debug, Deserialize)]
struct OptionOhlcQuery {
    symbol: String,
    expiry: String,
    strike: f64,
    range: String,
    interval: String,
    #[serde(default = "default_leg")]
    leg: String,
}

fn default_leg() -> String {
    "both".to_string()
}

async fn get_option_ohlc(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<OptionOhlcQuery>,
) -> axum::response::Response {
    let resolved = match resolve_option_params(&q.range, &q.interval) {
        Ok(r) => r,
        Err(msg) => return bad_request(msg),
    };

    if !["CE", "PE", "both"].contains(&q.leg.as_str()) {
        return bad_request("invalid leg; must be CE, PE, or both");
    }

    let since = chrono::Utc::now() - chrono::Duration::days(resolved.lookback_days);

    let windowed_sql = format!(
        r#"
        WITH grouped AS (
            SELECT
                bucket_start,
                (CAST(strftime('%s', bucket_start) AS INTEGER) / {bucket_secs}) * {bucket_secs} AS bucket_group,
                ce_open, ce_high, ce_low, ce_close, ce_volume, ce_oi_close,
                pe_open, pe_high, pe_low, pe_close, pe_volume, pe_oi_close,
                ROW_NUMBER() OVER (PARTITION BY (CAST(strftime('%s', bucket_start) AS INTEGER) / {bucket_secs}) ORDER BY bucket_start ASC) AS rn_first,
                ROW_NUMBER() OVER (PARTITION BY (CAST(strftime('%s', bucket_start) AS INTEGER) / {bucket_secs}) ORDER BY bucket_start DESC) AS rn_last
            FROM option_ohlc_1m
            WHERE symbol = ? AND expiry = ? AND strike_price = ? AND bucket_start >= ?
        )
        SELECT
            bucket_group,
            MAX(CASE WHEN rn_first = 1 THEN ce_open END) AS ce_open,
            MAX(ce_high) AS ce_high,
            MIN(ce_low) AS ce_low,
            MAX(CASE WHEN rn_last = 1 THEN ce_close END) AS ce_close,
            MAX(CASE WHEN rn_last = 1 THEN ce_volume END) AS ce_volume,
            MAX(CASE WHEN rn_last = 1 THEN ce_oi_close END) AS ce_oi_close,
            MAX(CASE WHEN rn_first = 1 THEN pe_open END) AS pe_open,
            MAX(pe_high) AS pe_high,
            MIN(pe_low) AS pe_low,
            MAX(CASE WHEN rn_last = 1 THEN pe_close END) AS pe_close,
            MAX(CASE WHEN rn_last = 1 THEN pe_volume END) AS pe_volume,
            MAX(CASE WHEN rn_last = 1 THEN pe_oi_close END) AS pe_oi_close
        FROM grouped
        GROUP BY bucket_group
        ORDER BY bucket_group ASC
        "#,
        bucket_secs = resolved.bucket_secs,
    );

    // Safe: `windowed_sql` only interpolates our own trusted constant
    // (bucket_secs) — never request-supplied strings. All actual request
    // values (symbol, expiry, strike, since) are bind parameters below.
    let rows = match sqlx::query(sqlx::AssertSqlSafe(windowed_sql))
        .bind(&q.symbol)
        .bind(&q.expiry)
        .bind(q.strike)
        .bind(since.to_rfc3339())
        .fetch_all(&state.pool)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("get_option_ohlc query failed: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiErrorBody { error: "query failed".into() }))
                .into_response();
        }
    };

    let mut bars = Vec::with_capacity(rows.len());
    for row in rows {
        let bucket_group: i64 = match row.try_get("bucket_group") {
            Ok(v) => v,
            Err(_) => continue,
        };

        let ce_open: Option<f64> = row.try_get("ce_open").ok();
        let ce_high: Option<f64> = row.try_get("ce_high").ok();
        let ce_low: Option<f64> = row.try_get("ce_low").ok();
        let ce_close: Option<f64> = row.try_get("ce_close").ok();
        let ce_volume: Option<i64> = row.try_get("ce_volume").ok();
        let ce_oi_close: Option<f64> = row.try_get("ce_oi_close").ok();

        let pe_open: Option<f64> = row.try_get("pe_open").ok();
        let pe_high: Option<f64> = row.try_get("pe_high").ok();
        let pe_low: Option<f64> = row.try_get("pe_low").ok();
        let pe_close: Option<f64> = row.try_get("pe_close").ok();
        let pe_volume: Option<i64> = row.try_get("pe_volume").ok();
        let pe_oi_close: Option<f64> = row.try_get("pe_oi_close").ok();

        // Gap rule: skip buckets with no data for the requested leg(s) at all.
        let has_ce = ce_close.is_some();
        let has_pe = pe_close.is_some();
        let include = match q.leg.as_str() {
            "CE" => has_ce,
            "PE" => has_pe,
            _ => has_ce || has_pe,
        };
        if !include {
            continue;
        }

        let bucket_start = chrono::DateTime::<chrono::Utc>::from_timestamp(bucket_group, 0)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default();

        let (ce_open, ce_high, ce_low, ce_close, ce_volume, ce_oi_close) = if q.leg == "PE" {
            (None, None, None, None, None, None)
        } else {
            (ce_open, ce_high, ce_low, ce_close, ce_volume, ce_oi_close)
        };
        let (pe_open, pe_high, pe_low, pe_close, pe_volume, pe_oi_close) = if q.leg == "CE" {
            (None, None, None, None, None, None)
        } else {
            (pe_open, pe_high, pe_low, pe_close, pe_volume, pe_oi_close)
        };

        bars.push(OptionBar {
            bucket_start,
            ce_open,
            ce_high,
            ce_low,
            ce_close,
            ce_volume,
            ce_oi_close,
            pe_open,
            pe_high,
            pe_low,
            pe_close,
            pe_volume,
            pe_oi_close,
        });
    }

    Json(bars).into_response()
}

// ============================================================================
// GET /health
// ============================================================================

#[derive(Debug, Serialize)]
struct HealthResponse {
    db_connected: bool,
    last_index_tick_at: Option<String>,
    last_option_tick_at: Option<String>,
    aggregation_watermarks: HashMap<String, String>,
    session_mode: String,
}

async fn get_health(State(state): State<Arc<ApiState>>) -> axum::response::Response {
    let db_connected = sqlx::query("SELECT 1").fetch_one(&state.pool).await.is_ok();

    let last_index_tick_at: Option<String> =
        sqlx::query("SELECT time FROM index_ticks ORDER BY time DESC LIMIT 1")
            .fetch_optional(&state.pool)
            .await
            .ok()
            .flatten()
            .and_then(|r| r.try_get("time").ok());

    let last_option_tick_at: Option<String> =
        sqlx::query("SELECT time FROM option_ticks ORDER BY time DESC LIMIT 1")
            .fetch_optional(&state.pool)
            .await
            .ok()
            .flatten()
            .and_then(|r| r.try_get("time").ok());

    let mut watermarks = HashMap::new();
    if let Ok(rows) = sqlx::query("SELECT table_name, last_bucket_end FROM aggregation_state")
        .fetch_all(&state.pool)
        .await
    {
        for row in rows {
            if let (Ok(name), Ok(end)) =
                (row.try_get::<String, _>("table_name"), row.try_get::<String, _>("last_bucket_end"))
            {
                watermarks.insert(name, end);
            }
        }
    }

    let mode: SessionMode = state.session.mode().await;

    let resp = HealthResponse {
        db_connected,
        last_index_tick_at,
        last_option_tick_at,
        aggregation_watermarks: watermarks,
        session_mode: mode.label().to_string(),
    };

    // Referenced to keep the market_clock import meaningful for future use
    // (e.g. adding NSE-vs-local clock skew to /health).
    let _ = market_clock::get_ist_now();

    Json(resp).into_response()
}