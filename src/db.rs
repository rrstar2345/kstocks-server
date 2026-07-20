use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use sqlx::QueryBuilder;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::settings::DatabaseConfig;
use crate::stats::SharedStats;

// ============================================================================
// ROW TYPES
// ============================================================================

#[derive(Debug, Clone)]
pub struct IndexTickRow {
    pub time: DateTime<Utc>,
    pub index_name: String,
    pub current_price: f64,
    pub change: f64,
    pub per_change: f64,
    pub previous_close: f64,
    pub open: f64,
    pub low: f64,
    pub high: f64,
    pub ind_status: String,
    pub mkt_status: String,
    pub dissemination_time: String,
}

#[derive(Debug, Clone)]
pub struct OptionTickRow {
    pub time: DateTime<Utc>,
    pub symbol: String,
    pub expiry: String,
    pub strike_price: f64,

    pub ce_last_price: Option<f64>,
    pub ce_change: Option<f64>,
    pub ce_volume: Option<i64>,
    pub ce_oi: Option<f64>,
    pub ce_bid: Option<f64>,
    pub ce_ask: Option<f64>,

    pub pe_last_price: Option<f64>,
    pub pe_change: Option<f64>,
    pub pe_volume: Option<i64>,
    pub pe_oi: Option<f64>,
    pub pe_bid: Option<f64>,
    pub pe_ask: Option<f64>,
}

pub type IndexTickSender = mpsc::Sender<IndexTickRow>;
pub type OptionTickSender = mpsc::Sender<OptionTickRow>;

// ============================================================================
// CONNECT + SCHEMA
// ============================================================================

pub async fn init_pool(db_config: &DatabaseConfig) -> Result<SqlitePool> {
    let connect_options = SqliteConnectOptions::new()
        .filename(&db_config.connection_string)
        .create_if_missing(true);

    let pool = SqlitePool::connect_with(connect_options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to SQLite database: {}", e))?;

    // Performance PRAGMAs for ingestion (same pattern as the desktop app).
    sqlx::query("PRAGMA journal_mode = WAL;").execute(&pool).await?;
    sqlx::query("PRAGMA synchronous = NORMAL;").execute(&pool).await?;
    sqlx::query("PRAGMA temp_store = MEMORY;").execute(&pool).await?;

    create_schema(&pool).await?;

    Ok(pool)
}

/// Plain SQLite schema (no SYMBOL type, no PARTITION BY DAY — those were
/// QuestDB-specific). Indexes are added explicitly to keep the common
/// lookup patterns (by index_name/time, by symbol/expiry/strike/time) fast.
///
/// Column/table naming is kept OHLC-aggregation-friendly for later: raw
/// ticks stay in `index_ticks` / `option_ticks`, and future 1m/3m/5m/... bar
/// tables can be added alongside without touching these. A monthly purge of
/// old raw ticks (`DELETE ... WHERE time < ?` followed by `VACUUM`/
/// `PRAGMA optimize`) can be layered on later as a separate job.
async fn create_schema(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS index_ticks (
            time TEXT NOT NULL,
            index_name TEXT NOT NULL,
            current_price REAL,
            change REAL,
            per_change REAL,
            previous_close REAL,
            open REAL,
            low REAL,
            high REAL,
            ind_status TEXT,
            mkt_status TEXT,
            dissemination_time TEXT
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_index_ticks_name_time
        ON index_ticks(index_name, time);
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS option_ticks (
            time TEXT NOT NULL,
            symbol TEXT NOT NULL,
            expiry TEXT NOT NULL,
            strike_price REAL,

            ce_last_price REAL,
            ce_change REAL,
            ce_volume INTEGER,
            ce_oi REAL,
            ce_bid REAL,
            ce_ask REAL,

            pe_last_price REAL,
            pe_change REAL,
            pe_volume INTEGER,
            pe_oi REAL,
            pe_bid REAL,
            pe_ask REAL
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_option_ticks_symbol_expiry_strike_time
        ON option_ticks(symbol, expiry, strike_price, time);
        "#,
    )
    .execute(pool)
    .await?;

    info!("SQLite schema verified: index_ticks and option_ticks");

    Ok(())
}

// ============================================================================
// BATCHED WRITERS
// ============================================================================

/// Spawn a background task that buffers `IndexTickRow`s in memory and
/// flushes them to the DB either when `batch_max_rows` is reached or
/// `batch_max_wait_ms` elapses, whichever comes first.
pub fn start_index_tick_writer(
    pool: SqlitePool,
    db_config: DatabaseConfig,
    stats: SharedStats,
) -> (IndexTickSender, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel::<IndexTickRow>(20_000);

    let handle = tokio::spawn(async move {
        let batch_max = db_config.batch_max_rows;
        let batch_max_wait = Duration::from_millis(db_config.batch_max_wait_ms);
        let mut buf: Vec<IndexTickRow> = Vec::with_capacity(batch_max);

        loop {
            let first = tokio::select! {
                v = rx.recv() => v,
                _ = tokio::time::sleep(batch_max_wait) => {
                    if !buf.is_empty() {
                        flush_index_batch(&pool, &mut buf, &stats).await;
                    }
                    continue;
                },
            };

            let Some(row) = first else {
                if !buf.is_empty() {
                    flush_index_batch(&pool, &mut buf, &stats).await;
                }
                break;
            };
            buf.push(row);

            while buf.len() < batch_max {
                match tokio::time::timeout(Duration::from_millis(10), rx.recv()).await {
                    Ok(Some(r)) => buf.push(r),
                    _ => break,
                }
            }

            {
                let mut s = stats.write().await;
                s.indices_db.rows_pending = buf.len();
            }

            if !buf.is_empty() {
                flush_index_batch(&pool, &mut buf, &stats).await;
            }
        }
    });

    (tx, handle)
}

async fn flush_index_batch(pool: &SqlitePool, buf: &mut Vec<IndexTickRow>, stats: &SharedStats) {
    match insert_index_batch(pool, buf).await {
        Ok(n) => {
            let mut s = stats.write().await;
            s.indices_db.rows_written += n as u64;
            s.indices_db.rows_pending = 0;
            s.indices_db.last_flush_rows = n;
            s.indices_db.last_flush_at = Some(chrono::Local::now());
            s.indices_db.last_error = None;
        }
        Err(e) => {
            error!("Index tick batch insert failed: {}", e);
            let mut s = stats.write().await;
            s.indices_db.last_error = Some(e.to_string());
        }
    }
    buf.clear();
}

/// SQLite handles a single multi-row `INSERT INTO ... VALUES (...), (...), ...`
/// well, so we use `QueryBuilder::push_values` to build one batched statement
/// per flush instead of looping row-by-row (which QuestDB's Postgres-wire
/// endpoint required).
async fn insert_index_batch(pool: &SqlitePool, rows: &[IndexTickRow]) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut builder = QueryBuilder::new(
        r#"
        INSERT INTO index_ticks (
            time, index_name, current_price, change, per_change,
            previous_close, open, low, high, ind_status, mkt_status, dissemination_time
        )
        "#,
    );

    builder.push_values(rows, |mut b, row| {
        b.push_bind(row.time.to_rfc3339())
            .push_bind(&row.index_name)
            .push_bind(row.current_price)
            .push_bind(row.change)
            .push_bind(row.per_change)
            .push_bind(row.previous_close)
            .push_bind(row.open)
            .push_bind(row.low)
            .push_bind(row.high)
            .push_bind(&row.ind_status)
            .push_bind(&row.mkt_status)
            .push_bind(&row.dissemination_time);
    });

    let query = builder.build();
    let res = query.execute(pool).await?;

    Ok(res.rows_affected() as usize)
}

/// Spawn a background task that buffers `OptionTickRow`s and flushes them in
/// the same batched fashion as the index writer.
pub fn start_option_tick_writer(
    pool: SqlitePool,
    db_config: DatabaseConfig,
    stats: SharedStats,
) -> (OptionTickSender, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel::<OptionTickRow>(20_000);

    let handle = tokio::spawn(async move {
        let batch_max = db_config.batch_max_rows;
        let batch_max_wait = Duration::from_millis(db_config.batch_max_wait_ms);
        let mut buf: Vec<OptionTickRow> = Vec::with_capacity(batch_max);

        loop {
            let first = tokio::select! {
                v = rx.recv() => v,
                _ = tokio::time::sleep(batch_max_wait) => {
                    if !buf.is_empty() {
                        flush_option_batch(&pool, &mut buf, &stats).await;
                    }
                    continue;
                },
            };

            let Some(row) = first else {
                if !buf.is_empty() {
                    flush_option_batch(&pool, &mut buf, &stats).await;
                }
                break;
            };
            buf.push(row);

            while buf.len() < batch_max {
                match tokio::time::timeout(Duration::from_millis(10), rx.recv()).await {
                    Ok(Some(r)) => buf.push(r),
                    _ => break,
                }
            }

            {
                let mut s = stats.write().await;
                s.options_db.rows_pending = buf.len();
            }

            if !buf.is_empty() {
                flush_option_batch(&pool, &mut buf, &stats).await;
            }
        }
    });

    (tx, handle)
}

async fn flush_option_batch(pool: &SqlitePool, buf: &mut Vec<OptionTickRow>, stats: &SharedStats) {
    match insert_option_batch(pool, buf).await {
        Ok(n) => {
            let mut s = stats.write().await;
            s.options_db.rows_written += n as u64;
            s.options_db.rows_pending = 0;
            s.options_db.last_flush_rows = n;
            s.options_db.last_flush_at = Some(chrono::Local::now());
            s.options_db.last_error = None;
        }
        Err(e) => {
            error!("Option tick batch insert failed: {}", e);
            let mut s = stats.write().await;
            s.options_db.last_error = Some(e.to_string());
        }
    }
    buf.clear();
}

async fn insert_option_batch(pool: &SqlitePool, rows: &[OptionTickRow]) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut builder = QueryBuilder::new(
        r#"
        INSERT INTO option_ticks (
            time, symbol, expiry, strike_price,
            ce_last_price, ce_change, ce_volume, ce_oi, ce_bid, ce_ask,
            pe_last_price, pe_change, pe_volume, pe_oi, pe_bid, pe_ask
        )
        "#,
    );

    builder.push_values(rows, |mut b, row| {
        b.push_bind(row.time.to_rfc3339())
            .push_bind(&row.symbol)
            .push_bind(&row.expiry)
            .push_bind(row.strike_price)
            .push_bind(row.ce_last_price)
            .push_bind(row.ce_change)
            .push_bind(row.ce_volume)
            .push_bind(row.ce_oi)
            .push_bind(row.ce_bid)
            .push_bind(row.ce_ask)
            .push_bind(row.pe_last_price)
            .push_bind(row.pe_change)
            .push_bind(row.pe_volume)
            .push_bind(row.pe_oi)
            .push_bind(row.pe_bid)
            .push_bind(row.pe_ask);
    });

    let query = builder.build();
    let res = query.execute(pool).await?;

    Ok(res.rows_affected() as usize)
}