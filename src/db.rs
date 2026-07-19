use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPoolOptions, PgPool};
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

pub async fn init_pool(db_config: &DatabaseConfig) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(db_config.max_connections)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db_config.connection_string)
        .await
        .map_err(|e| anyhow!("Failed to connect to QuestDB (Postgres wire protocol): {}", e))?;

    create_schema(&pool).await?;

    Ok(pool)
}

/// QuestDB schema notes (differs from vanilla Postgres/TimescaleDB):
///
/// - No `CREATE EXTENSION` step: QuestDB is time-series-native, so there's no
///   separate extension to enable. Every table gets a *designated timestamp*
///   column (`TIMESTAMP` clause) instead of a Timescale "hypertable".
/// - `PARTITION BY DAY` is QuestDB's equivalent of Timescale's chunking. This
///   is what will make your planned monthly purge cheap later: dropping a
///   whole day's partition (`ALTER TABLE ... DROP PARTITION WHERE time < ...`)
///   is near-instant, versus a row-by-row DELETE.
/// - `SYMBOL` is a QuestDB-specific low-cardinality string type (like an
///   automatically-interned/indexed enum). Using it for `index_name`,
///   `symbol`, and `expiry` means later OHLC/aggregation queries that
///   `GROUP BY` or filter on these columns stay fast without a manual index.
/// - No `ON CONFLICT` / upsert support the Postgres way — batched plain
///   INSERTs (already how this app writes) are the correct and recommended
///   ingestion pattern for QuestDB.
///
/// Future OHLC tables (not created here yet, since you're still deciding on
/// exact shape): QuestDB has a built-in `SAMPLE BY` clause tailor-made for
/// rolling 1-minute ticks up into 1m/3m/5m/10m/15m/30m OHLC bars, e.g.
/// `SELECT symbol, first(price) o, max(price) h, min(price) l, last(price) c
///  FROM option_ticks SAMPLE BY 5m;` — so the aggregation job can likely be a
/// scheduled `INSERT INTO option_ticks_ohlc_5m SELECT ... SAMPLE BY 5m` rather
/// than hand-rolled bucketing logic.
async fn create_schema(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS index_ticks (
            time TIMESTAMP,
            index_name SYMBOL CAPACITY 32 CACHE,
            current_price DOUBLE,
            change DOUBLE,
            per_change DOUBLE,
            previous_close DOUBLE,
            open DOUBLE,
            low DOUBLE,
            high DOUBLE,
            ind_status SYMBOL CAPACITY 8 CACHE,
            mkt_status SYMBOL CAPACITY 8 CACHE,
            dissemination_time STRING
        ) TIMESTAMP(time) PARTITION BY DAY;
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS option_ticks (
            time TIMESTAMP,
            symbol SYMBOL CAPACITY 16 CACHE,
            expiry SYMBOL CAPACITY 64 CACHE,
            strike_price DOUBLE,

            ce_last_price DOUBLE,
            ce_change DOUBLE,
            ce_volume LONG,
            ce_oi DOUBLE,
            ce_bid DOUBLE,
            ce_ask DOUBLE,

            pe_last_price DOUBLE,
            pe_change DOUBLE,
            pe_volume LONG,
            pe_oi DOUBLE,
            pe_bid DOUBLE,
            pe_ask DOUBLE
        ) TIMESTAMP(time) PARTITION BY DAY;
        "#,
    )
    .execute(pool)
    .await?;

    info!("QuestDB schema verified: index_ticks and option_ticks (partitioned by day)");

    Ok(())
}

// ============================================================================
// BATCHED WRITERS
// ============================================================================

/// Spawn a background task that buffers `IndexTickRow`s in memory and
/// flushes them to the DB either when `batch_max_rows` is reached or
/// `batch_max_wait_ms` elapses, whichever comes first.
pub fn start_index_tick_writer(
    pool: PgPool,
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

async fn flush_index_batch(pool: &PgPool, buf: &mut Vec<IndexTickRow>, stats: &SharedStats) {
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

/// QuestDB's Postgres-wire endpoint does not reliably support a single
/// prepared statement with multiple `VALUES (...), (...), ...` rows the way
/// real Postgres/Timescale does. The recommended and well-supported pattern
/// instead is: one parameterized `INSERT` per row, executed repeatedly over
/// a single held connection/transaction — which is what this does. It's
/// still a single round-trip-efficient batch from the caller's point of view
/// (one connection acquisition, one commit) even though QuestDB executes the
/// inserts one at a time internally.
async fn insert_index_batch(pool: &PgPool, rows: &[IndexTickRow]) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut tx = pool.begin().await?;
    let mut inserted = 0usize;

    for row in rows {
        let res = sqlx::query(
            r#"
            INSERT INTO index_ticks (
                time, index_name, current_price, change, per_change,
                previous_close, open, low, high, ind_status, mkt_status, dissemination_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
        )
        .bind(row.time)
        .bind(&row.index_name)
        .bind(row.current_price)
        .bind(row.change)
        .bind(row.per_change)
        .bind(row.previous_close)
        .bind(row.open)
        .bind(row.low)
        .bind(row.high)
        .bind(&row.ind_status)
        .bind(&row.mkt_status)
        .bind(&row.dissemination_time)
        .execute(&mut *tx)
        .await?;

        inserted += res.rows_affected() as usize;
    }

    tx.commit().await?;
    Ok(inserted)
}

/// Spawn a background task that buffers `OptionTickRow`s and flushes them in
/// the same batched fashion as the index writer.
pub fn start_option_tick_writer(
    pool: PgPool,
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

async fn flush_option_batch(pool: &PgPool, buf: &mut Vec<OptionTickRow>, stats: &SharedStats) {
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

async fn insert_option_batch(pool: &PgPool, rows: &[OptionTickRow]) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut tx = pool.begin().await?;
    let mut inserted = 0usize;

    for row in rows {
        let res = sqlx::query(
            r#"
            INSERT INTO option_ticks (
                time, symbol, expiry, strike_price,
                ce_last_price, ce_change, ce_volume, ce_oi, ce_bid, ce_ask,
                pe_last_price, pe_change, pe_volume, pe_oi, pe_bid, pe_ask
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            "#,
        )
        .bind(row.time)
        .bind(&row.symbol)
        .bind(&row.expiry)
        .bind(row.strike_price)
        .bind(row.ce_last_price)
        .bind(row.ce_change)
        .bind(row.ce_volume)
        .bind(row.ce_oi)
        .bind(row.ce_bid)
        .bind(row.ce_ask)
        .bind(row.pe_last_price)
        .bind(row.pe_change)
        .bind(row.pe_volume)
        .bind(row.pe_oi)
        .bind(row.pe_bid)
        .bind(row.pe_ask)
        .execute(&mut *tx)
        .await?;

        inserted += res.rows_affected() as usize;
    }

    tx.commit().await?;
    Ok(inserted)
}