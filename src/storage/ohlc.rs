//! OHLC aggregation: raw ticks -> `index_ohlc_1m` / `option_ohlc_1m`, and a
//! daily rollup `index_ohlc_1m` -> `index_ohlc_1d`.
//!
//! Both tiers are idempotent (`INSERT ... ON CONFLICT ... DO UPDATE`) and
//! watermark-driven (`aggregation_state`), so a run is always safe to
//! repeat and never rescans the whole tick history. Only fully-elapsed
//! 1-minute buckets are ever aggregated; the in-progress current minute is
//! always excluded. Buckets with zero raw ticks are simply absent from the
//! output tier — no nulls, no fill-forward.

use anyhow::Result;
use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc};
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use tracing::{error, info};

const INDEX_1M_TABLE: &str = "index_ohlc_1m";
const OPTION_1M_TABLE: &str = "option_ohlc_1m";
const INDEX_1D_TABLE: &str = "index_ohlc_1d";

/// Default watermark used the very first time a tier has never run before:
/// far enough in the past to pick up all existing raw history.
const EPOCH_START: &str = "1970-01-01T00:00:00Z";

// ============================================================================
// WATERMARK HELPERS
// ============================================================================

async fn get_watermark(pool: &SqlitePool, table_name: &str) -> Result<DateTime<Utc>> {
    let row = sqlx::query("SELECT last_bucket_end FROM aggregation_state WHERE table_name = ?")
        .bind(table_name)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(r) => {
            let s: String = r.try_get("last_bucket_end")?;
            Ok(DateTime::parse_from_rfc3339(&s)?.with_timezone(&Utc))
        }
        None => Ok(DateTime::parse_from_rfc3339(EPOCH_START)?.with_timezone(&Utc)),
    }
}

/// Advance the watermark. Callers must only invoke this after the
/// corresponding aggregation transaction has committed.
async fn set_watermark(pool: &SqlitePool, table_name: &str, new_end: DateTime<Utc>) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO aggregation_state (table_name, last_bucket_end)
        VALUES (?, ?)
        ON CONFLICT (table_name) DO UPDATE SET last_bucket_end = excluded.last_bucket_end
        "#,
    )
    .bind(table_name)
    .bind(new_end.to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

/// Floor `t` down to the start of its containing minute.
fn floor_to_minute(t: DateTime<Utc>) -> DateTime<Utc> {
    let secs = t.timestamp();
    let floored = secs - secs.rem_euclid(60);
    Utc.timestamp_opt(floored, 0).single().unwrap_or(t)
}

// ============================================================================
// 1-MINUTE AGGREGATION: index_ticks -> index_ohlc_1m
// ============================================================================

/// Run one pass of 1-minute index OHLC aggregation. Scans raw ticks in
/// `[watermark, cutoff)` where `cutoff` is the start of the current
/// (in-progress) minute, so only fully-elapsed buckets are ever produced.
pub async fn aggregate_index_1m(pool: &SqlitePool) -> Result<()> {
    let watermark = get_watermark(pool, INDEX_1M_TABLE).await?;
    let cutoff = floor_to_minute(Utc::now());

    if watermark >= cutoff {
        return Ok(()); // nothing new to aggregate yet
    }

    // SQLite bucket_start expression: floor each tick's `time` down to the
    // minute by truncating the seconds/fraction portion of the ISO string.
    // Ticks are stored via `to_rfc3339()`, i.e. `YYYY-MM-DDTHH:MM:SS...`.
    let rows = sqlx::query(
        r#"
        SELECT
            index_name,
            substr(time, 1, 16) || ':00Z' AS bucket_start,
            current_price,
            time
        FROM index_ticks
        WHERE time >= ? AND time < ?
        ORDER BY index_name, time ASC
        "#,
    )
    .bind(watermark.to_rfc3339())
    .bind(cutoff.to_rfc3339())
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        set_watermark(pool, INDEX_1M_TABLE, cutoff).await?;
        return Ok(());
    }

    use std::collections::HashMap;
    struct Bar {
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        tick_count: i64,
    }

    let mut bars: HashMap<(String, String), Bar> = HashMap::new();

    for row in &rows {
        let index_name: String = row.try_get("index_name")?;
        let bucket_start: String = row.try_get("bucket_start")?;
        let price: f64 = row.try_get("current_price")?;

        let key = (index_name, bucket_start);
        bars.entry(key)
            .and_modify(|b| {
                b.high = b.high.max(price);
                b.low = b.low.min(price);
                b.close = price;
                b.tick_count += 1;
            })
            .or_insert(Bar { open: price, high: price, low: price, close: price, tick_count: 1 });
    }

    let mut tx = pool.begin().await?;
    for ((index_name, bucket_start), bar) in &bars {
        sqlx::query(
            r#"
            INSERT INTO index_ohlc_1m (index_name, bucket_start, open, high, low, close, tick_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (index_name, bucket_start) DO UPDATE SET
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                tick_count = excluded.tick_count
            "#,
        )
        .bind(index_name)
        .bind(bucket_start)
        .bind(bar.open)
        .bind(bar.high)
        .bind(bar.low)
        .bind(bar.close)
        .bind(bar.tick_count)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    // Watermark advances only after the commit above succeeds.
    set_watermark(pool, INDEX_1M_TABLE, cutoff).await?;

    info!("Aggregated {} index_ohlc_1m bar(s) up to {}", bars.len(), cutoff.to_rfc3339());
    Ok(())
}

/// Run one pass of 1-minute option OHLC aggregation (wide CE/PE shape).
pub async fn aggregate_option_1m(pool: &SqlitePool) -> Result<()> {
    let watermark = get_watermark(pool, OPTION_1M_TABLE).await?;
    let cutoff = floor_to_minute(Utc::now());

    if watermark >= cutoff {
        return Ok(());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            symbol,
            expiry,
            strike_price,
            substr(time, 1, 16) || ':00Z' AS bucket_start,
            ce_last_price, ce_volume, ce_oi,
            pe_last_price, pe_volume, pe_oi
        FROM option_ticks
        WHERE time >= ? AND time < ?
        ORDER BY symbol, expiry, strike_price, time ASC
        "#,
    )
    .bind(watermark.to_rfc3339())
    .bind(cutoff.to_rfc3339())
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        set_watermark(pool, OPTION_1M_TABLE, cutoff).await?;
        return Ok(());
    }

    use std::collections::HashMap;
    #[derive(Default)]
    struct Bar {
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

        tick_count: i64,
    }

    fn fold_leg(
        price: Option<f64>,
        open: &mut Option<f64>,
        high: &mut Option<f64>,
        low: &mut Option<f64>,
        close: &mut Option<f64>,
    ) {
        if let Some(p) = price {
            *open = open.or(Some(p));
            *high = Some(high.map_or(p, |h| h.max(p)));
            *low = Some(low.map_or(p, |l| l.min(p)));
            *close = Some(p);
        }
    }

    let mut bars: HashMap<(String, String, i64, String), Bar> = HashMap::new();

    for row in &rows {
        let symbol: String = row.try_get("symbol")?;
        let expiry: String = row.try_get("expiry")?;
        let strike_price: f64 = row.try_get("strike_price")?;
        let bucket_start: String = row.try_get("bucket_start")?;

        let ce_last_price: Option<f64> = row.try_get("ce_last_price")?;
        let ce_volume: Option<i64> = row.try_get("ce_volume")?;
        let ce_oi: Option<f64> = row.try_get("ce_oi")?;
        let pe_last_price: Option<f64> = row.try_get("pe_last_price")?;
        let pe_volume: Option<i64> = row.try_get("pe_volume")?;
        let pe_oi: Option<f64> = row.try_get("pe_oi")?;

        // Strike is keyed as fixed-point-ish string bits via bit pattern to
        // keep exact equality in the HashMap key (avoids float key issues).
        let strike_key = strike_price.to_bits() as i64;
        let key = (symbol, expiry, strike_key, bucket_start);

        let bar = bars.entry(key).or_default();
        fold_leg(ce_last_price, &mut bar.ce_open, &mut bar.ce_high, &mut bar.ce_low, &mut bar.ce_close);
        fold_leg(pe_last_price, &mut bar.pe_open, &mut bar.pe_high, &mut bar.pe_low, &mut bar.pe_close);
        if ce_volume.is_some() {
            bar.ce_volume = ce_volume;
        }
        if ce_oi.is_some() {
            bar.ce_oi_close = ce_oi;
        }
        if pe_volume.is_some() {
            bar.pe_volume = pe_volume;
        }
        if pe_oi.is_some() {
            bar.pe_oi_close = pe_oi;
        }
        bar.tick_count += 1;
    }

    let mut tx = pool.begin().await?;
    for ((symbol, expiry, strike_bits, bucket_start), bar) in &bars {
        let strike_price = f64::from_bits(*strike_bits as u64);
        let expiry_date = parse_expiry_date(expiry);

        sqlx::query(
            r#"
            INSERT INTO option_ohlc_1m (
                symbol, expiry, expiry_date, strike_price, bucket_start,
                ce_open, ce_high, ce_low, ce_close, ce_volume, ce_oi_close,
                pe_open, pe_high, pe_low, pe_close, pe_volume, pe_oi_close,
                tick_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, expiry, strike_price, bucket_start) DO UPDATE SET
                expiry_date = excluded.expiry_date,
                ce_open = excluded.ce_open,
                ce_high = excluded.ce_high,
                ce_low = excluded.ce_low,
                ce_close = excluded.ce_close,
                ce_volume = excluded.ce_volume,
                ce_oi_close = excluded.ce_oi_close,
                pe_open = excluded.pe_open,
                pe_high = excluded.pe_high,
                pe_low = excluded.pe_low,
                pe_close = excluded.pe_close,
                pe_volume = excluded.pe_volume,
                pe_oi_close = excluded.pe_oi_close,
                tick_count = excluded.tick_count
            "#,
        )
        .bind(symbol)
        .bind(expiry)
        .bind(expiry_date)
        .bind(strike_price)
        .bind(bucket_start)
        .bind(bar.ce_open)
        .bind(bar.ce_high)
        .bind(bar.ce_low)
        .bind(bar.ce_close)
        .bind(bar.ce_volume)
        .bind(bar.ce_oi_close)
        .bind(bar.pe_open)
        .bind(bar.pe_high)
        .bind(bar.pe_low)
        .bind(bar.pe_close)
        .bind(bar.pe_volume)
        .bind(bar.pe_oi_close)
        .bind(bar.tick_count)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    set_watermark(pool, OPTION_1M_TABLE, cutoff).await?;

    info!("Aggregated {} option_ohlc_1m bar(s) up to {}", bars.len(), cutoff.to_rfc3339());
    Ok(())
}

/// Best-effort parse of the free-form `expiry` string (as sent by NSE,
/// typically `DD-Mon-YYYY`, e.g. "25-Jul-2026") into a comparable
/// `YYYY-MM-DD` date. Falls back to a far-future sentinel if unparseable,
/// so a bad string never causes an early/incorrect purge.
fn parse_expiry_date(expiry: &str) -> String {
    if let Ok(d) = NaiveDate::parse_from_str(expiry, "%d-%b-%Y") {
        return d.format("%Y-%m-%d").to_string();
    }
    if let Ok(d) = NaiveDate::parse_from_str(expiry, "%Y-%m-%d") {
        return d.format("%Y-%m-%d").to_string();
    }
    "9999-12-31".to_string()
}

// ============================================================================
// DAILY ROLLUP: index_ohlc_1m -> index_ohlc_1d
// ============================================================================

/// Roll up `index_ohlc_1m` bars into daily bars in `index_ohlc_1d`. Intended
/// to run once per day after market close; safe to rerun (upsert).
pub async fn rollup_index_1d(pool: &SqlitePool) -> Result<()> {
    let watermark = get_watermark(pool, INDEX_1D_TABLE).await?;
    // Only roll up full days that are already behind the 1m watermark, so
    // we never roll up a day whose 1m aggregation might still be partial.
    let index_1m_watermark = get_watermark(pool, INDEX_1M_TABLE).await?;
    let cutoff_day_start = floor_to_day(index_1m_watermark);

    if watermark >= cutoff_day_start {
        return Ok(());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            index_name,
            substr(bucket_start, 1, 10) || 'T00:00:00Z' AS day_start,
            open, high, low, close, tick_count, bucket_start
        FROM index_ohlc_1m
        WHERE bucket_start >= ? AND bucket_start < ?
        ORDER BY index_name, bucket_start ASC
        "#,
    )
    .bind(watermark.to_rfc3339())
    .bind(cutoff_day_start.to_rfc3339())
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        set_watermark(pool, INDEX_1D_TABLE, cutoff_day_start).await?;
        return Ok(());
    }

    use std::collections::HashMap;
    struct Bar {
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        tick_count: i64,
    }

    let mut bars: HashMap<(String, String), Bar> = HashMap::new();

    for row in &rows {
        let index_name: String = row.try_get("index_name")?;
        let day_start: String = row.try_get("day_start")?;
        let open: f64 = row.try_get("open")?;
        let high: f64 = row.try_get("high")?;
        let low: f64 = row.try_get("low")?;
        let close: f64 = row.try_get("close")?;
        let tick_count: i64 = row.try_get("tick_count")?;

        let key = (index_name, day_start);
        bars.entry(key)
            .and_modify(|b| {
                b.high = b.high.max(high);
                b.low = b.low.min(low);
                b.close = close;
                b.tick_count += tick_count;
            })
            .or_insert(Bar { open, high, low, close, tick_count });
    }

    let mut tx = pool.begin().await?;
    for ((index_name, day_start), bar) in &bars {
        sqlx::query(
            r#"
            INSERT INTO index_ohlc_1d (index_name, bucket_start, open, high, low, close, tick_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (index_name, bucket_start) DO UPDATE SET
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                tick_count = excluded.tick_count
            "#,
        )
        .bind(index_name)
        .bind(day_start)
        .bind(bar.open)
        .bind(bar.high)
        .bind(bar.low)
        .bind(bar.close)
        .bind(bar.tick_count)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    set_watermark(pool, INDEX_1D_TABLE, cutoff_day_start).await?;

    info!("Rolled up {} index_ohlc_1d bar(s) up to {}", bars.len(), cutoff_day_start.to_rfc3339());
    Ok(())
}

fn floor_to_day(t: DateTime<Utc>) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(t.year(), t.month(), t.day(), 0, 0, 0).single().unwrap_or(t)
}

// ============================================================================
// SCHEDULERS
// ============================================================================

/// Spawn a background task that runs 1-minute aggregation (index + option)
/// every `run_interval_secs`.
pub fn spawn_1m_aggregation_loop(pool: SqlitePool, run_interval_secs: u64) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(run_interval_secs));
        loop {
            interval.tick().await;
            if let Err(e) = aggregate_index_1m(&pool).await {
                error!("index_ohlc_1m aggregation failed: {}", e);
            }
            if let Err(e) = aggregate_option_1m(&pool).await {
                error!("option_ohlc_1m aggregation failed: {}", e);
            }
        }
    })
}

/// Spawn a background task that runs the daily 1m->1d rollup once per day,
/// shortly after NSE market close (16:00 IST).
pub fn spawn_daily_rollup_loop(pool: SqlitePool) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let sleep_secs = seconds_until_next_run(16, 15); // 16:15 IST, after close
            tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
            if let Err(e) = rollup_index_1d(&pool).await {
                error!("index_ohlc_1d rollup failed: {}", e);
            }
        }
    })
}

/// Seconds from now until the next occurrence of `hour:minute` IST.
fn seconds_until_next_run(hour: u32, minute: u32) -> u64 {
    let now = crate::market::market_clock::get_ist_now();
    let ist_offset = Duration::seconds(5 * 3600 + 30 * 60);
    let ist_now = now + ist_offset;

    let mut target = ist_now
        .date_naive()
        .and_hms_opt(hour, minute, 0)
        .expect("valid time")
        .and_utc();

    if target <= ist_now {
        target += Duration::days(1);
    }

    let target_utc = target - ist_offset;
    let now_utc = Utc::now();
    (target_utc - now_utc).num_seconds().max(1) as u64
}
