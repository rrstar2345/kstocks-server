//! Purge routine for raw ticks and OHLC tiers, gated on aggregation
//! watermarks so we never delete data that hasn't been safely aggregated
//! yet. All windows/grace periods come from `settings::RetentionConfig`.

use anyhow::Result;
use chrono::{Duration, Utc};
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use tracing::{error, info, warn};

use crate::settings::RetentionConfig;

const INDEX_1M_TABLE: &str = "index_ohlc_1m";
const OPTION_1M_TABLE: &str = "option_ohlc_1m";
const INDEX_1D_TABLE: &str = "index_ohlc_1d";

async fn get_watermark(pool: &SqlitePool, table_name: &str) -> Result<Option<chrono::DateTime<Utc>>> {
    let row = sqlx::query("SELECT last_bucket_end FROM aggregation_state WHERE table_name = ?")
        .bind(table_name)
        .fetch_optional(pool)
        .await?;

    Ok(match row {
        Some(r) => {
            let s: String = r.try_get("last_bucket_end")?;
            Some(chrono::DateTime::parse_from_rfc3339(&s)?.with_timezone(&Utc))
        }
        None => None,
    })
}

/// Delete raw `index_ticks` older than `raw_ticks_keep_trading_days`,
/// gated on `index_ohlc_1m`'s watermark actually covering that range (so we
/// never drop ticks that haven't been aggregated yet).
async fn purge_index_ticks(pool: &SqlitePool, cfg: &RetentionConfig) -> Result<()> {
    let Some(watermark) = get_watermark(pool, INDEX_1M_TABLE).await? else {
        warn!("Skipping index_ticks purge: index_ohlc_1m has no watermark yet");
        return Ok(());
    };

    let cutoff = Utc::now() - Duration::days(cfg.raw_ticks_keep_trading_days);
    // Never purge past what's already been aggregated.
    let safe_cutoff = cutoff.min(watermark);

    let res = sqlx::query("DELETE FROM index_ticks WHERE time < ?")
        .bind(safe_cutoff.to_rfc3339())
        .execute(pool)
        .await?;

    info!("Purged {} index_ticks row(s) older than {}", res.rows_affected(), safe_cutoff.to_rfc3339());
    Ok(())
}

/// Delete raw `option_ticks` older than `raw_ticks_keep_trading_days`,
/// gated the same way on `option_ohlc_1m`'s watermark.
async fn purge_option_ticks(pool: &SqlitePool, cfg: &RetentionConfig) -> Result<()> {
    let Some(watermark) = get_watermark(pool, OPTION_1M_TABLE).await? else {
        warn!("Skipping option_ticks purge: option_ohlc_1m has no watermark yet");
        return Ok(());
    };

    let cutoff = Utc::now() - Duration::days(cfg.raw_ticks_keep_trading_days);
    let safe_cutoff = cutoff.min(watermark);

    let res = sqlx::query("DELETE FROM option_ticks WHERE time < ?")
        .bind(safe_cutoff.to_rfc3339())
        .execute(pool)
        .await?;

    info!("Purged {} option_ticks row(s) older than {}", res.rows_affected(), safe_cutoff.to_rfc3339());
    Ok(())
}

/// Delete `index_ohlc_1m` rows older than `index_ohlc_1m_keep_days`, gated
/// on `index_ohlc_1d`'s watermark covering that range.
async fn purge_index_ohlc_1m(pool: &SqlitePool, cfg: &RetentionConfig) -> Result<()> {
    let Some(watermark) = get_watermark(pool, INDEX_1D_TABLE).await? else {
        warn!("Skipping index_ohlc_1m purge: index_ohlc_1d has no watermark yet");
        return Ok(());
    };

    let cutoff = Utc::now() - Duration::days(cfg.index_ohlc_1m_keep_days);
    let safe_cutoff = cutoff.min(watermark);

    let res = sqlx::query("DELETE FROM index_ohlc_1m WHERE bucket_start < ?")
        .bind(safe_cutoff.to_rfc3339())
        .execute(pool)
        .await?;

    info!("Purged {} index_ohlc_1m row(s) older than {}", res.rows_affected(), safe_cutoff.to_rfc3339());
    Ok(())
}

/// Delete `option_ohlc_1m` rows whose `expiry_date` is more than
/// `option_ohlc_1m_expiry_grace_days` in the past. This is a hard
/// structural rule based on the immutable expiry fact, not a rolling
/// window, so it needs no watermark gating.
async fn purge_expired_option_ohlc_1m(pool: &SqlitePool, cfg: &RetentionConfig) -> Result<()> {
    let cutoff_date = (Utc::now() - Duration::days(cfg.option_ohlc_1m_expiry_grace_days))
        .date_naive()
        .format("%Y-%m-%d")
        .to_string();

    let res = sqlx::query("DELETE FROM option_ohlc_1m WHERE expiry_date < ?")
        .bind(&cutoff_date)
        .execute(pool)
        .await?;

    info!(
        "Purged {} option_ohlc_1m row(s) with expiry_date < {}",
        res.rows_affected(),
        cutoff_date
    );
    Ok(())
}

/// Delete `index_ohlc_1d` rows older than `index_ohlc_1d_keep_days`. A
/// value of 0 means "keep indefinitely".
async fn purge_index_ohlc_1d(pool: &SqlitePool, cfg: &RetentionConfig) -> Result<()> {
    if cfg.index_ohlc_1d_keep_days <= 0 {
        return Ok(());
    }

    let cutoff = Utc::now() - Duration::days(cfg.index_ohlc_1d_keep_days);

    let res = sqlx::query("DELETE FROM index_ohlc_1d WHERE bucket_start < ?")
        .bind(cutoff.to_rfc3339())
        .execute(pool)
        .await?;

    info!("Purged {} index_ohlc_1d row(s) older than {}", res.rows_affected(), cutoff.to_rfc3339());
    Ok(())
}

/// Run all daily purges in order (raw ticks -> 1m tiers -> expired options
/// -> optional 1d cap), then `PRAGMA optimize`. Intended to run once daily,
/// after the daily rollup.
pub async fn run_daily_purge(pool: &SqlitePool, cfg: &RetentionConfig) {
    if let Err(e) = purge_index_ticks(pool, cfg).await {
        error!("index_ticks purge failed: {}", e);
    }
    if let Err(e) = purge_option_ticks(pool, cfg).await {
        error!("option_ticks purge failed: {}", e);
    }
    if let Err(e) = purge_index_ohlc_1m(pool, cfg).await {
        error!("index_ohlc_1m purge failed: {}", e);
    }
    if let Err(e) = purge_expired_option_ohlc_1m(pool, cfg).await {
        error!("option_ohlc_1m expiry purge failed: {}", e);
    }
    if let Err(e) = purge_index_ohlc_1d(pool, cfg).await {
        error!("index_ohlc_1d purge failed: {}", e);
    }

    if let Err(e) = sqlx::query("PRAGMA optimize;").execute(pool).await {
        error!("PRAGMA optimize failed: {}", e);
    } else {
        info!("PRAGMA optimize completed");
    }
}

/// `VACUUM` briefly locks the whole database file, so it runs on its own,
/// less-frequent (weekly) schedule, separate from the daily purge.
pub async fn run_vacuum(pool: &SqlitePool) {
    match sqlx::query("VACUUM;").execute(pool).await {
        Ok(_) => info!("VACUUM completed"),
        Err(e) => error!("VACUUM failed: {}", e),
    }
}

// ============================================================================
// SCHEDULERS
// ============================================================================

/// Spawn a background task that runs the daily purge once per day, shortly
/// after the daily 1m->1d rollup (16:15 IST), and `VACUUM` once a week.
pub fn spawn_retention_loop(pool: SqlitePool, cfg: RetentionConfig) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut days_since_vacuum: u32 = 0;
        loop {
            let sleep_secs = seconds_until_next_run(16, 30); // after the 16:15 rollup
            tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;

            run_daily_purge(&pool, &cfg).await;

            days_since_vacuum += 1;
            if days_since_vacuum >= 7 {
                run_vacuum(&pool).await;
                days_since_vacuum = 0;
            }
        }
    })
}

fn seconds_until_next_run(hour: u32, minute: u32) -> u64 {
    let now = crate::market_clock::get_ist_now();
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