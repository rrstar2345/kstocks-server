//! `GET /ohlc/option` — option OHLC bars (wide CE/PE shape) from `option_ohlc_1m`.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::sync::Arc;
use tracing::error;

use super::{bad_request, interval_to_secs, ApiErrorBody, ApiState, Resolved};

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

#[derive(Debug, Deserialize)]
pub(super) struct OptionOhlcQuery {
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

pub(super) async fn get_option_ohlc(
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
