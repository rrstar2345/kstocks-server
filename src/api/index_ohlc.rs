//! `GET /ohlc/index` — index OHLC bars from `index_ohlc_1m` / `index_ohlc_1d`.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::sync::Arc;
use tracing::error;

use super::{bad_request, interval_to_secs, ApiErrorBody, ApiState, Resolved};

#[derive(Debug, Serialize)]
struct Bar {
    bucket_start: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
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

#[derive(Debug, Deserialize)]
pub(super) struct IndexOhlcQuery {
    name: String,
    range: String,
    interval: String,
}

pub(super) async fn get_index_ohlc(
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
