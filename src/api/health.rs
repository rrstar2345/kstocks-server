//! `GET /health` — db connectivity, last raw-tick timestamps, aggregation
//! watermarks, and current session mode.

use axum::extract::State;
use axum::response::{IntoResponse, Json};
use serde::Serialize;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;

use crate::market::market_clock::SessionMode;

use super::ApiState;

#[derive(Debug, Serialize)]
struct HealthResponse {
    db_connected: bool,
    last_index_tick_at: Option<String>,
    last_option_tick_at: Option<String>,
    aggregation_watermarks: HashMap<String, String>,
    session_mode: String,
}

pub(super) async fn get_health(State(state): State<Arc<ApiState>>) -> axum::response::Response {
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

    Json(resp).into_response()
}
