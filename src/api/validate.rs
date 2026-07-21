//! `GET /validate` — the desktop app calls this on every launch, sending
//! its stored client key. Returns whether the key is currently `approved`,
//! so the app can gate its main screen accordingly. Pending and revoked
//! both surface as `not_approved` with a status string; the app decides
//! how to message that to the user.

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Json};
use serde::Serialize;
use std::sync::Arc;

use super::client_auth::{authenticate, AuthOutcome};
use super::ApiState;

#[derive(Debug, Serialize)]
pub(super) struct ValidateResponse {
    approved: bool,
    status: String,
}

pub(super) async fn get_validate(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
) -> axum::response::Response {
    match authenticate(&headers, &state.users_pool).await {
        AuthOutcome::Approved(_) => {
            Json(ValidateResponse { approved: true, status: "approved".to_string() })
                .into_response()
        }
        AuthOutcome::NotApproved(row) => {
            Json(ValidateResponse { approved: false, status: row.status }).into_response()
        }
        AuthOutcome::Invalid => (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(super::ApiErrorBody { error: "invalid or unknown key".to_string() }),
        )
            .into_response(),
    }
}