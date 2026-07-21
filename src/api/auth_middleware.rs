//! Middleware guarding the read-only OHLC routes: requires an `approved`
//! client key on every request. Pending/revoked/invalid all get the same
//! 401 response, matching `client_auth::authenticate`'s outcomes.

use axum::extract::State;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Json};
use std::sync::Arc;

use super::client_auth::{authenticate, AuthOutcome};
use super::ApiState;

pub(super) async fn require_approved_client(
    State(state): State<Arc<ApiState>>,
    req: Request,
    next: Next,
) -> axum::response::Response {
    match authenticate(req.headers(), &state.users_pool).await {
        AuthOutcome::Approved(_) => next.run(req).await,
        _ => (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(super::ApiErrorBody { error: "unauthorized".to_string() }),
        )
            .into_response(),
    }
}