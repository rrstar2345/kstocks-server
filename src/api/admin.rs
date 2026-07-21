//! `/admin/*` — registration review and client revocation. Guarded by
//! `require_admin_token`, checked against the hash written by the
//! `kstocks-server admin generate|regenerate` CLI subcommand (the only way
//! to mint or rotate that token — this router never creates one itself).

use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use serde::Serialize;
use std::sync::Arc;

use crate::users::keys::hash_secret;
use crate::users::{self, ClientRow, ClientStatus};

use super::{ApiErrorBody, ApiState};

pub(super) fn admin_router() -> Router<Arc<ApiState>> {
    Router::new()
        .route("/admin/registrations", get(list_registrations))
        .route("/admin/registrations/{id}/approve", post(approve_registration))
        .route("/admin/registrations/{id}/decline", post(decline_registration))
        .route("/admin/clients/{id}/revoke", post(revoke_client))
    // Auth is applied by the caller in `api/mod.rs` via
    // `.layer(from_fn_with_state(state.clone(), require_admin_token))`,
    // once the shared `ApiState` is constructed.
}

/// Middleware: require `Authorization: Bearer <admin_token>` matching the
/// hash stored in `admin_token`.
pub(super) async fn require_admin_token(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> axum::response::Response {
    let Some(token) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    else {
        return unauthorized();
    };

    let stored_hash = match users::get_admin_token_hash(&state.users_pool).await {
        Ok(Some(h)) => h,
        Ok(None) => {
            // No admin token has ever been generated on this server.
            return unauthorized();
        }
        Err(e) => {
            tracing::error!("Failed to read admin token: {}", e);
            return internal_error();
        }
    };

    if hash_secret(token) != stored_hash {
        return unauthorized();
    }

    next.run(req).await
}

fn unauthorized() -> axum::response::Response {
    (StatusCode::UNAUTHORIZED, Json(ApiErrorBody { error: "unauthorized".to_string() }))
        .into_response()
}

fn internal_error() -> axum::response::Response {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiErrorBody { error: "internal error".to_string() }))
        .into_response()
}

// ============================================================================
// HANDLERS
// ============================================================================

#[derive(Debug, Serialize)]
struct ClientSummary {
    id: i64,
    username: String,
    key_id: String,
    status: String,
    registered_ip: String,
    created_at: String,
    updated_at: String,
}

impl From<ClientRow> for ClientSummary {
    fn from(r: ClientRow) -> Self {
        Self {
            id: r.id,
            username: r.username,
            key_id: r.key_id,
            status: r.status,
            registered_ip: r.registered_ip,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

/// `GET /admin/registrations` — lists all clients (any status) so the admin
/// can review pending requests alongside already-approved/revoked ones.
async fn list_registrations(State(state): State<Arc<ApiState>>) -> axum::response::Response {
    match users::list_all(&state.users_pool).await {
        Ok(rows) => {
            let summaries: Vec<ClientSummary> = rows.into_iter().map(ClientSummary::from).collect();
            Json(summaries).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to list registrations: {}", e);
            internal_error()
        }
    }
}

async fn approve_registration(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<i64>,
) -> axum::response::Response {
    set_status_checked(&state, id, ClientStatus::Approved).await
}

async fn decline_registration(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<i64>,
) -> axum::response::Response {
    set_status_checked(&state, id, ClientStatus::Declined).await
}

/// `POST /admin/clients/{id}/revoke` — works on any current status
/// (typically `approved`), immediately cutting off `/ohlc/*` and
/// `/validate` access without deleting the row (future telemetry keyed on
/// `id` stays intact).
async fn revoke_client(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<i64>,
) -> axum::response::Response {
    set_status_checked(&state, id, ClientStatus::Revoked).await
}

async fn set_status_checked(
    state: &ApiState,
    id: i64,
    new_status: ClientStatus,
) -> axum::response::Response {
    match users::find_by_id(&state.users_pool, id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (StatusCode::NOT_FOUND, Json(ApiErrorBody { error: "no such client".to_string() }))
                .into_response();
        }
        Err(e) => {
            tracing::error!("Failed to look up client {}: {}", id, e);
            return internal_error();
        }
    }

    match users::set_status(&state.users_pool, id, new_status).await {
        Ok(()) => Json(serde_json::json!({ "id": id, "status": new_status.as_str() })).into_response(),
        Err(e) => {
            tracing::error!("Failed to update client {} status: {}", id, e);
            internal_error()
        }
    }
}