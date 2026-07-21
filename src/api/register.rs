//! `POST /register` — unauthenticated client registration.
//!
//! Rate limiting: a username may have at most one row that is `pending` or
//! `approved` at a time (enforced by re-checking existing status before
//! insert); a `revoked` username may re-register (overwrites the existing
//! row with a fresh key, same `id`, so future telemetry keyed on `id` isn't
//! orphaned). Separately, an IP that has registered 5+ times in the last
//! 24h is throttled regardless of username, to blunt scripted abuse from a
//! single source; this doesn't stop distributed spam, but dormant
//! `pending` keys are harmless by construction (the OHLC API and
//! `/validate` both reject anything short of `approved`), so the residual
//! risk is DB/admin-review noise, not unauthorized access.

use axum::extract::{ConnectInfo, State};
use axum::response::{IntoResponse, Json};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

use crate::users::keys::{generate_client_key, sanitize_username};
use crate::users::{self, ClientStatus};

use super::{bad_request, ApiState};

/// Max registrations (any username) accepted from a single IP within 24h.
const MAX_REGISTRATIONS_PER_IP_PER_DAY: i64 = 5;

#[derive(Debug, Deserialize)]
pub(super) struct RegisterRequest {
    username: String,
}

#[derive(Debug, Serialize)]
pub(super) struct RegisterResponse {
    status: &'static str,
    /// The full client key: `<username>-<key_id>-<secret>`. Shown exactly
    /// once, here. The client must store it locally and never display it.
    api_key: String,
}

pub(super) async fn post_register(
    State(state): State<Arc<ApiState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(req): Json<RegisterRequest>,
) -> axum::response::Response {
    let Some(username) = sanitize_username(&req.username) else {
        return bad_request("username must contain at least one alphanumeric character");
    };

    let ip = addr.ip().to_string();

    let existing = match users::find_by_username(&state.users_pool, &username).await {
        Ok(row) => row,
        Err(e) => return internal_error(e),
    };

    // Existing pending/approved/declined registration for this username:
    // reject. Only a revoked username may register again.
    if let Some(row) = &existing {
        match ClientStatus::from_str(&row.status) {
            Some(ClientStatus::Revoked) => {} // falls through to re-register below
            _ => {
                return bad_request(format!(
                    "username already has a registration in '{}' status",
                    row.status
                ));
            }
        }
    }

    let recent_from_ip = match users::count_recent_registrations_by_ip(&state.users_pool, &ip).await
    {
        Ok(n) => n,
        Err(e) => return internal_error(e),
    };
    if recent_from_ip >= MAX_REGISTRATIONS_PER_IP_PER_DAY {
        return (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            Json(super::ApiErrorBody {
                error: "too many registration attempts from this network today".to_string(),
            }),
        )
            .into_response();
    }

    let generated = generate_client_key(&username);

    let result = if let Some(row) = existing {
        users::reregister(&state.users_pool, row.id, &generated.key_id, &generated.secret_hash, &ip)
            .await
    } else {
        users::insert_registration(
            &state.users_pool,
            &username,
            &generated.key_id,
            &generated.secret_hash,
            &ip,
        )
        .await
        .map(|_| ())
    };

    if let Err(e) = result {
        return internal_error(e);
    }

    info!("New client registration: username={} ip={} status=pending", username, ip);

    Json(RegisterResponse { status: "pending", api_key: generated.plaintext_key }).into_response()
}

fn internal_error(e: anyhow::Error) -> axum::response::Response {
    tracing::error!("Registration error: {}", e);
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        Json(super::ApiErrorBody { error: "internal error".to_string() }),
    )
        .into_response()
}