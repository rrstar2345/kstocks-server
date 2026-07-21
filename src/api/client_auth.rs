//! Shared client-key validation logic used by both `GET /validate` and the
//! `/ohlc/*` auth middleware. Pending and revoked keys are rejected
//! identically in both places — only `approved` passes.

use axum::http::HeaderMap;

use crate::users::keys::{hash_secret, parse_client_key};
use crate::users::{self, ClientRow, ClientStatus};

pub(super) enum AuthOutcome {
    /// Key is well-formed, exists, and is approved.
    #[allow(dead_code)]
    Approved(ClientRow),
    /// Key parsed and matched a row, but that row isn't approved.
    NotApproved(ClientRow),
    /// Missing header, malformed key, unknown key_id, or secret mismatch.
    /// Deliberately not distinguished further in the response — telling a
    /// caller "wrong secret" vs "unknown key_id" helps an attacker enumerate
    /// valid key_ids.
    Invalid,
}

/// Extract the bearer token from `Authorization: Bearer <key>`.
fn extract_bearer(headers: &HeaderMap) -> Option<&str> {
    headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?.strip_prefix("Bearer ")
}

/// Validate the `Authorization` header against the users DB.
pub(super) async fn authenticate(
    headers: &HeaderMap,
    users_pool: &sqlx::SqlitePool,
) -> AuthOutcome {
    let Some(raw) = extract_bearer(headers) else {
        return AuthOutcome::Invalid;
    };
    let Some(parsed) = parse_client_key(raw) else {
        return AuthOutcome::Invalid;
    };

    let row = match users::find_by_key_id(users_pool, &parsed.key_id).await {
        Ok(Some(row)) => row,
        _ => return AuthOutcome::Invalid,
    };

    // Constant-time-ish check isn't critical here (hash comparison, not the
    // raw secret), but we still compare the hash rather than any prefix.
    if row.username != parsed.username || row.secret_hash != hash_secret(&parsed.secret) {
        return AuthOutcome::Invalid;
    }

    match ClientStatus::from_str(&row.status) {
        Some(ClientStatus::Approved) => AuthOutcome::Approved(row),
        _ => AuthOutcome::NotApproved(row),
    }
}