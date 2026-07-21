//! Client API key format, generation, and hashing.
//!
//! A client key looks like `<username>-<key_id>-<secret>`, e.g.
//! `johndoe-adc214s3-2jfh79gs`:
//!   - `username`: chosen by the client at registration, sanitized.
//!   - `key_id`: 8-char alphanumeric, server-generated, public (used as the
//!     row lookup key — cheap to index, doesn't leak anything on its own).
//!   - `secret`: 8-char alphanumeric, server-generated, sent to the client
//!     exactly once in the registration response. The server never stores
//!     the plaintext secret, only `hash(secret)`.
//!
//! Splitting `key_id`/`secret` mirrors the Stripe/GitHub-style token
//! pattern: a DB leak of the `key_id` + hash alone doesn't hand out a
//! working credential, since the hash isn't reversible and the client key
//! lookup only needs `key_id` (not a full-table hash comparison).

use rand::distr::Alphanumeric;
use rand::Rng;
use sha2::{Digest, Sha256};

const KEY_ID_LEN: usize = 8;
const SECRET_LEN: usize = 8;

/// Generate a fresh random alphanumeric segment of the given length.
fn random_segment(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// A freshly generated client credential pair, before it's ever sent
/// anywhere. `secret_hash` is what gets persisted; `plaintext_key` is
/// returned to the client exactly once and never stored server-side.
pub struct GeneratedClientKey {
    pub key_id: String,
    pub secret_hash: String,
    /// Full key as handed to the client: `<username>-<key_id>-<secret>`.
    pub plaintext_key: String,
}

/// Generate a new `key_id`/`secret` pair for `username`. Does not touch the
/// database — callers persist `key_id` + `secret_hash` and return
/// `plaintext_key` to the caller once.
pub fn generate_client_key(username: &str) -> GeneratedClientKey {
    let key_id = random_segment(KEY_ID_LEN);
    let secret = random_segment(SECRET_LEN);
    let secret_hash = hash_secret(&secret);
    let plaintext_key = format!("{}-{}-{}", username, key_id, secret);

    GeneratedClientKey { key_id, secret_hash, plaintext_key }
}

/// SHA-256 hash of a secret (client secret or admin token), hex-encoded.
/// No per-value salt: both the client secret and admin token are already
/// high-entropy random strings (not user-chosen passwords), so a fixed hash
/// is sufficient here and keeps lookups a simple indexed equality check.
pub fn hash_secret(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hex::encode(hasher.finalize())
}

/// A parsed (but not yet validated) client key from an `Authorization`
/// header.
pub struct ParsedClientKey {
    pub username: String,
    pub key_id: String,
    pub secret: String,
}

/// Split `<username>-<key_id>-<secret>` into its three parts. Username may
/// itself contain hyphens, so this splits from the right: the last two
/// hyphen-delimited segments are always `key_id` and `secret`.
pub fn parse_client_key(raw: &str) -> Option<ParsedClientKey> {
    let mut parts: Vec<&str> = raw.rsplitn(3, '-').collect();
    if parts.len() != 3 {
        return None;
    }
    // rsplitn yields [secret, key_id, username] (reverse order).
    let secret = parts.remove(0);
    let key_id = parts.remove(0);
    let username = parts.remove(0);

    if username.is_empty() || key_id.len() != KEY_ID_LEN || secret.len() != SECRET_LEN {
        return None;
    }

    Some(ParsedClientKey {
        username: username.to_string(),
        key_id: key_id.to_string(),
        secret: secret.to_string(),
    })
}

/// Generate a fresh high-entropy admin token (not tied to a username).
/// Returns `(plaintext_token, token_hash)`; only the hash is persisted.
pub fn generate_admin_token() -> (String, String) {
    let token: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(40)
        .map(char::from)
        .collect();
    let hash = hash_secret(&token);
    (token, hash)
}

/// Sanitize a client-supplied username: lowercase alphanumeric only, so it
/// can safely sit inside the hyphen-delimited key format without ambiguity.
/// Returns `None` if nothing usable remains.
pub fn sanitize_username(raw: &str) -> Option<String> {
    let cleaned: String = raw
        .trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect();

    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}