//! `kstocks-users.db`: client registration/auth state, kept in its own
//! SQLite file separate from the market-data DB.
//!
//! Kept separate (rather than folded into `kstocks.db`) so that:
//!   - Auth/PII data has a different backup/security posture than tick data.
//!   - The `admin` CLI subcommand can open this file directly without any
//!     coordination with a running server process on the market-data DB.
//!   - A retention/VACUUM issue on one side can never affect the other.

pub mod admin_cli;
pub mod keys;

use anyhow::Result;
use chrono::Utc;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use tracing::info;

/// Registration/approval lifecycle for a client. Pending and Revoked are
/// treated identically by the OHLC API (no access) — only Approved passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStatus {
    Pending,
    Approved,
    Declined,
    Revoked,
}

impl ClientStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ClientStatus::Pending => "pending",
            ClientStatus::Approved => "approved",
            ClientStatus::Declined => "declined",
            ClientStatus::Revoked => "revoked",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => ClientStatus::Pending,
            "approved" => ClientStatus::Approved,
            "declined" => ClientStatus::Declined,
            "revoked" => ClientStatus::Revoked,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ClientRow {
    pub id: i64,
    pub username: String,
    pub key_id: String,
    pub secret_hash: String,
    pub status: String,
    pub registered_ip: String,
    pub created_at: String,
    pub updated_at: String,
}

/// Open (creating if needed) the users database and ensure its schema
/// exists. Uses the same WAL/NORMAL pragmas as the market-data DB for
/// consistency, though write volume here is trivial by comparison.
pub async fn init_users_pool(connection_string: &str) -> Result<SqlitePool> {
    let connect_options = SqliteConnectOptions::new()
        .filename(connection_string)
        .create_if_missing(true);

    let pool = SqlitePool::connect_with(connect_options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to users database: {}", e))?;

    sqlx::query("PRAGMA journal_mode = WAL;").execute(&pool).await?;
    sqlx::query("PRAGMA synchronous = NORMAL;").execute(&pool).await?;

    create_schema(&pool).await?;

    Ok(pool)
}

async fn create_schema(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            key_id TEXT NOT NULL UNIQUE,
            secret_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            registered_ip TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_clients_key_id ON clients(key_id);")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_clients_registered_ip ON clients(registered_ip);")
        .execute(pool)
        .await?;

    // Single-row table: the current admin token hash. Regenerating
    // overwrites the one row, invalidating the previous token.
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS admin_token (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            token_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    info!("Users database schema verified: clients, admin_token");

    Ok(())
}

// ============================================================================
// CLIENT QUERIES
// ============================================================================

/// Look up a client's current status/row by username, if a registration
/// already exists (in any status).
pub async fn find_by_username(pool: &SqlitePool, username: &str) -> Result<Option<ClientRow>> {
    let row = sqlx::query_as::<_, ClientRow>("SELECT * FROM clients WHERE username = ?")
        .bind(username)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}

/// Look up a client by `key_id` (used on every authenticated request).
pub async fn find_by_key_id(pool: &SqlitePool, key_id: &str) -> Result<Option<ClientRow>> {
    let row = sqlx::query_as::<_, ClientRow>("SELECT * FROM clients WHERE key_id = ?")
        .bind(key_id)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}

pub async fn find_by_id(pool: &SqlitePool, id: i64) -> Result<Option<ClientRow>> {
    let row = sqlx::query_as::<_, ClientRow>("SELECT * FROM clients WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(row)
}

#[allow(dead_code)]
pub async fn list_by_status(pool: &SqlitePool, status: ClientStatus) -> Result<Vec<ClientRow>> {
    let rows = sqlx::query_as::<_, ClientRow>(
        "SELECT * FROM clients WHERE status = ? ORDER BY created_at DESC",
    )
    .bind(status.as_str())
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn list_all(pool: &SqlitePool) -> Result<Vec<ClientRow>> {
    let rows = sqlx::query_as::<_, ClientRow>("SELECT * FROM clients ORDER BY created_at DESC")
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

/// Count registrations from `ip` created within the last 24h, regardless of
/// username — a secondary throttle against scripted abuse from one source,
/// separate from the primary per-username uniqueness constraint.
pub async fn count_recent_registrations_by_ip(pool: &SqlitePool, ip: &str) -> Result<i64> {
    let since = (Utc::now() - chrono::Duration::hours(24)).to_rfc3339();
    let count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM clients WHERE registered_ip = ? AND created_at > ?")
            .bind(ip)
            .bind(since)
            .fetch_one(pool)
            .await?;
    Ok(count.0)
}

/// Insert a brand-new registration in `pending` status. Fails at the DB
/// level (UNIQUE constraint) if the username already has a row — callers
/// should check `find_by_username` first for a clean error message, but
/// this is the actual integrity guarantee.
pub async fn insert_registration(
    pool: &SqlitePool,
    username: &str,
    key_id: &str,
    secret_hash: &str,
    registered_ip: &str,
) -> Result<i64> {
    let now = Utc::now().to_rfc3339();
    let res = sqlx::query(
        r#"
        INSERT INTO clients (username, key_id, secret_hash, status, registered_ip, created_at, updated_at)
        VALUES (?, ?, ?, 'pending', ?, ?, ?)
        "#,
    )
    .bind(username)
    .bind(key_id)
    .bind(secret_hash)
    .bind(registered_ip)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;

    Ok(res.last_insert_rowid())
}

/// Overwrite a previously `revoked` row with a fresh pending registration
/// (new key pair), rather than leaving duplicate rows for the same
/// username. Telemetry rows (future) key off the numeric `id`, which is
/// preserved, so nothing is orphaned by this.
pub async fn reregister(
    pool: &SqlitePool,
    id: i64,
    key_id: &str,
    secret_hash: &str,
    registered_ip: &str,
) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        r#"
        UPDATE clients
        SET key_id = ?, secret_hash = ?, status = 'pending', registered_ip = ?, updated_at = ?
        WHERE id = ?
        "#,
    )
    .bind(key_id)
    .bind(secret_hash)
    .bind(registered_ip)
    .bind(&now)
    .bind(id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn set_status(pool: &SqlitePool, id: i64, status: ClientStatus) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    sqlx::query("UPDATE clients SET status = ?, updated_at = ? WHERE id = ?")
        .bind(status.as_str())
        .bind(&now)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

// ============================================================================
// ADMIN TOKEN
// ============================================================================

/// Store (overwriting any existing) admin token hash. Used by the `admin
/// generate`/`admin regenerate` CLI subcommand.
pub async fn set_admin_token_hash(pool: &SqlitePool, token_hash: &str) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        r#"
        INSERT INTO admin_token (id, token_hash, created_at) VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET token_hash = excluded.token_hash, created_at = excluded.created_at
        "#,
    )
    .bind(token_hash)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_admin_token_hash(pool: &SqlitePool) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as("SELECT token_hash FROM admin_token WHERE id = 1")
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.0))
}