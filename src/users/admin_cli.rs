//! One-shot admin-token management, invoked via `kstocks-server admin
//! generate|regenerate`. Deliberately does not start any streamers or the
//! HTTP API — it only touches `kstocks-users.db` and exits. This is the
//! only way to mint an admin token: it requires shell access to the host
//! the server runs on, so the admin API itself never has a code path that
//! can create or change its own credential.

use anyhow::Result;

use crate::users::keys::generate_admin_token;
use crate::users::{init_users_pool, set_admin_token_hash};

/// Generate a new admin token, overwriting any existing one, and print the
/// plaintext token once. The server (or any prior admin session) will need
/// this new token for all subsequent `/admin/*` requests.
pub async fn run_generate(users_db_path: &str) -> Result<()> {
    let pool = init_users_pool(users_db_path).await?;
    let (plaintext, hash) = generate_admin_token();
    set_admin_token_hash(&pool, &hash).await?;

    println!("Admin token generated. This is shown once — store it securely:");
    println!();
    println!("{}", plaintext);
    println!();
    println!("Any previously issued admin token is now invalid.");

    Ok(())
}