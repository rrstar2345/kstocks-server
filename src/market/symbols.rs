use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{info, warn};

use crate::market::http;
use crate::settings::AppConfig;

/// One entry from `getAllIndices`.
#[derive(Debug, Deserialize, Clone)]
struct IndexEntry {
    #[serde(rename = "fnoIndexName")]
    fno_index_name: Option<String>,
    #[serde(rename = "indicesLongName")]
    #[allow(dead_code)]
    indices_long_name: Option<String>,
    #[serde(rename = "indicesShortName")]
    #[allow(dead_code)]
    indices_short_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IndexTrackerResponse {
    data: HashMap<String, IndexEntry>,
}

#[derive(Debug, Deserialize)]
struct OptionInfoResponse {
    #[serde(rename = "expiryDates")]
    expiry_dates: Vec<String>,
}

/// A resolved F&O symbol + its nearest expiry date, ready to be used to
/// build an option_ticks WebSocket URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolExpiry {
    pub symbol: String,
    pub expiry: String,
}

/// Fetch all distinct `fnoIndexName` values (non-null) from the
/// `indices_info` endpoint, e.g. NIFTY, NIFTYNXT50, BANKNIFTY, FINNIFTY, MIDCPNIFTY.
pub async fn fetch_fno_symbols(config: &AppConfig) -> Result<Vec<String>> {
    let url = &config.system.indices_info.base;

    let response = http::get(url)
        .send()
        .await
        .map_err(|e| anyhow!("Failed to fetch indices info: {}", e))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| anyhow!("Failed to read indices info response body: {}", e))?;

    if !status.is_success() {
        return Err(anyhow!(
            "indices_info request failed with status {}: {}",
            status,
            &body[..body.len().min(300)]
        ));
    }

    let parsed: IndexTrackerResponse = serde_json::from_str(&body)
        .map_err(|e| anyhow!("Failed to parse indices info response: {}", e))?;

    let mut symbols: Vec<String> = parsed
        .data
        .values()
        .filter_map(|entry| entry.fno_index_name.clone())
        .collect();

    symbols.sort();
    symbols.dedup();

    info!("Resolved {} F&O symbols: {:?}", symbols.len(), symbols);
    Ok(symbols)
}

/// Fetch the expiry dates for a given F&O symbol and return the first
/// (nearest) one.
pub async fn fetch_nearest_expiry(config: &AppConfig, symbol: &str) -> Result<String> {
    let url = format!(
        "{}?symbol={}",
        config.system.option_info.base,
        urlencoding::encode(symbol)
    );

    let response = http::get(&url)
        .send()
        .await
        .map_err(|e| anyhow!("Failed to fetch option info for {}: {}", symbol, e))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| anyhow!("Failed to read option info response body for {}: {}", symbol, e))?;

    if !status.is_success() {
        return Err(anyhow!(
            "option_info request failed with status {} for {}: {}",
            status,
            symbol,
            &body[..body.len().min(300)]
        ));
    }

    let parsed: OptionInfoResponse = serde_json::from_str(&body)
        .map_err(|e| anyhow!("Failed to parse option info response for {}: {}", symbol, e))?;

    parsed
        .expiry_dates
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("No expiry dates returned for {}", symbol))
}

/// Resolve the full set of symbol+expiry pairs to stream: fetches the F&O
/// symbol list, then the nearest expiry for each. Symbols whose expiry fetch
/// fails are skipped (logged as a warning) rather than aborting the whole run.
pub async fn resolve_symbol_expiries(config: &AppConfig) -> Result<Vec<SymbolExpiry>> {
    let symbols = fetch_fno_symbols(config).await?;
    let mut result = Vec::with_capacity(symbols.len());

    for symbol in symbols {
        match fetch_nearest_expiry(config, &symbol).await {
            Ok(expiry) => {
                info!("{} -> nearest expiry {}", symbol, expiry);
                result.push(SymbolExpiry { symbol, expiry });
            }
            Err(e) => {
                warn!("Skipping {}: failed to resolve expiry: {}", symbol, e);
            }
        }
    }

    if result.is_empty() {
        return Err(anyhow!("Could not resolve any symbol/expiry pairs"));
    }

    Ok(result)
}
