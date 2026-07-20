use anyhow::{anyhow, Result};
use chrono::Utc;
use futures::stream::StreamExt;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::market::market_clock::{SessionMode, SharedSessionState};
use crate::settings::AppConfig;
use crate::stats::{ConnState, SharedStats};
use crate::storage::{OptionTickRow, OptionTickSender};

/// Helper: deserialize a null/absent numeric field as `None` rather than
/// erroring. NSE option ticks legitimately send null for illiquid strikes.
fn null_as_none_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<f64>::deserialize(deserializer)
}

fn null_as_none_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<u64>::deserialize(deserializer)
}

/// Individual option leg (CE or PE). All numeric fields are optional because
/// NSE sends null for strikes with no trading activity.
#[derive(Debug, Deserialize, Clone, Default)]
struct OptionLeg {
    #[serde(rename = "totalTradedVolume", default, deserialize_with = "null_as_none_u64")]
    total_traded_volume: Option<u64>,
    #[serde(rename = "lastPrice", default, deserialize_with = "null_as_none_f64")]
    last_price: Option<f64>,
    #[serde(default, deserialize_with = "null_as_none_f64")]
    change: Option<f64>,
    #[serde(rename = "openInterest", default, deserialize_with = "null_as_none_f64")]
    open_interest: Option<f64>,
    #[serde(rename = "buyPrice1", default, deserialize_with = "null_as_none_f64")]
    buy_price1: Option<f64>,
    #[serde(rename = "sellPrice1", default, deserialize_with = "null_as_none_f64")]
    sell_price1: Option<f64>,
}

/// NSE sends `flag: "HEARTBEAT"` (all fields otherwise null/zeroed) on this
/// stream during off hours to keep the socket alive. These aren't real
/// ticks and must not count as activity or reset the idle timer.
const HEARTBEAT_FLAG: &str = "HEARTBEAT";

/// Complete option chain tick message from NSE's `fo/mbp` WSS.
#[derive(Debug, Deserialize, Clone)]
struct OptionChainMessage {
    #[serde(rename = "expiryDates")]
    expiry_dates: String,
    #[serde(rename = "strikePrice")]
    strike_price: f64,
    #[serde(rename = "PE", alias = "pe", default)]
    pe: Option<OptionLeg>,
    #[serde(rename = "CE", alias = "ce", default)]
    ce: Option<OptionLeg>,
    #[serde(default)]
    flag: Option<String>,
}

/// Run a single symbol/expiry's option streamer with automatic reconnect,
/// alternating between Active (held-open connection) and Idle (hourly poll)
/// modes based on `session`.
pub async fn run(
    config: AppConfig,
    symbol: String,
    expiry: String,
    tx: OptionTickSender,
    stats: SharedStats,
    session: SharedSessionState,
) {
    let stream_key = format!("options:{}:{}", symbol, expiry);

    {
        let mut s = stats.write().await;
        s.ensure_stream(&stream_key);
    }

    loop {
        let mode = session.mode().await;

        match mode {
            SessionMode::Active => {
                {
                    let mut s = stats.write().await;
                    let stat = s.ensure_stream(&stream_key);
                    stat.state = ConnState::Connecting;
                }

                match stream_active(&config, &symbol, &expiry, &tx, &stats, &stream_key, &session).await {
                    Ok(_) => info!("Options stream [{}] closed gracefully", stream_key),
                    Err(e) => {
                        warn!("Options stream [{}] error: {}", stream_key, e);
                        let mut s = stats.write().await;
                        let stat = s.ensure_stream(&stream_key);
                        stat.last_error = Some(e.to_string());
                        stat.reconnect_count += 1;
                        stat.state = ConnState::Reconnecting;
                    }
                }

                if tx.is_closed() {
                    let mut s = stats.write().await;
                    let stat = s.ensure_stream(&stream_key);
                    stat.state = ConnState::Stopped;
                    break;
                }

                tokio::time::sleep(tokio::time::Duration::from_secs(
                    config.runtime.reconnect_delay_seconds,
                ))
                .await;
            }
            SessionMode::Idle => {
                {
                    let mut s = stats.write().await;
                    let stat = s.ensure_stream(&stream_key);
                    stat.state = ConnState::Idle;
                }

                if let Err(e) =
                    poll_once(&config, &symbol, &expiry, &tx, &stats, &stream_key, &session).await
                {
                    debug!("Options idle poll [{}] error (expected outside market hours): {}", stream_key, e);
                }

                if tx.is_closed() {
                    let mut s = stats.write().await;
                    let stat = s.ensure_stream(&stream_key);
                    stat.state = ConnState::Stopped;
                    break;
                }

                tokio::time::sleep(tokio::time::Duration::from_secs(
                    config.runtime.idle_poll_interval_secs,
                ))
                .await;
            }
        }
    }
}

/// Build the option_ticks WSS URL: base + ?symbol=<symbol>&expiry=<expiry>
fn build_ws_url(config: &AppConfig, symbol: &str, expiry: &str) -> String {
    format!(
        "{}?symbol={}&expiry={}",
        config.system.option_ticks.base,
        urlencoding::encode(symbol),
        urlencoding::encode(expiry),
    )
}

/// Parse and (if not a heartbeat) forward one message. Returns `true` if the
/// message represented real activity (i.e. was not a heartbeat).
async fn handle_message(
    text: &str,
    symbol: &str,
    tx: &OptionTickSender,
    stats: &SharedStats,
    stream_key: &str,
    session: &SharedSessionState,
) -> Result<bool> {
    let parsed: OptionChainMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            warn!("[{}] JSON parse error: {}", stream_key, e);
            return Ok(false);
        }
    };

    if parsed.flag.as_deref() == Some(HEARTBEAT_FLAG) {
        // Off-hours keep-alive; not real activity.
        return Ok(false);
    }

    let ce = parsed.ce.unwrap_or_default();
    let pe = parsed.pe.unwrap_or_default();

    let row = OptionTickRow {
        time: Utc::now(),
        symbol: symbol.to_string(),
        expiry: parsed.expiry_dates,
        strike_price: parsed.strike_price,

        ce_last_price: ce.last_price,
        ce_change: ce.change,
        ce_volume: ce.total_traded_volume.map(|v| v as i64),
        ce_oi: ce.open_interest,
        ce_bid: ce.buy_price1,
        ce_ask: ce.sell_price1,

        pe_last_price: pe.last_price,
        pe_change: pe.change,
        pe_volume: pe.total_traded_volume.map(|v| v as i64),
        pe_oi: pe.open_interest,
        pe_bid: pe.buy_price1,
        pe_ask: pe.sell_price1,
    };

    {
        let mut s = stats.write().await;
        let stat = s.ensure_stream(stream_key);
        stat.ticks_received += 1;
        stat.last_tick_at = Some(chrono::Local::now());
    }

    session.record_activity().await;

    if tx.send(row).await.is_err() {
        return Err(anyhow!("DB writer channel closed"));
    }

    Ok(true)
}

/// Active mode: hold the WSS connection open and process messages as they
/// arrive, checking after each one whether we should drop back to Idle.
async fn stream_active(
    config: &AppConfig,
    symbol: &str,
    expiry: &str,
    tx: &OptionTickSender,
    stats: &SharedStats,
    stream_key: &str,
    session: &SharedSessionState,
) -> Result<()> {
    let url = build_ws_url(config, symbol, expiry);
    info!("Connecting options stream [{}] -> {} (active mode)", stream_key, url);

    let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| anyhow!("WebSocket connection failed for {}: {}", stream_key, e))?;

    {
        let mut s = stats.write().await;
        let stat = s.ensure_stream(stream_key);
        stat.state = ConnState::Connected;
    }

    let (_, mut read) = ws_stream.split();

    loop {
        if session.mode().await == SessionMode::Idle {
            info!("Switching options stream [{}] to idle mode; closing active connection.", stream_key);
            return Ok(());
        }

        let next = tokio::time::timeout(tokio::time::Duration::from_secs(30), read.next()).await;

        let msg_result = match next {
            Ok(Some(r)) => r,
            Ok(None) => break,
            Err(_) => continue, // timeout; loop back to re-check mode
        };

        match msg_result {
            Ok(Message::Text(text)) => {
                handle_message(&text, symbol, tx, stats, stream_key, session).await?;
            }
            Ok(Message::Close(_)) => {
                info!("Options WebSocket [{}] closed by server", stream_key);
                break;
            }
            Ok(_) => {}
            Err(e) => {
                return Err(anyhow!("Options WebSocket error [{}]: {}", stream_key, e));
            }
        }
    }

    Ok(())
}

/// Idle mode: briefly connect, listen for up to `idle_poll_listen_secs`, and
/// disconnect. Any message seen flips the shared session to Active via
/// `record_activity`, so the next loop iteration switches to the held-open
/// connection.
async fn poll_once(
    config: &AppConfig,
    symbol: &str,
    expiry: &str,
    tx: &OptionTickSender,
    stats: &SharedStats,
    stream_key: &str,
    session: &SharedSessionState,
) -> Result<()> {
    let url = build_ws_url(config, symbol, expiry);

    let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| anyhow!("WebSocket connection failed for {} (idle poll): {}", stream_key, e))?;

    debug!("Options idle poll [{}]: connected, listening briefly", stream_key);

    let (_, mut read) = ws_stream.split();
    let listen_for = tokio::time::Duration::from_secs(config.runtime.idle_poll_listen_secs);
    let deadline = tokio::time::Instant::now() + listen_for;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        match tokio::time::timeout(remaining, read.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                handle_message(&text, symbol, tx, stats, stream_key, session).await?;
                if session.mode().await == SessionMode::Active {
                    break;
                }
            }
            Ok(Some(Ok(Message::Close(_)))) => break,
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => {
                return Err(anyhow!("Options WebSocket error [{}] during idle poll: {}", stream_key, e))
            }
            Ok(None) => break,
            Err(_) => break, // overall listen window elapsed
        }
    }

    Ok(())
}