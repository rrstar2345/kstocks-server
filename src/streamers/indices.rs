use anyhow::{anyhow, Result};
use chrono::Utc;
use futures::stream::StreamExt;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::db::{IndexTickRow, IndexTickSender};
use crate::market_clock::{SessionMode, SharedSessionState};
use crate::settings::AppConfig;
use crate::stats::{ConnState, SharedStats};

pub const STREAM_NAME: &str = "indices";

/// NSE sends `indexName: "HEARTBEAT"` (numeric fields mostly 0) on this
/// stream during off hours to keep the socket alive. These aren't real
/// ticks and must not count as activity or reset the idle timer.
const HEARTBEAT_INDEX_NAME: &str = "HEARTBEAT";

/// Wire shape of a single message on the indices WSS. Matches the sample:
/// {"indexName":"NIFTY 50","brdCstIndexName":"NIFTY 50","currentPrice":23961.45,
///  "perChange":-0.39,"change":-94.55,"previousClose":24056.0,
///  "recievedTime":"29-Jun-2026 13:28","dessiminationTime":"2026-06-29 13:28:18",
///  "open":24061.75,"low":23925.4,"high":24120.0,"indStatus":"Close","indValue":0.0,
///  "indChange":0.0,"indPerChange":0.0,"indRecievedTime":null,"mktStatus":"Open"}
#[derive(Debug, Deserialize, Clone)]
struct IndexStreamMessage {
    #[serde(rename = "indexName")]
    index_name: String,
    #[serde(rename = "currentPrice")]
    current_price: f64,
    #[serde(rename = "perChange")]
    per_change: f64,
    change: f64,
    #[serde(rename = "previousClose")]
    previous_close: f64,
    #[serde(rename = "dessiminationTime")]
    dissemination_time: String,
    open: f64,
    low: f64,
    high: f64,
    #[serde(rename = "indStatus")]
    ind_status: String,
    #[serde(rename = "mktStatus")]
    mkt_status: String,
}

/// Run the indices streamer with automatic reconnect, alternating between
/// Active (held-open connection) and Idle (hourly poll) modes based on
/// `session`.
pub async fn run(config: AppConfig, tx: IndexTickSender, stats: SharedStats, session: SharedSessionState) {
    {
        let mut s = stats.write().await;
        s.ensure_stream(STREAM_NAME);
    }

    loop {
        let mode = session.mode().await;

        match mode {
            SessionMode::Active => {
                {
                    let mut s = stats.write().await;
                    let stat = s.ensure_stream(STREAM_NAME);
                    stat.state = ConnState::Connecting;
                }

                match stream_active(&config, &tx, &stats, &session).await {
                    Ok(_) => info!("Indices stream closed gracefully"),
                    Err(e) => {
                        warn!("Indices stream error: {}", e);
                        let mut s = stats.write().await;
                        let stat = s.ensure_stream(STREAM_NAME);
                        stat.last_error = Some(e.to_string());
                        stat.reconnect_count += 1;
                        stat.state = ConnState::Reconnecting;
                    }
                }

                if tx.is_closed() {
                    let mut s = stats.write().await;
                    let stat = s.ensure_stream(STREAM_NAME);
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
                    let stat = s.ensure_stream(STREAM_NAME);
                    stat.state = ConnState::Idle;
                }

                if let Err(e) = poll_once(&config, &tx, &stats, &session).await {
                    debug!("Indices idle poll error (expected outside market hours): {}", e);
                }

                if tx.is_closed() {
                    let mut s = stats.write().await;
                    let stat = s.ensure_stream(STREAM_NAME);
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

/// Parse and (if not a heartbeat) forward one message. Returns `true` if the
/// message represented real activity (i.e. was not a heartbeat).
async fn handle_message(
    text: &str,
    tx: &IndexTickSender,
    stats: &SharedStats,
    session: &SharedSessionState,
) -> Result<bool> {
    let msg = match serde_json::from_str::<IndexStreamMessage>(text) {
        Ok(m) => m,
        Err(e) => {
            warn!("Failed to parse indices stream message: {}. Message: {}", e, text);
            return Ok(false);
        }
    };

    if msg.index_name == HEARTBEAT_INDEX_NAME {
        // Off-hours keep-alive; not real activity.
        return Ok(false);
    }

    let row = IndexTickRow {
        time: Utc::now(),
        index_name: msg.index_name,
        current_price: msg.current_price,
        change: msg.change,
        per_change: msg.per_change,
        previous_close: msg.previous_close,
        open: msg.open,
        low: msg.low,
        high: msg.high,
        ind_status: msg.ind_status,
        mkt_status: msg.mkt_status,
        dissemination_time: msg.dissemination_time,
    };

    {
        let mut s = stats.write().await;
        let stat = s.ensure_stream(STREAM_NAME);
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
    tx: &IndexTickSender,
    stats: &SharedStats,
    session: &SharedSessionState,
) -> Result<()> {
    let url = config.system.indices_streamer.base.clone();

    let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| anyhow!("Failed to connect to indices WebSocket: {}", e))?;

    info!("Connected to indices WebSocket stream (active mode)");
    {
        let mut s = stats.write().await;
        let stat = s.ensure_stream(STREAM_NAME);
        stat.state = ConnState::Connected;
    }

    let (_, mut read) = ws_stream.split();

    loop {
        // Re-check mode between messages so a switch to Idle (due to
        // inactivity or leaving the trading window) closes this connection
        // promptly rather than waiting indefinitely for the next message.
        if session.mode().await == SessionMode::Idle {
            info!("Switching indices stream to idle mode; closing active connection.");
            return Ok(());
        }

        let next = tokio::time::timeout(tokio::time::Duration::from_secs(30), read.next()).await;

        let msg_result = match next {
            Ok(Some(r)) => r,
            Ok(None) => break, // stream ended
            Err(_) => continue, // timeout; loop back to re-check mode
        };

        match msg_result {
            Ok(Message::Text(text)) => {
                if let Err(e) = handle_message(&text, tx, stats, session).await {
                    return Err(e);
                }
            }
            Ok(Message::Close(_)) => {
                info!("Indices WebSocket closed by server");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                return Err(anyhow!("Indices WebSocket error: {}", e));
            }
        }
    }

    Ok(())
}

/// Idle mode: briefly connect, listen for up to `idle_poll_listen_secs`, and
/// disconnect. If any real (non-heartbeat) message arrives, `record_activity`
/// flips the shared session to Active so the next loop iteration switches
/// to the held-open connection.
async fn poll_once(
    config: &AppConfig,
    tx: &IndexTickSender,
    stats: &SharedStats,
    session: &SharedSessionState,
) -> Result<()> {
    let url = config.system.indices_streamer.base.clone();

    let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| anyhow!("Failed to connect to indices WebSocket (idle poll): {}", e))?;

    debug!("Indices idle poll: connected, listening briefly");

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
                let _ = handle_message(&text, tx, stats, session).await;
                if session.mode().await == SessionMode::Active {
                    // Real activity seen; hand back to the active loop.
                    break;
                }
            }
            Ok(Some(Ok(Message::Close(_)))) => break,
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => return Err(anyhow!("Indices WebSocket error during idle poll: {}", e)),
            Ok(None) => break,
            Err(_) => break, // overall listen window elapsed
        }
    }

    Ok(())
}