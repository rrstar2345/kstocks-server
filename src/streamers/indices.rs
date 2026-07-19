use anyhow::{anyhow, Result};
use chrono::Utc;
use futures::stream::StreamExt;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::db::{IndexTickRow, IndexTickSender};
use crate::settings::AppConfig;
use crate::stats::{ConnState, SharedStats};

pub const STREAM_NAME: &str = "indices";

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

/// Run the indices streamer with automatic reconnect. Loops forever (until
/// the tick channel is closed), updating `stats` as it goes.
pub async fn run(config: AppConfig, tx: IndexTickSender, stats: SharedStats) {
    {
        let mut s = stats.write().await;
        s.ensure_stream(STREAM_NAME);
    }

    loop {
        {
            let mut s = stats.write().await;
            let stat = s.ensure_stream(STREAM_NAME);
            stat.state = ConnState::Connecting;
        }

        match stream_once(&config, &tx, &stats).await {
            Ok(_) => {
                info!("Indices stream closed gracefully");
            }
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
}

async fn stream_once(config: &AppConfig, tx: &IndexTickSender, stats: &SharedStats) -> Result<()> {
    let url = config.system.indices_streamer.base.clone();

    let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| anyhow!("Failed to connect to indices WebSocket: {}", e))?;

    info!("Connected to indices WebSocket stream");
    {
        let mut s = stats.write().await;
        let stat = s.ensure_stream(STREAM_NAME);
        stat.state = ConnState::Connected;
    }

    let (_, mut read) = ws_stream.split();

    while let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                match serde_json::from_str::<IndexStreamMessage>(&text) {
                    Ok(msg) => {
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

                        if tx.send(row).await.is_err() {
                            info!("DB writer channel closed; stopping indices stream.");
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse indices stream message: {}. Message: {}", e, text);
                    }
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
