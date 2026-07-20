//! Market-hours awareness, driven by NSE's own IST clock rather than the
//! local system clock (so this works correctly regardless of which region
//! the server is deployed in).
//!
//! Two session modes:
//! - `Active`: trading window (8:30 AM - 4:00 PM IST, Mon-Fri, i.e. the
//!   9:00-15:30 window with a 30-minute buffer either side) and no more than
//!   `inactive_switch_after_secs` has elapsed since the last real tick.
//! - `Idle`: outside the window, or inside it but quiet for more than
//!   `inactive_switch_after_secs` — the streamers poll instead of holding a
//!   live connection open.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};
use serde::Deserialize;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::RwLock;
use tracing::warn;

use crate::http;
use crate::settings::AppConfig;

#[derive(Debug, Deserialize)]
struct CurrentTimeResponse {
    data: CurrentTimeData,
}

#[derive(Debug, Deserialize)]
struct CurrentTimeData {
    #[serde(rename = "currentTime")]
    current_time: String,
}

/// Cached offset between NSE's reported IST time and this process's local
/// `Utc::now()`, stored as milliseconds (NSE_IST_as_UTC - Utc::now()) so we
/// can cheaply derive "current NSE time" between refreshes without hitting
/// the API on every check. Guarded by a coarse staleness check in
/// `get_ist_now`.
static OFFSET_MS: AtomicI64 = AtomicI64::new(i64::MIN);
static LAST_REFRESH_UNIX_MS: AtomicI64 = AtomicI64::new(0);

/// How often to re-sync with the NSE time endpoint. The offset should be
/// essentially constant (modulo NTP drift), so this doesn't need to be
/// frequent, but refreshing periodically protects against clock drift on
/// long-running processes.
const REFRESH_INTERVAL_MS: i64 = 15 * 60 * 1000;

/// IST is UTC+5:30, fixed year-round (no DST).
const IST_OFFSET_SECS: i64 = 5 * 3600 + 30 * 60;

async fn fetch_nse_ist_time(config: &AppConfig) -> Result<DateTime<Utc>> {
    let url = &config.system.current_time.base;

    let response = http::get(url)
        .send()
        .await
        .map_err(|e| anyhow!("Failed to fetch NSE current time: {}", e))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| anyhow!("Failed to read NSE current time response body: {}", e))?;

    if !status.is_success() {
        return Err(anyhow!(
            "NSE current time request failed with status {}: {}",
            status,
            &body[..body.len().min(300)]
        ));
    }

    let parsed: CurrentTimeResponse = serde_json::from_str(&body)
        .map_err(|e| anyhow!("Failed to parse NSE current time response: {}", e))?;

    // "2026-07-20 11:05:08" is naive IST wall-clock time; convert to UTC by
    // subtracting the fixed 5:30 offset.
    let naive = NaiveDateTime::parse_from_str(&parsed.data.current_time, "%Y-%m-%d %H:%M:%S")
        .map_err(|e| anyhow!("Failed to parse NSE current time value '{}': {}", parsed.data.current_time, e))?;

    let as_utc = naive - chrono::Duration::seconds(IST_OFFSET_SECS);
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(as_utc, Utc))
}

/// Refresh the cached NSE/local offset. Safe to call frequently; internally
/// only hits the network if the cache is stale or uninitialized.
pub async fn refresh_if_stale(config: &AppConfig) {
    let now_ms = Utc::now().timestamp_millis();
    let last = LAST_REFRESH_UNIX_MS.load(Ordering::Relaxed);
    let initialized = OFFSET_MS.load(Ordering::Relaxed) != i64::MIN;

    if initialized && now_ms - last < REFRESH_INTERVAL_MS {
        return;
    }

    match fetch_nse_ist_time(config).await {
        Ok(nse_utc) => {
            let offset = nse_utc.timestamp_millis() - now_ms;
            OFFSET_MS.store(offset, Ordering::Relaxed);
            LAST_REFRESH_UNIX_MS.store(now_ms, Ordering::Relaxed);
        }
        Err(e) => {
            warn!("Failed to refresh NSE time offset: {} (using previous offset/local clock)", e);
        }
    }
}

/// Current time, as best known from NSE (falls back to local UTC if the NSE
/// endpoint has never been reached successfully yet).
pub fn get_ist_now() -> DateTime<Utc> {
    let offset = OFFSET_MS.load(Ordering::Relaxed);
    let now = Utc::now();
    if offset == i64::MIN {
        now
    } else {
        now + chrono::Duration::milliseconds(offset)
    }
}

/// True if `t` (a UTC instant, interpreted as IST for wall-clock purposes)
/// falls within the buffered trading window: 8:30 AM - 4:00 PM IST, Mon-Fri.
pub fn is_within_trading_window(t: DateTime<Utc>) -> bool {
    let ist = t + chrono::Duration::seconds(IST_OFFSET_SECS);

    // Weekday check (Mon-Fri).
    use chrono::Weekday::*;
    match ist.weekday() {
        Sat | Sun => return false,
        _ => {}
    }

    let minutes_since_midnight = ist.hour() as i64 * 60 + ist.minute() as i64;
    let window_start = 8 * 60 + 30; // 8:30 AM
    let window_end = 16 * 60; // 4:00 PM

    minutes_since_midnight >= window_start && minutes_since_midnight < window_end
}

// ============================================================================
// SESSION MODE
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionMode {
    Active,
    Idle,
}

impl SessionMode {
    pub fn label(&self) -> &'static str {
        match self {
            SessionMode::Active => "ACTIVE",
            SessionMode::Idle => "IDLE",
        }
    }
}

/// Shared, process-wide session mode plus the timestamp of the last
/// "real" (non-heartbeat) tick seen by any stream. Used by streamers to
/// decide whether to hold an open WSS connection (Active) or poll hourly
/// (Idle).
pub struct SessionState {
    inner: RwLock<SessionInner>,
}

struct SessionInner {
    mode: SessionMode,
    last_activity_at: DateTime<Utc>,
}

impl SessionState {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(SessionInner {
                mode: SessionMode::Idle,
                last_activity_at: get_ist_now(),
            }),
        }
    }

    pub async fn mode(&self) -> SessionMode {
        self.inner.read().await.mode
    }

    /// Record a real (non-heartbeat) tick. Switches to Active if we were
    /// Idle (e.g. an off-hours poll unexpectedly saw live data).
    pub async fn record_activity(&self) {
        let mut inner = self.inner.write().await;
        inner.last_activity_at = get_ist_now();
        inner.mode = SessionMode::Active;
    }

    /// Re-evaluate the mode given the current time and config thresholds.
    /// Called periodically by a supervisor task. Returns the (possibly
    /// updated) mode.
    pub async fn tick(&self, inactive_switch_after_secs: i64) -> SessionMode {
        let now = get_ist_now();
        let mut inner = self.inner.write().await;

        if !is_within_trading_window(now) {
            inner.mode = SessionMode::Idle;
            return inner.mode;
        }

        // Within trading window: stay/become Active unless quiet too long.
        let quiet_for = now.signed_duration_since(inner.last_activity_at).num_seconds();
        if inner.mode == SessionMode::Active && quiet_for > inactive_switch_after_secs {
            inner.mode = SessionMode::Idle;
        } else if inner.mode == SessionMode::Idle {
            // Entering the trading window fresh (e.g. process just started,
            // or we've just crossed 8:30 AM): give it a chance as Active.
            inner.mode = SessionMode::Active;
            inner.last_activity_at = now;
        }

        inner.mode
    }
}

pub type SharedSessionState = std::sync::Arc<SessionState>;

pub fn new_shared_session_state() -> SharedSessionState {
    std::sync::Arc::new(SessionState::new())
}