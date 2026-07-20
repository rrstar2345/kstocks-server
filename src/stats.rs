use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use chrono::{DateTime, Local};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnState {
    Connecting,
    Connected,
    Reconnecting,
    Idle,
    Stopped,
}

impl ConnState {
    pub fn label(&self) -> &'static str {
        match self {
            ConnState::Connecting => "CONNECTING",
            ConnState::Connected => "CONNECTED",
            ConnState::Reconnecting => "RECONNECTING",
            ConnState::Idle => "IDLE (polling)",
            ConnState::Stopped => "STOPPED",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamStat {
    pub name: String,
    pub state: ConnState,
    pub ticks_received: u64,
    pub last_tick_at: Option<DateTime<Local>>,
    pub last_error: Option<String>,
    pub reconnect_count: u64,
}

impl StreamStat {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            state: ConnState::Connecting,
            ticks_received: 0,
            last_tick_at: None,
            last_error: None,
            reconnect_count: 0,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DbStat {
    pub rows_written: u64,
    pub rows_pending: usize,
    pub last_flush_at: Option<DateTime<Local>>,
    pub last_flush_rows: usize,
    pub last_error: Option<String>,
}

#[derive(Debug, Default)]
pub struct AppStats {
    pub streams: HashMap<String, StreamStat>,
    pub indices_db: DbStat,
    pub options_db: DbStat,
    pub started_at: Option<DateTime<Local>>,
    pub session_mode_label: String,
}

pub type SharedStats = Arc<RwLock<AppStats>>;

pub fn new_shared_stats() -> SharedStats {
    Arc::new(RwLock::new(AppStats {
        started_at: Some(Local::now()),
        ..Default::default()
    }))
}

impl AppStats {
    pub fn ensure_stream(&mut self, name: &str) -> &mut StreamStat {
        self.streams
            .entry(name.to_string())
            .or_insert_with(|| StreamStat::new(name))
    }
}