//! Everything related to fetching/streaming NSE market data: the shared
//! HTTP client, NSE-clock-derived session state, F&O symbol/expiry
//! resolution, and the live WSS streamers.

pub mod http;
pub mod market_clock;
pub mod streamers;
pub mod symbols;
