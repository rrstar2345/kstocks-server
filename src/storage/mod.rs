//! Everything related to persisting and processing market data in SQLite:
//! raw tick ingestion (batched writers), OHLC aggregation, and retention
//! (purge/vacuum).

pub mod ohlc;
pub mod retention;
pub mod ticks;

// Re-export the most commonly used items at `storage::` level so callers
// don't need to know which submodule they live in.
pub use ticks::{
    init_pool, start_index_tick_writer, start_option_tick_writer, IndexTickRow, IndexTickSender,
    OptionTickRow, OptionTickSender,
};
