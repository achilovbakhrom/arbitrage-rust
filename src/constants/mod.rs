// use std::time::Duration;

// pub const API_TIMEOUT: Duration = Duration::from_secs(5);
// pub const WEBSOCKET_BUFFER_SIZE: usize = 100;
// pub const DEFAULT_DEPTH: usize = 5;

use std::time::Duration;

// Core system timeouts
pub const API_TIMEOUT: Duration = Duration::from_secs(5); // Timeout for exchange API calls

// WebSocket configuration
pub const WEBSOCKET_BUFFER_SIZE: usize = 100; // Max symbols to subscribe to in one WebSocket connection

// Default orderbook depth for market data
pub const DEFAULT_DEPTH: usize = 5; // Number of price levels to track (bids/asks)
