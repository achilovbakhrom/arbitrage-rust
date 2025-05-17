use async_trait::async_trait;
use anyhow::Result;
use crate::models::{ level::Level, symbol::Symbol };

#[derive(Debug, Clone)]
pub struct DepthResponse {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub last_update_id: u64,
}

/// Exchange client trait that defines common operations for all exchanges
#[async_trait]
pub trait ExchangeClient: Send + Sync {
    /// Get the name of the exchange
    fn name(&self) -> &str;

    /// Fetch all available trading symbols
    async fn get_all_symbols(&self) -> Result<Vec<Symbol>>;

    /// Fetch only active spot trading symbols
    async fn get_active_spot_symbols(&self) -> Result<Vec<Symbol>>;

    /// Get symbol information by symbol name
    async fn get_symbol(&self, symbol: &str) -> Result<Symbol>;

    /// Check if the exchange is operational
    async fn is_operational(&self) -> Result<bool>;

    async fn fetch_depth(&self, symbol: &str, limit: usize) -> Result<DepthResponse>;
}
