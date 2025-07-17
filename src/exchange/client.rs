use async_trait::async_trait;
use anyhow::Result;
use crate::models::{ level::Level, symbol::Symbol };

#[derive(Debug, Clone)]
pub struct DepthResponse {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub last_update_id: u64,
}

#[async_trait]
pub trait ExchangeClient: Send + Sync {
    async fn get_all_symbols(&self) -> Result<Vec<Symbol>>;
    async fn get_active_spot_symbols(&self) -> Result<Vec<Symbol>>;
    async fn is_operational(&self) -> Result<bool>;
    async fn fetch_depth(&self, symbol: &str, limit: usize) -> Result<DepthResponse>;
}
