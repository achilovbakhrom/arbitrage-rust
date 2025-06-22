use std::fmt;
use std::sync::Arc;

/// Lightweight trading symbol representation
#[derive(Debug, Clone)]
pub struct Symbol {
    pub symbol: Arc<str>, // Using Arc<str> to reduce clone costs
    pub base_asset: Arc<str>,
    pub quote_asset: Arc<str>,
    pub price_precision: u8,
    pub qty_precision: u8,
    pub is_trading: bool,
    pub is_spot_trading_allowed: bool,
}

impl fmt::Display for Symbol {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.base_asset, self.quote_asset)
    }
}
