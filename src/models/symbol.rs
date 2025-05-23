use std::fmt;
use std::sync::Arc;

/// Lightweight trading symbol representation
#[derive(Debug, Clone)]
pub struct Symbol {
    pub symbol: Arc<str>, // Using Arc<str> to reduce clone costs
    pub base_asset: Arc<str>,
    pub quote_asset: Arc<str>,
    pub min_qty: Option<f64>,
    pub max_qty: Option<f64>,
    pub price_precision: u8,
    pub qty_precision: u8,
    pub is_trading: bool,
    pub is_spot_trading_allowed: bool,
}

impl Symbol {
    /// Returns the formatted symbol without allocating a new String
    #[inline]
    pub fn formatted_symbol(&self) -> String {
        format!("{}/{}", self.base_asset, self.quote_asset)
    }
}

impl fmt::Display for Symbol {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.base_asset, self.quote_asset)
    }
}
