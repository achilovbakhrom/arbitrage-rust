use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Symbol {
    pub symbol: Arc<str>,
    pub base_asset: Arc<str>,
    pub quote_asset: Arc<str>,
    pub price_precision: u8,
    pub qty_precision: u8,
    pub is_trading: bool,
    pub is_spot_trading_allowed: bool,
    pub min_notional: f64,
    pub max_notional: Option<f64>,
    pub min_qty: f64,
    pub max_qty: f64,
    pub step_size: f64,
}

impl fmt::Display for Symbol {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.base_asset, self.quote_asset)
    }
}
