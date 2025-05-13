use std::sync::Arc;

/// A potential triangular arbitrage path
#[derive(Debug, Clone)]
pub struct TriangularPath {
    pub first_symbol: Arc<str>,
    pub second_symbol: Arc<str>,
    pub third_symbol: Arc<str>,
    pub first_is_base_to_quote: bool,
    pub second_is_base_to_quote: bool,
    pub third_is_base_to_quote: bool,
    pub start_asset: Arc<str>,
    pub end_asset: Arc<str>,
}
