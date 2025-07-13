use std::sync::Arc;
use crate::exchange::fix_api::{ MarketOrder, OrderSide };
use crate::models::symbol_map::SymbolMap;
use crate::arbitrage::detector::ArbitrageOpportunity;

#[inline(always)]
pub fn create_order_with_precision(
    symbol: String,
    is_base_to_quote: bool,
    amount: f64,
    qty_precision: u8,
    price_precision: u8
) -> MarketOrder {
    if is_base_to_quote {
        MarketOrder {
            symbol,
            side: OrderSide::Sell,
            quantity: amount,
            quote_quantity: None,
            qty_precision: Some(qty_precision),
            price_precision: Some(price_precision),
        }
    } else {
        MarketOrder {
            symbol,
            side: OrderSide::Buy,
            quantity: 0.0,
            quote_quantity: Some(amount),
            qty_precision: Some(qty_precision),
            price_precision: Some(price_precision),
        }
    }
}

#[inline(always)]
pub fn get_symbol_precision(symbol_map: &Arc<SymbolMap>, symbol: &str) -> (u8, u8) {
    symbol_map
        .get(symbol)
        .map(|info| (info.qty_precision, info.price_precision))
        .unwrap_or((8, 8))
}

pub struct TriangularTradeData {
    pub symbols: [String; 3],
    pub directions: [bool; 3],
    pub start_amount: f64,
}

impl TriangularTradeData {
    #[inline]
    pub fn from_opportunity(opportunity: &ArbitrageOpportunity) -> Self {
        let path = &opportunity.path;
        Self {
            symbols: [
                path.first_symbol.to_string(),
                path.second_symbol.to_string(),
                path.third_symbol.to_string(),
            ],
            directions: [
                path.first_is_base_to_quote,
                path.second_is_base_to_quote,
                path.third_is_base_to_quote,
            ],
            start_amount: opportunity.start_amount,
        }
    }
}
