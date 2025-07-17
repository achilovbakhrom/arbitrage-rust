use std::sync::Arc;
use crate::exchange::fix_api::{ MarketOrder, OrderSide };
use crate::models::symbol_map::SymbolMap;
use crate::arbitrage::detector::ArbitrageOpportunity;

const PRECISION_FACTORS: [f64; 9] = [
    1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0, 100000000.0,
];

#[inline(always)]
pub fn apply_precision(value: f64, precision: u8) -> f64 {
    if precision == 0 {
        return value.floor();
    }

    let factor = if precision <= 8 {
        unsafe { *PRECISION_FACTORS.get_unchecked(precision as usize) }
    } else {
        (10f64).powi(precision as i32)
    };

    (value * factor).floor() / factor
}

#[inline(always)]
pub fn get_symbol_precision(symbol_map: &Arc<SymbolMap>, symbol: &str) -> (u8, u8) {
    symbol_map
        .get(symbol)
        .map(|info| (info.qty_precision, info.price_precision))
        .unwrap_or((8, 8))
}

#[inline]
pub fn create_order_with_precision(
    symbol: String,
    is_base_to_quote: bool,
    amount: f64,
    qty_precision: u8,
    price_precision: u8
) -> MarketOrder {
    if is_base_to_quote {
        let precise_quantity = apply_precision(amount, qty_precision);
        MarketOrder {
            symbol,
            side: OrderSide::Sell,
            quantity: precise_quantity,
            quote_quantity: None,
            qty_precision: Some(qty_precision),
            price_precision: Some(price_precision),
        }
    } else {
        let precise_quote_quantity = apply_precision(amount, price_precision);
        MarketOrder {
            symbol,
            side: OrderSide::Buy,
            quantity: 0.0,
            quote_quantity: Some(precise_quote_quantity),
            qty_precision: Some(qty_precision),
            price_precision: Some(price_precision),
        }
    }
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

#[inline]
pub fn calculate_next_amount(
    current_result_qty: f64,
    current_result_quote: f64,
    current_is_base_to_quote: bool,
    next_is_base_to_quote: bool,
    next_qty_precision: u8,
    next_price_precision: u8
) -> f64 {
    let raw_amount = if current_is_base_to_quote {
        current_result_quote
    } else {
        current_result_qty
    };

    if next_is_base_to_quote {
        apply_precision(raw_amount, next_qty_precision)
    } else {
        apply_precision(raw_amount, next_price_precision)
    }
}

#[inline]
pub fn validate_trade_amount(amount: f64, symbol: &str) -> Result<(), &'static str> {
    if amount <= 0.0 {
        return Err("Trade amount must be positive");
    }
    if amount.is_nan() || amount.is_infinite() {
        return Err("Trade amount must be finite");
    }
    if symbol.is_empty() {
        return Err("Symbol cannot be empty");
    }
    Ok(())
}
