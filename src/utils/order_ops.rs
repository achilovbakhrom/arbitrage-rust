use std::sync::Arc;
use crate::exchange::fix_api::{ MarketOrder, OrderSide };
use crate::models::symbol_map::SymbolMap;
use crate::models::symbol::Symbol;
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

/// Validates if a quantity meets the step size requirement with tolerance for floating-point errors
#[inline]
pub fn is_valid_step_size(quantity: f64, step_size: f64) -> bool {
    if step_size <= 0.0 {
        return true;
    }
    
    let remainder = (quantity / step_size) % 1.0;
    // Allow for floating-point precision errors
    remainder.abs() < 1e-10 || (1.0 - remainder).abs() < 1e-10
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

pub fn validate_order_filters(order: &MarketOrder, symbol_info: &Symbol, estimated_price: f64) -> Result<MarketOrder, String> {
    use tracing::debug;
    
    let mut validated_order = order.clone();
    
    // Get the raw quantity based on order type
    let raw_qty = if let Some(quote_qty) = order.quote_quantity {
        // For buy orders (using quote quantity), calculate base quantity
        quote_qty / estimated_price
    } else {
        // For sell orders (using base quantity)
        order.quantity
    };
    
    debug!(
        "Validating order for {}: raw_qty={:.8}, step_size={:.8}, min_qty={:.8}, min_notional={:.8}",
        symbol_info.symbol, raw_qty, symbol_info.step_size, symbol_info.min_qty, symbol_info.min_notional
    );
    
    // Apply LOT_SIZE step size adjustment with proper precision handling
    let adjusted_qty = if symbol_info.step_size > 0.0 {
        // Use high precision arithmetic to avoid floating point errors
        let step_multiplier = (raw_qty / symbol_info.step_size).round();
        let rounded_qty = step_multiplier * symbol_info.step_size;
        
        // Apply precision to ensure clean decimal representation
        let final_qty = apply_precision(rounded_qty, symbol_info.qty_precision);
        
        debug!(
            "Step size adjustment for {}: raw_qty={:.8} -> step_multiplier={:.2} -> rounded_qty={:.8} -> final_qty={:.8}",
            symbol_info.symbol, raw_qty, step_multiplier, rounded_qty, final_qty
        );
        
        final_qty
    } else {
        // Apply precision even if no step size
        apply_precision(raw_qty, symbol_info.qty_precision)
    };
    
    // Validate minimum quantity
    if adjusted_qty < symbol_info.min_qty {
        return Err(format!(
            "Order quantity {:.8} is below minimum {:.8} for {} (step_size: {:.8})",
            adjusted_qty, symbol_info.min_qty, symbol_info.symbol, symbol_info.step_size
        ));
    }
    
    // Validate maximum quantity  
    if adjusted_qty > symbol_info.max_qty {
        return Err(format!(
            "Order quantity {:.8} exceeds maximum {:.8} for {} (step_size: {:.8})",
            adjusted_qty, symbol_info.max_qty, symbol_info.symbol, symbol_info.step_size
        ));
    }
    
    // Calculate final notional value with adjusted quantity
    let final_notional = adjusted_qty * estimated_price;
    
    // Validate NOTIONAL filter  
    if final_notional < symbol_info.min_notional {
        return Err(format!(
            "Order notional {:.8} is below minimum {:.8} for {} (qty: {:.8} @ {:.8})",
            final_notional, symbol_info.min_notional, symbol_info.symbol, adjusted_qty, estimated_price
        ));
    }
    
    if let Some(max_notional) = symbol_info.max_notional {
        if final_notional > max_notional {
            return Err(format!(
                "Order notional {:.8} exceeds maximum {:.8} for {} (qty: {:.8} @ {:.8})",
                final_notional, max_notional, symbol_info.symbol, adjusted_qty, estimated_price
            ));
        }
    }
    
    // Final step size validation to ensure adjustment worked
    if !is_valid_step_size(adjusted_qty, symbol_info.step_size) {
        return Err(format!(
            "Order quantity {:.8} does not meet step size {:.8} for {} after adjustment",
            adjusted_qty, symbol_info.step_size, symbol_info.symbol
        ));
    }
    
    // Update the validated order with adjusted values
    if order.quote_quantity.is_some() {
        // For buy orders, update quote quantity based on adjusted quantity
        validated_order.quote_quantity = Some(apply_precision(final_notional, symbol_info.price_precision));
        validated_order.quantity = 0.0; // Market buy orders use quote quantity
    } else {
        // For sell orders, update base quantity
        validated_order.quantity = adjusted_qty;
        validated_order.quote_quantity = None;
    }
    
    Ok(validated_order)
}
