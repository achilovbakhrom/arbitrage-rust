// src/utils/precision.rs
use std::sync::Arc;
use crate::models::symbol::Symbol;
use dashmap::DashMap;

/// Ultra-fast precision handler with cached calculations
pub struct PrecisionHandler {
    // Cache symbol precision data for fast lookups
    symbol_cache: Arc<DashMap<Arc<str>, (u8, u8, Option<f64>, Option<f64>)>>, // (price_prec, qty_prec, min_qty, max_qty)
    // Pre-computed power-of-10 multipliers for common precisions
    multipliers: [f64; 16], // Up to 15 decimal places
}

impl PrecisionHandler {
    pub fn new() -> Self {
        let mut multipliers = [1.0; 16];
        for i in 1..16 {
            multipliers[i] = multipliers[i - 1] * 10.0;
        }

        Self {
            symbol_cache: Arc::new(DashMap::with_capacity(2000)),
            multipliers,
        }
    }

    /// Cache symbol precision data for fast access
    #[inline]
    pub fn cache_symbol(&self, symbol: &Symbol) {
        self.symbol_cache.insert(symbol.symbol.clone(), (
            symbol.price_precision,
            symbol.qty_precision,
            symbol.min_qty,
            symbol.max_qty,
        ));
    }

    /// Ultra-fast price rounding using cached multipliers
    #[inline(always)]
    pub fn round_price(&self, symbol: &Arc<str>, price: f64) -> f64 {
        if let Some(cached) = self.symbol_cache.get(symbol) {
            let precision = cached.0 as usize;
            if precision < self.multipliers.len() {
                let multiplier = unsafe { *self.multipliers.get_unchecked(precision) };
                (price * multiplier).round() / multiplier
            } else {
                price
            }
        } else {
            price
        }
    }

    /// Ultra-fast quantity rounding with min/max validation
    #[inline(always)]
    pub fn round_and_validate_quantity(&self, symbol: &Arc<str>, quantity: f64) -> Option<f64> {
        if let Some(cached) = self.symbol_cache.get(symbol) {
            let (_, qty_precision, min_qty, max_qty) = *cached;

            // Fast precision rounding
            let rounded_qty = if (qty_precision as usize) < self.multipliers.len() {
                let multiplier = unsafe { *self.multipliers.get_unchecked(qty_precision as usize) };
                (quantity * multiplier).round() / multiplier
            } else {
                quantity
            };

            // Fast min/max validation
            if let Some(min) = min_qty {
                if rounded_qty < min {
                    return None;
                }
            }

            if let Some(max) = max_qty {
                if rounded_qty > max {
                    return None;
                }
            }

            Some(rounded_qty)
        } else {
            Some(quantity) // No precision data available
        }
    }

    /// Validate if a quantity meets exchange requirements
    #[inline(always)]
    pub fn is_valid_quantity(&self, symbol: &Arc<str>, quantity: f64) -> bool {
        self.round_and_validate_quantity(symbol, quantity).is_some()
    }

    /// Get symbol precision info (price_prec, qty_prec)
    #[inline(always)]
    pub fn get_precision(&self, symbol: &Arc<str>) -> Option<(u8, u8)> {
        self.symbol_cache.get(symbol).map(|cached| (cached.0, cached.1))
    }

    /// Batch cache multiple symbols for efficiency
    pub fn cache_symbols(&self, symbols: &[Symbol]) {
        for symbol in symbols {
            self.cache_symbol(symbol);
        }
    }
}
