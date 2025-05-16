// src/arbitrage/detector.rs

use std::str::FromStr;
use std::sync::Arc;
use tracing::{ debug, info };
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::cell::RefCell;

use crate::models::triangular_path::TriangularPath;
use crate::models::level::Level;
use crate::orderbook::manager::OrderBookManager;

/// Represents an arbitrage opportunity
#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub path: Arc<TriangularPath>,
    pub profit_ratio: Decimal,
    pub start_amount: Decimal,
    pub end_amount: Decimal,
    pub fee_adjusted: bool,
}

impl ArbitrageOpportunity {
    /// Calculate profit percentage
    #[inline]
    pub fn profit_percentage(&self) -> Decimal {
        (self.profit_ratio - dec!(1.0)) * dec!(100.0)
    }
}

/// Cache-friendly Decimal conversion
#[inline]
fn f64_to_decimal(value: f64) -> Decimal {
    // This is faster than Decimal::from_f64 in hot paths
    Decimal::from_str(&value.to_string()).unwrap_or(Decimal::ZERO)
}

/// Detects arbitrage opportunities from orderbooks
pub struct ArbitrageDetector {
    /// Orderbook manager
    orderbook_manager: Arc<OrderBookManager>,
    /// Fee rate to consider in calculations
    fee_rate: Decimal,
    /// One minus fee rate (cached for performance)
    one_minus_fee: Decimal,
    /// Minimum profit threshold to report
    min_profit_threshold: Decimal,
    /// Cache for opportunities to reduce allocations
    opportunity_cache: RefCell<Vec<ArbitrageOpportunity>>,
}

impl ArbitrageDetector {
    /// Create a new arbitrage detector
    #[inline]
    pub fn new(
        orderbook_manager: Arc<OrderBookManager>,
        fee_rate: Decimal,
        min_profit_threshold: Decimal
    ) -> Self {
        Self {
            orderbook_manager,
            fee_rate,
            one_minus_fee: dec!(1.0) - fee_rate,
            min_profit_threshold,
            opportunity_cache: RefCell::new(Vec::with_capacity(100)),
        }
    }

    /// Check a single triangular path for arbitrage opportunity
    #[inline]
    pub fn check_path(
        &self,
        path: &Arc<TriangularPath>,
        start_amount: Decimal
    ) -> Option<ArbitrageOpportunity> {
        // Get orderbook top of book for each leg
        let first_tob = self.orderbook_manager.get_top_of_book(&path.first_symbol)?;
        let second_tob = self.orderbook_manager.get_top_of_book(&path.second_symbol)?;
        let third_tob = self.orderbook_manager.get_top_of_book(&path.third_symbol)?;

        // Extract prices based on trade direction (with early returns to avoid allocations)
        let first_price = if path.first_is_base_to_quote {
            // Selling base at bid price
            first_tob.0.as_ref().map(|l| f64_to_decimal(l.price))?
        } else {
            // Buying base at ask price
            first_tob.1.as_ref().map(|l| f64_to_decimal(l.price))?
        };

        let second_price = if path.second_is_base_to_quote {
            // Selling base at bid price
            second_tob.0.as_ref().map(|l| f64_to_decimal(l.price))?
        } else {
            // Buying base at ask price
            second_tob.1.as_ref().map(|l| f64_to_decimal(l.price))?
        };

        let third_price = if path.third_is_base_to_quote {
            // Selling base at bid price
            third_tob.0.as_ref().map(|l| f64_to_decimal(l.price))?
        } else {
            // Buying base at ask price
            third_tob.1.as_ref().map(|l| f64_to_decimal(l.price))?
        };

        // Calculate amounts through the path
        let mut amount = start_amount;

        // First leg
        if path.first_is_base_to_quote {
            // Selling base for quote
            amount = amount * first_price;
        } else {
            // Buying base with quote
            amount = amount / first_price;
        }
        // Apply fee
        amount = amount * self.one_minus_fee;

        // Second leg
        if path.second_is_base_to_quote {
            // Selling base for quote
            amount = amount * second_price;
        } else {
            // Buying base with quote
            amount = amount / second_price;
        }
        // Apply fee
        amount = amount * self.one_minus_fee;

        // Third leg
        if path.third_is_base_to_quote {
            // Selling base for quote
            amount = amount * third_price;
        } else {
            // Buying base with quote
            amount = amount / third_price;
        }
        // Apply fee
        amount = amount * self.one_minus_fee;

        // Calculate profit ratio
        let profit_ratio = amount / start_amount;

        // Check if profitable (threshold comparison)
        if profit_ratio > dec!(1.0) + self.min_profit_threshold {
            Some(ArbitrageOpportunity {
                path: Arc::clone(path),
                profit_ratio,
                start_amount,
                end_amount: amount,
                fee_adjusted: true,
            })
        } else {
            None
        }
    }

    /// Scan multiple paths for arbitrage opportunities with minimal allocations
    /// Returns a Vec of opportunities found (owned, not borrowed)
    pub fn scan_paths(
        &self,
        paths: &[Arc<TriangularPath>],
        start_amount: Decimal
    ) -> Vec<ArbitrageOpportunity> {
        let mut opportunities = self.opportunity_cache.borrow_mut();
        opportunities.clear();

        for path in paths {
            if let Some(opportunity) = self.check_path(path, start_amount) {
                // Only log occasionally to reduce allocations
                if opportunities.len() < 3 {
                    debug!(
                        path = ?opportunity.path.first_symbol,
                        profit_pct = %opportunity.profit_percentage(),
                        "Found arbitrage opportunity"
                    );
                }
                opportunities.push(opportunity);
            }
        }

        // Sort by profit ratio descending
        opportunities.sort_by(|a, b| b.profit_ratio.cmp(&a.profit_ratio));

        // Return a clone of the opportunities (avoids the reference issue)
        // This is a bit less efficient but still reuses the same allocation
        opportunities.clone()
    }

    /// Get the top N opportunities from the last scan
    /// This avoids copying the entire vector when you only need a few items
    pub fn top_opportunities(&self, n: usize) -> Vec<ArbitrageOpportunity> {
        let opportunities = self.opportunity_cache.borrow();
        opportunities.iter().take(n).cloned().collect()
    }
}
