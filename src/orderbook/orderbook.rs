// src/orderbook/orderbook.rs

use std::collections::BTreeMap;
use crate::enums::side::Side;
use ordered_float::OrderedFloat;
use crate::models::level::{ Level, LevelUpdate };

/// Local in-memory orderbook with depth limit and state tracking
#[derive(Debug, Clone)]
pub struct OrderBook {
    /// Sorted bids map: price -> quantity
    pub bids: BTreeMap<OrderedFloat<f64>, f64>,
    /// Sorted asks map: price -> quantity
    pub asks: BTreeMap<OrderedFloat<f64>, f64>,
    /// Last applied update id
    pub last_update_id: u64,
    /// Maximum number of levels to retain per side
    pub max_depth: usize,
    /// Indicates if book has been initialized
    pub valid: bool,
    /// Marks the book as needing a reset
    pub dirty: bool,
    /// Allows trades/incremental updates
    pub trades_allowed: bool,
}

impl OrderBook {
    /// Create a new empty orderbook with a depth limit
    #[inline]
    pub fn new(max_depth: usize) -> Self {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
            max_depth,
            valid: false,
            dirty: false,
            trades_allowed: false,
        }
    }

    /// Initialize or reset the book from a full snapshot
    #[inline]
    pub fn apply_snapshot(&mut self, bids: Vec<Level>, asks: Vec<Level>, update_id: u64) {
        self.bids.clear();
        self.asks.clear();

        // Insert bids (highest first, so we can truncate easily)
        for lvl in bids.into_iter().take(self.max_depth) {
            self.bids.insert(OrderedFloat(lvl.price), lvl.quantity);
        }

        // Insert asks
        for lvl in asks.into_iter().take(self.max_depth) {
            self.asks.insert(OrderedFloat(lvl.price), lvl.quantity);
        }

        self.last_update_id = update_id;
        self.valid = true;
        self.dirty = false;
        self.trades_allowed = true;
    }

    /// Apply a single update with no allocations
    #[inline]
    pub fn apply_single_update(&mut self, side: Side, price: f64, quantity: f64, update_id: u64) {
        if !self.valid || !self.trades_allowed || update_id <= self.last_update_id {
            return;
        }

        let side_map = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };

        let key = OrderedFloat(price);

        if quantity == 0.0 {
            side_map.remove(&key);
        } else {
            side_map.insert(key, quantity);
        }

        self.last_update_id = update_id;

        // Enforce depth limit
        self.enforce_depth_limit(side);
    }

    /// Enforce depth limit for a specific side
    #[inline]
    fn enforce_depth_limit(&mut self, side: Side) {
        match side {
            Side::Bid => {
                // For bids, remove lowest prices if we exceed max depth
                while self.bids.len() > self.max_depth {
                    if let Some(first_key) = self.bids.keys().next().cloned() {
                        self.bids.remove(&first_key);
                    } else {
                        break; // Safety check
                    }
                }
            }
            Side::Ask => {
                // For asks, remove highest prices if we exceed max depth
                while self.asks.len() > self.max_depth {
                    if let Some(last_key) = self.asks.keys().rev().next().cloned() {
                        self.asks.remove(&last_key);
                    } else {
                        break; // Safety check
                    }
                }
            }
        }
    }

    /// Reset book on error or mismatch
    #[inline]
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
        self.trades_allowed = false;
    }

    /// Best (highest) bid - zero allocation
    #[inline]
    pub fn best_bid(&self) -> Option<Level> {
        self.bids
            .iter()
            .next_back()
            .map(|(k, &v)| Level { price: k.into_inner(), quantity: v })
    }

    /// Best (lowest) ask - zero allocation
    #[inline]
    pub fn best_ask(&self) -> Option<Level> {
        self.asks
            .iter()
            .next()
            .map(|(k, &v)| Level { price: k.into_inner(), quantity: v })
    }

    /// Snapshot of top-of-book - zero allocation
    #[inline]
    pub fn snapshot(&self) -> (Option<Level>, Option<Level>) {
        (self.best_bid(), self.best_ask())
    }

    /// Check if the orderbook is valid and usable
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.valid && !self.dirty && self.trades_allowed
    }
}
