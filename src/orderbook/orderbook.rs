use std::collections::BTreeMap;
use crate::enums::side::Side;
use ordered_float::OrderedFloat;
use crate::models::level::{ Level, LevelUpdate };

/// Local in-memory orderbook with depth limit, version tracking, and state flags
#[derive(Debug)]
pub struct OrderBook {
    /// Sorted bids map: price -> quantity
    pub bids: BTreeMap<OrderedFloat<f64>, f64>,
    /// Sorted asks map: price -> quantity
    pub asks: BTreeMap<OrderedFloat<f64>, f64>,
    /// Last applied update id
    pub last_update_id: u64,
    /// Maximum number of levels to retain per side
    pub max_depth: usize,
    /// Indicates if book has been initialized via snapshot
    pub valid: bool,
    /// Marks the book as needing a full reset
    pub dirty: bool,
    /// Allows trades/incremental updates to be applied
    pub trades_allowed: bool,
}

impl OrderBook {
    /// Create a new empty orderbook with a depth limit
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
    /// Keeps only up to `max_depth` levels
    pub fn apply_snapshot(&mut self, bids: Vec<Level>, asks: Vec<Level>, update_id: u64) {
        self.bids.clear();
        self.asks.clear();
        for lvl in bids.into_iter().take(self.max_depth) {
            self.bids.insert(OrderedFloat(lvl.price), lvl.quantity);
        }
        for lvl in asks.into_iter().take(self.max_depth) {
            self.asks.insert(OrderedFloat(lvl.price), lvl.quantity);
        }
        self.last_update_id = update_id;
        self.valid = true;
        self.dirty = false;
        self.trades_allowed = true;
    }

    /// Apply incremental updates to the book.
    /// Ignores updates with ID <= last_update_id to ensure ordering.
    pub fn apply_update(&mut self, updates: Vec<LevelUpdate>) {
        if !self.valid || !self.trades_allowed {
            return;
        }
        let mut max_id = self.last_update_id;
        for upd in updates {
            if upd.update_id <= self.last_update_id {
                continue;
            }
            let side_map = match upd.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            let key = OrderedFloat(upd.price);
            if upd.quantity == 0.0 {
                side_map.remove(&key);
            } else {
                side_map.insert(key, upd.quantity);
            }
            if upd.update_id > max_id {
                max_id = upd.update_id;
            }
        }
        self.last_update_id = max_id;
        // Enforce depth limit after updates
        while self.bids.len() > self.max_depth {
            // Remove worst bid (lowest price)
            if let Some(first_key) = self.bids.keys().next().cloned() {
                self.bids.remove(&first_key);
            }
        }
        while self.asks.len() > self.max_depth {
            // Remove worst ask (highest price)
            if let Some(last_key) = self.asks.keys().rev().next().cloned() {
                self.asks.remove(&last_key);
            }
        }
    }

    /// Reset book on error or mismatch
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
        self.trades_allowed = false;
    }

    /// Best (highest) bid
    pub fn best_bid(&self) -> Option<Level> {
        self.bids
            .iter()
            .next_back()
            .map(|(k, &v)| Level { price: k.into_inner(), quantity: v })
    }

    /// Best (lowest) ask
    pub fn best_ask(&self) -> Option<Level> {
        self.asks
            .iter()
            .next()
            .map(|(k, &v)| Level { price: k.into_inner(), quantity: v })
    }

    /// Snapshot of top-of-book
    pub fn snapshot(&self) -> (Option<Level>, Option<Level>) {
        (self.best_bid(), self.best_ask())
    }
}
