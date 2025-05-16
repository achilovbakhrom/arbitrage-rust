// src/orderbook/manager.rs

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::{ debug, error, info, warn };

use crate::models::level::{ Level, LevelUpdate };
use crate::enums::side::Side;
use super::orderbook::OrderBook;

/// Manages multiple orderbooks for different symbols
#[derive(Debug)]
pub struct OrderBookManager {
    /// Maps symbol to orderbook
    books: RwLock<HashMap<Arc<str>, OrderBook>>,
    /// Default depth to use for new orderbooks
    default_depth: usize,
}

impl OrderBookManager {
    /// Create a new orderbook manager with default depth
    #[inline]
    pub fn new(default_depth: usize) -> Self {
        Self {
            books: RwLock::new(HashMap::with_capacity(100)),
            default_depth,
        }
    }

    /// Get or create an orderbook for a symbol (lock-free read path)
    #[inline]
    pub fn get_book(&self, symbol: &Arc<str>) -> Option<OrderBook> {
        let books = self.books.read();
        books.get(symbol).cloned()
    }

    /// Apply a snapshot to initialize or reset an orderbook
    #[inline]
    pub fn apply_snapshot(
        &self,
        symbol: Arc<str>,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        update_id: u64
    ) {
        let mut books = self.books.write();

        let book = books.entry(symbol.clone()).or_insert_with(|| {
            debug!(?symbol, "Creating new orderbook for snapshot");
            OrderBook::new(self.default_depth)
        });

        // Convert to Level structs only if needed
        let bids_levels: Vec<Level> = bids
            .iter()
            .map(|(price, qty)| Level { price: *price, quantity: *qty })
            .collect();

        let asks_levels: Vec<Level> = asks
            .iter()
            .map(|(price, qty)| Level { price: *price, quantity: *qty })
            .collect();

        book.apply_snapshot(bids_levels, asks_levels, update_id);

        // Log only occasionally to reduce allocation
        if update_id % 100 == 0 {
            debug!(?symbol, update_id, bid_count = bids.len(), ask_count = asks.len(), "Applied orderbook snapshot");
        }
    }

    /// Apply updates to an existing orderbook (no allocation version)
    #[inline]
    pub fn apply_update(
        &self,
        symbol: &Arc<str>,
        side: Side,
        price: f64,
        quantity: f64,
        update_id: u64
    ) {
        let mut books = self.books.write();

        if let Some(book) = books.get_mut(symbol) {
            if !book.valid {
                return;
            }

            book.apply_single_update(side, price, quantity, update_id);
        }
    }

    /// Get the best bid and ask for a symbol (lock-free read)
    #[inline]
    pub fn get_top_of_book(&self, symbol: &Arc<str>) -> Option<(Option<Level>, Option<Level>)> {
        self.get_book(symbol).map(|book| book.snapshot())
    }

    /// Mark an orderbook as needing a refresh
    #[inline]
    pub fn mark_dirty(&self, symbol: &Arc<str>) {
        let mut books = self.books.write();

        if let Some(book) = books.get_mut(symbol) {
            book.mark_dirty();
        }
    }
}
