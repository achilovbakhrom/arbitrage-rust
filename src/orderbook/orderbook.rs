use std::collections::BTreeMap;
use std::sync::atomic::{ AtomicU64, AtomicU8, Ordering };
use ordered_float::OrderedFloat;
use crate::models::level::Level;
use parking_lot::{Mutex, RwLock};
use tracing::{ debug, warn };
use std::fmt;

/// Current state of the orderbook
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderBookState {
    /// Initial state, needs full snapshot
    Uninitialized = 0,
    /// Snapshot applied, processing diffs
    Synced = 1,
    /// Needs reinitialization due to sequence gap
    OutOfSync = 2,
}

/// Local in-memory orderbook with depth limit and state tracking
pub struct OrderBook {
    /// Sorted bids map: price -> quantity (protected by RwLock)
    pub bids: RwLock<BTreeMap<OrderedFloat<f64>, f64>>,
    /// Sorted asks map: price -> quantity (protected by RwLock)
    pub asks: RwLock<BTreeMap<OrderedFloat<f64>, f64>>,
    /// First update ID in event (used for version tracking)
    pub first_update_id: AtomicU64,
    /// Last applied update id (used for version tracking)
    pub last_update_id: AtomicU64,
    /// Maximum number of levels to retain per side
    pub max_depth: usize,
    /// Current orderbook state
    pub state: AtomicU8,
    /// Mutex for operations that need exclusive access (like reset)
    update_mutex: Mutex<()>,
    /// Buffer for events that arrive before the snapshot
    event_buffer: RwLock<Vec<(u64, u64, Vec<(f64, f64)>, Vec<(f64, f64)>)>>,
}

impl fmt::Debug for OrderBook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderBook")
            .field("bids", &self.bids)
            .field("asks", &self.asks)
            .field("first_update_id", &self.first_update_id)
            .field("last_update_id", &self.last_update_id)
            .field("max_depth", &self.max_depth)
            .field("state", &self.state)
            .field("update_mutex", &self.update_mutex)
            .finish()
    }
}

impl OrderBook {
    /// Create a new empty orderbook with a depth limit
    #[inline]
    pub fn new(max_depth: usize) -> Self {
        OrderBook {
            bids: RwLock::new(BTreeMap::new()),
            asks: RwLock::new(BTreeMap::new()),
            first_update_id: AtomicU64::new(0),
            last_update_id: AtomicU64::new(0),
            max_depth,
            state: AtomicU8::new(OrderBookState::Uninitialized as u8),
            update_mutex: Mutex::new(()),
            event_buffer: RwLock::new(Vec::with_capacity(100)), // Pre-allocate a reasonable buffer
        }
    }

    /// Get current state of the orderbook
    #[inline]
    pub fn get_state(&self) -> OrderBookState {
        match self.state.load(Ordering::Acquire) {
            0 => OrderBookState::Uninitialized,
            1 => OrderBookState::Synced,
            _ => OrderBookState::OutOfSync,
        }
    }

    /// Set the state of the orderbook
    #[inline]
    fn set_state(&self, state: OrderBookState) {
        self.state.store(state as u8, Ordering::Release);
    }

    // Updated src/orderbook/orderbook.rs
    pub fn apply_snapshot(
        &self,
        bids: Vec<Level>,
        asks: Vec<Level>,
        snapshot_last_update_id: u64
    ) {
        // Acquire exclusive lock for the snapshot operation
        let _lock = self.update_mutex.lock();

        // First - check if we already have a valid state (to avoid reprocessing)
        if
            self.get_state() == OrderBookState::Synced &&
            self.last_update_id.load(Ordering::Acquire) >= snapshot_last_update_id
        {
            debug!(
                "Snapshot skipped - already have newer data. Snapshot lastUpdateId: {}, current lastUpdateId: {}",
                snapshot_last_update_id,
                self.last_update_id.load(Ordering::Acquire)
            );
            return;
        }

        debug!("Applying orderbook snapshot with lastUpdateId: {}", snapshot_last_update_id);

        // Reset and update bids
        {
            let mut bids_map = self.bids.write();
            bids_map.clear();

            // Insert bids (highest first for quick access to best bid)
            for lvl in bids.into_iter().take(self.max_depth) {
                if lvl.quantity > 0.0 {
                    bids_map.insert(OrderedFloat(lvl.price), lvl.quantity);
                }
            }
        }

        // Reset and update asks
        {
            let mut asks_map = self.asks.write();
            asks_map.clear();

            // Insert asks (lowest first for quick access to best ask)
            for lvl in asks.into_iter().take(self.max_depth) {
                if lvl.quantity > 0.0 {
                    asks_map.insert(OrderedFloat(lvl.price), lvl.quantity);
                }
            }
        }

        // Update state after successful snapshot application
        self.last_update_id.store(snapshot_last_update_id, Ordering::Release);

        // Get a copy of buffered events before processing
        let buffered_events = {
            let buffer = self.event_buffer.read();
            buffer.clone()
        };

        debug!(
            "Processing {} buffered events after snapshot with lastUpdateId: {}",
            buffered_events.len(),
            snapshot_last_update_id
        );

        // Keep track of whether we processed the first valid event after the snapshot
        let mut first_processed = false;
        let mut last_processed_id = snapshot_last_update_id;

        // Process buffered events
        for (first_id, last_id, bids, asks) in buffered_events {
            // Rule 4: Drop any event where u (last_id) <= lastUpdateId in the snapshot
            if last_id <= snapshot_last_update_id {
                debug!(
                    "Dropping buffered event: last_id={} <= snapshot_last_id={}",
                    last_id,
                    snapshot_last_update_id
                );
                continue;
            }

            // If this is the first event we're processing
            if !first_processed {
                // Rule 5: First event must have U (first_id) <= lastUpdateId AND u (last_id) > lastUpdateId
                if first_id <= snapshot_last_update_id && last_id > snapshot_last_update_id {
                    // Process this event
                    self.process_price_updates(&bids, &asks);
                    last_processed_id = last_id;
                    first_processed = true;
                    debug!(
                        "Processed first valid event: first_id={}, last_id={}",
                        first_id,
                        last_id
                    );
                } else {
                    // If the first event doesn't match criteria, we need to restart
                    warn!(
                        "First event after snapshot doesn't match criteria: first_id={}, last_id={}, snapshot_id={}",
                        first_id,
                        last_id,
                        snapshot_last_update_id
                    );
                    // Keep state as uninitialized - we need a new snapshot
                    return;
                }
            } else {
                // For subsequent events, Rule 6: U (first_id) should be equal to the previous event's u+1
                if first_id != last_processed_id + 1 {
                    warn!(
                        "Gap in update IDs: first_id={} != last_processed_id+1={}",
                        first_id,
                        last_processed_id + 1
                    );
                    // Keep state as uninitialized - we need a new snapshot
                    return;
                }

                // Process this event
                self.process_price_updates(&bids, &asks);
                last_processed_id = last_id;
                debug!("Processed subsequent event: first_id={}, last_id={}", first_id, last_id);
            }
        }

        // Set final state only if everything processed successfully
        if first_processed {
            // Store the final processed update ID
            self.last_update_id.store(last_processed_id, Ordering::Release);

            // Clear the buffer now that we've processed it
            let mut buffer = self.event_buffer.write();
            buffer.clear();

            debug!("All buffered events processed, orderbook synced with lastUpdateId: {}", last_processed_id);
        }

        // Set the state to synced
        self.set_state(OrderBookState::Synced);
    }

    pub fn apply_depth_update(
        &self,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        first_update_id: u64, // 'U' in Binance docs
        last_update_id: u64 // 'u' in Binance docs
    ) -> Result<bool, ()> {
        // If we're not initialized or synced, buffer the event for later processing
        if self.get_state() != OrderBookState::Synced {
            debug!(
                "Book not synced (state={:?}), buffering update: first_id={}, last_id={}",
                self.get_state(),
                first_update_id,
                last_update_id
            );

            // Clone the data to avoid lifetime issues
            let bids_clone = bids.to_vec();
            let asks_clone = asks.to_vec();

            // Buffer the event
            self.buffer_event(first_update_id, last_update_id, bids_clone, asks_clone);
            return Ok(false);
        }

        // We are synced - check if update is still relevant
        let current_last_id = self.last_update_id.load(Ordering::Acquire);

        // Rule 4: Drop any event where u <= lastUpdateId
        if last_update_id <= current_last_id {
            debug!(
                "Dropping outdated event: event_last_id={}, book_last_id={}",
                last_update_id,
                current_last_id
            );
            return Ok(true); // Successfully processed (by dropping)
        }

        // Rule 6: Check if this event continues the sequence (U = previous u + 1)
        if first_update_id != current_last_id + 1 {
            warn!(
                "Gap in update IDs: first_id={} != book_last_id+1={}",
                first_update_id,
                current_last_id + 1
            );
            self.set_state(OrderBookState::OutOfSync);
            return Ok(false);
        }

        // Process the price updates
        self.process_price_updates(bids, asks);

        // Update our last update ID
        self.last_update_id.store(last_update_id, Ordering::Release);

        debug!(
            "Applied update: first_id={}, last_id={}, new_book_last_id={}",
            first_update_id,
            last_update_id,
            last_update_id
        );

        Ok(true)
    }

    pub fn buffer_event(
        &self,
        first_update_id: u64,
        last_update_id: u64,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>
    ) {
        // Only buffer if we're not synced
        if self.get_state() == OrderBookState::Synced {
            return;
        }

        let mut buffer = self.event_buffer.write();

        // Buffer management: limit size to avoid memory issues
        if buffer.len() >= 1000 {
            // Strategy: remove oldest events, but try to maintain sequence
            debug!("Event buffer full (1000 events), removing oldest events");

            // Sort by first_update_id to ensure we remove oldest
            buffer.sort_by_key(|&(first_id, _, _, _)| first_id);

            // Remove oldest 10% to make room
            let to_remove = buffer.len() / 10;
            buffer.drain(0..to_remove);
        }

        // Add the event to the buffer
        buffer.push((first_update_id, last_update_id, bids, asks));

        // Sort by first_update_id to ensure events are in order
        buffer.sort_by_key(|&(first_id, _, _, _)| first_id);

        debug!(
            "Buffered event: first_id={}, last_id={}, buffer_size={}",
            first_update_id,
            last_update_id,
            buffer.len()
        );
    }

    fn process_price_updates(&self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        // Apply bid updates
        {
            let mut bids_map = self.bids.write();
            for &(price, quantity) in bids {
                let key = OrderedFloat(price);

                // Rule 7 & 8: If the quantity is 0, remove the price level
                if quantity == 0.0 {
                    // Remove price level
                    bids_map.remove(&key);
                } else {
                    // Update or add price level
                    bids_map.insert(key, quantity);
                }
            }

            // Enforce depth limit for bids - we want to keep highest bids
            if bids_map.len() > self.max_depth {
                // Sort in ascending order and truncate the first (lowest) elements
                let mut prices: Vec<_> = bids_map.keys().cloned().collect();
                prices.sort(); // Ascending order

                // Remove lowest bids to maintain max depth
                let remove_count = bids_map.len() - self.max_depth;
                for i in 0..remove_count {
                    bids_map.remove(&prices[i]);
                }
            }
        }

        // Apply ask updates
        {
            let mut asks_map = self.asks.write();
            for &(price, quantity) in asks {
                let key = OrderedFloat(price);

                // Rule 7 & 8: If the quantity is 0, remove the price level
                if quantity == 0.0 {
                    // Remove price level
                    asks_map.remove(&key);
                } else {
                    // Update or add price level
                    asks_map.insert(key, quantity);
                }
            }

            // Enforce depth limit for asks - we want to keep lowest asks
            if asks_map.len() > self.max_depth {
                // Sort in descending order and truncate the first (highest) elements
                let mut prices: Vec<_> = asks_map.keys().cloned().collect();
                prices.sort_by(|a, b| b.cmp(a)); // Descending order

                // Remove highest asks to maintain max depth
                let remove_count = asks_map.len() - self.max_depth;
                for i in 0..remove_count {
                    asks_map.remove(&prices[i]);
                }
            }
        }
    }

    /// Best (highest) bid - zero allocation
    pub fn best_bid(&self) -> Option<Level> {
        // Fast check to avoid unnecessary work
        if self.get_state() != OrderBookState::Synced {
            return None;
        }

        let bids = self.bids.read();
        bids.iter()
            .next_back() // Get the highest bid (at the end for BTreeMap)
            .map(|(k, &v)| Level { price: k.into_inner(), quantity: v })
    }

    /// Best (lowest) ask - zero allocation
    pub fn best_ask(&self) -> Option<Level> {
        // Fast check to avoid unnecessary work
        if self.get_state() != OrderBookState::Synced {
            return None;
        }

        let asks = self.asks.read();
        asks.iter()
            .next() // Get the lowest ask (at the beginning for BTreeMap)
            .map(|(k, &v)| Level { price: k.into_inner(), quantity: v })
    }

    /// Snapshot of top-of-book - zero allocation
    pub fn snapshot(&self) -> (Option<Level>, Option<Level>) {
        // Fast check to avoid unnecessary work
        if self.get_state() != OrderBookState::Synced {
            return (None, None);
        }

        let best_bid = self.best_bid();
        let best_ask = self.best_ask();
        (best_bid, best_ask)
    }

    /// Get the mid-price
    pub fn mid_price(&self) -> Option<f64> {
        let (bid, ask) = self.snapshot();
        match (bid, ask) {
            (Some(bid_level), Some(ask_level)) => {
                Some((bid_level.price + ask_level.price) / 2.0)
            }
            _ => None,
        }
    }

    /// Get all bid levels up to max_depth
    pub fn get_bids(&self, depth: Option<usize>) -> Vec<Level> {
        // Fast check to avoid unnecessary work
        if self.get_state() != OrderBookState::Synced {
            return Vec::new();
        }

        let bids = self.bids.read();
        let limit = depth.unwrap_or(self.max_depth).min(bids.len());

        bids.iter()
            .rev() // Reverse to get highest bids first
            .take(limit)
            .map(|(k, &v)| Level { price: k.into_inner(), quantity: v })
            .collect()
    }

    /// Get all ask levels up to max_depth
    pub fn get_asks(&self, depth: Option<usize>) -> Vec<Level> {
        // Fast check to avoid unnecessary work
        if self.get_state() != OrderBookState::Synced {
            return Vec::new();
        }

        let asks = self.asks.read();
        let limit = depth.unwrap_or(self.max_depth).min(asks.len());

        asks.iter()
            .take(limit)
            .map(|(k, &v)| Level { price: k.into_inner(), quantity: v })
            .collect()
    }

    /// Check if the orderbook is synced and usable
    #[inline]
    pub fn is_synced(&self) -> bool {
        self.get_state() == OrderBookState::Synced
    }

    /// Get the current last update ID
    #[inline]
    pub fn get_last_update_id(&self) -> u64 {
        self.last_update_id.load(Ordering::Relaxed)
    }

    /// Calculate the spread (ask - bid)
    pub fn spread(&self) -> Option<f64> {
        let (bid, ask) = self.snapshot();
        match (bid, ask) {
            (Some(bid_level), Some(ask_level)) => { Some(ask_level.price - bid_level.price) }
            _ => None,
        }
    }

    /// Calculate the spread as a percentage of the mid-price
    pub fn spread_percentage(&self) -> Option<f64> {
        let (bid, ask) = self.snapshot();
        match (bid, ask) {
            (Some(bid_level), Some(ask_level)) => {
                let spread = ask_level.price - bid_level.price;
                let mid_price = (ask_level.price + bid_level.price) / 2.0;
                Some((spread / mid_price) * 100.0)
            }
            _ => None,
        }
    }

    /// Check if the orderbook is empty (no bids or asks)
    pub fn is_empty(&self) -> bool {
        let bids = self.bids.read();
        let asks = self.asks.read();
        bids.is_empty() || asks.is_empty()
    }

    /// Calculate total volume at bid side
    pub fn bid_volume(&self) -> f64 {
        let bids = self.bids.read();
        bids.values().sum()
    }

    /// Calculate total volume at ask side
    pub fn ask_volume(&self) -> f64 {
        let asks = self.asks.read();
        asks.values().sum()
    }

    /// Calculate order imbalance (bid volume - ask volume)
    pub fn volume_imbalance(&self) -> f64 {
        let bid_vol = self.bid_volume();
        let ask_vol = self.ask_volume();
        bid_vol - ask_vol
    }
}
