use std::collections::BTreeMap;
use std::sync::atomic::{ AtomicU64, AtomicU8, Ordering };
use ordered_float::OrderedFloat;
use crate::models::level::Level;
use tokio::sync::{ RwLock, Mutex };
use tracing::{ debug, info };
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

    // Fix for apply_snapshot method in src/orderbook/orderbook.rs

    // Updated src/orderbook/orderbook.rs
    pub async fn apply_snapshot(&self, bids: Vec<Level>, asks: Vec<Level>, last_update_id: u64) {
        // Acquire exclusive lock for the snapshot operation
        let _lock = self.update_mutex.lock().await;

        // Update state to uninitialized while we apply the snapshot
        self.set_state(OrderBookState::Uninitialized);

        // Reset and update bids
        {
            let mut bids_map = self.bids.write().await;
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
            let mut asks_map = self.asks.write().await;
            asks_map.clear();

            // Insert asks (lowest first for quick access to best ask)
            for lvl in asks.into_iter().take(self.max_depth) {
                if lvl.quantity > 0.0 {
                    asks_map.insert(OrderedFloat(lvl.price), lvl.quantity);
                }
            }
        }

        // Update state after successful snapshot application
        // According to Binance docs, we set our lastUpdateId to the snapshot's lastUpdateId
        self.last_update_id.store(last_update_id, Ordering::Release);
        self.set_state(OrderBookState::Synced);

        debug!(
            "Applied full orderbook snapshot with last_update_id: {} {:#?}",
            last_update_id,
            self.get_state()
        );
    }

    pub async fn apply_depth_update(
        &self,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        first_update_id: u64, // 'U' in Binance docs
        last_update_id: u64 // 'u' in Binance docs
    ) -> Result<bool, ()> {
        debug!("apply_depth_update {:?}", self.get_state());
        // Check if we're synced
        if self.get_state() != OrderBookState::Synced {
            debug!("Order book not synced, cannot apply update");
            return Ok(false);
        }

        // Get current lastUpdateId from our book
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

        if current_last_id > first_update_id {
            debug!(
                "Out of sequence event: first_id={}, last_id={}, book_last_id={}",
                first_update_id,
                last_update_id,
                current_last_id
            );
            self.set_state(OrderBookState::OutOfSync);
            return Ok(false);
        }

        // Rule 6: For later updates, each event's U should be equal to previous event's u+1
        // We can't fully implement this logic here as we process events one by one
        // Instead, we just check that the update makes sense with our current state

        // Process the price updates
        self.process_price_updates(bids, asks).await;

        // Update our last update ID
        self.last_update_id.store(last_update_id, Ordering::Release);
        Ok(true)
    }

    async fn process_price_updates(&self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        // Apply bid updates
        {
            let mut bids_map = self.bids.write().await;
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
            let mut asks_map = self.asks.write().await;
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
    pub async fn best_bid(&self) -> Option<Level> {
        // Fast check to avoid unnecessary work
        if self.get_state() != OrderBookState::Synced {
            return None;
        }

        let bids = self.bids.read().await;
        bids.iter()
            .next_back() // Get the highest bid (at the end for BTreeMap)
            .map(|(k, &v)| Level { price: k.into_inner(), quantity: v })
    }

    /// Best (lowest) ask - zero allocation
    pub async fn best_ask(&self) -> Option<Level> {
        // Fast check to avoid unnecessary work
        if self.get_state() != OrderBookState::Synced {
            return None;
        }

        let asks = self.asks.read().await;
        asks.iter()
            .next() // Get the lowest ask (at the beginning for BTreeMap)
            .map(|(k, &v)| Level { price: k.into_inner(), quantity: v })
    }

    /// Snapshot of top-of-book - zero allocation
    pub async fn snapshot(&self) -> (Option<Level>, Option<Level>) {
        // Fast check to avoid unnecessary work
        if self.get_state() != OrderBookState::Synced {
            return (None, None);
        }

        let best_bid = self.best_bid().await;
        let best_ask = self.best_ask().await;
        (best_bid, best_ask)
    }

    /// Get the mid-price
    pub async fn mid_price(&self) -> Option<f64> {
        let (bid, ask) = self.snapshot().await;
        match (bid, ask) {
            (Some(bid_level), Some(ask_level)) => {
                Some((bid_level.price + ask_level.price) / 2.0)
            }
            _ => None,
        }
    }

    /// Get all bid levels up to max_depth
    pub async fn get_bids(&self, depth: Option<usize>) -> Vec<Level> {
        // Fast check to avoid unnecessary work
        if self.get_state() != OrderBookState::Synced {
            return Vec::new();
        }

        let bids = self.bids.read().await;
        let limit = depth.unwrap_or(self.max_depth).min(bids.len());

        bids.iter()
            .rev() // Reverse to get highest bids first
            .take(limit)
            .map(|(k, &v)| Level { price: k.into_inner(), quantity: v })
            .collect()
    }

    /// Get all ask levels up to max_depth
    pub async fn get_asks(&self, depth: Option<usize>) -> Vec<Level> {
        // Fast check to avoid unnecessary work
        if self.get_state() != OrderBookState::Synced {
            return Vec::new();
        }

        let asks = self.asks.read().await;
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
    pub async fn spread(&self) -> Option<f64> {
        let (bid, ask) = self.snapshot().await;
        match (bid, ask) {
            (Some(bid_level), Some(ask_level)) => { Some(ask_level.price - bid_level.price) }
            _ => None,
        }
    }

    /// Calculate the spread as a percentage of the mid-price
    pub async fn spread_percentage(&self) -> Option<f64> {
        let (bid, ask) = self.snapshot().await;
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
    pub async fn is_empty(&self) -> bool {
        let bids = self.bids.read().await;
        let asks = self.asks.read().await;
        bids.is_empty() || asks.is_empty()
    }

    /// Calculate total volume at bid side
    pub async fn bid_volume(&self) -> f64 {
        let bids = self.bids.read().await;
        bids.values().sum()
    }

    /// Calculate total volume at ask side
    pub async fn ask_volume(&self) -> f64 {
        let asks = self.asks.read().await;
        asks.values().sum()
    }

    /// Calculate order imbalance (bid volume - ask volume)
    pub async fn volume_imbalance(&self) -> f64 {
        let bid_vol = self.bid_volume().await;
        let ask_vol = self.ask_volume().await;
        bid_vol - ask_vol
    }
}
