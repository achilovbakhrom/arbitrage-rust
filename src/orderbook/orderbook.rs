use std::sync::atomic::{ AtomicU64, AtomicU8, Ordering };
use crossbeam::atomic::AtomicCell;
use crate::models::level::Level;
use parking_lot::{ Mutex, RwLock };
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

const MAX_DEPTH: usize = 100;

/// Ultra-fast orderbook with lock-free operations where possible
pub struct OrderBook {
    bids: Box<[(f64, f64); MAX_DEPTH]>,
    asks: Box<[(f64, f64); MAX_DEPTH]>,

    bid_count: AtomicCell<u16>,
    ask_count: AtomicCell<u16>,

    /// Version tracking
    pub last_update_id: AtomicU64,

    /// Configuration
    pub max_depth: u16,

    /// State management
    pub state: AtomicU8,
    update_mutex: Mutex<()>,

    /// Hot path caches - most frequently accessed data
    cached_best_bid: AtomicCell<(f64, f64)>,
    cached_best_ask: AtomicCell<(f64, f64)>,
    cached_mid_price: AtomicCell<f64>,

    /// Cold path data - less frequently accessed
    event_buffer: RwLock<Vec<(u64, u64, Vec<(f64, f64)>, Vec<(f64, f64)>)>>,
    pub first_update_id: AtomicU64,
}

impl fmt::Debug for OrderBook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderBook")
            .field("bid_count", &self.bid_count.load())
            .field("ask_count", &self.ask_count.load())
            .field("last_update_id", &self.last_update_id)
            .field("max_depth", &self.max_depth)
            .field("state", &self.state)
            .finish()
    }
}

impl OrderBook {
    /// Create a new empty orderbook with optimized defaults
    #[inline]
    pub fn new(max_depth: usize) -> Self {
        let max_depth = max_depth.min(MAX_DEPTH) as u16;

        // Use Box to heap-allocate large arrays and avoid stack overflow
        let bids = Box::new([(0.0, 0.0); MAX_DEPTH]);
        let asks = Box::new([(0.0, 0.0); MAX_DEPTH]);

        OrderBook {
            bids,
            asks,
            bid_count: AtomicCell::new(0),
            ask_count: AtomicCell::new(0),
            last_update_id: AtomicU64::new(0),
            max_depth,
            state: AtomicU8::new(OrderBookState::Uninitialized as u8),
            update_mutex: Mutex::new(()),
            cached_best_bid: AtomicCell::new((0.0, 0.0)),
            cached_best_ask: AtomicCell::new((0.0, 0.0)),
            cached_mid_price: AtomicCell::new(0.0),
            event_buffer: RwLock::new(Vec::with_capacity(64)), // Smaller initial capacity
            first_update_id: AtomicU64::new(0),
        }
    }

    /// Get current state - optimized for hot path
    #[inline(always)]
    pub fn get_state(&self) -> OrderBookState {
        unsafe {
            // SAFETY: We only store valid enum values
            std::mem::transmute(self.state.load(Ordering::Acquire))
        }
    }

    /// Set the state of the orderbook
    #[inline(always)]
    fn set_state(&self, state: OrderBookState) {
        self.state.store(state as u8, Ordering::Release);
    }

    /// Check if a price level is empty (quantity is 0.0)
    #[inline(always)]
    fn is_empty_level(level: (f64, f64)) -> bool {
        level.1 == 0.0
    }

    /// Ultra-fast bid insertion with branch optimization
    #[inline(always)]
    unsafe fn insert_bid_unchecked(&self, price: f64, quantity: f64) -> bool {
        let bids_ptr = self.bids.as_ptr() as *mut (f64, f64);
        let mut count = self.bid_count.load() as usize;

        if quantity == 0.0 {
            // Remove operation - find and shift
            for i in 0..count {
                let level = bids_ptr.add(i);
                if (*level).0 == price {
                    // Shift remaining elements
                    std::ptr::copy(level.add(1), level, count - i - 1);
                    count -= 1;
                    self.bid_count.store(count as u16);
                    return true;
                }
            }
            return false;
        }

        // Insert/update operation
        let mut insert_pos = count;

        // Find position (bids sorted highest to lowest)
        for i in 0..count {
            let level = bids_ptr.add(i);
            if (*level).0 == price {
                // Update existing
                (*level).1 = quantity;
                return true;
            } else if (*level).0 < price {
                insert_pos = i;
                break;
            }
        }

        // Insert new level if within depth limit
        if insert_pos < (self.max_depth as usize) {
            if count < (self.max_depth as usize) {
                // Shift elements to make room
                if insert_pos < count {
                    std::ptr::copy(
                        bids_ptr.add(insert_pos),
                        bids_ptr.add(insert_pos + 1),
                        count - insert_pos
                    );
                }
                count += 1;
            } else if insert_pos < count {
                // Shift and replace worst level
                std::ptr::copy(
                    bids_ptr.add(insert_pos),
                    bids_ptr.add(insert_pos + 1),
                    count - insert_pos - 1
                );
            } else {
                return false; // Price too low to include
            }

            let level = bids_ptr.add(insert_pos);
            *level = (price, quantity);
            self.bid_count.store(count as u16);
            true
        } else {
            false
        }
    }

    /// Ultra-fast ask insertion with branch optimization
    #[inline(always)]
    unsafe fn insert_ask_unchecked(&self, price: f64, quantity: f64) -> bool {
        let asks_ptr = self.asks.as_ptr() as *mut (f64, f64);
        let mut count = self.ask_count.load() as usize;

        if quantity == 0.0 {
            // Remove operation
            for i in 0..count {
                let level = asks_ptr.add(i);
                if (*level).0 == price {
                    std::ptr::copy(level.add(1), level, count - i - 1);
                    count -= 1;
                    self.ask_count.store(count as u16);
                    return true;
                }
            }
            return false;
        }

        // Insert/update operation
        let mut insert_pos = count;

        // Find position (asks sorted lowest to highest)
        for i in 0..count {
            let level = asks_ptr.add(i);
            if (*level).0 == price {
                // Update existing
                (*level).1 = quantity;
                return true;
            } else if (*level).0 > price {
                insert_pos = i;
                break;
            }
        }

        // Insert new level if within depth limit
        if insert_pos < (self.max_depth as usize) {
            if count < (self.max_depth as usize) {
                if insert_pos < count {
                    std::ptr::copy(
                        asks_ptr.add(insert_pos),
                        asks_ptr.add(insert_pos + 1),
                        count - insert_pos
                    );
                }
                count += 1;
            } else if insert_pos < count {
                std::ptr::copy(
                    asks_ptr.add(insert_pos),
                    asks_ptr.add(insert_pos + 1),
                    count - insert_pos - 1
                );
            } else {
                return false;
            }

            let level = asks_ptr.add(insert_pos);
            *level = (price, quantity);
            self.ask_count.store(count as u16);
            true
        } else {
            false
        }
    }

    /// Update caches after modifications - branchless where possible
    #[inline(always)]
    fn update_caches(&self) {
        let bid_count = self.bid_count.load();
        let ask_count = self.ask_count.load();

        // Update best bid cache
        let best_bid = if bid_count > 0 {
            unsafe { *self.bids.as_ptr() }
        } else {
            (0.0, 0.0)
        };
        self.cached_best_bid.store(best_bid);

        // Update best ask cache
        let best_ask = if ask_count > 0 {
            unsafe { *self.asks.as_ptr() }
        } else {
            (0.0, 0.0)
        };
        self.cached_best_ask.store(best_ask);

        // Update mid price cache
        let mid_price = if !Self::is_empty_level(best_bid) && !Self::is_empty_level(best_ask) {
            (best_bid.0 + best_ask.0) * 0.5
        } else {
            0.0
        };
        self.cached_mid_price.store(mid_price);
    }

    /// Batch process price updates for maximum throughput
    pub fn process_price_updates(&self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        // Process all updates without cache updates
        unsafe {
            for &(price, quantity) in bids {
                self.insert_bid_unchecked(price, quantity);
            }

            for &(price, quantity) in asks {
                self.insert_ask_unchecked(price, quantity);
            }
        }

        // Single cache update at the end
        self.update_caches();
    }

    /// Apply snapshot with optimized bulk operations
    pub fn apply_snapshot(&self, bids: Vec<Level>, asks: Vec<Level>, snapshot_last_update_id: u64) {
        let _lock = self.update_mutex.lock();

        if
            self.get_state() == OrderBookState::Synced &&
            self.last_update_id.load(Ordering::Acquire) >= snapshot_last_update_id
        {
            return;
        }

        debug!("Applying snapshot: {}", snapshot_last_update_id);

        // Reset state
        self.bid_count.store(0);
        self.ask_count.store(0);

        // Bulk copy bids (already sorted highest to lowest)
        let bid_len = bids.len().min(self.max_depth as usize);
        unsafe {
            let bids_ptr = self.bids.as_ptr() as *mut (f64, f64);
            for (i, level) in bids.into_iter().take(bid_len).enumerate() {
                if level.quantity > 0.0 {
                    *bids_ptr.add(i) = (level.price, level.quantity);
                }
            }
        }
        self.bid_count.store(bid_len as u16);

        // Bulk copy asks (already sorted lowest to highest)
        let ask_len = asks.len().min(self.max_depth as usize);
        unsafe {
            let asks_ptr = self.asks.as_ptr() as *mut (f64, f64);
            for (i, level) in asks.into_iter().take(ask_len).enumerate() {
                if level.quantity > 0.0 {
                    *asks_ptr.add(i) = (level.price, level.quantity);
                }
            }
        }
        self.ask_count.store(ask_len as u16);

        self.last_update_id.store(snapshot_last_update_id, Ordering::Release);
        self.update_caches();

        // Process buffered events (simplified)
        self.process_buffered_events(snapshot_last_update_id);
        self.set_state(OrderBookState::Synced);
    }

    /// Streamlined buffered event processing
    fn process_buffered_events(&self, snapshot_last_update_id: u64) {
        let events = {
            let buffer = self.event_buffer.read();
            if buffer.is_empty() {
                return;
            }
            buffer.clone()
        };

        let mut last_processed_id = snapshot_last_update_id;
        let mut processed_any = false;

        for (first_id, last_id, bids, asks) in events {
            if last_id <= snapshot_last_update_id {
                continue;
            }

            if !processed_any {
                if first_id <= snapshot_last_update_id && last_id > snapshot_last_update_id {
                    self.process_price_updates(&bids, &asks);
                    last_processed_id = last_id;
                    processed_any = true;
                }
            } else if first_id == last_processed_id + 1 {
                self.process_price_updates(&bids, &asks);
                last_processed_id = last_id;
            } else {
                warn!("Gap in update sequence");
                return;
            }
        }

        if processed_any {
            self.last_update_id.store(last_processed_id, Ordering::Release);
            self.event_buffer.write().clear();
        }
    }

    /// Hot path: Get best bid (zero allocation, cache-optimized)
    #[inline(always)]
    pub fn best_bid(&self) -> Option<Level> {
        if self.get_state() == OrderBookState::Synced {
            let cached = self.cached_best_bid.load();
            if !Self::is_empty_level(cached) {
                return Some(Level { price: cached.0, quantity: cached.1 });
            }
        }
        None
    }

    /// Hot path: Get best ask (zero allocation, cache-optimized)
    #[inline(always)]
    pub fn best_ask(&self) -> Option<Level> {
        if self.get_state() == OrderBookState::Synced {
            let cached = self.cached_best_ask.load();
            if !Self::is_empty_level(cached) {
                return Some(Level { price: cached.0, quantity: cached.1 });
            }
        }
        None
    }

    /// Hot path: Get top of book snapshot
    #[inline(always)]
    pub fn snapshot(&self) -> (Option<Level>, Option<Level>) {
        if self.get_state() == OrderBookState::Synced {
            let bid = self.cached_best_bid.load();
            let ask = self.cached_best_ask.load();

            let bid_level = if !Self::is_empty_level(bid) {
                Some(Level { price: bid.0, quantity: bid.1 })
            } else {
                None
            };

            let ask_level = if !Self::is_empty_level(ask) {
                Some(Level { price: ask.0, quantity: ask.1 })
            } else {
                None
            };

            (bid_level, ask_level)
        } else {
            (None, None)
        }
    }

    /// Hot path: Get mid price (cached)
    #[inline(always)]
    pub fn mid_price(&self) -> Option<f64> {
        if self.get_state() == OrderBookState::Synced {
            let mid = self.cached_mid_price.load();
            if mid > 0.0 {
                Some(mid)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Hot path: Get spread
    #[inline(always)]
    pub fn spread(&self) -> Option<f64> {
        if self.get_state() == OrderBookState::Synced {
            let bid = self.cached_best_bid.load();
            let ask = self.cached_best_ask.load();

            if !Self::is_empty_level(bid) && !Self::is_empty_level(ask) {
                Some(ask.0 - bid.0)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Apply depth update with optimized fast path
    pub fn apply_depth_update(
        &self,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        first_update_id: u64,
        last_update_id: u64
    ) -> Result<bool, ()> {
        if self.get_state() != OrderBookState::Synced {
            self.buffer_event(first_update_id, last_update_id, bids.to_vec(), asks.to_vec());
            return Ok(false);
        }

        let current_last_id = self.last_update_id.load(Ordering::Acquire);

        if last_update_id <= current_last_id {
            return Ok(true);
        }

        if first_update_id != current_last_id + 1 {
            self.set_state(OrderBookState::OutOfSync);
            return Ok(false);
        }

        self.process_price_updates(bids, asks);
        self.last_update_id.store(last_update_id, Ordering::Release);
        Ok(true)
    }

    // Simplified utility methods
    pub fn buffer_event(
        &self,
        first_update_id: u64,
        last_update_id: u64,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>
    ) {
        if self.get_state() == OrderBookState::Synced {
            return;
        }
        let mut buffer = self.event_buffer.write();
        if buffer.len() >= 500 {
            // Reduced buffer size
            buffer.drain(0..50);
        }
        buffer.push((first_update_id, last_update_id, bids, asks));
    }

    #[inline(always)]
    pub fn get_cached_top_of_book(&self) -> (Option<(f64, f64)>, Option<(f64, f64)>) {
        let bid = self.cached_best_bid.load();
        let ask = self.cached_best_ask.load();

        let bid_tuple = if !Self::is_empty_level(bid) { Some(bid) } else { None };
        let ask_tuple = if !Self::is_empty_level(ask) { Some(ask) } else { None };

        (bid_tuple, ask_tuple)
    }

    // Cold path methods (less optimized)
    pub fn get_bids(&self, depth: Option<usize>) -> Vec<Level> {
        if self.get_state() != OrderBookState::Synced {
            return Vec::new();
        }

        let count = self.bid_count.load() as usize;
        let limit = depth.unwrap_or(self.max_depth as usize).min(count);

        let mut result = Vec::with_capacity(limit);
        unsafe {
            let bids_ptr = self.bids.as_ptr();
            for i in 0..limit {
                let level = *bids_ptr.add(i);
                if !Self::is_empty_level(level) {
                    result.push(Level { price: level.0, quantity: level.1 });
                }
            }
        }
        result
    }

    pub fn get_asks(&self, depth: Option<usize>) -> Vec<Level> {
        if self.get_state() != OrderBookState::Synced {
            return Vec::new();
        }

        let count = self.ask_count.load() as usize;
        let limit = depth.unwrap_or(self.max_depth as usize).min(count);

        let mut result = Vec::with_capacity(limit);
        unsafe {
            let asks_ptr = self.asks.as_ptr();
            for i in 0..limit {
                let level = *asks_ptr.add(i);
                if !Self::is_empty_level(level) {
                    result.push(Level { price: level.0, quantity: level.1 });
                }
            }
        }
        result
    }

    #[inline(always)]
    pub fn is_synced(&self) -> bool {
        self.get_state() == OrderBookState::Synced
    }

    #[inline(always)]
    pub fn get_last_update_id(&self) -> u64 {
        self.last_update_id.load(Ordering::Relaxed)
    }

    pub fn spread_percentage(&self) -> Option<f64> {
        if let Some(mid) = self.mid_price() {
            if let Some(spread_val) = self.spread() {
                Some((spread_val / mid) * 100.0)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn is_empty(&self) -> bool {
        self.bid_count.load() == 0 || self.ask_count.load() == 0
    }

    pub fn bid_volume(&self) -> f64 {
        let count = self.bid_count.load() as usize;
        let mut total = 0.0;
        unsafe {
            let bids_ptr = self.bids.as_ptr();
            for i in 0..count {
                total += (*bids_ptr.add(i)).1; // .1 is quantity
            }
        }
        total
    }

    pub fn ask_volume(&self) -> f64 {
        let count = self.ask_count.load() as usize;
        let mut total = 0.0;
        unsafe {
            let asks_ptr = self.asks.as_ptr();
            for i in 0..count {
                total += (*asks_ptr.add(i)).1; // .1 is quantity
            }
        }
        total
    }

    pub fn volume_imbalance(&self) -> f64 {
        self.bid_volume() - self.ask_volume()
    }
}
