// src/orderbook/orderbook.rs - Aggressive sub-microsecond optimization
use std::sync::atomic::{ AtomicU64, AtomicU8, Ordering };
use crossbeam::atomic::AtomicCell;
use crate::models::level::Level;
use parking_lot::{ Mutex, RwLock };
use tracing::{ debug, warn };
use std::fmt;

/// Current state of the orderbook
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderBookState {
    Uninitialized = 0,
    Synced = 1,
    OutOfSync = 2,
}

const MAX_DEPTH: usize = 5;
const PRICE_EPSILON: f64 = 1e-8; // For price comparison

// CRITICAL: Single cache line for hot data (64 bytes)
#[repr(C, align(64))]
struct HotData {
    bid_count: AtomicCell<u8>, // 1 byte
    ask_count: AtomicCell<u8>, // 1 byte
    state: AtomicU8, // 1 byte
    _padding1: [u8; 5], // 5 bytes padding
    last_update_id: AtomicU64, // 8 bytes
    best_bid_price: AtomicCell<f64>, // 8 bytes
    best_bid_qty: AtomicCell<f64>, // 8 bytes
    best_ask_price: AtomicCell<f64>, // 8 bytes
    best_ask_qty: AtomicCell<f64>, // 8 bytes
    mid_price: AtomicCell<f64>, // 8 bytes
    _padding2: [u8; 8], // 8 bytes padding = 64 bytes total
}

// Separate cache-aligned arrays for price levels
#[repr(C, align(64))]
struct PriceLevels {
    bids: [(f64, f64); MAX_DEPTH],
    asks: [(f64, f64); MAX_DEPTH],
}

/// ULTRA-AGGRESSIVE: Lock-free orderbook with minimal operations
pub struct OrderBook {
    // Hot data in single cache line
    hot: HotData,

    // Price level arrays in separate cache lines
    levels: Box<PriceLevels>,

    // Configuration
    max_depth: u8,

    // Cold path data (rarely accessed)
    update_mutex: Mutex<()>,
    event_buffer: RwLock<Vec<(u64, u64, Vec<(f64, f64)>, Vec<(f64, f64)>)>>,
}

impl fmt::Debug for OrderBook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderBook")
            .field("bid_count", &self.hot.bid_count.load())
            .field("ask_count", &self.hot.ask_count.load())
            .field("last_update_id", &self.hot.last_update_id)
            .field("state", &self.hot.state)
            .finish()
    }
}

impl OrderBook {
    pub fn new(max_depth: usize) -> Self {
        let max_depth = (max_depth.min(MAX_DEPTH) as u8).max(1);

        OrderBook {
            hot: HotData {
                bid_count: AtomicCell::new(0),
                ask_count: AtomicCell::new(0),
                state: AtomicU8::new(OrderBookState::Uninitialized as u8),
                _padding1: [0; 5],
                last_update_id: AtomicU64::new(0),
                best_bid_price: AtomicCell::new(0.0),
                best_bid_qty: AtomicCell::new(0.0),
                best_ask_price: AtomicCell::new(0.0),
                best_ask_qty: AtomicCell::new(0.0),
                mid_price: AtomicCell::new(0.0),
                _padding2: [0; 8],
            },
            levels: Box::new(PriceLevels {
                bids: [(0.0, 0.0); MAX_DEPTH],
                asks: [(0.0, 0.0); MAX_DEPTH],
            }),
            max_depth,
            update_mutex: Mutex::new(()),
            event_buffer: RwLock::new(Vec::with_capacity(16)),
        }
    }

    #[inline(always)]
    pub fn get_state(&self) -> OrderBookState {
        unsafe { std::mem::transmute(self.hot.state.load(Ordering::Relaxed)) }
    }

    #[inline(always)]
    fn set_state(&self, state: OrderBookState) {
        self.hot.state.store(state as u8, Ordering::Relaxed);
    }

    /// ZERO-ALLOCATION, LOCK-FREE hot path - targeting sub-microsecond performance
    #[inline(always)]
    pub fn apply_depth_update_raw(
        &self,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        first_update_id: u64,
        last_update_id: u64
    ) -> Result<bool, ()> {
        // OPTIMIZATION 1: Single load, fast state check
        let state = self.hot.state.load(Ordering::Relaxed);
        if state != (OrderBookState::Synced as u8) {
            // Avoid allocation in hot path - defer to cold path
            return Ok(false);
        }

        // OPTIMIZATION 2: Fast sequence validation
        let current_id = self.hot.last_update_id.load(Ordering::Relaxed);
        if last_update_id <= current_id {
            return Ok(true);
        }
        if first_update_id != current_id + 1 {
            self.set_state(OrderBookState::OutOfSync);
            return Ok(false);
        }

        // OPTIMIZATION 3: Fast path for empty updates
        if bids.is_empty() && asks.is_empty() {
            self.hot.last_update_id.store(last_update_id, Ordering::Relaxed);
            return Ok(true);
        }

        // OPTIMIZATION 4: Inline processing to avoid function call overhead
        self.process_updates_inline(bids, asks);

        // OPTIMIZATION 5: Single atomic store
        self.hot.last_update_id.store(last_update_id, Ordering::Relaxed);

        Ok(true)
    }

    /// INLINED: Process updates without function call overhead - UNSAFE for performance
    #[inline(always)]
    fn process_updates_inline(&self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        unsafe {
            let levels_ptr = self.levels.as_ref() as *const PriceLevels as *mut PriceLevels;

            // Process bids with aggressive inlining
            if !bids.is_empty() {
                self.update_side_inline_unsafe(bids, &mut (*levels_ptr).bids, true);
            }

            // Process asks with aggressive inlining
            if !asks.is_empty() {
                self.update_side_inline_unsafe(asks, &mut (*levels_ptr).asks, false);
            }
        }

        // Single cache update
        self.update_best_prices();
    }

    /// ULTRA-FAST: Update one side with minimal branching - UNSAFE for max performance
    #[inline(always)]
    unsafe fn update_side_inline_unsafe(
        &self,
        updates: &[(f64, f64)],
        levels: &mut [(f64, f64); MAX_DEPTH],
        is_bids: bool
    ) {
        let current_count = if is_bids {
            self.hot.bid_count.load() as usize
        } else {
            self.hot.ask_count.load() as usize
        };

        let mut count = current_count;
        let max_count = self.max_depth as usize;

        // Process each update with minimal overhead
        for &(price, quantity) in updates {
            if quantity == 0.0 {
                // REMOVE: Find and remove level
                count = self.remove_level_fast_unsafe(levels, count, price);
            } else {
                // INSERT/UPDATE: Find position and insert/update
                count = self.insert_level_fast_unsafe(
                    levels,
                    count,
                    max_count,
                    price,
                    quantity,
                    is_bids
                );
            }
        }

        // Single atomic store
        if is_bids {
            self.hot.bid_count.store(count as u8);
        } else {
            self.hot.ask_count.store(count as u8);
        }
    }

    /// OPTIMIZED: Remove level with minimal memory operations - UNSAFE
    #[inline(always)]
    unsafe fn remove_level_fast_unsafe(
        &self,
        levels: &mut [(f64, f64); MAX_DEPTH],
        count: usize,
        price: f64
    ) -> usize {
        // Linear search (faster than binary search for small arrays)
        for i in 0..count {
            if (levels[i].0 - price).abs() < PRICE_EPSILON {
                // Found - shift remaining elements
                let src = levels.as_ptr().add(i + 1);
                let dst = levels.as_mut_ptr().add(i);
                std::ptr::copy(src, dst, count - i - 1);
                return count - 1;
            }
        }
        count // Not found
    }

    /// OPTIMIZED: Insert/update level with minimal operations - UNSAFE
    #[inline(always)]
    unsafe fn insert_level_fast_unsafe(
        &self,
        levels: &mut [(f64, f64); MAX_DEPTH],
        count: usize,
        max_count: usize,
        price: f64,
        quantity: f64,
        is_bids: bool
    ) -> usize {
        // Find insertion position
        let mut insert_pos = count;

        for i in 0..count {
            let level_price = levels[i].0;

            // Check for exact match first
            if (level_price - price).abs() < PRICE_EPSILON {
                levels[i].1 = quantity;
                return count; // Updated existing
            }

            // Find insertion point based on sort order
            let should_insert_here = if is_bids {
                level_price < price // Bids: high to low
            } else {
                level_price > price // Asks: low to high
            };

            if should_insert_here {
                insert_pos = i;
                break;
            }
        }

        // Insert new level if within max depth
        if insert_pos < max_count {
            let new_count = if count < max_count {
                // Shift elements to make room
                if insert_pos < count {
                    let src = levels.as_ptr().add(insert_pos);
                    let dst = levels.as_mut_ptr().add(insert_pos + 1);
                    std::ptr::copy(src, dst, count - insert_pos);
                }
                count + 1
            } else {
                // Replace worst level
                if insert_pos < count {
                    let src = levels.as_ptr().add(insert_pos);
                    let dst = levels.as_mut_ptr().add(insert_pos + 1);
                    std::ptr::copy(src, dst, count - insert_pos - 1);
                }
                count
            };

            levels[insert_pos] = (price, quantity);
            return new_count;
        }

        count // Outside max depth
    }

    /// OPTIMIZED: Update cached best prices in single operation
    #[inline(always)]
    fn update_best_prices(&self) {
        let bid_count = self.hot.bid_count.load() as usize;
        let ask_count = self.hot.ask_count.load() as usize;

        unsafe {
            let levels_ptr = self.levels.as_ref() as *const PriceLevels;

            // Update best bid
            if bid_count > 0 {
                let (price, qty) = (*levels_ptr).bids[0];
                self.hot.best_bid_price.store(price);
                self.hot.best_bid_qty.store(qty);
            } else {
                self.hot.best_bid_price.store(0.0);
                self.hot.best_bid_qty.store(0.0);
            }

            // Update best ask
            if ask_count > 0 {
                let (price, qty) = (*levels_ptr).asks[0];
                self.hot.best_ask_price.store(price);
                self.hot.best_ask_qty.store(qty);
            } else {
                self.hot.best_ask_price.store(0.0);
                self.hot.best_ask_qty.store(0.0);
            }
        }

        // Update mid price
        let bid_price = self.hot.best_bid_price.load();
        let ask_price = self.hot.best_ask_price.load();
        let mid = if bid_price > 0.0 && ask_price > 0.0 {
            (bid_price + ask_price) * 0.5
        } else {
            0.0
        };
        self.hot.mid_price.store(mid);
    }

    /// Apply snapshot with bulk operations (cold path)
    pub fn apply_snapshot(&self, bids: Vec<Level>, asks: Vec<Level>, snapshot_last_update_id: u64) {
        let _lock = self.update_mutex.lock();

        if
            self.get_state() == OrderBookState::Synced &&
            self.hot.last_update_id.load(Ordering::Relaxed) >= snapshot_last_update_id
        {
            return;
        }

        debug!("Applying snapshot: {}", snapshot_last_update_id);

        // Reset counts
        self.hot.bid_count.store(0);
        self.hot.ask_count.store(0);

        let max_depth = self.max_depth as usize;
        let bid_len = bids.len().min(max_depth);
        let ask_len = asks.len().min(max_depth);

        // Bulk copy bids and asks using unsafe for performance
        unsafe {
            let levels_ptr = self.levels.as_ref() as *const PriceLevels as *mut PriceLevels;

            // Bulk copy bids
            for (i, level) in bids.into_iter().take(bid_len).enumerate() {
                if level.quantity > 0.0 {
                    (*levels_ptr).bids[i] = (level.price, level.quantity);
                }
            }

            // Bulk copy asks
            for (i, level) in asks.into_iter().take(ask_len).enumerate() {
                if level.quantity > 0.0 {
                    (*levels_ptr).asks[i] = (level.price, level.quantity);
                }
            }
        }

        self.hot.bid_count.store(bid_len as u8);
        self.hot.ask_count.store(ask_len as u8);
        self.hot.last_update_id.store(snapshot_last_update_id, Ordering::Relaxed);

        self.update_best_prices();
        self.process_buffered_events(snapshot_last_update_id);
        self.set_state(OrderBookState::Synced);
    }

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
                    self.process_updates_inline(&bids, &asks);
                    last_processed_id = last_id;
                    processed_any = true;
                }
            } else if first_id == last_processed_id + 1 {
                self.process_updates_inline(&bids, &asks);
                last_processed_id = last_id;
            } else {
                warn!("Gap in update sequence");
                return;
            }
        }

        if processed_any {
            self.hot.last_update_id.store(last_processed_id, Ordering::Relaxed);
            self.event_buffer.write().clear();
        }
    }

    /// Hot path: Get top of book snapshot
    #[inline(always)]
    pub fn snapshot(&self) -> (Option<Level>, Option<Level>) {
        if self.hot.state.load(Ordering::Relaxed) != (OrderBookState::Synced as u8) {
            return (None, None);
        }

        let bid_price = self.hot.best_bid_price.load();
        let bid_qty = self.hot.best_bid_qty.load();
        let ask_price = self.hot.best_ask_price.load();
        let ask_qty = self.hot.best_ask_qty.load();

        let bid_level = if bid_price > 0.0 && bid_qty > 0.0 {
            Some(Level { price: bid_price, quantity: bid_qty })
        } else {
            None
        };

        let ask_level = if ask_price > 0.0 && ask_qty > 0.0 {
            Some(Level { price: ask_price, quantity: ask_qty })
        } else {
            None
        };

        (bid_level, ask_level)
    }

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
        if buffer.len() >= 50 {
            buffer.drain(0..10);
        }
        buffer.push((first_update_id, last_update_id, bids, asks));
    }

    #[inline(always)]
    pub fn get_cached_top_of_book(&self) -> (Option<(f64, f64)>, Option<(f64, f64)>) {
        let bid_price = self.hot.best_bid_price.load();
        let bid_qty = self.hot.best_bid_qty.load();
        let ask_price = self.hot.best_ask_price.load();
        let ask_qty = self.hot.best_ask_qty.load();

        let bid = if bid_price > 0.0 && bid_qty > 0.0 { Some((bid_price, bid_qty)) } else { None };

        let ask = if ask_price > 0.0 && ask_qty > 0.0 { Some((ask_price, ask_qty)) } else { None };

        (bid, ask)
    }

    #[inline(always)]
    pub fn is_synced(&self) -> bool {
        self.hot.state.load(Ordering::Relaxed) == (OrderBookState::Synced as u8)
    }
}
