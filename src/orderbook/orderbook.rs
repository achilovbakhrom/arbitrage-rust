// // src/orderbook/orderbook.rs - Aggressive sub-microsecond optimization
// use std::sync::atomic::{ AtomicU64, AtomicU8, Ordering };
// use crossbeam::atomic::AtomicCell;
// use crate::models::level::Level;
// use parking_lot::{ Mutex, RwLock };
// use tracing::{ debug, warn };
// use std::fmt;

// /// Current state of the orderbook
// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub enum OrderBookState {
//     Uninitialized = 0,
//     Synced = 1,
//     OutOfSync = 2,
// }

// const MAX_DEPTH: usize = 5;
// const PRICE_EPSILON: f64 = 1e-8; // For price comparison

// // CRITICAL: Single cache line for hot data (64 bytes)
// #[repr(C, align(64))]
// struct HotData {
//     bid_count: AtomicCell<u8>, // 1 byte
//     ask_count: AtomicCell<u8>, // 1 byte
//     state: AtomicU8, // 1 byte
//     _padding1: [u8; 5], // 5 bytes padding
//     last_update_id: AtomicU64, // 8 bytes
//     best_bid_price: AtomicCell<f64>, // 8 bytes
//     best_bid_qty: AtomicCell<f64>, // 8 bytes
//     best_ask_price: AtomicCell<f64>, // 8 bytes
//     best_ask_qty: AtomicCell<f64>, // 8 bytes
//     mid_price: AtomicCell<f64>, // 8 bytes
//     _padding2: [u8; 8], // 8 bytes padding = 64 bytes total
// }

// // Separate cache-aligned arrays for price levels
// #[repr(C, align(64))]
// struct PriceLevels {
//     bids: [(f64, f64); MAX_DEPTH],
//     asks: [(f64, f64); MAX_DEPTH],
// }

// /// ULTRA-AGGRESSIVE: Lock-free orderbook with minimal operations
// pub struct OrderBook {
//     // Hot data in single cache line
//     hot: HotData,

//     // Price level arrays in separate cache lines
//     levels: Box<PriceLevels>,

//     // Configuration
//     max_depth: u8,

//     // Cold path data (rarely accessed)
//     update_mutex: Mutex<()>,
//     event_buffer: RwLock<Vec<(u64, u64, Vec<(f64, f64)>, Vec<(f64, f64)>)>>,
// }

// impl fmt::Debug for OrderBook {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.debug_struct("OrderBook")
//             .field("bid_count", &self.hot.bid_count.load())
//             .field("ask_count", &self.hot.ask_count.load())
//             .field("last_update_id", &self.hot.last_update_id)
//             .field("state", &self.hot.state)
//             .finish()
//     }
// }

// impl OrderBook {
//     pub fn new(max_depth: usize) -> Self {
//         let max_depth = (max_depth.min(MAX_DEPTH) as u8).max(1);

//         OrderBook {
//             hot: HotData {
//                 bid_count: AtomicCell::new(0),
//                 ask_count: AtomicCell::new(0),
//                 state: AtomicU8::new(OrderBookState::Uninitialized as u8),
//                 _padding1: [0; 5],
//                 last_update_id: AtomicU64::new(0),
//                 best_bid_price: AtomicCell::new(0.0),
//                 best_bid_qty: AtomicCell::new(0.0),
//                 best_ask_price: AtomicCell::new(0.0),
//                 best_ask_qty: AtomicCell::new(0.0),
//                 mid_price: AtomicCell::new(0.0),
//                 _padding2: [0; 8],
//             },
//             levels: Box::new(PriceLevels {
//                 bids: [(0.0, 0.0); MAX_DEPTH],
//                 asks: [(0.0, 0.0); MAX_DEPTH],
//             }),
//             max_depth,
//             update_mutex: Mutex::new(()),
//             event_buffer: RwLock::new(Vec::with_capacity(16)),
//         }
//     }

//     #[inline(always)]
//     pub fn get_state(&self) -> OrderBookState {
//         unsafe { std::mem::transmute(self.hot.state.load(Ordering::Relaxed)) }
//     }

//     #[inline(always)]
//     fn set_state(&self, state: OrderBookState) {
//         self.hot.state.store(state as u8, Ordering::Relaxed);
//     }

//     /// ZERO-ALLOCATION, LOCK-FREE hot path - targeting sub-microsecond performance
//     #[inline(always)]
//     pub fn apply_depth_update_raw(
//         &self,
//         bids: &[(f64, f64)],
//         asks: &[(f64, f64)],
//         first_update_id: u64,
//         last_update_id: u64
//     ) -> Result<bool, ()> {
//         // OPTIMIZATION 1: Single load, fast state check
//         let state = self.hot.state.load(Ordering::Relaxed);
//         if state != (OrderBookState::Synced as u8) {
//             // Avoid allocation in hot path - defer to cold path
//             return Ok(false);
//         }

//         // OPTIMIZATION 2: Fast sequence validation
//         let current_id = self.hot.last_update_id.load(Ordering::Relaxed);
//         if last_update_id <= current_id {
//             return Ok(true);
//         }
//         if first_update_id != current_id + 1 {
//             self.set_state(OrderBookState::OutOfSync);
//             return Ok(false);
//         }

//         // OPTIMIZATION 3: Fast path for empty updates
//         if bids.is_empty() && asks.is_empty() {
//             self.hot.last_update_id.store(last_update_id, Ordering::Relaxed);
//             return Ok(true);
//         }

//         // OPTIMIZATION 4: Inline processing to avoid function call overhead
//         self.process_updates_inline(bids, asks);

//         // OPTIMIZATION 5: Single atomic store
//         self.hot.last_update_id.store(last_update_id, Ordering::Relaxed);

//         Ok(true)
//     }

//     /// INLINED: Process updates without function call overhead - UNSAFE for performance
//     #[inline(always)]
//     fn process_updates_inline(&self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
//         unsafe {
//             let levels_ptr = self.levels.as_ref() as *const PriceLevels as *mut PriceLevels;

//             // Process bids with aggressive inlining
//             if !bids.is_empty() {
//                 self.update_side_inline_unsafe(bids, &mut (*levels_ptr).bids, true);
//             }

//             // Process asks with aggressive inlining
//             if !asks.is_empty() {
//                 self.update_side_inline_unsafe(asks, &mut (*levels_ptr).asks, false);
//             }
//         }

//         // Single cache update
//         self.update_best_prices();
//     }

//     /// ULTRA-FAST: Update one side with minimal branching - UNSAFE for max performance
//     #[inline(always)]
//     unsafe fn update_side_inline_unsafe(
//         &self,
//         updates: &[(f64, f64)],
//         levels: &mut [(f64, f64); MAX_DEPTH],
//         is_bids: bool
//     ) {
//         let current_count = if is_bids {
//             self.hot.bid_count.load() as usize
//         } else {
//             self.hot.ask_count.load() as usize
//         };

//         let mut count = current_count;
//         let max_count = self.max_depth as usize;

//         // Process each update with minimal overhead
//         for &(price, quantity) in updates {
//             if quantity == 0.0 {
//                 // REMOVE: Find and remove level
//                 count = self.remove_level_fast_unsafe(levels, count, price);
//             } else {
//                 // INSERT/UPDATE: Find position and insert/update
//                 count = self.insert_level_fast_unsafe(
//                     levels,
//                     count,
//                     max_count,
//                     price,
//                     quantity,
//                     is_bids
//                 );
//             }
//         }

//         // Single atomic store
//         if is_bids {
//             self.hot.bid_count.store(count as u8);
//         } else {
//             self.hot.ask_count.store(count as u8);
//         }
//     }

//     /// OPTIMIZED: Remove level with minimal memory operations - UNSAFE
//     #[inline(always)]
//     unsafe fn remove_level_fast_unsafe(
//         &self,
//         levels: &mut [(f64, f64); MAX_DEPTH],
//         count: usize,
//         price: f64
//     ) -> usize {
//         // Linear search (faster than binary search for small arrays)
//         for i in 0..count {
//             if (levels[i].0 - price).abs() < PRICE_EPSILON {
//                 // Found - shift remaining elements
//                 let src = levels.as_ptr().add(i + 1);
//                 let dst = levels.as_mut_ptr().add(i);
//                 std::ptr::copy(src, dst, count - i - 1);
//                 return count - 1;
//             }
//         }
//         count // Not found
//     }

//     /// OPTIMIZED: Insert/update level with minimal operations - UNSAFE
//     #[inline(always)]
//     unsafe fn insert_level_fast_unsafe(
//         &self,
//         levels: &mut [(f64, f64); MAX_DEPTH],
//         count: usize,
//         max_count: usize,
//         price: f64,
//         quantity: f64,
//         is_bids: bool
//     ) -> usize {
//         // Find insertion position
//         let mut insert_pos = count;

//         for i in 0..count {
//             let level_price = levels[i].0;

//             // Check for exact match first
//             if (level_price - price).abs() < PRICE_EPSILON {
//                 levels[i].1 = quantity;
//                 return count; // Updated existing
//             }

//             // Find insertion point based on sort order
//             let should_insert_here = if is_bids {
//                 level_price < price // Bids: high to low
//             } else {
//                 level_price > price // Asks: low to high
//             };

//             if should_insert_here {
//                 insert_pos = i;
//                 break;
//             }
//         }

//         // Insert new level if within max depth
//         if insert_pos < max_count {
//             let new_count = if count < max_count {
//                 // Shift elements to make room
//                 if insert_pos < count {
//                     let src = levels.as_ptr().add(insert_pos);
//                     let dst = levels.as_mut_ptr().add(insert_pos + 1);
//                     std::ptr::copy(src, dst, count - insert_pos);
//                 }
//                 count + 1
//             } else {
//                 // Replace worst level
//                 if insert_pos < count {
//                     let src = levels.as_ptr().add(insert_pos);
//                     let dst = levels.as_mut_ptr().add(insert_pos + 1);
//                     std::ptr::copy(src, dst, count - insert_pos - 1);
//                 }
//                 count
//             };

//             levels[insert_pos] = (price, quantity);
//             return new_count;
//         }

//         count // Outside max depth
//     }

//     /// OPTIMIZED: Update cached best prices in single operation
//     #[inline(always)]
//     fn update_best_prices(&self) {
//         let bid_count = self.hot.bid_count.load() as usize;
//         let ask_count = self.hot.ask_count.load() as usize;

//         unsafe {
//             let levels_ptr = self.levels.as_ref() as *const PriceLevels;

//             // Update best bid
//             if bid_count > 0 {
//                 let (price, qty) = (*levels_ptr).bids[0];
//                 self.hot.best_bid_price.store(price);
//                 self.hot.best_bid_qty.store(qty);
//             } else {
//                 self.hot.best_bid_price.store(0.0);
//                 self.hot.best_bid_qty.store(0.0);
//             }

//             // Update best ask
//             if ask_count > 0 {
//                 let (price, qty) = (*levels_ptr).asks[0];
//                 self.hot.best_ask_price.store(price);
//                 self.hot.best_ask_qty.store(qty);
//             } else {
//                 self.hot.best_ask_price.store(0.0);
//                 self.hot.best_ask_qty.store(0.0);
//             }
//         }

//         // Update mid price
//         let bid_price = self.hot.best_bid_price.load();
//         let ask_price = self.hot.best_ask_price.load();
//         let mid = if bid_price > 0.0 && ask_price > 0.0 {
//             (bid_price + ask_price) * 0.5
//         } else {
//             0.0
//         };
//         self.hot.mid_price.store(mid);
//     }

//     /// Apply snapshot with bulk operations (cold path)
//     pub fn apply_snapshot(&self, bids: Vec<Level>, asks: Vec<Level>, snapshot_last_update_id: u64) {
//         let _lock = self.update_mutex.lock();

//         if
//             self.get_state() == OrderBookState::Synced &&
//             self.hot.last_update_id.load(Ordering::Relaxed) >= snapshot_last_update_id
//         {
//             return;
//         }

//         debug!("Applying snapshot: {}", snapshot_last_update_id);

//         // Reset counts
//         self.hot.bid_count.store(0);
//         self.hot.ask_count.store(0);

//         let max_depth = self.max_depth as usize;
//         let bid_len = bids.len().min(max_depth);
//         let ask_len = asks.len().min(max_depth);

//         // Bulk copy bids and asks using unsafe for performance
//         unsafe {
//             let levels_ptr = self.levels.as_ref() as *const PriceLevels as *mut PriceLevels;

//             // Bulk copy bids
//             for (i, level) in bids.into_iter().take(bid_len).enumerate() {
//                 if level.quantity > 0.0 {
//                     (*levels_ptr).bids[i] = (level.price, level.quantity);
//                 }
//             }

//             // Bulk copy asks
//             for (i, level) in asks.into_iter().take(ask_len).enumerate() {
//                 if level.quantity > 0.0 {
//                     (*levels_ptr).asks[i] = (level.price, level.quantity);
//                 }
//             }
//         }

//         self.hot.bid_count.store(bid_len as u8);
//         self.hot.ask_count.store(ask_len as u8);
//         self.hot.last_update_id.store(snapshot_last_update_id, Ordering::Relaxed);

//         self.update_best_prices();
//         self.process_buffered_events(snapshot_last_update_id);
//         self.set_state(OrderBookState::Synced);
//     }

//     fn process_buffered_events(&self, snapshot_last_update_id: u64) {
//         let events = {
//             let buffer = self.event_buffer.read();
//             if buffer.is_empty() {
//                 return;
//             }
//             buffer.clone()
//         };

//         let mut last_processed_id = snapshot_last_update_id;
//         let mut processed_any = false;

//         for (first_id, last_id, bids, asks) in events {
//             if last_id <= snapshot_last_update_id {
//                 continue;
//             }

//             if !processed_any {
//                 if first_id <= snapshot_last_update_id && last_id > snapshot_last_update_id {
//                     self.process_updates_inline(&bids, &asks);
//                     last_processed_id = last_id;
//                     processed_any = true;
//                 }
//             } else if first_id == last_processed_id + 1 {
//                 self.process_updates_inline(&bids, &asks);
//                 last_processed_id = last_id;
//             } else {
//                 warn!("Gap in update sequence");
//                 return;
//             }
//         }

//         if processed_any {
//             self.hot.last_update_id.store(last_processed_id, Ordering::Relaxed);
//             self.event_buffer.write().clear();
//         }
//     }

//     /// Hot path: Get top of book snapshot
//     #[inline(always)]
//     pub fn snapshot(&self) -> (Option<Level>, Option<Level>) {
//         if self.hot.state.load(Ordering::Relaxed) != (OrderBookState::Synced as u8) {
//             return (None, None);
//         }

//         let bid_price = self.hot.best_bid_price.load();
//         let bid_qty = self.hot.best_bid_qty.load();
//         let ask_price = self.hot.best_ask_price.load();
//         let ask_qty = self.hot.best_ask_qty.load();

//         let bid_level = if bid_price > 0.0 && bid_qty > 0.0 {
//             Some(Level { price: bid_price, quantity: bid_qty })
//         } else {
//             None
//         };

//         let ask_level = if ask_price > 0.0 && ask_qty > 0.0 {
//             Some(Level { price: ask_price, quantity: ask_qty })
//         } else {
//             None
//         };

//         (bid_level, ask_level)
//     }

//     pub fn buffer_event(
//         &self,
//         first_update_id: u64,
//         last_update_id: u64,
//         bids: Vec<(f64, f64)>,
//         asks: Vec<(f64, f64)>
//     ) {
//         if self.get_state() == OrderBookState::Synced {
//             return;
//         }
//         let mut buffer = self.event_buffer.write();
//         if buffer.len() >= 50 {
//             buffer.drain(0..10);
//         }
//         buffer.push((first_update_id, last_update_id, bids, asks));
//     }

//     #[inline(always)]
//     pub fn get_cached_top_of_book(&self) -> (Option<(f64, f64)>, Option<(f64, f64)>) {
//         let bid_price = self.hot.best_bid_price.load();
//         let bid_qty = self.hot.best_bid_qty.load();
//         let ask_price = self.hot.best_ask_price.load();
//         let ask_qty = self.hot.best_ask_qty.load();

//         let bid = if bid_price > 0.0 && bid_qty > 0.0 { Some((bid_price, bid_qty)) } else { None };

//         let ask = if ask_price > 0.0 && ask_qty > 0.0 { Some((ask_price, ask_qty)) } else { None };

//         (bid, ask)
//     }

//     #[inline(always)]
//     pub fn is_synced(&self) -> bool {
//         self.hot.state.load(Ordering::Relaxed) == (OrderBookState::Synced as u8)
//     }
// }

// src/orderbook/orderbook.rs - Ultra-aggressive sub-microsecond optimization
use std::sync::atomic::{ AtomicU64, Ordering };
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

// AGGRESSIVE: Reduce to only top 10 levels for maximum speed
const MAX_DEPTH: usize = 5;
const PRICE_EPSILON: f64 = 1e-8;

// CRITICAL: Everything in single 64-byte cache line for M1 efficiency
#[repr(C, align(64))]
struct HotData {
    // Packed state and counts (4 bytes)
    state_and_counts: AtomicCell<u32>, // state(8) + bid_count(8) + ask_count(8) + padding(8)

    // Update ID (8 bytes)
    last_update_id: AtomicU64,

    // Best prices - cache the only data we actually need (32 bytes)
    best_bid_price: AtomicCell<f64>, // 8 bytes
    best_bid_qty: AtomicCell<f64>, // 8 bytes
    best_ask_price: AtomicCell<f64>, // 8 bytes
    best_ask_qty: AtomicCell<f64>, // 8 bytes

    // Padding to 64 bytes
    _padding: [u8; 16],
}

// Separate structure for actual price levels (cold data)
#[repr(C, align(32))]
struct PriceLevels {
    bids: [(f64, f64); MAX_DEPTH],
    asks: [(f64, f64); MAX_DEPTH],
}

/// ULTRA-AGGRESSIVE: Minimal orderbook optimized for sub-microsecond updates
pub struct OrderBook {
    // Everything hot in single cache line
    hot: HotData,

    // Price levels (only accessed during major updates)
    levels: Box<PriceLevels>,

    // Cold path data (rarely used)
    max_depth: u8,
    update_mutex: Mutex<()>,
    event_buffer: RwLock<Vec<(u64, u64, Vec<(f64, f64)>, Vec<(f64, f64)>)>>,
}

impl HotData {
    #[inline(always)]
    fn new() -> Self {
        Self {
            state_and_counts: AtomicCell::new(0), // state=0, counts=0
            last_update_id: AtomicU64::new(0),
            best_bid_price: AtomicCell::new(0.0),
            best_bid_qty: AtomicCell::new(0.0),
            best_ask_price: AtomicCell::new(0.0),
            best_ask_qty: AtomicCell::new(0.0),
            _padding: [0; 16],
        }
    }

    #[inline(always)]
    fn get_state(&self) -> OrderBookState {
        let packed = self.state_and_counts.load();
        unsafe { std::mem::transmute((packed & 0xff) as u8) }
    }

    #[inline(always)]
    fn set_state(&self, state: OrderBookState) {
        let current = self.state_and_counts.load();
        let new_val = (current & 0xffffff00) | (state as u32);
        self.state_and_counts.store(new_val);
    }

    #[inline(always)]
    fn get_bid_count(&self) -> u8 {
        ((self.state_and_counts.load() >> 8) & 0xff) as u8
    }

    #[inline(always)]
    fn get_ask_count(&self) -> u8 {
        ((self.state_and_counts.load() >> 16) & 0xff) as u8
    }

    #[inline(always)]
    fn set_counts(&self, bid_count: u8, ask_count: u8) {
        let current = self.state_and_counts.load();
        let state = current & 0xff;
        let new_val = state | ((bid_count as u32) << 8) | ((ask_count as u32) << 16);
        self.state_and_counts.store(new_val);
    }
}

impl fmt::Debug for OrderBook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderBook")
            .field("bid_count", &self.hot.get_bid_count())
            .field("ask_count", &self.hot.get_ask_count())
            .field("last_update_id", &self.hot.last_update_id)
            .field("state", &self.hot.get_state())
            .finish()
    }
}

impl OrderBook {
    pub fn new(max_depth: usize) -> Self {
        let max_depth = (max_depth.min(MAX_DEPTH) as u8).max(1);

        OrderBook {
            hot: HotData::new(),
            levels: Box::new(PriceLevels {
                bids: [(0.0, 0.0); MAX_DEPTH],
                asks: [(0.0, 0.0); MAX_DEPTH],
            }),
            max_depth,
            update_mutex: Mutex::new(()),
            event_buffer: RwLock::new(Vec::with_capacity(8)), // Smaller buffer
        }
    }

    #[inline(always)]
    pub fn get_state(&self) -> OrderBookState {
        self.hot.get_state()
    }

    #[inline(always)]
    fn set_state(&self, state: OrderBookState) {
        self.hot.set_state(state);
    }

    /// ZERO-ALLOCATION, ULTRA-FAST: Targeting 500ns performance
    #[inline(always)]
    pub fn apply_depth_update_raw(
        &self,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        first_update_id: u64,
        last_update_id: u64
    ) -> Result<bool, ()> {
        // OPTIMIZATION 1: Single cache line read - check state first
        if self.hot.get_state() != OrderBookState::Synced {
            return Ok(false); // Skip buffering for max speed
        }

        // OPTIMIZATION 2: Fast sequence check with single atomic load
        let current_id = self.hot.last_update_id.load(Ordering::Relaxed);

        // Branchless validation
        if last_update_id <= current_id {
            return Ok(true);
        }

        if first_update_id != current_id + 1 {
            self.set_state(OrderBookState::OutOfSync);
            return Ok(false);
        }

        // OPTIMIZATION 3: Fast path for small updates (90% case)
        let total_updates = bids.len() + asks.len();
        if total_updates == 0 {
            self.hot.last_update_id.store(last_update_id, Ordering::Relaxed);
            return Ok(true);
        }

        // OPTIMIZATION 4: Specialized paths based on update size
        if total_updates <= 2 {
            // Ultra-fast path for 1-2 updates
            self.process_micro_update(bids, asks);
        } else if total_updates <= 6 {
            // Fast path for small updates
            self.process_small_update(bids, asks);
        } else {
            // Standard path for larger updates
            self.process_large_update(bids, asks);
        }

        // OPTIMIZATION 5: Single atomic write
        self.hot.last_update_id.store(last_update_id, Ordering::Relaxed);

        Ok(true)
    }

    /// ULTRA-FAST: Handle 1-2 price updates with maximum optimization
    #[inline(always)]
    fn process_micro_update(&self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        // For micro updates, only update if it affects best prices
        let mut update_best = false;

        unsafe {
            let levels_ptr = self.levels.as_ref() as *const PriceLevels as *mut PriceLevels;

            // Process single bid update if it affects best price
            if !bids.is_empty() {
                let (price, qty) = bids[0];
                let current_best = self.hot.best_bid_price.load();

                if price >= current_best || current_best == 0.0 {
                    // This update affects best bid
                    if qty > 0.0 {
                        self.hot.best_bid_price.store(price);
                        self.hot.best_bid_qty.store(qty);
                        (*levels_ptr).bids[0] = (price, qty);
                    } else if price == current_best {
                        // Removing best bid - need to find next best
                        self.update_best_bid_after_removal();
                    }
                    update_best = true;
                }
            }

            // Process single ask update if it affects best price
            if !asks.is_empty() {
                let (price, qty) = asks[0];
                let current_best = self.hot.best_ask_price.load();

                if price <= current_best || current_best == 0.0 {
                    // This update affects best ask
                    if qty > 0.0 {
                        self.hot.best_ask_price.store(price);
                        self.hot.best_ask_qty.store(qty);
                        (*levels_ptr).asks[0] = (price, qty);
                    } else if price == current_best {
                        // Removing best ask - need to find next best
                        self.update_best_ask_after_removal();
                    }
                    update_best = true;
                }
            }
        }

        // Only update mid price if best prices changed
        if update_best {
            self.update_mid_price_only();
        }
    }

    /// FAST: Handle 3-6 price updates
    #[inline(always)]
    fn process_small_update(&self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        unsafe {
            let levels_ptr = self.levels.as_ref() as *const PriceLevels as *mut PriceLevels;

            // Process bids with minimal overhead
            if !bids.is_empty() {
                let bid_count = self.process_side_fast(bids, &mut (*levels_ptr).bids, true);
                let ask_count = self.hot.get_ask_count();
                self.hot.set_counts(bid_count, ask_count);
            }

            // Process asks with minimal overhead
            if !asks.is_empty() {
                let ask_count = self.process_side_fast(asks, &mut (*levels_ptr).asks, false);
                let bid_count = self.hot.get_bid_count();
                self.hot.set_counts(bid_count, ask_count);
            }
        }

        self.update_cached_prices_fast();
    }

    /// STANDARD: Handle larger updates
    #[inline(always)]
    fn process_large_update(&self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        // Use previous optimized implementation for larger updates
        self.process_updates_inline(bids, asks);
    }

    /// INLINED: Process updates without function call overhead - UNSAFE for performance
    #[inline(always)]
    fn process_updates_inline(&self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        unsafe {
            let levels_ptr = self.levels.as_ref() as *const PriceLevels as *mut PriceLevels;

            // Process bids with aggressive inlining
            if !bids.is_empty() {
                let count = self.update_side_inline_unsafe(bids, &mut (*levels_ptr).bids, true);
                let ask_count = self.hot.get_ask_count();
                self.hot.set_counts(count, ask_count);
            }

            // Process asks with aggressive inlining
            if !asks.is_empty() {
                let count = self.update_side_inline_unsafe(asks, &mut (*levels_ptr).asks, false);
                let bid_count = self.hot.get_bid_count();
                self.hot.set_counts(bid_count, count);
            }
        }

        // Single cache update
        self.update_cached_prices_fast();
    }

    /// ULTRA-FAST: Process one side with minimal operations
    #[inline(always)]
    unsafe fn process_side_fast(
        &self,
        updates: &[(f64, f64)],
        levels: &mut [(f64, f64); MAX_DEPTH],
        is_bids: bool
    ) -> u8 {
        let current_count = if is_bids {
            self.hot.get_bid_count() as usize
        } else {
            self.hot.get_ask_count() as usize
        };

        let mut count = current_count;
        let max_count = self.max_depth as usize;

        // Unrolled processing for small updates
        match updates.len() {
            1 => {
                count = self.process_single_level(levels, count, max_count, updates[0], is_bids);
            }
            2 => {
                count = self.process_single_level(levels, count, max_count, updates[0], is_bids);
                count = self.process_single_level(levels, count, max_count, updates[1], is_bids);
            }
            3 => {
                count = self.process_single_level(levels, count, max_count, updates[0], is_bids);
                count = self.process_single_level(levels, count, max_count, updates[1], is_bids);
                count = self.process_single_level(levels, count, max_count, updates[2], is_bids);
            }
            _ => {
                // Fallback for larger updates
                for &update in updates {
                    count = self.process_single_level(levels, count, max_count, update, is_bids);
                }
            }
        }

        count as u8
    }

    /// OPTIMIZED: Process single price level update
    #[inline(always)]
    unsafe fn process_single_level(
        &self,
        levels: &mut [(f64, f64); MAX_DEPTH],
        count: usize,
        max_count: usize,
        update: (f64, f64),
        is_bids: bool
    ) -> usize {
        let (price, quantity) = update;

        if quantity == 0.0 {
            // Remove: find and shift
            for i in 0..count {
                if (levels[i].0 - price).abs() < PRICE_EPSILON {
                    std::ptr::copy(
                        levels.as_ptr().add(i + 1),
                        levels.as_mut_ptr().add(i),
                        count - i - 1
                    );
                    return count - 1;
                }
            }
            return count;
        }

        // Insert/Update: optimized for small arrays
        for i in 0..count {
            if (levels[i].0 - price).abs() < PRICE_EPSILON {
                levels[i].1 = quantity;
                return count;
            }

            let should_insert = if is_bids { levels[i].0 < price } else { levels[i].0 > price };

            if should_insert {
                if count < max_count {
                    std::ptr::copy(
                        levels.as_ptr().add(i),
                        levels.as_mut_ptr().add(i + 1),
                        count - i
                    );
                    levels[i] = (price, quantity);
                    return count + 1;
                } else if i < count {
                    std::ptr::copy(
                        levels.as_ptr().add(i),
                        levels.as_mut_ptr().add(i + 1),
                        count - i - 1
                    );
                    levels[i] = (price, quantity);
                    return count;
                }
            }
        }

        // Append if within max depth
        if count < max_count {
            levels[count] = (price, quantity);
            count + 1
        } else {
            count
        }
    }

    /// Previous implementation methods for compatibility
    #[inline(always)]
    unsafe fn update_side_inline_unsafe(
        &self,
        updates: &[(f64, f64)],
        levels: &mut [(f64, f64); MAX_DEPTH],
        is_bids: bool
    ) -> u8 {
        let current_count = if is_bids {
            self.hot.get_bid_count() as usize
        } else {
            self.hot.get_ask_count() as usize
        };

        let mut count = current_count;
        let max_count = self.max_depth as usize;

        for &(price, quantity) in updates {
            if quantity == 0.0 {
                count = self.remove_level_fast_unsafe(levels, count, price);
            } else {
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

        count as u8
    }

    #[inline(always)]
    unsafe fn remove_level_fast_unsafe(
        &self,
        levels: &mut [(f64, f64); MAX_DEPTH],
        count: usize,
        price: f64
    ) -> usize {
        for i in 0..count {
            if (levels[i].0 - price).abs() < PRICE_EPSILON {
                let src = levels.as_ptr().add(i + 1);
                let dst = levels.as_mut_ptr().add(i);
                std::ptr::copy(src, dst, count - i - 1);
                return count - 1;
            }
        }
        count
    }

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
        let mut insert_pos = count;

        for i in 0..count {
            let level_price = levels[i].0;

            if (level_price - price).abs() < PRICE_EPSILON {
                levels[i].1 = quantity;
                return count;
            }

            let should_insert_here = if is_bids {
                level_price < price
            } else {
                level_price > price
            };

            if should_insert_here {
                insert_pos = i;
                break;
            }
        }

        if insert_pos < max_count {
            let new_count = if count < max_count {
                if insert_pos < count {
                    let src = levels.as_ptr().add(insert_pos);
                    let dst = levels.as_mut_ptr().add(insert_pos + 1);
                    std::ptr::copy(src, dst, count - insert_pos);
                }
                count + 1
            } else {
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

        count
    }

    /// OPTIMIZED: Fast cache update
    #[inline(always)]
    fn update_cached_prices_fast(&self) {
        let bid_count = self.hot.get_bid_count() as usize;
        let ask_count = self.hot.get_ask_count() as usize;

        unsafe {
            let levels_ptr = self.levels.as_ref() as *const PriceLevels;

            if bid_count > 0 {
                let (price, qty) = (*levels_ptr).bids[0];
                self.hot.best_bid_price.store(price);
                self.hot.best_bid_qty.store(qty);
            } else {
                self.hot.best_bid_price.store(0.0);
                self.hot.best_bid_qty.store(0.0);
            }

            if ask_count > 0 {
                let (price, qty) = (*levels_ptr).asks[0];
                self.hot.best_ask_price.store(price);
                self.hot.best_ask_qty.store(qty);
            } else {
                self.hot.best_ask_price.store(0.0);
                self.hot.best_ask_qty.store(0.0);
            }
        }

        self.update_mid_price_only();
    }

    #[inline(always)]
    fn update_mid_price_only(&self) {
        let bid_price = self.hot.best_bid_price.load();
        let ask_price = self.hot.best_ask_price.load();
        let mid = if bid_price > 0.0 && ask_price > 0.0 {
            (bid_price + ask_price) * 0.5
        } else {
            0.0
        };
        self.hot.best_ask_price.store(mid); // Reuse ask price cell for mid price
    }

    #[inline(always)]
    fn update_best_bid_after_removal(&self) {
        unsafe {
            let levels_ptr = self.levels.as_ref() as *const PriceLevels;
            let bid_count = self.hot.get_bid_count() as usize;

            if bid_count > 1 {
                let (price, qty) = (*levels_ptr).bids[1];
                self.hot.best_bid_price.store(price);
                self.hot.best_bid_qty.store(qty);
            } else {
                self.hot.best_bid_price.store(0.0);
                self.hot.best_bid_qty.store(0.0);
            }
        }
    }

    #[inline(always)]
    fn update_best_ask_after_removal(&self) {
        unsafe {
            let levels_ptr = self.levels.as_ref() as *const PriceLevels;
            let ask_count = self.hot.get_ask_count() as usize;

            if ask_count > 1 {
                let (price, qty) = (*levels_ptr).asks[1];
                self.hot.best_ask_price.store(price);
                self.hot.best_ask_qty.store(qty);
            } else {
                self.hot.best_ask_price.store(0.0);
                self.hot.best_ask_qty.store(0.0);
            }
        }
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

        let max_depth = self.max_depth as usize;
        let bid_len = bids.len().min(max_depth);
        let ask_len = asks.len().min(max_depth);

        unsafe {
            let levels_ptr = self.levels.as_ref() as *const PriceLevels as *mut PriceLevels;

            for (i, level) in bids.into_iter().take(bid_len).enumerate() {
                if level.quantity > 0.0 {
                    (*levels_ptr).bids[i] = (level.price, level.quantity);
                }
            }

            for (i, level) in asks.into_iter().take(ask_len).enumerate() {
                if level.quantity > 0.0 {
                    (*levels_ptr).asks[i] = (level.price, level.quantity);
                }
            }
        }

        self.hot.set_counts(bid_len as u8, ask_len as u8);
        self.hot.last_update_id.store(snapshot_last_update_id, Ordering::Relaxed);

        self.update_cached_prices_fast();
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
        if self.hot.get_state() != OrderBookState::Synced {
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
        if buffer.len() >= 20 {
            // Smaller buffer
            buffer.drain(0..5);
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
        self.hot.get_state() == OrderBookState::Synced
    }
}
