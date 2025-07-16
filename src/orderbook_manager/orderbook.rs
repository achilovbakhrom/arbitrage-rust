use std::sync::atomic::{ AtomicU64, AtomicU8, Ordering };
use std::cell::UnsafeCell;
use crate::models::level::Level;
use parking_lot::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderBookState {
    Uninitialized = 0,
    Synced = 1,
    OutOfSync = 2,
}

const MAX_DEPTH: usize = 5;
const PRICE_EPSILON: f64 = 1e-8;

#[repr(C, align(128))]
struct HotData {
    state: AtomicU8,
    last_update_id: AtomicU64,
    counts: UnsafeCell<(u8, u8)>,
    best_prices: UnsafeCell<(f64, f64, f64, f64, f64)>,
}

#[repr(C, align(64))]
struct Levels {
    bids: UnsafeCell<[(f64, f64); MAX_DEPTH]>,
    asks: UnsafeCell<[(f64, f64); MAX_DEPTH]>,
}

pub struct OrderBook {
    hot: HotData,
    levels: Box<Levels>,
    max_depth: u8,
    update_mutex: Mutex<()>,
}

unsafe impl Sync for OrderBook {}
unsafe impl Send for OrderBook {}

impl OrderBook {
    pub fn new(max_depth: usize) -> Self {
        let max_depth = (max_depth.min(MAX_DEPTH) as u8).max(1);

        OrderBook {
            hot: HotData {
                state: AtomicU8::new(OrderBookState::Uninitialized as u8),
                last_update_id: AtomicU64::new(0),
                counts: UnsafeCell::new((0, 0)),
                best_prices: UnsafeCell::new((0.0, 0.0, 0.0, 0.0, 0.0)),
            },
            levels: Box::new(Levels {
                bids: UnsafeCell::new([(0.0, 0.0); MAX_DEPTH]),
                asks: UnsafeCell::new([(0.0, 0.0); MAX_DEPTH]),
            }),
            max_depth,
            update_mutex: Mutex::new(()),
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

    #[inline(always)]
    pub fn apply_depth_update_raw(
        &self,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        first_update_id: u64,
        last_update_id: u64
    ) -> Result<bool, ()> {
        let current_state = self.hot.state.load(Ordering::Relaxed);
        if current_state != (OrderBookState::Synced as u8) {
            return Ok(false);
        }

        let current_id = self.hot.last_update_id.load(Ordering::Relaxed);
        if last_update_id <= current_id {
            return Ok(true);
        }
        if first_update_id != current_id + 1 {
            self.set_state(OrderBookState::OutOfSync);
            return Ok(false);
        }

        if bids.is_empty() && asks.is_empty() {
            self.hot.last_update_id.store(last_update_id, Ordering::Relaxed);
            return Ok(true);
        }

        unsafe {
            self.fast_update(bids, asks);
        }

        self.hot.last_update_id.store(last_update_id, Ordering::Relaxed);
        Ok(true)
    }

    #[inline(always)]
    unsafe fn fast_update(&self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        let counts_ptr = self.hot.counts.get();
        let (mut bid_count, mut ask_count) = *counts_ptr;

        let bids_ptr = self.levels.bids.get();
        let asks_ptr = self.levels.asks.get();

        let bids_changed = !bids.is_empty();
        let asks_changed = !asks.is_empty();

        if bids_changed {
            bid_count = self.fast_update_side(bids, &mut *bids_ptr, bid_count, true);
        }

        if asks_changed {
            ask_count = self.fast_update_side(asks, &mut *asks_ptr, ask_count, false);
        }

        *counts_ptr = (bid_count, ask_count);

        if bids_changed || asks_changed {
            self.update_best_prices();
        }
    }

    #[inline(always)]
    unsafe fn fast_update_side(
        &self,
        updates: &[(f64, f64)],
        levels: &mut [(f64, f64); MAX_DEPTH],
        mut current_count: u8,
        is_bids: bool
    ) -> u8 {
        let max_count = self.max_depth;

        for &(price, quantity) in updates {
            if quantity == 0.0 {
                current_count = self.fast_remove(levels, current_count, price);
            } else {
                current_count = self.fast_insert(
                    levels,
                    current_count,
                    max_count,
                    price,
                    quantity,
                    is_bids
                );
            }
        }

        current_count
    }

    #[inline(always)]
    unsafe fn fast_remove(
        &self,
        levels: &mut [(f64, f64); MAX_DEPTH],
        count: u8,
        price: f64
    ) -> u8 {
        let count_usize = count as usize;

        for i in 0..count_usize {
            let level_price = levels.get_unchecked(i).0;

            if (level_price - price).abs() < PRICE_EPSILON {
                if i + 1 < count_usize {
                    let src = levels.as_ptr().add(i + 1);
                    let dst = levels.as_mut_ptr().add(i);
                    std::ptr::copy_nonoverlapping(src, dst, count_usize - i - 1);
                }
                return count - 1;
            }
        }
        count
    }

    #[inline(always)]
    unsafe fn fast_insert(
        &self,
        levels: &mut [(f64, f64); MAX_DEPTH],
        count: u8,
        max_count: u8,
        price: f64,
        quantity: f64,
        is_bids: bool
    ) -> u8 {
        let count_usize = count as usize;
        let max_count_usize = max_count as usize;

        let insert_pos = self.fast_find_insert_pos(levels, count_usize, price, is_bids);

        if insert_pos < count_usize {
            let existing_price = levels.get_unchecked(insert_pos).0;
            if (existing_price - price).abs() < PRICE_EPSILON {
                levels.get_unchecked_mut(insert_pos).1 = quantity;
                return count;
            }
        }

        if insert_pos < max_count_usize {
            let new_count = if count_usize < max_count_usize {
                if insert_pos < count_usize {
                    let src = levels.as_ptr().add(insert_pos);
                    let dst = levels.as_mut_ptr().add(insert_pos + 1);
                    std::ptr::copy_nonoverlapping(src, dst, count_usize - insert_pos);
                }
                count + 1
            } else {
                if insert_pos < count_usize {
                    let src = levels.as_ptr().add(insert_pos);
                    let dst = levels.as_mut_ptr().add(insert_pos + 1);
                    std::ptr::copy_nonoverlapping(src, dst, count_usize - insert_pos - 1);
                }
                count
            };

            *levels.get_unchecked_mut(insert_pos) = (price, quantity);
            return new_count;
        }

        count
    }

    #[inline(always)]
    unsafe fn fast_find_insert_pos(
        &self,
        levels: &[(f64, f64); MAX_DEPTH],
        count: usize,
        price: f64,
        is_bids: bool
    ) -> usize {
        if count == 0 {
            return 0;
        }

        for i in 0..count {
            let level_price = levels.get_unchecked(i).0;
            let should_insert_here = if is_bids {
                level_price < price
            } else {
                level_price > price
            };
            if should_insert_here {
                return i;
            }
        }
        count
    }

    #[inline(always)]
    unsafe fn update_best_prices(&self) {
        let (bid_count, ask_count) = *self.hot.counts.get();
        let bids_ptr = self.levels.bids.get();
        let asks_ptr = self.levels.asks.get();
        let best_prices_ptr = self.hot.best_prices.get();

        let (bid_price, bid_qty) = if bid_count > 0 {
            *(*bids_ptr).get_unchecked(0)
        } else {
            (0.0, 0.0)
        };

        let (ask_price, ask_qty) = if ask_count > 0 {
            *(*asks_ptr).get_unchecked(0)
        } else {
            (0.0, 0.0)
        };

        let mid_price = if bid_price > 0.0 && ask_price > 0.0 {
            (bid_price + ask_price) * 0.5
        } else {
            0.0
        };

        *best_prices_ptr = (bid_price, bid_qty, ask_price, ask_qty, mid_price);
    }

    pub fn apply_snapshot(&self, bids: Vec<Level>, asks: Vec<Level>, snapshot_last_update_id: u64) {
        let _lock = self.update_mutex.lock();

        if
            self.get_state() == OrderBookState::Synced &&
            self.hot.last_update_id.load(Ordering::Relaxed) >= snapshot_last_update_id
        {
            return;
        }

        unsafe {
            let counts_ptr = self.hot.counts.get();
            *counts_ptr = (0, 0);

            let max_depth = self.max_depth as usize;
            let bid_len = bids.len().min(max_depth);
            let ask_len = asks.len().min(max_depth);

            let bids_ptr = self.levels.bids.get();
            let asks_ptr = self.levels.asks.get();

            std::ptr::write_bytes((*bids_ptr).as_mut_ptr(), 0, MAX_DEPTH);
            std::ptr::write_bytes((*asks_ptr).as_mut_ptr(), 0, MAX_DEPTH);

            for (i, level) in bids.into_iter().take(bid_len).enumerate() {
                if level.quantity > 0.0 {
                    (*bids_ptr)[i] = (level.price, level.quantity);
                }
            }

            for (i, level) in asks.into_iter().take(ask_len).enumerate() {
                if level.quantity > 0.0 {
                    (*asks_ptr)[i] = (level.price, level.quantity);
                }
            }

            *counts_ptr = (bid_len as u8, ask_len as u8);
            self.hot.last_update_id.store(snapshot_last_update_id, Ordering::Relaxed);

            self.update_best_prices();
            self.set_state(OrderBookState::Synced);
        }
    }

    #[inline(always)]
    pub fn is_synced(&self) -> bool {
        self.hot.state.load(Ordering::Relaxed) == (OrderBookState::Synced as u8)
    }
}

impl OrderBook {
    #[inline(always)]
    pub fn get_effective_price(&self, volume: f64, is_bid_side: bool) -> Option<f64> {
        if volume <= 0.0 || !self.is_synced() {
            return None;
        }

        unsafe {
            let (bid_count, ask_count) = *self.hot.counts.get();

            if is_bid_side {
                if bid_count == 0 {
                    return None;
                }
                let bids_ptr = self.levels.bids.get();
                self.calculate_vwap(&*bids_ptr, bid_count, volume)
            } else {
                if ask_count == 0 {
                    return None;
                }
                let asks_ptr = self.levels.asks.get();
                self.calculate_vwap(&*asks_ptr, ask_count, volume)
            }
        }
    }

    #[inline(always)]
    unsafe fn calculate_vwap(
        &self,
        levels: &[(f64, f64); MAX_DEPTH],
        count: u8,
        target_volume: f64
    ) -> Option<f64> {
        let mut remaining_volume = target_volume;
        let mut weighted_price_sum = 0.0;
        let mut total_filled = 0.0;

        for i in 0..count as usize {
            let (price, available_qty) = *levels.get_unchecked(i);

            if available_qty <= 0.0 {
                continue;
            }

            let fill_amount = remaining_volume.min(available_qty);
            weighted_price_sum += price * fill_amount;
            total_filled += fill_amount;
            remaining_volume -= fill_amount;

            if remaining_volume <= 0.0 {
                break;
            }
        }

        if total_filled >= target_volume {
            Some(weighted_price_sum / total_filled)
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn has_sufficient_liquidity(&self, volume: f64, is_bid_side: bool) -> bool {
        if volume <= 0.0 || !self.is_synced() {
            return false;
        }

        unsafe {
            let (bid_count, ask_count) = *self.hot.counts.get();

            if is_bid_side {
                if bid_count == 0 {
                    return false;
                }
                let bids_ptr = self.levels.bids.get();
                self.check_liquidity(&*bids_ptr, bid_count, volume)
            } else {
                if ask_count == 0 {
                    return false;
                }
                let asks_ptr = self.levels.asks.get();
                self.check_liquidity(&*asks_ptr, ask_count, volume)
            }
        }
    }

    #[inline(always)]
    unsafe fn check_liquidity(
        &self,
        levels: &[(f64, f64); MAX_DEPTH],
        count: u8,
        target_volume: f64
    ) -> bool {
        let mut available_volume = 0.0;

        for i in 0..count as usize {
            let (_, qty) = *levels.get_unchecked(i);
            available_volume += qty;

            if available_volume >= target_volume {
                return true;
            }
        }

        false
    }
}
