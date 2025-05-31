use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{ AtomicUsize, AtomicBool, AtomicU64, Ordering };
use std::ptr::NonNull;
use std::hint::unreachable_unchecked;

use dashmap::DashMap;
use std::thread;

use crate::models::triangular_path::TriangularPath;
use crate::orderbook::manager::OrderBookManager;
use crate::orderbook::orderbook::OrderBook;
use crate::arbitrage::executor::ArbitrageExecutor;

// Ultra-compact opportunity for immediate execution
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct FastOpportunity {
    pub path_index: u16,
    pub profit_ratio: f32, // f32 for cache efficiency
    pub execution_time_ns: u32,
    pub liquidity_score: u8, // 0-255 scale
}

/// Minimal arbitrage opportunity for display only
#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub path: Arc<TriangularPath>,
    pub profit_ratio: f64,
    pub start_amount: f64,
    pub end_amount: f64,
    pub execution_time_ns: u64,
    pub min_volume_satisfied: bool,
    pub liquidity_scores: [f64; 3],
}

impl ArbitrageOpportunity {
    #[inline(always)]
    pub fn profit_percentage(&self) -> f64 {
        (self.profit_ratio - 1.0) * 100.0
    }

    #[inline(always)]
    pub fn has_sufficient_liquidity(&self) -> bool {
        self.min_volume_satisfied && self.liquidity_scores.iter().all(|&score| score > 0.5)
    }

    pub fn display(&self) -> String {
        format!(
            "{} → {} → {} | Profit: {:.4}% | Volume: OK | Time: {}ns",
            self.path.first_symbol,
            self.path.second_symbol,
            self.path.third_symbol,
            self.profit_percentage(),
            self.execution_time_ns
        )
    }
}

// Pre-computed path data for zero-allocation hot path
#[repr(C, align(64))] // Cache line aligned
#[derive(Clone, Debug)]
struct PathData {
    // Path references (8 bytes each)
    first_symbol: *const Arc<str>,
    second_symbol: *const Arc<str>,
    third_symbol: *const Arc<str>,

    // Direction flags (1 byte each, packed)
    directions: u8, // bits: [unused:5][third:1][second:1][first:1]

    // Pre-computed values (4 bytes each)
    fee_multiplier_cubed: f32,
    min_threshold: f32,
    volume_multiplier: f32,

    // Padding to 64 bytes for cache alignment
    _padding: [u8; 41],
}

unsafe impl Send for PathData {}
unsafe impl Sync for PathData {}

impl PathData {
    #[inline(always)]
    fn first_is_base_to_quote(&self) -> bool {
        (self.directions & 0b001) != 0
    }

    #[inline(always)]
    fn second_is_base_to_quote(&self) -> bool {
        (self.directions & 0b010) != 0
    }

    #[inline(always)]
    fn third_is_base_to_quote(&self) -> bool {
        (self.directions & 0b100) != 0
    }
}

// Symbol to path indices mapping - optimized for cache efficiency
const MAX_PATHS_PER_SYMBOL: usize = 16;

#[repr(C, align(64))]
struct SymbolPaths {
    count: u8,
    indices: [u16; MAX_PATHS_PER_SYMBOL],
    _padding: [u8; 15], // Align to 64 bytes
}

/// Hyper-optimized arbitrage detector for maximum performance
pub struct ArbitrageDetectorState {
    // Core data (cache-optimized layout)
    pub orderbook_manager: Arc<OrderBookManager>,
    pub all_paths: Arc<Vec<Arc<TriangularPath>>>,

    // Hot path data - pre-computed and cache-aligned
    path_data: Box<[PathData]>, // Heap allocated, cache-aligned array
    symbol_map: Arc<DashMap<Arc<str>, SymbolPaths>>,

    // Execution control - separate cache line
    executor: Arc<ArbitrageExecutor>,
    is_executing: Arc<AtomicBool>,
    execution_counter: Arc<AtomicU64>,

    // Configuration constants
    start_amount: f32,
    min_volume_multiplier: f32,
    min_profit_for_execution: f32, // Immediate execution threshold

    // Performance monitoring
    pub stats_counter: Arc<AtomicUsize>,
    pub profit_counter: Arc<AtomicUsize>,
    last_execution_ns: Arc<AtomicU64>,
}

impl ArbitrageDetectorState {
    /// HYPER-OPTIMIZED: Check single path with zero allocations
    #[inline(always)]
    unsafe fn check_path_zero_alloc(&self, path_idx: u16) -> Option<FastOpportunity> {
        let path_data = self.path_data.get_unchecked(path_idx as usize);

        // Get orderbooks via raw pointer arithmetic for maximum speed
        let books = &self.orderbook_manager.books;

        let first_book = books.get(&**path_data.first_symbol)?;
        let second_book = books.get(&**path_data.second_symbol)?;
        let third_book = books.get(&**path_data.third_symbol)?;

        // Ultra-fast sync check with branch prediction hint
        if unlikely(!first_book.is_synced() || !second_book.is_synced() || !third_book.is_synced()) {
            return None;
        }

        // Get cached prices (single atomic load per book)
        let (first_bid, first_ask) = first_book.get_cached_top_of_book();
        let (second_bid, second_ask) = second_book.get_cached_top_of_book();
        let (third_bid, third_ask) = third_book.get_cached_top_of_book();

        // Branch-free price selection using intrinsics
        let (first_price, first_qty) = if path_data.first_is_base_to_quote() {
            first_bid?
        } else {
            first_ask?
        };

        let (second_price, second_qty) = if path_data.second_is_base_to_quote() {
            second_bid?
        } else {
            second_ask?
        };

        let (third_price, third_qty) = if path_data.third_is_base_to_quote() {
            third_bid?
        } else {
            third_ask?
        };

        // SIMD-style profit calculation using pre-computed values
        let mut amount = self.start_amount;

        // Branchless execution path calculation
        amount = if path_data.first_is_base_to_quote() {
            amount * (first_price as f32)
        } else {
            amount / (first_price as f32)
        };

        amount = if path_data.second_is_base_to_quote() {
            amount * (second_price as f32)
        } else {
            amount / (second_price as f32)
        };

        amount = if path_data.third_is_base_to_quote() {
            amount * (third_price as f32)
        } else {
            amount / (third_price as f32)
        };

        // Apply pre-computed fee multiplier
        amount *= path_data.fee_multiplier_cubed;

        let profit_ratio = amount / self.start_amount;

        // Fast profitability check with branch prediction
        if unlikely(profit_ratio <= path_data.min_threshold) {
            return None;
        }

        // Ultra-fast volume check (approximation for speed)
        let min_vol = self.start_amount * self.min_volume_multiplier;
        let volume_ok =
            (first_qty as f32) >= min_vol &&
            (second_qty as f32) >= min_vol &&
            (third_qty as f32) >= min_vol;

        if unlikely(!volume_ok) {
            return None;
        }

        // Fast liquidity score (simplified for performance)
        let avg_qty = ((first_qty + second_qty + third_qty) as f32) / 3.0;
        let liquidity_score = ((avg_qty / min_vol).min(2.0) * 127.5) as u8;

        Some(FastOpportunity {
            path_index: path_idx,
            profit_ratio,
            execution_time_ns: 0, // Will be filled by caller
            liquidity_score,
        })
    }

    /// IMMEDIATE EXECUTION: Execute opportunity instantly with minimal overhead
    #[inline(always)]
    fn execute_immediately_fast(&self, opportunity: FastOpportunity) -> bool {
        // Single atomic operation to claim execution
        if
            self.is_executing
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
        {
            return false; // Another execution in progress
        }

        // Get path for execution
        let path = &self.all_paths[opportunity.path_index as usize];

        // Create minimal opportunity for executor
        let exec_opportunity = ArbitrageOpportunity {
            path: path.clone(),
            profit_ratio: opportunity.profit_ratio as f64,
            start_amount: self.start_amount as f64,
            end_amount: (self.start_amount as f64) * (opportunity.profit_ratio as f64),
            execution_time_ns: 0,
            min_volume_satisfied: true,
            liquidity_scores: [0.8, 0.8, 0.8], // Simplified for speed
        };

        // Execute synchronously for maximum speed
        let execution_start = Instant::now();
        let result = self.executor.execute(&exec_opportunity);
        let execution_time = execution_start.elapsed().as_nanos() as u64;

        // Update counters
        self.execution_counter.fetch_add(1, Ordering::Relaxed);
        self.last_execution_ns.store(execution_time, Ordering::Relaxed);

        if result.success {
            self.profit_counter.fetch_add(1, Ordering::Relaxed);

            // Minimal success logging (production)
            eprintln!(
                "EXEC: {:.4}% profit in {}μs",
                (opportunity.profit_ratio - 1.0) * 100.0,
                execution_time / 1000
            );
        }

        // Release execution lock
        self.is_executing.store(false, Ordering::Release);

        result.success
    }

    /// ULTRA-FAST: Handle orderbook update with immediate execution
    #[inline]
    pub fn handle_update(&self, symbol: &Arc<str>, _book: &Arc<OrderBook>) {
        // Rate limiting: max 1 check per 10μs globally
        let now_ns = std::time::SystemTime
            ::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let last_exec = self.last_execution_ns.load(Ordering::Relaxed);
        if now_ns.saturating_sub(last_exec) < 10_000 {
            // 10μs
            return;
        }

        // Fast path: Skip if already executing
        if self.is_executing.load(Ordering::Relaxed) {
            return;
        }

        // Get paths for this symbol
        if let Some(symbol_paths) = self.symbol_map.get(symbol) {
            let count = symbol_paths.count as usize;

            // Process paths with immediate execution on first profitable find
            for i in 0..count.min(8) {
                // Limit to 8 paths for latency
                let path_idx = unsafe { *symbol_paths.indices.get_unchecked(i) };

                if let Some(opportunity) = (unsafe { self.check_path_zero_alloc(path_idx) }) {
                    // Immediate execution if profitable enough
                    if opportunity.profit_ratio >= self.min_profit_for_execution {
                        if self.execute_immediately_fast(opportunity) {
                            // Success - exit immediately to avoid processing more
                            self.stats_counter.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    }
                }
            }
        }
    }

    /// Get execution statistics
    pub fn get_execution_stats(&self) -> (u64, u64, u64) {
        (
            self.execution_counter.load(Ordering::Relaxed),
            self.profit_counter.load(Ordering::Relaxed) as u64,
            self.last_execution_ns.load(Ordering::Relaxed),
        )
    }

    pub fn run_stats_task(&self) {
        let stats_counter = self.stats_counter.clone();
        let profit_counter = self.profit_counter.clone();
        let execution_counter = self.execution_counter.clone();

        thread::Builder
            ::new()
            .name("arb-stats".to_string())
            .spawn(move || {
                loop {
                    thread::sleep(std::time::Duration::from_secs(30));
                    let scans = stats_counter.swap(0, Ordering::Relaxed);
                    let profits = profit_counter.load(Ordering::Relaxed);
                    let executions = execution_counter.load(Ordering::Relaxed);

                    if scans > 0 {
                        eprintln!(
                            "Stats: {} opportunities found, {} executions, {} profitable",
                            scans,
                            executions,
                            profits
                        );
                    }
                }
            })
            .expect("Failed to spawn stats thread");
    }
}

// Zero-cost clone
impl Clone for ArbitrageDetectorState {
    fn clone(&self) -> Self {
        Self {
            orderbook_manager: self.orderbook_manager.clone(),
            all_paths: self.all_paths.clone(),
            path_data: self.path_data.clone(),
            symbol_map: self.symbol_map.clone(),
            executor: self.executor.clone(),
            is_executing: self.is_executing.clone(),
            execution_counter: self.execution_counter.clone(),
            start_amount: self.start_amount,
            min_volume_multiplier: self.min_volume_multiplier,
            min_profit_for_execution: self.min_profit_for_execution,
            stats_counter: self.stats_counter.clone(),
            profit_counter: self.profit_counter.clone(),
            last_execution_ns: self.last_execution_ns.clone(),
        }
    }
}

/// Create hyper-optimized detector with immediate execution
pub fn create_ultra_fast_detector_with_volume_validation(
    orderbook_manager: Arc<OrderBookManager>,
    fee_rate: f64,
    min_profit_threshold: f64,
    paths: Vec<Arc<TriangularPath>>,
    start_amount: f64,
    executor: Option<Arc<ArbitrageExecutor>>,
    min_volume_multiplier: f64,
    _volume_depth_check: usize,
    is_perf: bool
) -> Arc<ArbitrageDetectorState> {
    let executor = executor.expect("Executor required for immediate execution");

    // Pre-compute all path data for zero-allocation hot path
    let mut path_data = Vec::with_capacity(paths.len());

    for path in &paths {
        let directions =
            (path.first_is_base_to_quote as u8) |
            ((path.second_is_base_to_quote as u8) << 1) |
            ((path.third_is_base_to_quote as u8) << 2);

        path_data.push(PathData {
            first_symbol: &path.first_symbol as *const Arc<str>,
            second_symbol: &path.second_symbol as *const Arc<str>,
            third_symbol: &path.third_symbol as *const Arc<str>,
            directions,
            fee_multiplier_cubed: ((1.0 - fee_rate) as f32).powi(3),
            min_threshold: (1.0 + min_profit_threshold) as f32,
            volume_multiplier: min_volume_multiplier as f32,
            _padding: [0; 41],
        });
    }

    // Build optimized symbol mapping
    let symbol_map = Arc::new(DashMap::with_capacity(paths.len() * 3));

    for (i, path) in paths.iter().enumerate() {
        let idx = i as u16;

        // Helper to add path to symbol
        let add_path = |symbol: &Arc<str>, idx: u16| {
            symbol_map
                .entry(symbol.clone())
                .and_modify(|entry: &mut SymbolPaths| {
                    if (entry.count as usize) < MAX_PATHS_PER_SYMBOL {
                        entry.indices[entry.count as usize] = idx;
                        entry.count += 1;
                    }
                })
                .or_insert_with(|| {
                    let mut sp = SymbolPaths {
                        count: 1,
                        indices: [0; MAX_PATHS_PER_SYMBOL],
                        _padding: [0; 15],
                    };
                    sp.indices[0] = idx;
                    sp
                });
        };

        add_path(&path.first_symbol, idx);
        add_path(&path.second_symbol, idx);
        add_path(&path.third_symbol, idx);
    }

    let detector_state = Arc::new(ArbitrageDetectorState {
        orderbook_manager: orderbook_manager.clone(),
        all_paths: Arc::new(paths),
        path_data: path_data.into_boxed_slice(),
        symbol_map,
        executor,
        is_executing: Arc::new(AtomicBool::new(false)),
        execution_counter: Arc::new(AtomicU64::new(0)),
        start_amount: start_amount as f32,
        min_volume_multiplier: min_volume_multiplier as f32,
        min_profit_for_execution: (1.0 + min_profit_threshold) as f32,
        stats_counter: Arc::new(AtomicUsize::new(0)),
        profit_counter: Arc::new(AtomicUsize::new(0)),
        last_execution_ns: Arc::new(AtomicU64::new(0)),
    });

    // Register ultra-fast callback
    let callback_state = detector_state.clone();
    orderbook_manager.register_update_callback(
        Arc::new(move |symbol, book| {
            callback_state.handle_update(symbol, book);
        })
    );

    if is_perf {
        detector_state.run_stats_task();
    }

    detector_state
}

/// Convenience function
pub fn create_detector(
    orderbook_manager: Arc<OrderBookManager>,
    fee_rate: f64,
    min_profit_threshold: f64,
    paths: Vec<Arc<TriangularPath>>,
    start_amount: f64,
    executor: Arc<ArbitrageExecutor>,
    min_volume_multiplier: f64,
    volume_depth_check: usize,
    is_perf: bool
) -> Arc<ArbitrageDetectorState> {
    create_ultra_fast_detector_with_volume_validation(
        orderbook_manager,
        fee_rate,
        min_profit_threshold,
        paths,
        start_amount,
        Some(executor),
        min_volume_multiplier,
        volume_depth_check,
        is_perf
    )
}

// Branch prediction hints for stable Rust
#[allow(unused)]
mod intrinsics {
    #[inline(always)]
    pub fn likely(b: bool) -> bool {
        if b { true } else { false }
    }

    #[inline(always)]
    pub fn unlikely(b: bool) -> bool {
        if b { true } else { false }
    }
}

// Re-export for compatibility
pub use intrinsics::*;
