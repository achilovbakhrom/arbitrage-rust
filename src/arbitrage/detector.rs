// use std::rc::Rc;
// // src/arbitrage/detector.rs - Updated with FIX integration
// use std::sync::Arc;
// use std::time::Instant;
// use std::sync::atomic::{ AtomicUsize, AtomicBool, AtomicU64, Ordering };

// use dashmap::DashMap;
// use std::thread;

// use crate::models::triangular_path::TriangularPath;
// use crate::orderbook::manager::OrderBookManager;
// use crate::orderbook::orderbook::OrderBook;
// use crate::executor::executor::ArbitrageExecutor;
// use crate::executor::fix_executor::{ FixArbitrageExecutor };

// // Ultra-compact opportunity for immediate execution
// #[repr(C, packed)]
// #[derive(Clone, Copy)]
// pub struct FastOpportunity {
//     pub path_index: u16,
//     pub profit_ratio: f32, // f32 for cache efficiency
//     pub execution_time_ns: u32,
//     pub liquidity_score: u8, // 0-255 scale
// }

// /// Minimal arbitrage opportunity for display only
// #[derive(Debug, Clone)]
// pub struct ArbitrageOpportunity {
//     pub path: Arc<TriangularPath>,
//     pub profit_ratio: f64,
//     pub start_amount: f64,
//     pub end_amount: f64,
//     pub execution_time_ns: u64,
//     pub min_volume_satisfied: bool,
//     pub liquidity_scores: [f64; 3],
// }

// impl ArbitrageOpportunity {
//     #[inline(always)]
//     pub fn profit_percentage(&self) -> f64 {
//         (self.profit_ratio - 1.0) * 100.0
//     }

//     #[inline(always)]
//     pub fn has_sufficient_liquidity(&self) -> bool {
//         self.min_volume_satisfied && self.liquidity_scores.iter().all(|&score| score > 0.5)
//     }

//     pub fn display(&self) -> String {
//         format!(
//             "{} → {} → {} | Profit: {:.4}% | Volume: OK | Time: {}ns",
//             self.path.first_symbol,
//             self.path.second_symbol,
//             self.path.third_symbol,
//             self.profit_percentage(),
//             self.execution_time_ns
//         )
//     }
// }

// // Pre-computed path data for zero-allocation hot path
// #[repr(C, align(64))] // Cache line aligned
// #[derive(Clone, Debug)]
// struct PathData {
//     // Path references (8 bytes each)
//     first_symbol: *const Arc<str>,
//     second_symbol: *const Arc<str>,
//     third_symbol: *const Arc<str>,

//     // Direction flags (1 byte each, packed)
//     directions: u8, // bits: [unused:5][third:1][second:1][first:1]

//     // Pre-computed values (4 bytes each)
//     fee_multiplier_cubed: f32,
//     min_threshold: f32,
//     volume_multiplier: f32,

//     // Padding to 64 bytes for cache alignment
//     _padding: [u8; 41],
// }

// unsafe impl Send for PathData {}
// unsafe impl Sync for PathData {}

// impl PathData {
//     #[inline(always)]
//     fn first_is_base_to_quote(&self) -> bool {
//         (self.directions & 0b001) != 0
//     }

//     #[inline(always)]
//     fn second_is_base_to_quote(&self) -> bool {
//         (self.directions & 0b010) != 0
//     }

//     #[inline(always)]
//     fn third_is_base_to_quote(&self) -> bool {
//         (self.directions & 0b100) != 0
//     }
// }

// // Symbol to path indices mapping - optimized for cache efficiency
// const MAX_PATHS_PER_SYMBOL: usize = 16;

// #[repr(C, align(64))]
// struct SymbolPaths {
//     count: u8,
//     indices: [u16; MAX_PATHS_PER_SYMBOL],
//     _padding: [u8; 15], // Align to 64 bytes
// }

// /// Hyper-optimized arbitrage detector for maximum performance with FIX execution
// pub struct ArbitrageDetectorState {
//     // Core data (cache-optimized layout)
//     pub orderbook_manager: Arc<OrderBookManager>,
//     pub all_paths: Arc<Vec<Arc<TriangularPath>>>,

//     // Hot path data - pre-computed and cache-aligned
//     path_data: Box<[PathData]>, // Heap allocated, cache-aligned array
//     symbol_map: Arc<DashMap<Arc<str>, SymbolPaths>>,

//     // Execution control - separate cache line
//     fallback_executor: Arc<ArbitrageExecutor>, // For simulation mode compatibility
//     fix_executor: Option<Arc<FixArbitrageExecutor>>, // Real/mock FIX executor
//     is_executing: Arc<AtomicBool>,
//     execution_counter: Arc<AtomicU64>,

//     // Configuration constants
//     start_amount: f32,
//     min_volume_multiplier: f32,
//     min_profit_for_execution: f32, // Immediate execution threshold
//     use_fix_execution: bool, // Whether to use FIX API or fallback executor

//     // Performance monitoring
//     pub stats_counter: Arc<AtomicUsize>,
//     pub profit_counter: Arc<AtomicUsize>,
//     last_execution_ns: Arc<AtomicU64>,
// }

// impl ArbitrageDetectorState {
//     /// Create a new detector with FIX execution support
//     pub fn new_with_fix_executor(
//         orderbook_manager: Arc<OrderBookManager>,
//         paths: Vec<Arc<TriangularPath>>,
//         fee_rate: f64,
//         min_profit_threshold: f64,
//         start_amount: f64,
//         min_volume_multiplier: f64,
//         fallback_executor: Arc<ArbitrageExecutor>,
//         fix_executor: Option<Arc<FixArbitrageExecutor>>
//     ) -> Arc<Self> {
//         // Pre-compute all path data for zero-allocation hot path
//         let mut path_data = Vec::with_capacity(paths.len());

//         for path in &paths {
//             let directions =
//                 (path.first_is_base_to_quote as u8) |
//                 ((path.second_is_base_to_quote as u8) << 1) |
//                 ((path.third_is_base_to_quote as u8) << 2);

//             path_data.push(PathData {
//                 first_symbol: &path.first_symbol as *const Arc<str>,
//                 second_symbol: &path.second_symbol as *const Arc<str>,
//                 third_symbol: &path.third_symbol as *const Arc<str>,
//                 directions,
//                 fee_multiplier_cubed: ((1.0 - fee_rate) as f32).powi(3),
//                 min_threshold: (1.0 + min_profit_threshold) as f32,
//                 volume_multiplier: min_volume_multiplier as f32,
//                 _padding: [0; 41],
//             });
//         }

//         // Build optimized symbol mapping
//         let symbol_map = Arc::new(DashMap::with_capacity(paths.len() * 3));

//         for (i, path) in paths.iter().enumerate() {
//             let idx = i as u16;

//             // Helper to add path to symbol
//             let add_path = |symbol: &Arc<str>, idx: u16| {
//                 symbol_map
//                     .entry(symbol.clone())
//                     .and_modify(|entry: &mut SymbolPaths| {
//                         if (entry.count as usize) < MAX_PATHS_PER_SYMBOL {
//                             entry.indices[entry.count as usize] = idx;
//                             entry.count += 1;
//                         }
//                     })
//                     .or_insert_with(|| {
//                         let mut sp = SymbolPaths {
//                             count: 1,
//                             indices: [0; MAX_PATHS_PER_SYMBOL],
//                             _padding: [0; 15],
//                         };
//                         sp.indices[0] = idx;
//                         sp
//                     });
//             };

//             add_path(&path.first_symbol, idx);
//             add_path(&path.second_symbol, idx);
//             add_path(&path.third_symbol, idx);
//         }

//         let use_fix_execution = fix_executor.is_some();

//         let detector_state = Arc::new(ArbitrageDetectorState {
//             orderbook_manager: orderbook_manager.clone(),
//             all_paths: Arc::new(paths),
//             path_data: path_data.into_boxed_slice(),
//             symbol_map,
//             fallback_executor,
//             fix_executor,
//             is_executing: Arc::new(AtomicBool::new(false)),
//             execution_counter: Arc::new(AtomicU64::new(0)),
//             start_amount: start_amount as f32,
//             min_volume_multiplier: min_volume_multiplier as f32,
//             min_profit_for_execution: (1.0 + min_profit_threshold) as f32,
//             use_fix_execution,
//             stats_counter: Arc::new(AtomicUsize::new(0)),
//             profit_counter: Arc::new(AtomicUsize::new(0)),
//             last_execution_ns: Arc::new(AtomicU64::new(0)),
//         });

//         // Register ultra-fast callback
//         let callback_state = detector_state.clone();
//         orderbook_manager.register_update_callback(
//             Arc::new(move |symbol, book| {
//                 callback_state.handle_update(symbol, book);
//             })
//         );

//         detector_state
//     }

//     /// HYPER-OPTIMIZED: Check single path with zero allocations
//     #[inline(always)]
//     unsafe fn check_path_zero_alloc(&self, path_idx: u16) -> Option<FastOpportunity> {
//         let path_data = self.path_data.get_unchecked(path_idx as usize);

//         // Get orderbooks via raw pointer arithmetic for maximum speed
//         let books = &self.orderbook_manager.books;

//         let first_book = books.get(&**path_data.first_symbol)?;
//         let second_book = books.get(&**path_data.second_symbol)?;
//         let third_book = books.get(&**path_data.third_symbol)?;

//         // Ultra-fast sync check with branch prediction hint
//         if unlikely(!first_book.is_synced() || !second_book.is_synced() || !third_book.is_synced()) {
//             return None;
//         }

//         // Get cached prices (single atomic load per book)
//         let (first_bid, first_ask) = first_book.get_cached_top_of_book();
//         let (second_bid, second_ask) = second_book.get_cached_top_of_book();
//         let (third_bid, third_ask) = third_book.get_cached_top_of_book();

//         // Branch-free price selection using intrinsics
//         let (first_price, first_qty) = if path_data.first_is_base_to_quote() {
//             first_bid?
//         } else {
//             first_ask?
//         };

//         let (second_price, second_qty) = if path_data.second_is_base_to_quote() {
//             second_bid?
//         } else {
//             second_ask?
//         };

//         let (third_price, third_qty) = if path_data.third_is_base_to_quote() {
//             third_bid?
//         } else {
//             third_ask?
//         };

//         // SIMD-style profit calculation using pre-computed values
//         let mut amount = self.start_amount;

//         // Branchless execution path calculation
//         amount = if path_data.first_is_base_to_quote() {
//             amount * (first_price as f32)
//         } else {
//             amount / (first_price as f32)
//         };

//         amount = if path_data.second_is_base_to_quote() {
//             amount * (second_price as f32)
//         } else {
//             amount / (second_price as f32)
//         };

//         amount = if path_data.third_is_base_to_quote() {
//             amount * (third_price as f32)
//         } else {
//             amount / (third_price as f32)
//         };

//         // Apply pre-computed fee multiplier
//         amount *= path_data.fee_multiplier_cubed;

//         let profit_ratio = amount / self.start_amount;

//         // Fast profitability check with branch prediction
//         if unlikely(profit_ratio <= path_data.min_threshold) {
//             return None;
//         }

//         // Ultra-fast volume check (approximation for speed)
//         let min_vol = self.start_amount * self.min_volume_multiplier;
//         let volume_ok =
//             (first_qty as f32) >= min_vol &&
//             (second_qty as f32) >= min_vol &&
//             (third_qty as f32) >= min_vol;

//         if unlikely(!volume_ok) {
//             return None;
//         }

//         // Fast liquidity score (simplified for performance)
//         let avg_qty = ((first_qty + second_qty + third_qty) as f32) / 3.0;
//         let liquidity_score = ((avg_qty / min_vol).min(2.0) * 127.5) as u8;

//         Some(FastOpportunity {
//             path_index: path_idx,
//             profit_ratio,
//             execution_time_ns: 0, // Will be filled by caller
//             liquidity_score,
//         })
//     }

//     /// IMMEDIATE EXECUTION: Execute opportunity instantly with FIX API or fallback
//     #[inline(always)]
//     fn execute_immediately_fast(&self, opportunity: FastOpportunity) -> bool {
//         // Single atomic operation to claim execution
//         if
//             self.is_executing
//                 .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
//                 .is_err()
//         {
//             return false; // Another execution in progress
//         }

//         // Get path for execution
//         let path = &self.all_paths[opportunity.path_index as usize];

//         // Create minimal opportunity for executor
//         let exec_opportunity = ArbitrageOpportunity {
//             path: path.clone(),
//             profit_ratio: opportunity.profit_ratio as f64,
//             start_amount: self.start_amount as f64,
//             end_amount: (self.start_amount as f64) * (opportunity.profit_ratio as f64),
//             execution_time_ns: 0,
//             min_volume_satisfied: true,
//             liquidity_scores: [0.8, 0.8, 0.8], // Simplified for speed
//         };

//         let execution_start = Instant::now();
//         let success = if self.use_fix_execution {
//             // Use FIX executor for real/mock trading
//             if let Some(fix_executor) = &self.fix_executor {
//                 let result = fix_executor.execute_triangular_arbitrage(&exec_opportunity);

//                 if result.success {
//                     tracing::info!(
//                         "FIX Execution successful: {:.4}% profit, {} trades, actual profit: ${:.2}",
//                         exec_opportunity.profit_percentage(),
//                         result.trades_executed,
//                         result.actual_profit
//                     );
//                 } else {
//                     tracing::warn!(
//                         "FIX Execution failed: {} trades completed, error: {:?}",
//                         result.trades_executed,
//                         result.error
//                     );
//                 }

//                 result.success
//             } else {
//                 false
//             }
//         } else {
//             // Use fallback executor for simulation
//             let result = self.fallback_executor.execute(&exec_opportunity);
//             result.success
//         };

//         let execution_time = execution_start.elapsed().as_nanos() as u64;

//         // Update counters
//         self.execution_counter.fetch_add(1, Ordering::Relaxed);
//         self.last_execution_ns.store(execution_time, Ordering::Relaxed);

//         if success {
//             self.profit_counter.fetch_add(1, Ordering::Relaxed);

//             // Minimal success logging (production)
//             eprintln!(
//                 "EXEC: {:.4}% profit in {}μs",
//                 (opportunity.profit_ratio - 1.0) * 100.0,
//                 execution_time / 1000
//             );
//         }

//         // Release execution lock
//         self.is_executing.store(false, Ordering::Release);

//         success
//     }

//     /// ULTRA-FAST: Handle orderbook update with immediate execution
//     #[inline]
//     pub fn handle_update(&self, symbol: &Rc<str>, _book: &Arc<OrderBook>) {
//         // Rate limiting: max 1 check per 10μs globally
//         let now_ns = std::time::SystemTime
//             ::now()
//             .duration_since(std::time::UNIX_EPOCH)
//             .unwrap_or_default()
//             .as_nanos() as u64;

//         let last_exec = self.last_execution_ns.load(Ordering::Relaxed);
//         if now_ns.saturating_sub(last_exec) < 10_000 {
//             // 10μs
//             return;
//         }

//         // Fast path: Skip if already executing
//         if self.is_executing.load(Ordering::Relaxed) {
//             return;
//         }

//         // Get paths for this symbol
//         if let Some(symbol_paths) = self.symbol_map.get(symbol) {
//             let count = symbol_paths.count as usize;

//             // Process paths with immediate execution on first profitable find
//             for i in 0..count.min(8) {
//                 // Limit to 8 paths for latency
//                 let path_idx = unsafe { *symbol_paths.indices.get_unchecked(i) };

//                 if let Some(opportunity) = (unsafe { self.check_path_zero_alloc(path_idx) }) {
//                     // Immediate execution if profitable enough
//                     if opportunity.profit_ratio >= self.min_profit_for_execution {
//                         if self.execute_immediately_fast(opportunity) {
//                             // Success - exit immediately to avoid processing more
//                             self.stats_counter.fetch_add(1, Ordering::Relaxed);
//                             return;
//                         }
//                     }
//                 }
//             }
//         }
//     }

//     /// Get execution statistics
//     pub fn get_execution_stats(&self) -> (u64, u64, u64) {
//         (
//             self.execution_counter.load(Ordering::Relaxed),
//             self.profit_counter.load(Ordering::Relaxed) as u64,
//             self.last_execution_ns.load(Ordering::Relaxed),
//         )
//     }

//     /// Get FIX executor statistics if available
//     pub fn get_fix_execution_stats(&self) -> Option<crate::executor::fix_executor::ExecutionStats> {
//         self.fix_executor.as_ref().map(|executor| executor.get_execution_stats())
//     }

//     pub fn run_stats_task(&self) {
//         let stats_counter = self.stats_counter.clone();
//         let profit_counter = self.profit_counter.clone();
//         let execution_counter = self.execution_counter.clone();
//         let fix_executor = self.fix_executor.clone();

//         thread::Builder
//             ::new()
//             .name("arb-stats".to_string())
//             .spawn(move || {
//                 loop {
//                     thread::sleep(std::time::Duration::from_secs(30));
//                     let scans = stats_counter.swap(0, Ordering::Relaxed);
//                     let profits = profit_counter.load(Ordering::Relaxed);
//                     let executions = execution_counter.load(Ordering::Relaxed);

//                     if scans > 0 {
//                         eprintln!(
//                             "Stats: {} opportunities found, {} executions, {} profitable",
//                             scans,
//                             executions,
//                             profits
//                         );

//                         // Print FIX executor stats if available
//                         if let Some(fix_exec) = &fix_executor {
//                             let fix_stats = fix_exec.get_execution_stats();
//                             eprintln!("FIX Stats: {}", fix_stats);
//                         }
//                     }
//                 }
//             })
//             .expect("Failed to spawn stats thread");
//     }
// }

// // Zero-cost clone
// impl Clone for ArbitrageDetectorState {
//     fn clone(&self) -> Self {
//         Self {
//             orderbook_manager: self.orderbook_manager.clone(),
//             all_paths: self.all_paths.clone(),
//             path_data: self.path_data.clone(),
//             symbol_map: self.symbol_map.clone(),
//             fallback_executor: self.fallback_executor.clone(),
//             fix_executor: self.fix_executor.clone(),
//             is_executing: self.is_executing.clone(),
//             execution_counter: self.execution_counter.clone(),
//             start_amount: self.start_amount,
//             min_volume_multiplier: self.min_volume_multiplier,
//             min_profit_for_execution: self.min_profit_for_execution,
//             use_fix_execution: self.use_fix_execution,
//             stats_counter: self.stats_counter.clone(),
//             profit_counter: self.profit_counter.clone(),
//             last_execution_ns: self.last_execution_ns.clone(),
//         }
//     }
// }

// /// Create detector with FIX executor support
// pub fn create_detector(
//     orderbook_manager: Arc<OrderBookManager>,
//     fee_rate: f64,
//     min_profit_threshold: f64,
//     paths: Vec<Arc<TriangularPath>>,
//     start_amount: f64,
//     fallback_executor: Arc<ArbitrageExecutor>,
//     fix_executor: Option<Arc<FixArbitrageExecutor>>,
//     min_volume_multiplier: f64,
//     is_perf: bool
// ) -> Arc<ArbitrageDetectorState> {
//     let detector = ArbitrageDetectorState::new_with_fix_executor(
//         orderbook_manager,
//         paths,
//         fee_rate,
//         min_profit_threshold,
//         start_amount,
//         min_volume_multiplier,
//         fallback_executor,
//         fix_executor
//     );

//     if is_perf {
//         detector.run_stats_task();
//     }

//     detector
// }

// // Branch prediction hints for better performance
// #[allow(unused)]
// mod intrinsics {
//     #[inline(always)]
//     pub fn likely(b: bool) -> bool {
//         if b { true } else { false }
//     }

//     #[inline(always)]
//     pub fn unlikely(b: bool) -> bool {
//         if b { true } else { false }
//     }
// }

// // Re-export for compatibility
// pub use intrinsics::*;

// src/arbitrage/detector.rs - Optimized version replacing existing
use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{ AtomicUsize, AtomicBool, AtomicU64, Ordering };

use dashmap::DashMap;
use std::thread;

use crate::models::triangular_path::TriangularPath;
use crate::orderbook::manager::OrderBookManager;
use crate::orderbook::orderbook::OrderBook;
use crate::executor::executor::ArbitrageExecutor;
use crate::executor::fix_executor::{ FixArbitrageExecutor };
use crate::utils::string_interner::SymbolInterner;

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

// Pre-computed path data for zero-allocation hot path - optimized with hashes
#[repr(C, align(64))] // Cache line aligned
#[derive(Clone, Debug)]
struct PathData {
    // Symbol hashes instead of pointers (8 bytes each)
    first_symbol_hash: u64,
    second_symbol_hash: u64,
    third_symbol_hash: u64,

    // Direction flags (1 byte, packed)
    directions: u8, // bits: [unused:5][third:1][second:1][first:1]

    // Pre-computed values (4 bytes each)
    fee_multiplier_cubed: f32,
    min_threshold: f32,
    volume_multiplier: f32,

    // Padding to 64 bytes for cache alignment
    _padding: [u8; 17],
}

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

// Symbol to path indices mapping - optimized for cache efficiency using hashes
const MAX_PATHS_PER_SYMBOL: usize = 16;

#[repr(C, align(64))]
struct SymbolPaths {
    count: u8,
    indices: [u16; MAX_PATHS_PER_SYMBOL],
    _padding: [u8; 15], // Align to 64 bytes
}

/// Hyper-optimized arbitrage detector for maximum performance with FIX execution
pub struct ArbitrageDetectorState {
    // Core data (cache-optimized layout)
    pub orderbook_manager: Arc<OrderBookManager>,
    pub all_paths: Arc<Vec<Arc<TriangularPath>>>,

    // String interner for symbol deduplication
    symbol_interner: Arc<SymbolInterner>,

    // Hot path data - pre-computed and cache-aligned
    path_data: Box<[PathData]>, // Heap allocated, cache-aligned array
    symbol_map: Arc<DashMap<u64, SymbolPaths>>, // Use hashes instead of Arc<str>

    // Hash to symbol mapping for reverse lookups
    hash_to_symbol: Arc<DashMap<u64, Arc<str>>>,

    // Execution control - separate cache line
    fallback_executor: Arc<ArbitrageExecutor>, // For simulation mode compatibility
    fix_executor: Option<Arc<FixArbitrageExecutor>>, // Real/mock FIX executor
    is_executing: Arc<AtomicBool>,
    execution_counter: Arc<AtomicU64>,

    // Configuration constants
    start_amount: f32,
    min_volume_multiplier: f32,
    min_profit_for_execution: f32, // Immediate execution threshold
    use_fix_execution: bool, // Whether to use FIX API or fallback executor

    // Performance monitoring
    pub stats_counter: Arc<AtomicUsize>,
    pub profit_counter: Arc<AtomicUsize>,
    last_execution_ns: Arc<AtomicU64>,
}

impl ArbitrageDetectorState {
    /// Create a new detector with FIX execution support and optimization
    pub fn new_with_fix_executor(
        orderbook_manager: Arc<OrderBookManager>,
        paths: Vec<Arc<TriangularPath>>,
        fee_rate: f64,
        min_profit_threshold: f64,
        start_amount: f64,
        min_volume_multiplier: f64,
        fallback_executor: Arc<ArbitrageExecutor>,
        fix_executor: Option<Arc<FixArbitrageExecutor>>
    ) -> Arc<Self> {
        let symbol_interner = Arc::new(SymbolInterner::new());

        // Preload all symbols from paths
        let all_symbols: Vec<String> = paths
            .iter()
            .flat_map(|path| {
                [
                    path.first_symbol.to_string(),
                    path.second_symbol.to_string(),
                    path.third_symbol.to_string(),
                ]
            })
            .collect();
        symbol_interner.preload(&all_symbols);

        // Pre-compute all path data for zero-allocation hot path
        let mut path_data = Vec::with_capacity(paths.len());
        let symbol_map = Arc::new(DashMap::with_capacity(paths.len() * 3));
        let hash_to_symbol = Arc::new(DashMap::with_capacity(paths.len() * 3));

        for (i, path) in paths.iter().enumerate() {
            // Intern symbols and compute hashes
            let first_symbol = symbol_interner.intern(&path.first_symbol);
            let second_symbol = symbol_interner.intern(&path.second_symbol);
            let third_symbol = symbol_interner.intern(&path.third_symbol);

            let first_hash = Self::hash_symbol(&first_symbol);
            let second_hash = Self::hash_symbol(&second_symbol);
            let third_hash = Self::hash_symbol(&third_symbol);

            // Store hash to symbol mappings
            hash_to_symbol.insert(first_hash, first_symbol.clone());
            hash_to_symbol.insert(second_hash, second_symbol.clone());
            hash_to_symbol.insert(third_hash, third_symbol.clone());

            let directions =
                (path.first_is_base_to_quote as u8) |
                ((path.second_is_base_to_quote as u8) << 1) |
                ((path.third_is_base_to_quote as u8) << 2);

            path_data.push(PathData {
                first_symbol_hash: first_hash,
                second_symbol_hash: second_hash,
                third_symbol_hash: third_hash,
                directions,
                fee_multiplier_cubed: ((1.0 - fee_rate) as f32).powi(3),
                min_threshold: (1.0 + min_profit_threshold) as f32,
                volume_multiplier: min_volume_multiplier as f32,
                _padding: [0; 17],
            });

            let idx = i as u16;

            // Helper to add path to symbol using hashes
            let add_path = |hash: u64, idx: u16| {
                symbol_map
                    .entry(hash)
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

            add_path(first_hash, idx);
            add_path(second_hash, idx);
            add_path(third_hash, idx);
        }

        let use_fix_execution = fix_executor.is_some();

        let detector_state = Arc::new(ArbitrageDetectorState {
            orderbook_manager: orderbook_manager.clone(),
            all_paths: Arc::new(paths),
            symbol_interner,
            path_data: path_data.into_boxed_slice(),
            symbol_map,
            hash_to_symbol,
            fallback_executor,
            fix_executor,
            is_executing: Arc::new(AtomicBool::new(false)),
            execution_counter: Arc::new(AtomicU64::new(0)),
            start_amount: start_amount as f32,
            min_volume_multiplier: min_volume_multiplier as f32,
            min_profit_for_execution: (1.0 + min_profit_threshold) as f32,
            use_fix_execution,
            stats_counter: Arc::new(AtomicUsize::new(0)),
            profit_counter: Arc::new(AtomicUsize::new(0)),
            last_execution_ns: Arc::new(AtomicU64::new(0)),
        });

        // Register ultra-fast callback
        let callback_state = detector_state.clone();
        orderbook_manager.register_update_callback(
            Arc::new(move |symbol: &str, book: &Arc<OrderBook>| {
                callback_state.handle_update_optimized(symbol, book);
            })
        );

        detector_state
    }

    #[inline]
    fn hash_symbol(symbol: &Arc<str>) -> u64 {
        use std::hash::{ Hash, Hasher };
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        symbol.hash(&mut hasher);
        hasher.finish()
    }

    /// HYPER-OPTIMIZED: Check single path with zero allocations and hash-based lookups
    #[inline(always)]
    unsafe fn check_path_zero_alloc(&self, path_idx: u16) -> Option<FastOpportunity> {
        let path_data = self.path_data.get_unchecked(path_idx as usize);

        // Get symbols by hash lookups (faster than pointer dereferencing)
        let first_symbol = self.hash_to_symbol.get(&path_data.first_symbol_hash)?;
        let second_symbol = self.hash_to_symbol.get(&path_data.second_symbol_hash)?;
        let third_symbol = self.hash_to_symbol.get(&path_data.third_symbol_hash)?;

        // Get orderbooks via optimized access
        let books = &self.orderbook_manager.books;

        let first_book = books.get(&*first_symbol)?;
        let second_book = books.get(&*second_symbol)?;
        let third_book = books.get(&*third_symbol)?;

        // Ultra-fast sync check with branch prediction hint
        if unlikely(!first_book.is_synced() || !second_book.is_synced() || !third_book.is_synced()) {
            return None;
        }

        // Get cached prices (single atomic load per book)
        let (first_bid, first_ask) = first_book.get_cached_top_of_book();
        let (second_bid, second_ask) = second_book.get_cached_top_of_book();
        let (third_bid, third_ask) = third_book.get_cached_top_of_book();

        // Branch-free price selection using pre-computed directions
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

    /// IMMEDIATE EXECUTION: Execute opportunity instantly with FIX API or fallback
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

        let execution_start = Instant::now();
        let success = if self.use_fix_execution {
            // Use FIX executor for real/mock trading
            if let Some(fix_executor) = &self.fix_executor {
                let result = fix_executor.execute_triangular_arbitrage(&exec_opportunity);

                if result.success {
                    tracing::info!(
                        "FIX Execution successful: {:.4}% profit, {} trades, actual profit: ${:.2}",
                        exec_opportunity.profit_percentage(),
                        result.trades_executed,
                        result.actual_profit
                    );
                } else {
                    tracing::warn!(
                        "FIX Execution failed: {} trades completed, error: {:?}",
                        result.trades_executed,
                        result.error
                    );
                }

                result.success
            } else {
                false
            }
        } else {
            // Use fallback executor for simulation
            let result = self.fallback_executor.execute(&exec_opportunity);
            result.success
        };

        let execution_time = execution_start.elapsed().as_nanos() as u64;

        // Update counters
        self.execution_counter.fetch_add(1, Ordering::Relaxed);
        self.last_execution_ns.store(execution_time, Ordering::Relaxed);

        if success {
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

        success
    }

    /// ULTRA-FAST: Handle orderbook update with immediate execution - optimized with hashes
    #[inline]
    pub fn handle_update_optimized(&self, symbol: &str, _book: &Arc<OrderBook>) {
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

        // Hash-based symbol lookup
        let symbol_arc = self.symbol_interner.intern(symbol);
        let symbol_hash = Self::hash_symbol(&symbol_arc);

        // Get paths for this symbol using hash
        if let Some(symbol_paths) = self.symbol_map.get(&symbol_hash) {
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

    /// Original handle_update method for compatibility
    #[inline]
    pub fn handle_update(&self, symbol: &Arc<str>, book: &Arc<OrderBook>) {
        self.handle_update_optimized(symbol.as_ref(), book);
    }

    /// Get execution statistics
    pub fn get_execution_stats(&self) -> (u64, u64, u64) {
        (
            self.execution_counter.load(Ordering::Relaxed),
            self.profit_counter.load(Ordering::Relaxed) as u64,
            self.last_execution_ns.load(Ordering::Relaxed),
        )
    }

    /// Get FIX executor statistics if available
    pub fn get_fix_execution_stats(&self) -> Option<crate::executor::fix_executor::ExecutionStats> {
        self.fix_executor.as_ref().map(|executor| executor.get_execution_stats())
    }

    /// Get optimization statistics
    pub fn get_optimization_stats(&self) -> DetectorOptimizationStats {
        let (interner_hashes, interner_strings) = self.symbol_interner.stats();

        DetectorOptimizationStats {
            total_paths: self.all_paths.len(),
            interner_hash_entries: interner_hashes,
            interner_string_entries: interner_strings,
            symbol_map_entries: self.symbol_map.len(),
            hash_to_symbol_entries: self.hash_to_symbol.len(),
        }
    }

    pub fn run_stats_task(&self) {
        let stats_counter = self.stats_counter.clone();
        let profit_counter = self.profit_counter.clone();
        let execution_counter = self.execution_counter.clone();
        let fix_executor = self.fix_executor.clone();

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

                        // Print FIX executor stats if available
                        if let Some(fix_exec) = &fix_executor {
                            let fix_stats = fix_exec.get_execution_stats();
                            eprintln!("FIX Stats: {}", fix_stats);
                        }
                    }
                }
            })
            .expect("Failed to spawn stats thread");
    }
}

#[derive(Debug)]
pub struct DetectorOptimizationStats {
    pub total_paths: usize,
    pub interner_hash_entries: usize,
    pub interner_string_entries: usize,
    pub symbol_map_entries: usize,
    pub hash_to_symbol_entries: usize,
}

// Zero-cost clone
impl Clone for ArbitrageDetectorState {
    fn clone(&self) -> Self {
        Self {
            orderbook_manager: self.orderbook_manager.clone(),
            all_paths: self.all_paths.clone(),
            symbol_interner: self.symbol_interner.clone(),
            path_data: self.path_data.clone(),
            symbol_map: self.symbol_map.clone(),
            hash_to_symbol: self.hash_to_symbol.clone(),
            fallback_executor: self.fallback_executor.clone(),
            fix_executor: self.fix_executor.clone(),
            is_executing: self.is_executing.clone(),
            execution_counter: self.execution_counter.clone(),
            start_amount: self.start_amount,
            min_volume_multiplier: self.min_volume_multiplier,
            min_profit_for_execution: self.min_profit_for_execution,
            use_fix_execution: self.use_fix_execution,
            stats_counter: self.stats_counter.clone(),
            profit_counter: self.profit_counter.clone(),
            last_execution_ns: self.last_execution_ns.clone(),
        }
    }
}

/// Create detector with FIX executor support - same signature for compatibility
pub fn create_detector(
    orderbook_manager: Arc<OrderBookManager>,
    fee_rate: f64,
    min_profit_threshold: f64,
    paths: Vec<Arc<TriangularPath>>,
    start_amount: f64,
    fallback_executor: Arc<ArbitrageExecutor>,
    fix_executor: Option<Arc<FixArbitrageExecutor>>,
    min_volume_multiplier: f64,
    is_perf: bool
) -> Arc<ArbitrageDetectorState> {
    // Preload symbols into orderbook manager if possible
    let all_symbols: Vec<String> = paths
        .iter()
        .flat_map(|path| {
            [
                path.first_symbol.to_string(),
                path.second_symbol.to_string(),
                path.third_symbol.to_string(),
            ]
        })
        .collect();

    orderbook_manager.preload_symbols(&all_symbols);

    let detector = ArbitrageDetectorState::new_with_fix_executor(
        orderbook_manager,
        paths,
        fee_rate,
        min_profit_threshold,
        start_amount,
        min_volume_multiplier,
        fallback_executor,
        fix_executor
    );

    if is_perf {
        detector.run_stats_task();
    }

    detector
}

// Branch prediction hints for better performance
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
