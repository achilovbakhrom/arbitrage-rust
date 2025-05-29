use std::borrow::Borrow;
use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{ AtomicUsize, AtomicBool, Ordering };
use tracing::info;
use colored::Colorize;

use dashmap::DashMap;
use std::thread;

use crate::models::triangular_path::TriangularPath;
use crate::orderbook::manager::OrderBookManager;
use crate::orderbook::orderbook::OrderBook;
use crate::arbitrage::executor::{ ArbitrageExecutor, ExecutionStrategy };

/// Represents an arbitrage opportunity
#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub path: Arc<TriangularPath>,
    pub profit_ratio: f64,
    pub start_amount: f64,
    pub end_amount: f64,
    pub execution_time_ns: u64, // Nanosecond precision for ultra-fast detection
}

impl ArbitrageOpportunity {
    /// Calculate profit percentage
    #[inline(always)]
    pub fn profit_percentage(&self) -> f64 {
        (self.profit_ratio - 1.0) * 100.0
    }

    /// Format opportunity for display
    pub fn display(&self) -> String {
        format!(
            "{} → {} → {} | Profit: {}% | Start: {} {} | End: {} {} | Time: {}ns",
            self.path.first_symbol.to_string().green(),
            self.path.second_symbol.to_string().yellow(),
            self.path.third_symbol.to_string().green(),
            self.profit_percentage().to_string().bright_green().bold(),
            self.start_amount,
            self.path.start_asset,
            self.end_amount,
            self.path.end_asset,
            self.execution_time_ns.to_string().cyan()
        )
    }
}

/// Ultra-fast arbitrage detector state with zero-overhead design
pub struct ArbitrageDetectorState {
    pub orderbook_manager: Arc<OrderBookManager>,
    pub symbol_to_paths: Arc<DashMap<Arc<str>, Vec<usize>>>,
    pub all_paths: Arc<Vec<Arc<TriangularPath>>>,
    pub stats_counter: Arc<AtomicUsize>,
    pub profit_counter: Arc<AtomicUsize>,

    // Pre-computed values (immutable after creation for zero overhead)
    fee_multiplier: f64,
    min_profit_threshold: f64,
    start_amount: f64,

    // Execution control (minimal overhead)
    executor: Option<Arc<ArbitrageExecutor>>, // Set once at creation, never changed
    is_executing: Arc<AtomicBool>, // Single atomic for execution state
}

impl ArbitrageDetectorState {
    /// Ultra-fast path checking with zero allocations and minimal branches
    #[inline(always)]
    pub fn check_path_ultra_fast(
        &self,
        path: &Arc<TriangularPath>
    ) -> Option<ArbitrageOpportunity> {
        let start_time = Instant::now();

        // Get orderbooks - single hash lookup each
        let books = &self.orderbook_manager.books;
        let first_book = books.get(&path.first_symbol)?;
        let second_book = books.get(&path.second_symbol)?;
        let third_book = books.get(&path.third_symbol)?;

        // Fast sync check - single atomic load each
        if !first_book.is_synced() || !second_book.is_synced() || !third_book.is_synced() {
            return None;
        }

        // Get cached prices - zero allocation, single atomic load each
        let (first_bid, first_ask) = first_book.get_cached_top_of_book();
        let (second_bid, second_ask) = second_book.get_cached_top_of_book();
        let (third_bid, third_ask) = third_book.get_cached_top_of_book();

        // Extract prices - branchless where possible
        let first_price = if path.first_is_base_to_quote { first_bid?.0 } else { first_ask?.0 };
        let second_price = if path.second_is_base_to_quote { second_bid?.0 } else { second_ask?.0 };
        let third_price = if path.third_is_base_to_quote { third_bid?.0 } else { third_ask?.0 };

        // Ultra-fast calculation - pure FPU operations
        let mut amount = self.start_amount;

        // Leg 1
        amount =
            (if path.first_is_base_to_quote {
                amount * first_price
            } else {
                amount / first_price
            }) * self.fee_multiplier;

        // Leg 2
        amount =
            (if path.second_is_base_to_quote {
                amount * second_price
            } else {
                amount / second_price
            }) * self.fee_multiplier;

        // Leg 3
        amount =
            (if path.third_is_base_to_quote {
                amount * third_price
            } else {
                amount / third_price
            }) * self.fee_multiplier;

        // Fast profitability check
        let profit_ratio = amount / self.start_amount;
        if profit_ratio <= 1.0 + self.min_profit_threshold {
            return None;
        }

        let execution_time_ns = start_time.elapsed().as_nanos() as u64;

        Some(ArbitrageOpportunity {
            path: Arc::clone(path),
            profit_ratio,
            start_amount: self.start_amount,
            end_amount: amount,
            execution_time_ns,
        })
    }

    /// Handle orderbook update with minimal overhead
    #[inline]
    pub fn handle_update(&self, symbol: &Arc<str>, book: &Arc<OrderBook>) {
        if !book.is_synced() {
            return;
        }

        // Fast execution check - single atomic load
        if self.is_executing.load(Ordering::Relaxed) {
            return;
        }

        // Get affected paths - single hash lookup
        if let Some(path_indices) = self.symbol_to_paths.get(symbol) {
            self.process_paths_immediate(path_indices.borrow(), symbol);
        }
    }

    /// Process paths immediately without any threading overhead
    #[inline]
    fn process_paths_immediate(&self, indices: &[usize], trigger_symbol: &Arc<str>) {
        let mut best_opportunity: Option<ArbitrageOpportunity> = None;

        // Check all affected paths in current thread - zero overhead
        for &idx in indices {
            if let Some(path) = self.all_paths.get(idx) {
                if let Some(opportunity) = self.check_path_ultra_fast(path) {
                    match &best_opportunity {
                        None => {
                            best_opportunity = Some(opportunity);
                        }
                        Some(current_best) => {
                            if opportunity.profit_ratio > current_best.profit_ratio {
                                best_opportunity = Some(opportunity);
                            }
                        }
                    }
                }
            }
        }

        // Execute immediately if profitable
        if let Some(opportunity) = best_opportunity {
            self.execute_immediately(opportunity, trigger_symbol);
        }
    }

    /// Execute opportunity with minimal overhead
    #[inline]
    fn execute_immediately(&self, opportunity: ArbitrageOpportunity, trigger_symbol: &Arc<str>) {
        // Fast atomic compare-and-swap to claim execution
        if
            self.is_executing
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        {
            println!("\n{}", "=== ARBITRAGE OPPORTUNITY ===".bright_purple().bold());
            println!("Triggered by: {}", trigger_symbol.to_string().cyan());
            println!("Opportunity: {}", opportunity.display());

            // Execute synchronously if executor available
            if let Some(ref executor) = self.executor {
                let execution_start = Instant::now();
                let result = executor.execute(&opportunity);
                let total_time_ns = execution_start.elapsed().as_nanos() as u64;

                if result.success {
                    info!(
                        "SUCCESS: Profit={:.6} {}, Exec={}μs, Total={}μs, Slippage={:.2}%",
                        result.profit_amount,
                        opportunity.path.end_asset,
                        result.execution_time_us,
                        total_time_ns / 1000,
                        result.slippage_factor * 100.0
                    );
                } else {
                    info!(
                        "FAILED: {} ({}μs)",
                        result.error.unwrap_or_else(|| "Unknown".to_string()),
                        result.execution_time_us
                    );
                }
            }

            println!("{}\n", "============================".bright_purple().bold());

            // Update counter and release execution lock
            self.profit_counter.fetch_add(1, Ordering::Relaxed);
            self.is_executing.store(false, Ordering::Release);
        }
        // If compare_exchange failed, another execution is in progress - skip silently
    }

    /// Minimal stats task
    pub fn run_stats_task(&self) {
        let stats_counter = self.stats_counter.clone();
        let profit_counter = self.profit_counter.clone();

        thread::Builder
            ::new()
            .name("arb-stats".to_string())
            .spawn(move || {
                loop {
                    thread::sleep(std::time::Duration::from_secs(60));
                    let scans = stats_counter.swap(0, Ordering::Relaxed);
                    let profits = profit_counter.swap(0, Ordering::Relaxed);
                    if scans > 0 || profits > 0 {
                        info!("Stats: {} scans, {} profits", scans, profits);
                    }
                }
            })
            .expect("Failed to spawn stats thread");
    }
}

// Zero-cost clone implementation
impl Clone for ArbitrageDetectorState {
    fn clone(&self) -> Self {
        Self {
            orderbook_manager: self.orderbook_manager.clone(),
            symbol_to_paths: self.symbol_to_paths.clone(),
            all_paths: self.all_paths.clone(),
            stats_counter: self.stats_counter.clone(),
            profit_counter: self.profit_counter.clone(),
            fee_multiplier: self.fee_multiplier,
            min_profit_threshold: self.min_profit_threshold,
            start_amount: self.start_amount,
            executor: self.executor.clone(),
            is_executing: self.is_executing.clone(),
        }
    }
}

/// Create ultra-fast detector with minimal allocations
pub fn create_ultra_fast_detector(
    orderbook_manager: Arc<OrderBookManager>,
    fee_rate: f64,
    min_profit_threshold: f64,
    paths: Vec<Arc<TriangularPath>>,
    start_amount: f64,
    executor: Option<Arc<ArbitrageExecutor>>,
    is_perf: bool
) -> Arc<ArbitrageDetectorState> {
    // Pre-allocate symbol mapping
    let symbol_to_paths = Arc::new(DashMap::with_capacity(paths.len() * 3));

    // Build mapping once
    for (i, path) in paths.iter().enumerate() {
        symbol_to_paths
            .entry(path.first_symbol.clone())
            .or_insert_with(|| Vec::with_capacity(4))
            .push(i);
        symbol_to_paths
            .entry(path.second_symbol.clone())
            .or_insert_with(|| Vec::with_capacity(4))
            .push(i);
        symbol_to_paths
            .entry(path.third_symbol.clone())
            .or_insert_with(|| Vec::with_capacity(4))
            .push(i);
    }

    let detector_state = Arc::new(ArbitrageDetectorState {
        orderbook_manager: orderbook_manager.clone(),
        symbol_to_paths,
        all_paths: Arc::new(paths),
        stats_counter: Arc::new(AtomicUsize::new(0)),
        profit_counter: Arc::new(AtomicUsize::new(0)),
        fee_multiplier: 1.0 - fee_rate,
        min_profit_threshold,
        start_amount,
        executor,
        is_executing: Arc::new(AtomicBool::new(false)),
    });

    // Register zero-overhead callback
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

/// Convenience function with executor
pub fn create_ultra_fast_detector_with_executor(
    orderbook_manager: Arc<OrderBookManager>,
    fee_rate: f64,
    min_profit_threshold: f64,
    paths: Vec<Arc<TriangularPath>>,
    start_amount: f64,
    executor: Arc<ArbitrageExecutor>,
    is_perf: bool
) -> Arc<ArbitrageDetectorState> {
    create_ultra_fast_detector(
        orderbook_manager,
        fee_rate,
        min_profit_threshold,
        paths,
        start_amount,
        Some(executor),
        is_perf
    )
}
