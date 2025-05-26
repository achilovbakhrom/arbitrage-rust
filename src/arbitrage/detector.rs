use std::borrow::Borrow;
use std::cell::RefCell;
use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{ AtomicUsize, Ordering };
use tracing::{ debug, info };
use colored::Colorize;

use dashmap::DashMap;
use std::thread;

use crate::models::triangular_path::TriangularPath;
use crate::orderbook::manager::OrderBookManager;
use crate::orderbook::orderbook::OrderBook;

/// Represents an arbitrage opportunity
#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub path: Arc<TriangularPath>,
    pub profit_ratio: f64,
    pub start_amount: f64,
    pub end_amount: f64,
    pub execution_time_ms: u64, // Time to detect opportunity in milliseconds
}

impl ArbitrageOpportunity {
    /// Calculate profit percentage
    #[inline]
    pub fn profit_percentage(&self) -> f64 {
        (self.profit_ratio - 1.0) * 100.0
    }

    /// Format opportunity for display
    pub fn display(&self) -> String {
        format!(
            "{} → {} → {} | Profit: {}% | Start: {} {} | End: {} {} | Time: {} ms",
            self.path.first_symbol.to_string().green(),
            self.path.second_symbol.to_string().yellow(),
            self.path.third_symbol.to_string().green(),
            self.profit_percentage().to_string().bright_green().bold(),
            self.start_amount,
            self.path.start_asset,
            self.end_amount,
            self.path.end_asset,
            self.execution_time_ms.to_string().cyan()
        )
    }
}

thread_local! {
    static OPPORTUNITY_BUFFER: RefCell<Vec<ArbitrageOpportunity>> = RefCell::new(
        Vec::with_capacity(32)
    );
}

/// State for the arbitrage detector
pub struct ArbitrageDetectorState {
    pub orderbook_manager: Arc<OrderBookManager>,
    pub fee_rate: f64,
    pub one_minus_fee: f64,
    pub min_profit_threshold: f64,
    pub symbol_to_paths: Arc<DashMap<Arc<str>, Vec<usize>>>,
    pub all_paths: Arc<Vec<Arc<TriangularPath>>>,
    pub processed_symbols: Arc<DashMap<String, Instant>>,
    pub stats_counter: Arc<AtomicUsize>,
    pub profit_counter: Arc<AtomicUsize>,
    pub start_amount: f64,

    // Pre-computed values for fast calculations
    fee_multiplier_f64: f64,
    min_profit_threshold_f64: f64,
}

impl ArbitrageDetectorState {
    /// Check a single triangular path for arbitrage opportunity
    #[inline]
    pub fn check_path(
        &self,
        path: &Arc<TriangularPath>,
        start_amount: f64
    ) -> Option<ArbitrageOpportunity> {
        let start_time = Instant::now();

        // Get orderbooks for each symbol in the path
        let books = &self.orderbook_manager.books;

        // Fast lookup using get_cached_top_of_book
        let first_book = books.get(&path.first_symbol)?;
        let second_book = books.get(&path.second_symbol)?;
        let third_book = books.get(&path.third_symbol)?;

        // Check if all books are synced
        if !first_book.is_synced() || !second_book.is_synced() || !third_book.is_synced() {
            return None;
        }

        // Get cached top of book values for ultra-fast access
        let (first_bid, first_ask) = first_book.get_cached_top_of_book();
        let (second_bid, second_ask) = second_book.get_cached_top_of_book();
        let (third_bid, third_ask) = third_book.get_cached_top_of_book();

        // Extract prices based on trade direction using f64 for speed
        let first_price = if path.first_is_base_to_quote { first_bid?.0 } else { first_ask?.0 };

        let second_price = if path.second_is_base_to_quote { second_bid?.0 } else { second_ask?.0 };

        let third_price = if path.third_is_base_to_quote { third_bid?.0 } else { third_ask?.0 };

        // Ultra-fast f64 calculation
        let mut amount_f64 = 100.0; // Use fixed start amount for speed

        // First leg
        if path.first_is_base_to_quote {
            amount_f64 *= first_price;
        } else {
            amount_f64 /= first_price;
        }
        amount_f64 *= self.fee_multiplier_f64;

        // Second leg
        if path.second_is_base_to_quote {
            amount_f64 *= second_price;
        } else {
            amount_f64 /= second_price;
        }
        amount_f64 *= self.fee_multiplier_f64;

        // Third leg
        if path.third_is_base_to_quote {
            amount_f64 *= third_price;
        } else {
            amount_f64 /= third_price;
        }
        amount_f64 *= self.fee_multiplier_f64;

        // Calculate profit ratio
        let profit_ratio_f64 = amount_f64 / 100.0;

        // Fast path: check if profitable early
        if profit_ratio_f64 <= 1.0 + self.min_profit_threshold_f64 {
            return None;
        }

        let execution_time_ms = start_time.elapsed().as_millis() as u64;

        Some(ArbitrageOpportunity {
            path: Arc::clone(path),
            profit_ratio: profit_ratio_f64,
            start_amount,
            end_amount: amount_f64,
            execution_time_ms,
        })
    }

    /// Handle an orderbook update - REMOVED async
    pub fn handle_update(&self, symbol: &Arc<str>, book: &Arc<OrderBook>) {
        if !book.is_synced() {
            return;
        }
        let start = Instant::now();

        // Find affected paths and process them
        if let Some(path_indices) = self.symbol_to_paths.get(symbol) {
            self.process_affected_paths(path_indices.borrow());
        }
        let elapsed = start.elapsed();
        debug!("Elapsed: {:2}", elapsed.as_micros());
    }

    #[inline]
    fn process_affected_paths(&self, indices: &[usize]) {
        OPPORTUNITY_BUFFER.with(|buffer| {
            let mut opportunities = buffer.borrow_mut();
            opportunities.clear(); // Reuse existing capacity

            for &idx in indices {
                if let Some(path) = self.all_paths.get(idx) {
                    if let Some(opportunity) = self.check_path(path, self.start_amount) {
                        opportunities.push(opportunity);
                    }
                }
            }

            // Process opportunities without additional allocations
            if !opportunities.is_empty() {
                self.profit_counter.fetch_add(opportunities.len(), Ordering::Relaxed);

                println!("\n{}", "=== ARBITRAGE OPPORTUNITIES ===".bright_purple().bold());

                for (i, opp) in opportunities.iter().enumerate() {
                    println!("#{}: {}", i + 1, opp.display());
                }

                println!("{}\n", "=============================".bright_purple().bold());
            }
        });
    }

    /// Run a periodic task to print statistics - runs in its own thread
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

                    info!(
                        "Arbitrage stats: {} update triggers processed, {} profitable opportunities found",
                        scans,
                        profits
                    );
                }
            })
            .expect("Failed to spawn stats thread");
    }
}

// Make state cloneable
impl Clone for ArbitrageDetectorState {
    fn clone(&self) -> Self {
        Self {
            orderbook_manager: self.orderbook_manager.clone(),
            fee_rate: self.fee_rate,
            one_minus_fee: self.one_minus_fee,
            min_profit_threshold: self.min_profit_threshold,
            symbol_to_paths: self.symbol_to_paths.clone(),
            all_paths: self.all_paths.clone(),
            processed_symbols: self.processed_symbols.clone(),
            stats_counter: self.stats_counter.clone(),
            profit_counter: self.profit_counter.clone(),
            start_amount: self.start_amount,
            fee_multiplier_f64: self.fee_multiplier_f64,
            min_profit_threshold_f64: self.min_profit_threshold_f64,
        }
    }
}

/// Create a new event-driven arbitrage detector
pub fn create_event_driven_detector(
    orderbook_manager: Arc<OrderBookManager>,
    fee_rate: f64,
    min_profit_threshold: f64,
    paths: Vec<Arc<TriangularPath>>,
    start_amount: f64
) -> Arc<ArbitrageDetectorState> {
    // Build symbol to paths mapping with pre-allocation
    let symbol_to_paths = Arc::new(DashMap::with_capacity(paths.len() * 3));

    // Build symbol to paths mapping
    for (i, path) in paths.iter().enumerate() {
        let first = path.first_symbol.clone();
        let second = path.second_symbol.clone();
        let third = path.third_symbol.clone();

        symbol_to_paths
            .entry(first)
            .or_insert_with(|| Vec::with_capacity(10))
            .push(i);
        symbol_to_paths
            .entry(second)
            .or_insert_with(|| Vec::with_capacity(10))
            .push(i);
        symbol_to_paths
            .entry(third)
            .or_insert_with(|| Vec::with_capacity(10))
            .push(i);
    }

    let processed_symbols = Arc::new(DashMap::with_capacity(1000));
    let all_paths = Arc::new(paths);
    let stats_counter = Arc::new(AtomicUsize::new(0));
    let profit_counter = Arc::new(AtomicUsize::new(0));

    let fee_multiplier_f64 = 1.0 - fee_rate;
    let min_profit_threshold_f64 = min_profit_threshold;

    let detector_state = Arc::new(ArbitrageDetectorState {
        orderbook_manager: orderbook_manager.clone(),
        fee_rate,
        one_minus_fee: 1.0 - fee_rate,
        min_profit_threshold,
        symbol_to_paths: symbol_to_paths.clone(),
        all_paths: all_paths.clone(),
        processed_symbols: processed_symbols.clone(),
        stats_counter: stats_counter.clone(),
        profit_counter: profit_counter.clone(),
        start_amount,
        fee_multiplier_f64,
        min_profit_threshold_f64: min_profit_threshold_f64 as f64,
    });

    // Register callback with orderbook manager - CHANGED to sync callback
    let callback_state = detector_state.clone();
    orderbook_manager.register_update_callback(
        Arc::new(move |symbol, book| {
            callback_state.handle_update(symbol, book);
        })
    );

    // Start a task to periodically print stats
    detector_state.run_stats_task();

    // Return the detector state
    detector_state
}

/// Print arbitrage opportunities that exceed threshold
pub fn print_opportunities(opportunities: &[ArbitrageOpportunity]) {
    if opportunities.is_empty() {
        return;
    }

    println!("\n{}", "=== ARBITRAGE OPPORTUNITIES ===".bright_purple().bold());

    for (i, opp) in opportunities.iter().enumerate() {
        println!("#{}: {}", i + 1, opp.display());
    }

    println!("{}\n", "=============================".bright_purple().bold());
}

/// Get the top N opportunities
pub fn top_opportunities(
    opportunities: &[ArbitrageOpportunity],
    n: usize
) -> Vec<&ArbitrageOpportunity> {
    opportunities.iter().take(n).collect()
}
