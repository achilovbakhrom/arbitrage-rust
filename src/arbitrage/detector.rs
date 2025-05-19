// src/arbitrage/detector.rs

use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{ AtomicUsize, Ordering };
use tokio::sync::{ Mutex, mpsc, Semaphore };
use futures::{ stream, StreamExt };
use tracing::{ debug, info, warn };
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use colored::Colorize;
use std::collections::{ HashSet, HashMap };
use dashmap::DashMap; // Use DashMap for concurrent access without locks

use crate::models::triangular_path::TriangularPath;
use crate::orderbook::manager::OrderBookManager;
use crate::orderbook::orderbook::OrderBook;

/// Maximum number of paths to check concurrently
const MAX_CONCURRENT_CHECKS: usize = 32;

/// Maximum batch size for scanning
const MAX_BATCH_SIZE: usize = 128;

/// Symbol cooldown in milliseconds (to avoid processing the same symbol too frequently)
const SYMBOL_COOLDOWN_MS: u64 = 100;

/// Represents an arbitrage opportunity
#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub path: Arc<TriangularPath>,
    pub profit_ratio: Decimal,
    pub start_amount: Decimal,
    pub end_amount: Decimal,
    pub fee_adjusted: bool,
    pub execution_time_ms: u64, // Time to detect opportunity in milliseconds
}

impl ArbitrageOpportunity {
    /// Calculate profit percentage
    #[inline]
    pub fn profit_percentage(&self) -> Decimal {
        (self.profit_ratio - dec!(1.0)) * dec!(100.0)
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

/// Cache-friendly Decimal conversion
#[inline]
fn f64_to_decimal(value: f64) -> Decimal {
    // This is faster than Decimal::from_f64 in hot paths
    Decimal::from_str(&value.to_string()).unwrap_or(Decimal::ZERO)
}

/// State for the arbitrage detector
pub struct ArbitrageDetectorState {
    pub orderbook_manager: Arc<OrderBookManager>,
    pub fee_rate: Decimal,
    pub one_minus_fee: Decimal,
    pub min_profit_threshold: Decimal,
    pub symbol_to_paths: Arc<DashMap<String, Vec<usize>>>,
    pub all_paths: Arc<Vec<Arc<TriangularPath>>>,
    pub processed_symbols: Arc<DashMap<String, Instant>>,
    pub stats_counter: Arc<AtomicUsize>,
    pub profit_counter: Arc<AtomicUsize>,
    pub start_amount: Decimal,
}

impl ArbitrageDetectorState {
    /// Check a single triangular path for arbitrage opportunity
    #[inline]
    pub async fn check_path(
        &self,
        path: &Arc<TriangularPath>,
        start_amount: Decimal
    ) -> Option<ArbitrageOpportunity> {
        let start_time = Instant::now();

        // Get orderbooks for each symbol in the path
        let books = &self.orderbook_manager.books;
        let first_book = books.get(&path.first_symbol)?.clone();
        let second_book = books.get(&path.second_symbol)?.clone();
        let third_book = books.get(&path.third_symbol)?.clone();

        // Check if all books are synced
        if !first_book.is_synced() || !second_book.is_synced() || !third_book.is_synced() {
            return None;
        }

        // Get current snapshots (top of book)
        let first_tob = first_book.snapshot().await;
        let second_tob = second_book.snapshot().await;
        let third_tob = third_book.snapshot().await;

        // Extract prices based on trade direction (with early returns to avoid allocations)
        let first_price = if path.first_is_base_to_quote {
            // Selling base at bid price
            first_tob.0.as_ref().map(|l| f64_to_decimal(l.price))?
        } else {
            // Buying base at ask price
            first_tob.1.as_ref().map(|l| f64_to_decimal(l.price))?
        };

        let second_price = if path.second_is_base_to_quote {
            // Selling base at bid price
            second_tob.0.as_ref().map(|l| f64_to_decimal(l.price))?
        } else {
            // Buying base at ask price
            second_tob.1.as_ref().map(|l| f64_to_decimal(l.price))?
        };

        let third_price = if path.third_is_base_to_quote {
            // Selling base at bid price
            third_tob.0.as_ref().map(|l| f64_to_decimal(l.price))?
        } else {
            // Buying base at ask price
            third_tob.1.as_ref().map(|l| f64_to_decimal(l.price))?
        };

        // Fast calculation path
        let mut amount = start_amount;

        // First leg
        if path.first_is_base_to_quote {
            // Selling base for quote
            amount = amount * first_price;
        } else {
            // Buying base with quote
            amount = amount / first_price;
        }
        // Apply fee
        amount = amount * self.one_minus_fee;

        // Second leg
        if path.second_is_base_to_quote {
            // Selling base for quote
            amount = amount * second_price;
        } else {
            // Buying base with quote
            amount = amount / second_price;
        }
        // Apply fee
        amount = amount * self.one_minus_fee;

        // Third leg
        if path.third_is_base_to_quote {
            // Selling base for quote
            amount = amount * third_price;
        } else {
            // Buying base with quote
            amount = amount / third_price;
        }
        // Apply fee
        amount = amount * self.one_minus_fee;

        // Calculate profit ratio
        let profit_ratio = amount / start_amount;
        // Fast path: check if profitable early to avoid creating ArbitrageOpportunity
        if profit_ratio <= dec!(1.0) + self.min_profit_threshold {
            return None;
        }

        let execution_time_ms = start_time.elapsed().as_millis() as u64;

        // Create the opportunity
        Some(ArbitrageOpportunity {
            path: Arc::clone(path),
            profit_ratio,
            start_amount,
            end_amount: amount,
            fee_adjusted: true,
            execution_time_ms,
        })
    }

    /// Handle an orderbook update
    pub fn handle_update(&self, symbol: &Arc<str>, book: &Arc<OrderBook>) {
        // Skip if book not synced
        if !book.is_synced() {
            return;
        }

        let symbol_str = symbol.to_string();

        // Skip if recently processed
        let now = Instant::now();
        {
            if let Some(last_processed) = self.processed_symbols.get(&symbol_str) {
                if last_processed.elapsed().as_millis() < (SYMBOL_COOLDOWN_MS as u128) {
                    return;
                }
            }
            // Update processed timestamp
            self.processed_symbols.insert(symbol_str.clone(), now);
        }

        // Find affected paths and process them
        if let Some(path_indices) = self.symbol_to_paths.get(&symbol_str) {
            let indices = path_indices.clone(); // Clone the small Vec of indices

            // Drop the reference to unblock the map
            drop(path_indices);

            // Only spawn a task if we actually have paths to check
            if !indices.is_empty() {
                // Increment the stats counter
                self.stats_counter.fetch_add(1, Ordering::Relaxed);

                // Clone context for the task
                let state = self.clone();
                let start_amount = self.start_amount;

                // Spawn lightweight task to avoid blocking the callback
                tokio::spawn(async move {
                    // Create a temporary buffer for path references
                    let mut paths_to_check = Vec::with_capacity(indices.len());

                    // Get paths to check
                    for &idx in &indices {
                        if let Some(path) = state.all_paths.get(idx) {
                            paths_to_check.push(Arc::clone(path));
                        }
                    }

                    // Skip if no paths to check
                    if paths_to_check.is_empty() {
                        return;
                    }

                    // Scan the affected paths
                    let mut opportunities = Vec::new();

                    for path in &paths_to_check {
                        // Inside the path checking loop, add for each path:

                        if let Some(opportunity) = state.check_path(path, start_amount).await {
                            opportunities.push(opportunity);
                        }
                        info!("check {:?}", path);
                    }

                    // Process opportunities
                    if !opportunities.is_empty() {
                        // Increment profit counter
                        state.profit_counter.fetch_add(opportunities.len(), Ordering::Relaxed);

                        // Sort by profit ratio descending
                        opportunities.sort_by(|a, b| b.profit_ratio.cmp(&a.profit_ratio));

                        // Print opportunities
                        if !opportunities.is_empty() {
                            println!(
                                "\n{}",
                                "=== ARBITRAGE OPPORTUNITIES ===".bright_purple().bold()
                            );

                            for (i, opp) in opportunities.iter().enumerate() {
                                println!("#{}: {}", i + 1, opp.display());
                            }

                            println!(
                                "{}\n",
                                "=============================".bright_purple().bold()
                            );
                        }
                    }
                });
            }
        }
    }

    /// Run a periodic task to print statistics
    pub async fn run_stats_task(&self) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

        loop {
            interval.tick().await;

            let scans = self.stats_counter.swap(0, Ordering::Relaxed);
            let profits = self.profit_counter.swap(0, Ordering::Relaxed);

            info!(
                "Arbitrage stats: {} update triggers processed, {} profitable opportunities found",
                scans,
                profits
            );
        }
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
        }
    }
}

/// Create a new event-driven arbitrage detector
pub async fn create_event_driven_detector(
    orderbook_manager: Arc<OrderBookManager>,
    fee_rate: Decimal,
    min_profit_threshold: Decimal,
    paths: Vec<Arc<TriangularPath>>,
    start_amount: Decimal
) -> Arc<ArbitrageDetectorState> {
    // Build symbol to paths mapping with pre-allocation
    let symbol_to_paths = Arc::new(DashMap::with_capacity(paths.len() * 3));

    // Build symbol to paths mapping
    for (i, path) in paths.iter().enumerate() {
        let first = path.first_symbol.to_string();
        let second = path.second_symbol.to_string();
        let third = path.third_symbol.to_string();

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

    let detector_state = Arc::new(ArbitrageDetectorState {
        orderbook_manager: orderbook_manager.clone(),
        fee_rate,
        one_minus_fee: dec!(1.0) - fee_rate,
        min_profit_threshold,
        symbol_to_paths: symbol_to_paths.clone(),
        all_paths: all_paths.clone(),
        processed_symbols: processed_symbols.clone(),
        stats_counter: stats_counter.clone(),
        profit_counter: profit_counter.clone(),
        start_amount,
    });

    // Register callback with orderbook manager
    let callback_state = detector_state.clone();
    orderbook_manager.register_update_callback(
        Arc::new(move |symbol, book| {
            callback_state.handle_update(symbol, book);
        })
    ).await;

    // Start a task to periodically print stats
    let stats_state = detector_state.clone();
    tokio::spawn(async move {
        stats_state.run_stats_task().await;
    });

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
