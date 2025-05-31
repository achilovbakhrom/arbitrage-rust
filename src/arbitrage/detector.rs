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
use crate::arbitrage::executor::ArbitrageExecutor;

/// Represents an arbitrage opportunity with volume validation
#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub path: Arc<TriangularPath>,
    pub profit_ratio: f64,
    pub start_amount: f64,
    pub end_amount: f64,
    pub execution_time_ns: u64,
    pub min_volume_satisfied: bool, // NEW: Track if volume requirements are met
    pub liquidity_scores: [f64; 3], // NEW: Liquidity score for each leg (0.0-1.0)
}

impl ArbitrageOpportunity {
    /// Calculate profit percentage
    #[inline(always)]
    pub fn profit_percentage(&self) -> f64 {
        (self.profit_ratio - 1.0) * 100.0
    }

    /// Check if this opportunity has sufficient liquidity for execution
    #[inline(always)]
    pub fn has_sufficient_liquidity(&self) -> bool {
        self.min_volume_satisfied && self.liquidity_scores.iter().all(|&score| score > 0.5)
    }

    /// Format opportunity for display with volume information
    pub fn display(&self) -> String {
        let liquidity_status = if self.has_sufficient_liquidity() {
            "✓ LIQUID".green()
        } else {
            "⚠ LOW LIQ".yellow()
        };

        format!(
            "{} → {} → {} | Profit: {}% | Start: {} {} | End: {} {} | {} | Liq: [{:.2},{:.2},{:.2}] | Time: {}ns",
            self.path.first_symbol.to_string().green(),
            self.path.second_symbol.to_string().yellow(),
            self.path.third_symbol.to_string().green(),
            self.profit_percentage().to_string().bright_green().bold(),
            self.start_amount,
            self.path.start_asset,
            self.end_amount,
            self.path.end_asset,
            liquidity_status,
            self.liquidity_scores[0],
            self.liquidity_scores[1],
            self.liquidity_scores[2],
            self.execution_time_ns.to_string().cyan()
        )
    }
}

/// Ultra-fast arbitrage detector state with volume validation
pub struct ArbitrageDetectorState {
    pub orderbook_manager: Arc<OrderBookManager>,
    pub symbol_to_paths: Arc<DashMap<Arc<str>, Vec<usize>>>,
    pub all_paths: Arc<Vec<Arc<TriangularPath>>>,
    pub stats_counter: Arc<AtomicUsize>,
    pub profit_counter: Arc<AtomicUsize>,

    // Pre-computed values
    fee_multiplier: f64,
    min_profit_threshold: f64,
    start_amount: f64,

    // NEW: Volume validation parameters
    min_volume_multiplier: f64, // Minimum volume as multiple of trade amount (e.g., 2.0 = 2x)
    volume_depth_check: usize, // How many price levels to check for volume

    // Execution control
    executor: Option<Arc<ArbitrageExecutor>>,
    is_executing: Arc<AtomicBool>,
}

impl ArbitrageDetectorState {
    /// Create new detector with volume validation parameters

    /// Calculate liquidity score for a given side and required volume
    #[inline(always)]
    fn calculate_liquidity_score(
        &self,
        book: &Arc<OrderBook>,
        is_bid_side: bool,
        required_volume: f64
    ) -> f64 {
        if !book.is_synced() {
            return 0.0;
        }

        // Get price levels up to the depth we want to check
        let levels = if is_bid_side {
            book.get_bids(Some(self.volume_depth_check))
        } else {
            book.get_asks(Some(self.volume_depth_check))
        };

        if levels.is_empty() {
            return 0.0;
        }

        let mut available_volume = 0.0;
        let mut weighted_price_sum = 0.0;
        let mut total_weight = 0.0;

        // Calculate available volume and volume-weighted average price
        for level in &levels {
            available_volume += level.quantity;

            // Weight by quantity (more liquid levels have more influence)
            let weight = level.quantity;
            weighted_price_sum += level.price * weight;
            total_weight += weight;

            // Stop if we have enough volume
            if available_volume >= required_volume {
                break;
            }
        }

        // Calculate liquidity score based on multiple factors
        let volume_ratio = (available_volume / required_volume).min(1.0);

        // Price impact score (how much price moves if we consume the volume)
        let price_impact_score = if levels.len() > 1 && total_weight > 0.0 {
            let avg_price = weighted_price_sum / total_weight;
            let best_price = levels[0].price;
            let price_deviation = ((avg_price - best_price).abs() / best_price).min(1.0);
            1.0 - price_deviation // Lower price impact = higher score
        } else {
            1.0
        };

        // Depth score (more levels = better liquidity)
        let depth_score = ((levels.len() as f64) / (self.volume_depth_check as f64)).min(1.0);

        // Combined liquidity score (weighted average)
        (volume_ratio * 0.6 + price_impact_score * 0.3 + depth_score * 0.1).max(0.0).min(1.0)
    }

    /// Enhanced path checking with volume validation
    #[inline(always)]
    pub fn check_path_with_volume_validation(
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

        // Get cached prices AND quantities - zero allocation, single atomic load each
        let (first_bid, first_ask) = first_book.get_cached_top_of_book();
        let (second_bid, second_ask) = second_book.get_cached_top_of_book();
        let (third_bid, third_ask) = third_book.get_cached_top_of_book();

        // Extract prices and quantities
        let (first_price, first_qty) = if path.first_is_base_to_quote {
            (first_bid?.0, first_bid?.1)
        } else {
            (first_ask?.0, first_ask?.1)
        };

        let (second_price, second_qty) = if path.second_is_base_to_quote {
            (second_bid?.0, second_bid?.1)
        } else {
            (second_ask?.0, second_ask?.1)
        };

        let (third_price, third_qty) = if path.third_is_base_to_quote {
            (third_bid?.0, third_bid?.1)
        } else {
            (third_ask?.0, third_ask?.1)
        };

        // Calculate required volumes for each leg
        let mut amount = self.start_amount;

        // Leg 1: Calculate volume needed
        let first_volume_needed = if path.first_is_base_to_quote {
            amount // We're selling base amount
        } else {
            amount / first_price // We're buying base with quote
        };

        // Calculate amount after first leg for second leg volume calculation
        amount =
            (if path.first_is_base_to_quote {
                amount * first_price
            } else {
                amount / first_price
            }) * self.fee_multiplier;

        // Leg 2: Calculate volume needed
        let second_volume_needed = if path.second_is_base_to_quote {
            amount // We're selling base amount
        } else {
            amount / second_price // We're buying base with quote
        };

        // Calculate amount after second leg for third leg volume calculation
        amount =
            (if path.second_is_base_to_quote {
                amount * second_price
            } else {
                amount / second_price
            }) * self.fee_multiplier;

        // Leg 3: Calculate volume needed
        let third_volume_needed = if path.third_is_base_to_quote {
            amount // We're selling base amount
        } else {
            amount / third_price // We're buying base with quote
        };

        // Complete the calculation
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

        // Volume validation - check if we have minimum required volume
        let min_required_volume = self.start_amount * self.min_volume_multiplier;

        // Quick volume check using cached top-of-book
        let first_volume_ok = first_qty >= first_volume_needed.max(min_required_volume);
        let second_volume_ok = second_qty >= second_volume_needed.max(min_required_volume);
        let third_volume_ok = third_qty >= third_volume_needed.max(min_required_volume);

        let min_volume_satisfied = first_volume_ok && second_volume_ok && third_volume_ok;

        // Calculate detailed liquidity scores
        let liquidity_scores = [
            self.calculate_liquidity_score(
                &first_book,
                path.first_is_base_to_quote,
                first_volume_needed
            ),
            self.calculate_liquidity_score(
                &second_book,
                path.second_is_base_to_quote,
                second_volume_needed
            ),
            self.calculate_liquidity_score(
                &third_book,
                path.third_is_base_to_quote,
                third_volume_needed
            ),
        ];

        let execution_time_ns = start_time.elapsed().as_nanos() as u64;

        Some(ArbitrageOpportunity {
            path: Arc::clone(path),
            profit_ratio,
            start_amount: self.start_amount,
            end_amount: amount,
            execution_time_ns,
            min_volume_satisfied,
            liquidity_scores,
        })
    }

    /// Handle orderbook update with volume-aware filtering
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
            self.process_paths_with_volume_check(path_indices.borrow(), symbol);
        }
    }

    /// Process paths with volume validation
    #[inline]
    fn process_paths_with_volume_check(&self, indices: &[usize], trigger_symbol: &Arc<str>) {
        let mut best_opportunity: Option<ArbitrageOpportunity> = None;

        // Check all affected paths in current thread
        for &idx in indices {
            if let Some(path) = self.all_paths.get(idx) {
                if let Some(opportunity) = self.check_path_with_volume_validation(path) {
                    // Only consider opportunities with sufficient liquidity
                    if opportunity.has_sufficient_liquidity() {
                        match &best_opportunity {
                            None => {
                                best_opportunity = Some(opportunity);
                            }
                            Some(current_best) => {
                                // Prefer opportunities with better liquidity, then profit
                                let opportunity_score =
                                    opportunity.profit_ratio *
                                    (1.0 + opportunity.liquidity_scores.iter().sum::<f64>() / 3.0);
                                let current_score =
                                    current_best.profit_ratio *
                                    (1.0 + current_best.liquidity_scores.iter().sum::<f64>() / 3.0);

                                if opportunity_score > current_score {
                                    best_opportunity = Some(opportunity);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Execute immediately if profitable and liquid
        if let Some(opportunity) = best_opportunity {
            self.execute_immediately(opportunity, trigger_symbol);
        }
    }

    /// Execute opportunity with volume validation
    #[inline]
    fn execute_immediately(&self, opportunity: ArbitrageOpportunity, trigger_symbol: &Arc<str>) {
        // Fast atomic compare-and-swap to claim execution
        if
            self.is_executing
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        {
            println!(
                "\n{}",
                "=== ARBITRAGE OPPORTUNITY (VOLUME VALIDATED) ===".bright_purple().bold()
            );
            println!("Triggered by: {}", trigger_symbol.to_string().cyan());
            println!("Opportunity: {}", opportunity.display());

            // Additional volume information
            println!("Volume Analysis:");
            println!(
                "  Min Volume Required: ${:.2}",
                self.start_amount * self.min_volume_multiplier
            );
            println!("  Liquidity Status: {}", if opportunity.has_sufficient_liquidity() {
                "✓ SUFFICIENT".green()
            } else {
                "⚠ INSUFFICIENT".yellow()
            });

            // Execute synchronously if executor available
            if let Some(ref executor) = self.executor {
                let execution_start = Instant::now();
                let result = executor.execute(&opportunity);
                let total_time_ns = execution_start.elapsed().as_nanos() as u64;

                if result.success {
                    info!(
                        "SUCCESS: Profit={:.6} {}, Exec={}μs, Total={}μs, Slippage={:.2}%, Liquidity=[{:.2},{:.2},{:.2}]",
                        result.profit_amount,
                        opportunity.path.end_asset,
                        result.execution_time_us,
                        total_time_ns / 1000,
                        result.slippage_factor * 100.0,
                        opportunity.liquidity_scores[0],
                        opportunity.liquidity_scores[1],
                        opportunity.liquidity_scores[2]
                    );
                } else {
                    info!(
                        "FAILED: {} ({}μs)",
                        result.error.unwrap_or_else(|| "Unknown".to_string()),
                        result.execution_time_us
                    );
                }
            }

            println!(
                "{}\n",
                "===============================================".bright_purple().bold()
            );

            // Update counter and release execution lock
            self.profit_counter.fetch_add(1, Ordering::Relaxed);
            self.is_executing.store(false, Ordering::Release);
        }
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
                        info!("Stats: {} scans, {} profits (volume-validated)", scans, profits);
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
            min_volume_multiplier: self.min_volume_multiplier,
            volume_depth_check: self.volume_depth_check,
            executor: self.executor.clone(),
            is_executing: self.is_executing.clone(),
        }
    }
}

/// Create ultra-fast detector with volume validation
pub fn create_ultra_fast_detector_with_volume_validation(
    orderbook_manager: Arc<OrderBookManager>,
    fee_rate: f64,
    min_profit_threshold: f64,
    paths: Vec<Arc<TriangularPath>>,
    start_amount: f64,
    executor: Option<Arc<ArbitrageExecutor>>,
    min_volume_multiplier: f64, // NEW: e.g., 2.0 means need 2x trade amount in volume
    volume_depth_check: usize, // NEW: e.g., 5 means check top 5 price levels
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
        min_volume_multiplier,
        volume_depth_check,
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

/// Convenience function with executor and volume validation
pub fn create_detector(
    orderbook_manager: Arc<OrderBookManager>,
    fee_rate: f64,
    min_profit_threshold: f64,
    paths: Vec<Arc<TriangularPath>>,
    start_amount: f64,
    executor: Arc<ArbitrageExecutor>,
    min_volume_multiplier: f64, // e.g., 2.0 = need 2x trade amount in liquidity
    volume_depth_check: usize, // e.g., 5 = check top 5 price levels for liquidity
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
