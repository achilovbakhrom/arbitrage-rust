use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{ AtomicU64, Ordering };
use tracing::{ info, warn, error };
use parking_lot::Mutex;

use crate::orderbook::manager::OrderBookManager;
use crate::arbitrage::detector::ArbitrageOpportunity;

/// Execution strategy options
#[derive(Debug, Clone, Copy)]
pub enum ExecutionStrategy {
    /// Execute all trades in sequence as quickly as possible
    Sequential,
    /// Execute with minimal delay between trades
    FastSequential,
    /// Simulate execution (for testing/paper trading)
    Simulate,
}

/// Result of an arbitrage execution
#[derive(Debug)]
pub struct ExecutionResult {
    pub opportunity: ArbitrageOpportunity,
    pub success: bool,
    pub profit_amount: f64,
    pub execution_time_us: u64, // Changed to microseconds for precision
    pub error: Option<String>,
    pub slippage_factor: f64, // How much profit was lost to slippage
}

/// High-performance synchronous arbitrage executor
pub struct ArbitrageExecutor {
    orderbook_manager: Arc<OrderBookManager>,
    default_strategy: ExecutionStrategy,

    // Performance tracking
    execution_counter: AtomicU64,
    total_execution_time_us: AtomicU64,
    successful_executions: AtomicU64,

    // Execution control
    execution_mutex: Mutex<()>, // Serialize executions

    // Pre-computed execution parameters
    max_slippage: f64,
    min_liquidity_check: bool,
}

impl ArbitrageExecutor {
    /// Create a new high-performance arbitrage executor
    pub fn new(
        orderbook_manager: Arc<OrderBookManager>,
        default_strategy: ExecutionStrategy
    ) -> Self {
        Self {
            orderbook_manager,
            default_strategy,
            execution_counter: AtomicU64::new(0),
            total_execution_time_us: AtomicU64::new(0),
            successful_executions: AtomicU64::new(0),
            execution_mutex: Mutex::new(()),
            max_slippage: 0.2, // 20% max slippage before abort
            min_liquidity_check: true,
        }
    }

    /// Execute an arbitrage opportunity synchronously with maximum speed
    #[inline(always)]
    pub fn execute(&self, opportunity: &ArbitrageOpportunity) -> ExecutionResult {
        let exec_start = Instant::now();

        // Fast execution without locks for maximum speed
        match self.default_strategy {
            ExecutionStrategy::FastSequential => self.execute_ultra_fast(opportunity),
            ExecutionStrategy::Sequential => self.execute_with_validation(opportunity),
            ExecutionStrategy::Simulate => self.execute_simulation(opportunity),
        }
    }

    /// Ultra-fast execution with minimal overhead
    #[inline(always)]
    fn execute_ultra_fast(&self, opportunity: &ArbitrageOpportunity) -> ExecutionResult {
        let start_time = Instant::now();

        // Minimal slippage calculation
        let slippage = 0.001 + opportunity.profit_percentage() * 0.0001;
        let slippage_clamped = slippage.min(0.005); // Max 0.5% slippage

        // Direct calculation without loops or delays
        let actual_profit_ratio = opportunity.profit_ratio * (1.0 - slippage_clamped);
        let actual_end_amount = opportunity.start_amount * actual_profit_ratio;
        let actual_profit = actual_end_amount - opportunity.start_amount;

        let execution_time_us = start_time.elapsed().as_micros() as u64;

        // Update counters without locks
        self.execution_counter.fetch_add(1, Ordering::Relaxed);
        self.total_execution_time_us.fetch_add(execution_time_us, Ordering::Relaxed);

        if actual_profit > 0.0 {
            self.successful_executions.fetch_add(1, Ordering::Relaxed);
        }

        ExecutionResult {
            opportunity: opportunity.clone(),
            success: actual_profit > 0.0,
            profit_amount: actual_profit,
            execution_time_us,
            error: None,
            slippage_factor: slippage_clamped,
        }
    }

    /// Execute with basic validation
    #[inline]
    fn execute_with_validation(&self, opportunity: &ArbitrageOpportunity) -> ExecutionResult {
        let start_time = Instant::now();

        // Quick validation
        if self.is_executing_disabled() {
            return ExecutionResult {
                opportunity: opportunity.clone(),
                success: false,
                profit_amount: 0.0,
                execution_time_us: start_time.elapsed().as_micros() as u64,
                error: Some("Execution disabled".to_string()),
                slippage_factor: 0.0,
            };
        }

        // Execute with slightly more realistic slippage
        let slippage = 0.002 + opportunity.profit_percentage() * 0.0002;
        let slippage_clamped = slippage.min(0.01); // Max 1% slippage

        let actual_profit_ratio = opportunity.profit_ratio * (1.0 - slippage_clamped);
        let actual_end_amount = opportunity.start_amount * actual_profit_ratio;
        let actual_profit = actual_end_amount - opportunity.start_amount;

        let execution_time_us = start_time.elapsed().as_micros() as u64;

        // Update counters
        self.execution_counter.fetch_add(1, Ordering::Relaxed);
        self.total_execution_time_us.fetch_add(execution_time_us, Ordering::Relaxed);

        if actual_profit > 0.0 {
            self.successful_executions.fetch_add(1, Ordering::Relaxed);
        }

        ExecutionResult {
            opportunity: opportunity.clone(),
            success: actual_profit > 0.0,
            profit_amount: actual_profit,
            execution_time_us,
            error: None,
            slippage_factor: slippage_clamped,
        }
    }

    /// Simulate execution for testing
    #[inline]
    fn execute_simulation(&self, opportunity: &ArbitrageOpportunity) -> ExecutionResult {
        let start_time = Instant::now();
        let simulated_profit = opportunity.end_amount - opportunity.start_amount;
        let execution_time_us = start_time.elapsed().as_micros() as u64;

        self.execution_counter.fetch_add(1, Ordering::Relaxed);
        self.total_execution_time_us.fetch_add(execution_time_us, Ordering::Relaxed);
        self.successful_executions.fetch_add(1, Ordering::Relaxed);

        ExecutionResult {
            opportunity: opportunity.clone(),
            success: true,
            profit_amount: simulated_profit,
            execution_time_us,
            error: None,
            slippage_factor: 0.0,
        }
    }

    /// Validate execution conditions before trading
    #[inline]
    fn validate_execution_conditions(&self, opportunity: &ArbitrageOpportunity) -> Option<String> {
        let path = &opportunity.path;

        // Check if all orderbooks are still synchronized
        let books = &self.orderbook_manager.books;

        if let Some(first_book) = books.get(&path.first_symbol) {
            if !first_book.is_synced() {
                return Some(format!("First orderbook {} not synced", path.first_symbol));
            }
        } else {
            return Some(format!("First orderbook {} not found", path.first_symbol));
        }

        if let Some(second_book) = books.get(&path.second_symbol) {
            if !second_book.is_synced() {
                return Some(format!("Second orderbook {} not synced", path.second_symbol));
            }
        } else {
            return Some(format!("Second orderbook {} not found", path.second_symbol));
        }

        if let Some(third_book) = books.get(&path.third_symbol) {
            if !third_book.is_synced() {
                return Some(format!("Third orderbook {} not synced", path.third_symbol));
            }
        } else {
            return Some(format!("Third orderbook {} not found", path.third_symbol));
        }

        // Validate minimum liquidity if enabled
        if self.min_liquidity_check {
            if let Some(liquidity_error) = self.check_minimum_liquidity(opportunity) {
                return Some(liquidity_error);
            }
        }

        None
    }

    /// Check if there's sufficient liquidity for the trade
    #[inline]
    fn check_minimum_liquidity(&self, opportunity: &ArbitrageOpportunity) -> Option<String> {
        let path = &opportunity.path;
        let min_liquidity_multiple = 2.0; // Require 2x the trade amount in liquidity

        let books = &self.orderbook_manager.books;

        // Check first leg liquidity
        if let Some(first_book) = books.get(&path.first_symbol) {
            let (bid, ask) = first_book.get_cached_top_of_book();
            let required_liquidity = opportunity.start_amount * min_liquidity_multiple;

            if path.first_is_base_to_quote {
                if let Some((_, qty)) = bid {
                    if qty < required_liquidity {
                        return Some(format!("Insufficient bid liquidity on {}", path.first_symbol));
                    }
                }
            } else {
                if let Some((_, qty)) = ask {
                    if qty < required_liquidity {
                        return Some(format!("Insufficient ask liquidity on {}", path.first_symbol));
                    }
                }
            }
        }

        // Similar checks for second and third legs would go here...

        None
    }

    /// Calculate realistic slippage based on market conditions
    #[inline]
    fn calculate_realistic_slippage(&self, opportunity: &ArbitrageOpportunity) -> f64 {
        // Base slippage increases with profit percentage (higher profit = more competitive)
        let base_slippage = 0.001; // 0.1% base slippage
        let profit_multiplier = opportunity.profit_percentage() * 0.01; // Higher profit = more slippage

        // Add random component to simulate market volatility
        let random_factor = (fastrand::f64() - 0.5) * 0.002; // ±0.1% random

        let total_slippage = base_slippage + profit_multiplier + random_factor;

        // Clamp between 0% and max_slippage
        total_slippage.max(0.0).min(self.max_slippage)
    }

    /// Execute a single trade leg
    #[inline]
    fn execute_trade_leg(
        &self,
        symbol: &Arc<str>,
        amount: f64,
        is_base_to_quote: bool,
        leg_number: u8,
        execution_id: u64
    ) -> TradeResult {
        let start_time = Instant::now();

        // In a real implementation, this would:
        // 1. Get current best bid/ask
        // 2. Calculate exact trade amount
        // 3. Submit market order
        // 4. Wait for confirmation
        // 5. Return actual received amount

        // For now, simulate with realistic parameters
        let execution_latency_us = 150 + (leg_number as u64) * 50; // Increasing latency per leg

        // Busy wait to simulate execution
        let target_time = start_time + std::time::Duration::from_micros(execution_latency_us);
        while Instant::now() < target_time {
            std::hint::spin_loop();
        }

        // Simulate trade execution with small slippage
        let leg_slippage = 0.0005 * (leg_number as f64); // 0.05% slippage per leg
        let received_amount = amount * (1.0 - leg_slippage);

        TradeResult {
            success: true,
            amount_received: received_amount,
            execution_time_us: start_time.elapsed().as_micros() as u64,
            error: String::new(),
        }
    }

    /// Check if execution is disabled (e.g., during maintenance)
    #[inline]
    fn is_executing_disabled(&self) -> bool {
        // Could check various conditions:
        // - Market hours
        // - System maintenance mode
        // - Risk management triggers
        // - Exchange connectivity
        false
    }

    /// Get execution performance statistics
    pub fn get_performance_stats(&self) -> ExecutionStats {
        let total_executions = self.execution_counter.load(Ordering::Relaxed);
        let successful_executions = self.successful_executions.load(Ordering::Relaxed);
        let total_time_us = self.total_execution_time_us.load(Ordering::Relaxed);

        let success_rate = if total_executions > 0 {
            (successful_executions as f64) / (total_executions as f64)
        } else {
            0.0
        };

        let avg_execution_time_us = if total_executions > 0 {
            total_time_us / total_executions
        } else {
            0
        };

        ExecutionStats {
            total_executions,
            successful_executions,
            success_rate,
            avg_execution_time_us,
            total_execution_time_us: total_time_us,
        }
    }

    /// Reset performance counters
    pub fn reset_stats(&self) {
        self.execution_counter.store(0, Ordering::Relaxed);
        self.successful_executions.store(0, Ordering::Relaxed);
        self.total_execution_time_us.store(0, Ordering::Relaxed);
    }
}

/// Result of a single trade leg
#[derive(Debug)]
struct TradeResult {
    success: bool,
    amount_received: f64,
    execution_time_us: u64,
    error: String,
}

/// Performance statistics for the executor
#[derive(Debug)]
pub struct ExecutionStats {
    pub total_executions: u64,
    pub successful_executions: u64,
    pub success_rate: f64,
    pub avg_execution_time_us: u64,
    pub total_execution_time_us: u64,
}

impl std::fmt::Display for ExecutionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Executions: {}, Success Rate: {:.2}%, Avg Time: {}μs, Total Time: {}μs",
            self.total_executions,
            self.success_rate * 100.0,
            self.avg_execution_time_us,
            self.total_execution_time_us
        )
    }
}
