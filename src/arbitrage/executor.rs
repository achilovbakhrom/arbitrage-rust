use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{ AtomicU64, Ordering };

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
        }
    }

    /// Execute an arbitrage opportunity synchronously with maximum speed
    #[inline(always)]
    pub fn execute(&self, opportunity: &ArbitrageOpportunity) -> ExecutionResult {
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
            success_rate,
            avg_execution_time_us,
            total_execution_time_us: total_time_us,
        }
    }
}

/// Performance statistics for the executor
#[derive(Debug)]
pub struct ExecutionStats {
    pub total_executions: u64,
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
