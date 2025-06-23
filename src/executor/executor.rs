use std::time::Instant;
use std::sync::atomic::{ AtomicU64, Ordering };

use crate::arbitrage::detector::ArbitrageOpportunity;

#[derive(Debug, Clone, Copy)]
pub enum ExecutionStrategy {
    Sequential,
    FastSequential,
    Simulate,
}

#[derive(Debug)]
pub struct ExecutionResult {
    pub success: bool,
}

pub struct ArbitrageExecutor {
    default_strategy: ExecutionStrategy,

    execution_counter: AtomicU64,
    total_execution_time_us: AtomicU64,
    successful_executions: AtomicU64,
}

impl ArbitrageExecutor {
    pub fn new(default_strategy: ExecutionStrategy) -> Self {
        Self {
            default_strategy,
            execution_counter: AtomicU64::new(0),
            total_execution_time_us: AtomicU64::new(0),
            successful_executions: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn execute(&self, opportunity: &ArbitrageOpportunity) -> ExecutionResult {
        match self.default_strategy {
            ExecutionStrategy::FastSequential => self.execute_ultra_fast(opportunity),
            ExecutionStrategy::Sequential => self.execute_with_validation(opportunity),
            ExecutionStrategy::Simulate => self.execute_simulation(opportunity),
        }
    }

    #[inline(always)]
    fn execute_ultra_fast(&self, opportunity: &ArbitrageOpportunity) -> ExecutionResult {
        let start_time = Instant::now();

        let slippage = 0.001 + opportunity.profit_percentage() * 0.0001;
        let slippage_clamped = slippage.min(0.005);

        let actual_profit_ratio = opportunity.profit_ratio * (1.0 - slippage_clamped);
        let actual_end_amount = opportunity.start_amount * actual_profit_ratio;
        let actual_profit = actual_end_amount - opportunity.start_amount;

        let execution_time_us = start_time.elapsed().as_micros() as u64;

        self.execution_counter.fetch_add(1, Ordering::Relaxed);
        self.total_execution_time_us.fetch_add(execution_time_us, Ordering::Relaxed);

        if actual_profit > 0.0 {
            self.successful_executions.fetch_add(1, Ordering::Relaxed);
        }

        ExecutionResult {
            success: actual_profit > 0.0,
        }
    }

    #[inline]
    fn execute_with_validation(&self, opportunity: &ArbitrageOpportunity) -> ExecutionResult {
        let start_time = Instant::now();

        if self.is_executing_disabled() {
            return ExecutionResult {
                success: false,
            };
        }

        let slippage = 0.002 + opportunity.profit_percentage() * 0.0002;
        let slippage_clamped = slippage.min(0.01);

        let actual_profit_ratio = opportunity.profit_ratio * (1.0 - slippage_clamped);
        let actual_end_amount = opportunity.start_amount * actual_profit_ratio;
        let actual_profit = actual_end_amount - opportunity.start_amount;

        let execution_time_us = start_time.elapsed().as_micros() as u64;

        self.execution_counter.fetch_add(1, Ordering::Relaxed);
        self.total_execution_time_us.fetch_add(execution_time_us, Ordering::Relaxed);

        if actual_profit > 0.0 {
            self.successful_executions.fetch_add(1, Ordering::Relaxed);
        }

        ExecutionResult {
            success: actual_profit > 0.0,
        }
    }

    #[inline]
    fn execute_simulation(&self, opportunity: &ArbitrageOpportunity) -> ExecutionResult {
        let start_time = Instant::now();
        let simulated_profit = opportunity.end_amount - opportunity.start_amount;
        let execution_time_us = start_time.elapsed().as_micros() as u64;

        self.execution_counter.fetch_add(1, Ordering::Relaxed);
        self.total_execution_time_us.fetch_add(execution_time_us, Ordering::Relaxed);
        self.successful_executions.fetch_add(1, Ordering::Relaxed);

        ExecutionResult {
            success: true,
        }
    }

    #[inline]
    fn is_executing_disabled(&self) -> bool {
        // Could check various conditions:
        // - Market hours
        // - System maintenance mode
        // - Risk management triggers
        // - Exchange connectivity
        false
    }
}
