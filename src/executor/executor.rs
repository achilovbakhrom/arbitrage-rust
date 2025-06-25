use std::sync::atomic::AtomicU64;

#[derive(Debug, Clone, Copy)]
pub enum ExecutionStrategy {
    Sequential,
    FastSequential,
    Simulate,
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
}
