use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Guards against concurrent arbitrage executions to prevent resource conflicts
pub struct ExecutionGuard {
    is_executing: AtomicBool,
    last_execution_start: std::sync::Mutex<Option<Instant>>,
}

impl ExecutionGuard {
    pub fn new() -> Self {
        Self {
            is_executing: AtomicBool::new(false),
            last_execution_start: std::sync::Mutex::new(None),
        }
    }

    /// Try to acquire execution lock. Returns true if acquired, false if already executing
    pub fn try_acquire(&self) -> bool {
        match self.is_executing.compare_exchange(
            false, 
            true, 
            Ordering::Acquire, 
            Ordering::Relaxed
        ) {
            Ok(_) => {
                // Successfully acquired lock
                if let Ok(mut last_start) = self.last_execution_start.lock() {
                    *last_start = Some(Instant::now());
                }
                debug!("Execution lock acquired");
                true
            }
            Err(_) => {
                // Lock already held
                self.log_blocked_execution();
                false
            }
        }
    }

    /// Release the execution lock
    pub fn release(&self) {
        self.is_executing.store(false, Ordering::Release);
        debug!("Execution lock released");
    }

    /// Check if execution is currently in progress
    pub fn is_executing(&self) -> bool {
        self.is_executing.load(Ordering::Acquire)
    }

    /// Get the duration of current execution if in progress
    pub fn current_execution_duration(&self) -> Option<Duration> {
        if self.is_executing() {
            if let Ok(last_start) = self.last_execution_start.lock() {
                last_start.map(|start| start.elapsed())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn log_blocked_execution(&self) {
        if let Some(duration) = self.current_execution_duration() {
            warn!(
                "🚫 BLOCKED: Arbitrage execution already in progress (running for {:.2}s). Skipping new opportunity.",
                duration.as_secs_f64()
            );
        } else {
            warn!("🚫 BLOCKED: Arbitrage execution already in progress. Skipping new opportunity.");
        }
    }
}

/// RAII guard that automatically releases the execution lock when dropped
pub struct ExecutionLockGuard<'a> {
    guard: &'a ExecutionGuard,
    acquired: bool,
}

impl<'a> ExecutionLockGuard<'a> {
    pub fn new(guard: &'a ExecutionGuard) -> Self {
        let acquired = guard.try_acquire();
        Self { guard, acquired }
    }

    pub fn is_acquired(&self) -> bool {
        self.acquired
    }
}

impl<'a> Drop for ExecutionLockGuard<'a> {
    fn drop(&mut self) {
        if self.acquired {
            self.guard.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_execution_guard_basic() {
        let guard = ExecutionGuard::new();
        
        // Should be able to acquire initially
        assert!(guard.try_acquire());
        assert!(guard.is_executing());
        
        // Should not be able to acquire while already executing
        assert!(!guard.try_acquire());
        
        // Should be able to acquire after release
        guard.release();
        assert!(!guard.is_executing());
        assert!(guard.try_acquire());
        
        guard.release();
    }

    #[test]
    fn test_raii_guard() {
        let guard = ExecutionGuard::new();
        
        {
            let lock = ExecutionLockGuard::new(&guard);
            assert!(lock.is_acquired());
            assert!(guard.is_executing());
        }
        
        // Should be released after guard drops
        assert!(!guard.is_executing());
    }

    #[test]
    fn test_concurrent_access() {
        let guard = std::sync::Arc::new(ExecutionGuard::new());
        let guard_clone = guard.clone();
        
        // First thread acquires lock
        let handle = thread::spawn(move || {
            let _lock = ExecutionLockGuard::new(&guard_clone);
            thread::sleep(Duration::from_millis(100));
        });
        
        // Give first thread time to acquire
        thread::sleep(Duration::from_millis(10));
        
        // Second thread should fail to acquire
        assert!(!guard.try_acquire());
        
        handle.join().unwrap();
        
        // Should be able to acquire after first thread completes
        assert!(guard.try_acquire());
        guard.release();
    }
}