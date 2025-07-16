use std::sync::{ Arc, Mutex };
use std::time::{ Instant, Duration };
use std::sync::atomic::{ AtomicBool, Ordering };
use std::collections::HashSet;
use std::thread;
use anyhow::Result;
use tracing::{ info, warn, debug, error };
use crate::arbitrage::detector::{ ArbitrageOpportunity, ExecutionCallback };
use crate::config::Config;
use crate::models::symbol_map::SymbolMap;
use crate::utils::{
    fix_ops::{ create_fix_client, execute_triangular_trades, FixClientType },
    order_ops::TriangularTradeData,
};

pub struct FixArbitrageExecutor {
    fix_client: FixClientType,
    symbol_map: Arc<SymbolMap>,
    shutdown: Arc<AtomicBool>,
    active_symbols: Arc<Mutex<HashSet<u16>>>,
    global_execution_lock: Arc<AtomicBool>,
    last_execution_time: Arc<Mutex<Option<Instant>>>,
    execution_delay_ms: u64,
    connection_health: Arc<AtomicBool>,
}

impl FixArbitrageExecutor {
    pub fn new(
        is_debug_mode: bool,
        fix_config: Option<crate::exchange::fix_api::FixConfig>,
        config: Config,
        symbol_map: Arc<SymbolMap>
    ) -> Result<Self> {
        let fix_client = create_fix_client(
            is_debug_mode,
            fix_config,
            config,
            &symbol_map
        )?.ok_or_else(|| anyhow::anyhow!("Failed to create FIX client"))?;

        Ok(Self {
            fix_client,
            symbol_map,
            shutdown: Arc::new(AtomicBool::new(false)),
            active_symbols: Arc::new(Mutex::new(HashSet::with_capacity(100))),
            global_execution_lock: Arc::new(AtomicBool::new(false)),
            last_execution_time: Arc::new(Mutex::new(None)),
            execution_delay_ms: 5000,
            connection_health: Arc::new(AtomicBool::new(true)),
        })
    }

    pub fn get_callback(&self) -> ExecutionCallback {
        info!("Enabled callback-based execution with enhanced throttling and error handling");

        let fix_client = self.fix_client.clone();
        let symbol_map = Arc::clone(&self.symbol_map);
        let active_symbols = Arc::clone(&self.active_symbols);
        let global_lock = Arc::clone(&self.global_execution_lock);
        let last_exec_time = Arc::clone(&self.last_execution_time);
        let delay_ms = self.execution_delay_ms;
        let connection_health = Arc::clone(&self.connection_health);

        Arc::new(move |opportunity: ArbitrageOpportunity| {
            if !connection_health.load(Ordering::Relaxed) {
                debug!("Skipping execution due to unhealthy connection");
                return;
            }

            if
                global_lock
                    .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
            {
                debug!("Rejected opportunity due to global execution lock");
                return;
            }

            {
                let last_time_guard = last_exec_time.lock().unwrap();
                if let Some(last_time) = *last_time_guard {
                    let elapsed = last_time.elapsed();
                    if elapsed < Duration::from_millis(delay_ms) {
                        let remaining = Duration::from_millis(delay_ms) - elapsed;
                        drop(last_time_guard);

                        info!(
                            "Waiting {:.2}s before next execution to respect rate limits",
                            remaining.as_secs_f64()
                        );
                        thread::sleep(remaining);
                    }
                }
            }

            let execution_start = Instant::now();
            let symbols = &opportunity.symbols_involved;

            let has_conflict = {
                let active = active_symbols.lock().unwrap();
                symbols.iter().any(|&sym| active.contains(&sym))
            };

            if has_conflict {
                global_lock.store(false, Ordering::Release);
                debug!("Rejected opportunity due to symbol conflict: {:?}", symbols);
                return;
            }

            {
                let mut active = active_symbols.lock().unwrap();
                for &symbol in symbols {
                    active.insert(symbol);
                }
            }

            info!("🔄 Starting triangular arbitrage execution: {}", opportunity.display());

            let trade_data = TriangularTradeData::from_opportunity(&opportunity);
            let success = execute_triangular_trades(&trade_data, &fix_client, &symbol_map);

            let execution_time = execution_start.elapsed().as_micros() as u64;

            {
                let mut active = active_symbols.lock().unwrap();
                for &symbol in symbols {
                    active.remove(&symbol);
                }
            }

            {
                let mut last_time_guard = last_exec_time.lock().unwrap();
                *last_time_guard = Some(Instant::now());
            }

            global_lock.store(false, Ordering::Release);

            match success {
                Ok(true) => {
                    info!(
                        "✅ FIX triangular arbitrage completed: {:.4}% profit in {:.2}ms",
                        opportunity.profit_percentage(),
                        (execution_time as f64) / 1000.0
                    );
                }
                Ok(false) => {
                    warn!("⚠️  FIX execution partially failed: {}", opportunity.display());
                }
                Err(e) => {
                    error!("❌ FIX execution failed: {} - Error: {}", opportunity.display(), e);

                    if e.to_string().contains("Connection") || e.to_string().contains("TLS") {
                        connection_health.store(false, Ordering::Relaxed);
                        error!("🔴 Connection health marked as unhealthy due to error: {}", e);
                    }
                }
            }
        })
    }

    pub fn connect(&self) -> Result<()> {
        self.fix_client.connect()?;
        self.connection_health.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn disconnect(&self) -> Result<()> {
        info!("Disconnecting FIX arbitrage executor...");
        self.shutdown.store(true, Ordering::Relaxed);
        self.connection_health.store(false, Ordering::Relaxed);
        self.fix_client.disconnect()?;
        info!("FIX arbitrage executor disconnected gracefully");
        Ok(())
    }
}
