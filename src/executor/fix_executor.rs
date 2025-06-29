use std::sync::{ Arc, Mutex };
use std::time::{ Instant, Duration };
use std::sync::atomic::{ AtomicU64, AtomicBool, Ordering };
use std::collections::HashSet;
use std::thread;

use anyhow::{ Result, Context };
use tracing::{ info, warn, debug };
use crate::arbitrage::detector::{ ArbitrageOpportunity, ExecutionCallback };
use crate::exchange::fix_api::{ FixClient, MarketOrder, OrderSide };
use crate::exchange::mock_fix_api::MockFixClient;
use crate::models::symbol_map::SymbolMap;

pub struct FixArbitrageExecutor {
    real_fix_client: Option<Arc<Mutex<FixClient>>>,
    mock_fix_client: Option<Arc<Mutex<MockFixClient>>>,
    symbol_map: Arc<SymbolMap>,
    is_debug_mode: bool,
    execution_counter: Arc<AtomicU64>,
    successful_executions: Arc<AtomicU64>,
    total_execution_time_us: Arc<AtomicU64>,
    rejected_counter: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    active_symbols: Arc<Mutex<HashSet<u16>>>,
    global_execution_lock: Arc<AtomicBool>,
    last_execution_time: Arc<Mutex<Option<Instant>>>,
    execution_delay_ms: u64,
}

impl FixArbitrageExecutor {
    pub fn new(
        is_debug_mode: bool,
        fix_config: Option<crate::exchange::fix_api::FixConfig>,
        symbol_map: Arc<SymbolMap>
    ) -> Result<Self> {
        let (real_fix_client, mock_fix_client) = if is_debug_mode {
            let mock_client = MockFixClient::new();
            (None, Some(Arc::new(Mutex::new(mock_client))))
        } else {
            if let Some(config) = fix_config {
                let real_client = FixClient::new(config).context("Failed to create FIX client")?;

                for symbol in symbol_map.get_all_symbols() {
                    real_client.update_symbol_precision(
                        &symbol.symbol,
                        symbol.qty_precision,
                        symbol.price_precision
                    );
                }

                (Some(Arc::new(Mutex::new(real_client))), None)
            } else {
                return Err(anyhow::anyhow!("FIX configuration required for production mode"));
            }
        };

        Ok(Self {
            real_fix_client,
            mock_fix_client,
            symbol_map,
            is_debug_mode,
            execution_counter: Arc::new(AtomicU64::new(0)),
            successful_executions: Arc::new(AtomicU64::new(0)),
            total_execution_time_us: Arc::new(AtomicU64::new(0)),
            rejected_counter: Arc::new(AtomicU64::new(0)),
            shutdown: Arc::new(AtomicBool::new(false)),
            active_symbols: Arc::new(Mutex::new(HashSet::with_capacity(100))),
            global_execution_lock: Arc::new(AtomicBool::new(false)),
            last_execution_time: Arc::new(Mutex::new(None)),
            execution_delay_ms: 3500,
        })
    }

    pub fn get_callback(&self) -> ExecutionCallback {
        info!("Enabled callback-based execution with global throttling");

        let real_client = self.real_fix_client.clone();
        let mock_client = self.mock_fix_client.clone();
        let is_debug = self.is_debug_mode;
        let symbol_map = self.symbol_map.clone();
        let executed_counter = self.execution_counter.clone();
        let successful_counter = self.successful_executions.clone();
        let time_counter = self.total_execution_time_us.clone();
        let rejected_counter = self.rejected_counter.clone();
        let active_symbols = self.active_symbols.clone();
        let global_lock = self.global_execution_lock.clone();
        let last_exec_time = self.last_execution_time.clone();
        let delay_ms = self.execution_delay_ms;

        Arc::new(move |opportunity: ArbitrageOpportunity| {
            if
                global_lock
                    .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
            {
                rejected_counter.fetch_add(1, Ordering::Relaxed);
                return;
            }

            {
                let last_time_guard = last_exec_time.lock().unwrap();
                if let Some(last_time) = *last_time_guard {
                    let elapsed = last_time.elapsed();
                    if elapsed < Duration::from_millis(delay_ms) {
                        let remaining = Duration::from_millis(delay_ms) - elapsed;
                        drop(last_time_guard);
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
                rejected_counter.fetch_add(1, Ordering::Relaxed);
                debug!("Rejected opportunity due to symbol conflict: {:?}", symbols);
                return;
            }

            {
                let mut active = active_symbols.lock().unwrap();
                for &symbol in symbols {
                    active.insert(symbol);
                }
            }

            let success = Self::execute_triangular_arbitrage_fast(
                &opportunity,
                &real_client,
                &mock_client,
                is_debug,
                &symbol_map
            );

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

            executed_counter.fetch_add(1, Ordering::Relaxed);
            time_counter.fetch_add(execution_time, Ordering::Relaxed);

            if success {
                successful_counter.fetch_add(1, Ordering::Relaxed);
                info!(
                    "FIX executed: {:.4}% profit in {}μs",
                    opportunity.profit_percentage(),
                    execution_time / 1000
                );
            } else {
                warn!("FIX execution failed: {}", opportunity.display());
            }
        })
    }

    fn execute_triangular_arbitrage_fast(
        opportunity: &ArbitrageOpportunity,
        real_fix_client: &Option<Arc<Mutex<FixClient>>>,
        mock_fix_client: &Option<Arc<Mutex<MockFixClient>>>,
        is_debug_mode: bool,
        symbol_map: &Arc<SymbolMap>
    ) -> bool {
        if is_debug_mode {
            if let Some(mock_client) = mock_fix_client {
                if let Ok(mut client) = mock_client.try_lock() {
                    let result = Self::execute_mock_triangular_trades_fast(
                        opportunity,
                        &mut client,
                        symbol_map
                    );
                    result.is_ok()
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            if let Some(real_client) = real_fix_client {
                if let Ok(mut client) = real_client.try_lock() {
                    let result = Self::execute_real_triangular_trades_fast(
                        opportunity,
                        &mut client,
                        symbol_map
                    );
                    result.is_ok()
                } else {
                    false
                }
            } else {
                false
            }
        }
    }

    fn execute_mock_triangular_trades_fast(
        opportunity: &ArbitrageOpportunity,
        client: &mut MockFixClient,
        symbol_map: &Arc<SymbolMap>
    ) -> Result<()> {
        let path = &opportunity.path;
        let symbols = [&path.first_symbol, &path.second_symbol, &path.third_symbol];
        let is_base_to_quote = [
            path.first_is_base_to_quote,
            path.second_is_base_to_quote,
            path.third_is_base_to_quote,
        ];

        let mut current_amount = opportunity.start_amount;

        for (i, &symbol) in symbols.iter().enumerate() {
            let precision_info = symbol_map.get(symbol);
            let (qty_precision, price_precision) = if let Some(info) = precision_info {
                (info.qty_precision, info.price_precision)
            } else {
                (8, 8)
            };

            let order = MarketOrder {
                symbol: symbol.to_string(),
                side: if is_base_to_quote[i] {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                },
                quantity: if is_base_to_quote[i] {
                    current_amount
                } else {
                    0.0
                },
                quote_quantity: if is_base_to_quote[i] {
                    None
                } else {
                    Some(current_amount)
                },
                qty_precision: Some(qty_precision),
                price_precision: Some(price_precision),
            };

            let execution_complete = Arc::new(AtomicBool::new(false));
            let execution_success = Arc::new(AtomicBool::new(false));
            let execution_amount = Arc::new(std::sync::Mutex::new(0.0));

            let complete_flag = execution_complete.clone();
            let success_flag = execution_success.clone();
            let amount_ref = execution_amount.clone();

            client.place_market_order_async(order, move |result| {
                if result.order_status == "FILLED" {
                    success_flag.store(true, Ordering::Relaxed);
                    if let Ok(mut amount) = amount_ref.try_lock() {
                        *amount = if result.side == "BUY" {
                            result.filled_qty
                        } else {
                            result.cum_quote_qty
                        };
                    }
                }
                complete_flag.store(true, Ordering::Relaxed);
            })?;

            let timeout = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !execution_complete.load(Ordering::Relaxed) {
                if std::time::Instant::now() > timeout {
                    return Err(anyhow::anyhow!("Trade {} execution timeout", i + 1));
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }

            if !execution_success.load(Ordering::Relaxed) {
                return Err(anyhow::anyhow!("Trade {} execution failed", i + 1));
            }

            if i < symbols.len() - 1 {
                current_amount = *execution_amount.lock().unwrap();
            }
        }

        Ok(())
    }

    fn execute_real_triangular_trades_fast(
        opportunity: &ArbitrageOpportunity,
        client: &mut FixClient,
        symbol_map: &Arc<SymbolMap>
    ) -> Result<()> {
        let path = &opportunity.path;
        let symbols = [&path.first_symbol, &path.second_symbol, &path.third_symbol];
        let is_base_to_quote = [
            path.first_is_base_to_quote,
            path.second_is_base_to_quote,
            path.third_is_base_to_quote,
        ];

        let mut current_amount = opportunity.start_amount;

        for (i, &symbol) in symbols.iter().enumerate() {
            let precision_info = symbol_map.get(symbol);
            let (qty_precision, price_precision) = if let Some(info) = precision_info {
                (info.qty_precision, info.price_precision)
            } else {
                (8, 8)
            };

            let order = MarketOrder {
                symbol: symbol.to_string(),
                side: if is_base_to_quote[i] {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                },
                quantity: if is_base_to_quote[i] {
                    current_amount
                } else {
                    0.0
                },
                quote_quantity: if is_base_to_quote[i] {
                    None
                } else {
                    Some(current_amount)
                },
                qty_precision: Some(qty_precision),
                price_precision: Some(price_precision),
            };

            let execution_complete = Arc::new(AtomicBool::new(false));
            let execution_success = Arc::new(AtomicBool::new(false));
            let execution_amount = Arc::new(std::sync::Mutex::new(0.0));

            let complete_flag = execution_complete.clone();
            let success_flag = execution_success.clone();
            let amount_ref = execution_amount.clone();

            client.place_market_order_async(order, move |result| {
                if result.order_status == "FILLED" {
                    success_flag.store(true, Ordering::Relaxed);
                    if let Ok(mut amount) = amount_ref.try_lock() {
                        *amount = if result.side == "BUY" {
                            result.filled_qty
                        } else {
                            result.cum_quote_qty
                        };
                    }
                }
                complete_flag.store(true, Ordering::Relaxed);
            })?;

            let timeout = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !execution_complete.load(Ordering::Relaxed) {
                if std::time::Instant::now() > timeout {
                    return Err(anyhow::anyhow!("Trade {} execution timeout", i + 1));
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }

            if !execution_success.load(Ordering::Relaxed) {
                return Err(anyhow::anyhow!("Trade {} execution failed", i + 1));
            }

            if i < symbols.len() - 1 {
                current_amount = *execution_amount.lock().unwrap();
            }
        }

        Ok(())
    }

    pub fn connect(&self) -> Result<()> {
        if self.is_debug_mode {
            if let Some(mock_client) = &self.mock_fix_client {
                let mut client = mock_client.lock().unwrap();
                client.connect()?;
                info!("Connected to Mock FIX API for debug mode");
            }
        } else {
            if let Some(real_client) = &self.real_fix_client {
                let mut client = real_client.lock().unwrap();
                client.connect()?;
                info!("Connected to Real FIX API for production trading");
            }
        }
        Ok(())
    }

    pub fn disconnect(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::Relaxed);

        if self.is_debug_mode {
            if let Some(mock_client) = &self.mock_fix_client {
                let mut client = mock_client.lock().unwrap();
                client.disconnect()?;
            }
        } else {
            if let Some(real_client) = &self.real_fix_client {
                let mut client = real_client.lock().unwrap();
                client.disconnect()?;
            }
        }

        info!("FIX arbitrage executor disconnected");
        Ok(())
    }

    pub fn force_execute_opportunity(&self, opportunity: ArbitrageOpportunity) -> bool {
        if
            self.global_execution_lock
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
        {
            return false;
        }

        let success = if self.is_debug_mode {
            if let Some(mock_client) = &self.mock_fix_client {
                if let Ok(mut client) = mock_client.try_lock() {
                    Self::execute_mock_triangular_trades_fast(
                        &opportunity,
                        &mut client,
                        &self.symbol_map
                    ).is_ok()
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            if let Some(real_client) = &self.real_fix_client {
                if let Ok(mut client) = real_client.try_lock() {
                    Self::execute_real_triangular_trades_fast(
                        &opportunity,
                        &mut client,
                        &self.symbol_map
                    ).is_ok()
                } else {
                    false
                }
            } else {
                false
            }
        };

        {
            let mut last_time_guard = self.last_execution_time.lock().unwrap();
            *last_time_guard = Some(Instant::now());
        }

        self.global_execution_lock.store(false, Ordering::Release);
        success
    }
}

#[derive(Debug)]
pub struct ExecutionStats {
    pub total_executions: u64,
    pub successful_executions: u64,
    pub rejected_executions: u64,
    pub success_rate: f64,
    pub avg_execution_time_us: u64,
}

impl std::fmt::Display for ExecutionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FIX Executions: {} total, {} successful, {} rejected, Success Rate: {:.2}%, Avg Time: {}μs",
            self.total_executions,
            self.successful_executions,
            self.rejected_executions,
            self.success_rate * 100.0,
            self.avg_execution_time_us
        )
    }
}
