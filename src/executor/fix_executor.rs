use std::sync::{ Arc, Mutex };
use std::time::{ Instant, Duration };
use std::sync::atomic::{ AtomicU64, AtomicBool, Ordering };
use std::collections::HashSet;
use std::thread;

use anyhow::{ Result, Context };
use tracing::{ info, warn, debug, error };
use crate::arbitrage::detector::{ ArbitrageOpportunity, ExecutionCallback };
use crate::exchange::fix_api::{ FixClient, MarketOrder, OrderSide, FixError };
use crate::exchange::mock_fix_api::MockFixClient;
use crate::models::symbol_map::SymbolMap;

#[derive(Debug)]
pub struct ExecutionStats {
    pub total_executions: u64,
    pub successful_executions: u64,
    pub rejected_executions: u64,
    pub failed_executions: u64,
    pub success_rate: f64,
    pub avg_execution_time_us: u64,
}

impl std::fmt::Display for ExecutionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FIX Executions: {} total, {} successful, {} rejected, {} failed, Success Rate: {:.2}%, Avg Time: {}μs",
            self.total_executions,
            self.successful_executions,
            self.rejected_executions,
            self.failed_executions,
            self.success_rate * 100.0,
            self.avg_execution_time_us
        )
    }
}

#[derive(Debug, Clone)]
struct TradeResult {
    success: bool,
    filled_qty: f64,
    avg_price: f64,
    cum_quote_qty: f64,
    error_message: Option<String>,
}

pub struct FixArbitrageExecutor {
    real_fix_client: Option<Arc<Mutex<FixClient>>>,
    mock_fix_client: Option<Arc<Mutex<MockFixClient>>>,
    symbol_map: Arc<SymbolMap>,
    is_debug_mode: bool,
    execution_counter: Arc<AtomicU64>,
    successful_executions: Arc<AtomicU64>,
    failed_executions: Arc<AtomicU64>,
    rejected_executions: Arc<AtomicU64>,
    total_execution_time_us: Arc<AtomicU64>,
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
            failed_executions: Arc::new(AtomicU64::new(0)),
            rejected_executions: Arc::new(AtomicU64::new(0)),
            total_execution_time_us: Arc::new(AtomicU64::new(0)),
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

        let real_client = self.real_fix_client.clone();
        let mock_client = self.mock_fix_client.clone();
        let is_debug = self.is_debug_mode;
        let symbol_map = self.symbol_map.clone();
        let executed_counter = self.execution_counter.clone();
        let successful_counter = self.successful_executions.clone();
        let failed_counter = self.failed_executions.clone();
        let rejected_counter = self.rejected_executions.clone();
        let time_counter = self.total_execution_time_us.clone();
        let active_symbols = self.active_symbols.clone();
        let global_lock = self.global_execution_lock.clone();
        let last_exec_time = self.last_execution_time.clone();
        let delay_ms = self.execution_delay_ms;
        let connection_health = self.connection_health.clone();

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
                rejected_counter.fetch_add(1, Ordering::Relaxed);
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

            info!("🔄 Starting triangular arbitrage execution: {}", opportunity.display());

            let success = Self::execute_triangular_arbitrage_safe(
                &opportunity,
                &real_client,
                &mock_client,
                is_debug,
                &symbol_map,
                &connection_health
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

            match success {
                Ok(true) => {
                    successful_counter.fetch_add(1, Ordering::Relaxed);
                    info!(
                        "✅ FIX triangular arbitrage completed: {:.4}% profit in {:.2}ms",
                        opportunity.profit_percentage(),
                        (execution_time as f64) / 1000.0
                    );
                }
                Ok(false) => {
                    failed_counter.fetch_add(1, Ordering::Relaxed);
                    warn!("⚠️  FIX execution partially failed: {}", opportunity.display());
                }
                Err(e) => {
                    failed_counter.fetch_add(1, Ordering::Relaxed);
                    error!("❌ FIX execution failed: {} - Error: {}", opportunity.display(), e);

                    if e.to_string().contains("Connection") || e.to_string().contains("TLS") {
                        connection_health.store(false, Ordering::Relaxed);
                        error!("🔴 Connection health marked as unhealthy due to error: {}", e);
                    }
                }
            }
        })
    }

    fn execute_triangular_arbitrage_safe(
        opportunity: &ArbitrageOpportunity,
        real_fix_client: &Option<Arc<Mutex<FixClient>>>,
        mock_fix_client: &Option<Arc<Mutex<MockFixClient>>>,
        is_debug_mode: bool,
        symbol_map: &Arc<SymbolMap>,
        connection_health: &Arc<AtomicBool>
    ) -> Result<bool> {
        if is_debug_mode {
            if let Some(mock_client) = mock_fix_client {
                let mut client = mock_client.lock().unwrap();
                Self::execute_mock_triangular_trades_safe(opportunity, &mut client, symbol_map)
            } else {
                Err(anyhow::anyhow!("Mock client not available"))
            }
        } else {
            if let Some(real_client) = real_fix_client {
                let mut client = real_client.lock().unwrap();
                Self::execute_real_triangular_trades_safe(
                    opportunity,
                    &mut client,
                    symbol_map,
                    connection_health
                )
            } else {
                Err(anyhow::anyhow!("Real client not available"))
            }
        }
    }

    fn execute_real_triangular_trades_safe(
        opportunity: &ArbitrageOpportunity,
        client: &mut FixClient,
        symbol_map: &Arc<SymbolMap>,
        connection_health: &Arc<AtomicBool>
    ) -> Result<bool> {
        if !client.is_connected() {
            return Err(anyhow::anyhow!("FIX client is not connected"));
        }

        let path = &opportunity.path;
        let symbols = [&path.first_symbol, &path.second_symbol, &path.third_symbol];
        let directions = [
            path.first_is_base_to_quote,
            path.second_is_base_to_quote,
            path.third_is_base_to_quote,
        ];

        let mut current_amount = opportunity.start_amount;
        let mut all_successful = true;

        for (i, &symbol) in symbols.iter().enumerate() {
            if !client.is_connected() {
                error!("FIX client disconnected during execution");
                connection_health.store(false, Ordering::Relaxed);
                return Err(anyhow::anyhow!("FIX client disconnected"));
            }

            let precision_info = symbol_map.get(symbol);
            let (qty_precision, price_precision) = if let Some(info) = precision_info {
                (info.qty_precision, info.price_precision)
            } else {
                (8, 8)
            };

            let order = if directions[i] {
                MarketOrder {
                    symbol: symbol.to_string(),
                    side: OrderSide::Sell,
                    quantity: current_amount,
                    quote_quantity: None,
                    qty_precision: Some(qty_precision),
                    price_precision: Some(price_precision),
                }
            } else {
                MarketOrder {
                    symbol: symbol.to_string(),
                    side: OrderSide::Buy,
                    quantity: 0.0,
                    quote_quantity: Some(current_amount),
                    qty_precision: Some(qty_precision),
                    price_precision: Some(price_precision),
                }
            };

            let trade_start = Instant::now();

            match client.place_market_order(order) {
                Ok(result) => {
                    let trade_time = trade_start.elapsed();

                    if result.order_status == "REJECTED" || result.exec_type == "REJECTED" {
                        error!("Order rejected: {:?}", result.reject_reason);
                        all_successful = false;
                        break;
                    }

                    debug!(
                        "Trade {}/{} placed: {} {} leaves_qty={:.8} in {:.2}ms",
                        i + 1,
                        symbols.len(),
                        result.side,
                        symbol,
                        result.leaves_qty,
                        trade_time.as_secs_f64() * 1000.0
                    );

                    if i < symbols.len() - 1 {
                        current_amount = result.leaves_qty;
                        if current_amount <= 0.0 {
                            error!("Invalid leaves_qty for next trade: {:.8}", current_amount);
                            all_successful = false;
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("Trade {}/{} failed: {}", i + 1, symbols.len(), e);
                    all_successful = false;
                    break;
                }
            }

            if i < symbols.len() - 1 {
                thread::sleep(Duration::from_millis(100));
            }
        }

        Ok(all_successful)
    }

    fn execute_mock_triangular_trades_safe(
        opportunity: &ArbitrageOpportunity,
        client: &mut MockFixClient,
        symbol_map: &Arc<SymbolMap>
    ) -> Result<bool> {
        let path = &opportunity.path;
        let symbols = [&path.first_symbol, &path.second_symbol, &path.third_symbol];
        let directions = [
            path.first_is_base_to_quote,
            path.second_is_base_to_quote,
            path.third_is_base_to_quote,
        ];

        let mut current_amount = opportunity.start_amount;
        let mut all_successful = true;

        for (i, &symbol) in symbols.iter().enumerate() {
            let precision_info = symbol_map.get(symbol);
            let (qty_precision, price_precision) = if let Some(info) = precision_info {
                (info.qty_precision, info.price_precision)
            } else {
                (8, 8)
            };

            let order = if directions[i] {
                MarketOrder {
                    symbol: symbol.to_string(),
                    side: OrderSide::Sell,
                    quantity: current_amount,
                    quote_quantity: None,
                    qty_precision: Some(qty_precision),
                    price_precision: Some(price_precision),
                }
            } else {
                MarketOrder {
                    symbol: symbol.to_string(),
                    side: OrderSide::Buy,
                    quantity: 0.0,
                    quote_quantity: Some(current_amount),
                    qty_precision: Some(qty_precision),
                    price_precision: Some(price_precision),
                }
            };

            match client.place_market_order(order) {
                Ok(result) => {
                    debug!(
                        "Mock trade {}/{}: {} @ {:.8}",
                        i + 1,
                        symbols.len(),
                        result.filled_qty,
                        result.avg_price
                    );

                    if i < symbols.len() - 1 {
                        current_amount = if directions[i] {
                            result.cum_quote_qty
                        } else {
                            result.filled_qty
                        };
                    }
                }
                Err(e) => {
                    error!("Mock trade {}/{} failed: {}", i + 1, symbols.len(), e);
                    all_successful = false;
                    break;
                }
            }

            thread::sleep(Duration::from_millis(50));
        }

        Ok(all_successful)
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
                self.connection_health.store(true, Ordering::Relaxed);
                info!("Connected to Real FIX API for production trading");
            }
        }
        Ok(())
    }

    pub fn disconnect(&self) -> Result<()> {
        info!("Disconnecting FIX arbitrage executor...");

        self.shutdown.store(true, Ordering::Relaxed);
        self.connection_health.store(false, Ordering::Relaxed);

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

        info!("FIX arbitrage executor disconnected gracefully");
        Ok(())
    }

    pub fn place_single_order(&self, order: MarketOrder) -> bool {
        if let Some(real_client) = &self.real_fix_client {
            let mut client = real_client.lock().unwrap();
            let result = client.place_market_order(order).unwrap();
            info!("result: {:?}", result);
            return true;
        }
        return false;
    }

    pub fn force_execute_opportunity(&self, opportunity: ArbitrageOpportunity) -> bool {
        if !self.connection_health.load(Ordering::Relaxed) {
            error!("Cannot execute - connection is unhealthy");
            return false;
        }

        if
            self.global_execution_lock
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
        {
            warn!("Cannot execute - another execution in progress");
            return false;
        }

        let success = if self.is_debug_mode {
            if let Some(mock_client) = &self.mock_fix_client {
                let mut client = mock_client.lock().unwrap();
                Self::execute_mock_triangular_trades_safe(
                    &opportunity,
                    &mut client,
                    &self.symbol_map
                ).unwrap_or(false)
            } else {
                false
            }
        } else {
            if let Some(real_client) = &self.real_fix_client {
                let mut client = real_client.lock().unwrap();
                Self::execute_real_triangular_trades_safe(
                    &opportunity,
                    &mut client,
                    &self.symbol_map,
                    &self.connection_health
                ).unwrap_or(false)
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

    pub fn get_execution_stats(&self) -> ExecutionStats {
        let total = self.execution_counter.load(Ordering::Relaxed);
        let successful = self.successful_executions.load(Ordering::Relaxed);
        let rejected = self.rejected_executions.load(Ordering::Relaxed);
        let failed = self.failed_executions.load(Ordering::Relaxed);
        let total_time = self.total_execution_time_us.load(Ordering::Relaxed);

        let success_rate = if total > 0 { (successful as f64) / (total as f64) } else { 0.0 };

        let avg_time = if total > 0 { total_time / total } else { 0 };

        ExecutionStats {
            total_executions: total,
            successful_executions: successful,
            rejected_executions: rejected,
            failed_executions: failed,
            success_rate,
            avg_execution_time_us: avg_time,
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.connection_health.load(Ordering::Relaxed)
    }

    pub fn reset_connection_health(&self) {
        self.connection_health.store(true, Ordering::Relaxed);
        info!("Connection health reset to healthy");
    }
}
