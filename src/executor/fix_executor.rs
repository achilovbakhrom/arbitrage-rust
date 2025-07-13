// use std::sync::{ Arc, Mutex };
// use std::time::{ Instant, Duration };
// use std::sync::atomic::{ AtomicU64, AtomicBool, Ordering };
// use std::collections::HashSet;
// use std::thread;

// use anyhow::{ Result, Context };
// use tracing::{ info, warn, debug, error };
// use crate::arbitrage::detector::{ ArbitrageOpportunity, ExecutionCallback };
// use crate::exchange::fix_api::{ FixClient, MarketOrder, OrderSide };
// use crate::exchange::mock_fix_api::MockFixClient;
// use crate::models::symbol_map::SymbolMap;

// #[derive(Debug)]
// pub struct ExecutionStats {
//     pub total_executions: u64,
//     pub successful_executions: u64,
//     pub rejected_executions: u64,
//     pub failed_executions: u64,
//     pub success_rate: f64,
//     pub avg_execution_time_us: u64,
// }

// impl std::fmt::Display for ExecutionStats {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "FIX Executions: {} total, {} successful, {} rejected, {} failed, Success Rate: {:.2}%, Avg Time: {}μs",
//             self.total_executions,
//             self.successful_executions,
//             self.rejected_executions,
//             self.failed_executions,
//             self.success_rate * 100.0,
//             self.avg_execution_time_us
//         )
//     }
// }

// pub struct FixArbitrageExecutor {
//     real_fix_client: Option<Arc<Mutex<FixClient>>>,
//     mock_fix_client: Option<Arc<Mutex<MockFixClient>>>,
//     symbol_map: Arc<SymbolMap>,
//     is_debug_mode: bool,
//     execution_counter: Arc<AtomicU64>,
//     successful_executions: Arc<AtomicU64>,
//     failed_executions: Arc<AtomicU64>,
//     rejected_executions: Arc<AtomicU64>,
//     total_execution_time_us: Arc<AtomicU64>,
//     shutdown: Arc<AtomicBool>,
//     active_symbols: Arc<Mutex<HashSet<u16>>>,
//     global_execution_lock: Arc<AtomicBool>,
//     last_execution_time: Arc<Mutex<Option<Instant>>>,
//     execution_delay_ms: u64,
//     connection_health: Arc<AtomicBool>,
// }

// impl FixArbitrageExecutor {
//     pub fn new(
//         is_debug_mode: bool,
//         fix_config: Option<crate::exchange::fix_api::FixConfig>,
//         symbol_map: Arc<SymbolMap>
//     ) -> Result<Self> {
//         let (real_fix_client, mock_fix_client) = if is_debug_mode {
//             let mock_client = MockFixClient::new();
//             (None, Some(Arc::new(Mutex::new(mock_client))))
//         } else if let Some(config) = fix_config {
//             let real_client = FixClient::new(config).context("Failed to create FIX client")?;

//             for symbol in symbol_map.get_all_symbols() {
//                 real_client.update_symbol_precision(
//                     &symbol.symbol,
//                     symbol.qty_precision,
//                     symbol.price_precision
//                 );
//             }

//             (Some(Arc::new(Mutex::new(real_client))), None)
//         } else {
//             return Err(anyhow::anyhow!("FIX configuration required for production mode"));
//         };

//         Ok(Self {
//             real_fix_client,
//             mock_fix_client,
//             symbol_map,
//             is_debug_mode,
//             execution_counter: Arc::new(AtomicU64::new(0)),
//             successful_executions: Arc::new(AtomicU64::new(0)),
//             failed_executions: Arc::new(AtomicU64::new(0)),
//             rejected_executions: Arc::new(AtomicU64::new(0)),
//             total_execution_time_us: Arc::new(AtomicU64::new(0)),
//             shutdown: Arc::new(AtomicBool::new(false)),
//             active_symbols: Arc::new(Mutex::new(HashSet::with_capacity(100))),
//             global_execution_lock: Arc::new(AtomicBool::new(false)),
//             last_execution_time: Arc::new(Mutex::new(None)),
//             execution_delay_ms: 5000,
//             connection_health: Arc::new(AtomicBool::new(true)),
//         })
//     }

//     pub fn get_callback(&self) -> ExecutionCallback {
//         info!("Enabled callback-based execution with enhanced throttling and error handling");

//         let real_client = self.real_fix_client.clone();
//         let mock_client = self.mock_fix_client.clone();
//         let is_debug = self.is_debug_mode;
//         let symbol_map = Arc::clone(&self.symbol_map);
//         let executed_counter = Arc::clone(&self.execution_counter);
//         let successful_counter = Arc::clone(&self.successful_executions);
//         let failed_counter = Arc::clone(&self.failed_executions);
//         let rejected_counter = Arc::clone(&self.rejected_executions);
//         let time_counter = Arc::clone(&self.total_execution_time_us);
//         let active_symbols = Arc::clone(&self.active_symbols);
//         let global_lock = Arc::clone(&self.global_execution_lock);
//         let last_exec_time = Arc::clone(&self.last_execution_time);
//         let delay_ms = self.execution_delay_ms;
//         let connection_health = Arc::clone(&self.connection_health);

//         Arc::new(move |opportunity: ArbitrageOpportunity| {
//             if !connection_health.load(Ordering::Relaxed) {
//                 debug!("Skipping execution due to unhealthy connection");
//                 return;
//             }

//             if
//                 global_lock
//                     .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
//                     .is_err()
//             {
//                 rejected_counter.fetch_add(1, Ordering::Relaxed);
//                 debug!("Rejected opportunity due to global execution lock");
//                 return;
//             }

//             {
//                 let last_time_guard = last_exec_time.lock().unwrap();
//                 if let Some(last_time) = *last_time_guard {
//                     let elapsed = last_time.elapsed();
//                     if elapsed < Duration::from_millis(delay_ms) {
//                         let remaining = Duration::from_millis(delay_ms) - elapsed;
//                         drop(last_time_guard);

//                         info!(
//                             "Waiting {:.2}s before next execution to respect rate limits",
//                             remaining.as_secs_f64()
//                         );
//                         thread::sleep(remaining);
//                     }
//                 }
//             }

//             let execution_start = Instant::now();
//             let symbols = &opportunity.symbols_involved;

//             let has_conflict = {
//                 let active = active_symbols.lock().unwrap();
//                 symbols.iter().any(|&sym| active.contains(&sym))
//             };

//             if has_conflict {
//                 global_lock.store(false, Ordering::Release);
//                 rejected_counter.fetch_add(1, Ordering::Relaxed);
//                 debug!("Rejected opportunity due to symbol conflict: {:?}", symbols);
//                 return;
//             }

//             {
//                 let mut active = active_symbols.lock().unwrap();
//                 for &symbol in symbols {
//                     active.insert(symbol);
//                 }
//             }

//             info!("🔄 Starting triangular arbitrage execution: {}", opportunity.display());

//             let success = Self::execute_triangular_arbitrage_safe(
//                 &opportunity,
//                 &real_client,
//                 &mock_client,
//                 is_debug,
//                 &symbol_map,
//                 &connection_health
//             );

//             let execution_time = execution_start.elapsed().as_micros() as u64;

//             {
//                 let mut active = active_symbols.lock().unwrap();
//                 for &symbol in symbols {
//                     active.remove(&symbol);
//                 }
//             }

//             {
//                 let mut last_time_guard = last_exec_time.lock().unwrap();
//                 *last_time_guard = Some(Instant::now());
//             }

//             global_lock.store(false, Ordering::Release);

//             executed_counter.fetch_add(1, Ordering::Relaxed);
//             time_counter.fetch_add(execution_time, Ordering::Relaxed);

//             match success {
//                 Ok(true) => {
//                     successful_counter.fetch_add(1, Ordering::Relaxed);
//                     info!(
//                         "✅ FIX triangular arbitrage completed: {:.4}% profit in {:.2}ms",
//                         opportunity.profit_percentage(),
//                         (execution_time as f64) / 1000.0
//                     );
//                 }
//                 Ok(false) => {
//                     failed_counter.fetch_add(1, Ordering::Relaxed);
//                     warn!("⚠️  FIX execution partially failed: {}", opportunity.display());
//                 }
//                 Err(e) => {
//                     failed_counter.fetch_add(1, Ordering::Relaxed);
//                     error!("❌ FIX execution failed: {} - Error: {}", opportunity.display(), e);

//                     if e.to_string().contains("Connection") || e.to_string().contains("TLS") {
//                         connection_health.store(false, Ordering::Relaxed);
//                         error!("🔴 Connection health marked as unhealthy due to error: {}", e);
//                     }
//                 }
//             }
//         })
//     }

//     fn execute_triangular_arbitrage_safe(
//         opportunity: &ArbitrageOpportunity,
//         real_fix_client: &Option<Arc<Mutex<FixClient>>>,
//         mock_fix_client: &Option<Arc<Mutex<MockFixClient>>>,
//         is_debug_mode: bool,
//         symbol_map: &Arc<SymbolMap>,
//         connection_health: &Arc<AtomicBool>
//     ) -> Result<bool> {
//         if is_debug_mode {
//             if let Some(mock_client) = mock_fix_client {
//                 let mut client = mock_client.lock().unwrap();
//                 Self::execute_mock_triangular_trades_safe(opportunity, &mut client, symbol_map)
//             } else {
//                 Err(anyhow::anyhow!("Mock client not available"))
//             }
//         } else if let Some(real_client) = real_fix_client {
//             let mut client = real_client.lock().unwrap();
//             Self::execute_real_triangular_trades_safe(
//                 opportunity,
//                 &mut client,
//                 symbol_map,
//                 connection_health
//             )
//         } else {
//             Err(anyhow::anyhow!("Real client not available"))
//         }
//     }

//     fn execute_real_triangular_trades_safe(
//         opportunity: &ArbitrageOpportunity,
//         client: &mut FixClient,
//         symbol_map: &Arc<SymbolMap>,
//         connection_health: &Arc<AtomicBool>
//     ) -> Result<bool> {
//         if !client.is_connected() {
//             return Err(anyhow::anyhow!("FIX client is not connected"));
//         }

//         let path = &opportunity.path;
//         let symbols = [&path.first_symbol, &path.second_symbol, &path.third_symbol];
//         let directions = [
//             path.first_is_base_to_quote,
//             path.second_is_base_to_quote,
//             path.third_is_base_to_quote,
//         ];

//         let mut current_amount = opportunity.start_amount;
//         let mut all_successful = true;

//         for (i, &symbol) in symbols.iter().enumerate() {
//             if !client.is_connected() {
//                 error!("FIX client disconnected during execution");
//                 connection_health.store(false, Ordering::Relaxed);
//                 return Err(anyhow::anyhow!("FIX client disconnected"));
//             }

//             let precision_info = symbol_map.get(symbol);
//             let (qty_precision, price_precision) = if let Some(info) = precision_info {
//                 (info.qty_precision, info.price_precision)
//             } else {
//                 (8, 8)
//             };

//             let order = if directions[i] {
//                 MarketOrder {
//                     symbol: symbol.to_string(),
//                     side: OrderSide::Sell,
//                     quantity: current_amount,
//                     quote_quantity: None,
//                     qty_precision: Some(qty_precision),
//                     price_precision: Some(price_precision),
//                 }
//             } else {
//                 MarketOrder {
//                     symbol: symbol.to_string(),
//                     side: OrderSide::Buy,
//                     quantity: 0.0,
//                     quote_quantity: Some(current_amount),
//                     qty_precision: Some(qty_precision),
//                     price_precision: Some(price_precision),
//                 }
//             };

//             let trade_start = Instant::now();

//             match client.place_market_order(order) {
//                 Ok(result) => {
//                     let trade_time = trade_start.elapsed();

//                     if result.order_status == "REJECTED" || result.exec_type == "REJECTED" {
//                         error!("Order rejected: {:?}", result.reject_reason);
//                         all_successful = false;
//                         break;
//                     }

//                     debug!(
//                         "Trade {}/{} placed: {} {} leaves_qty={:.8} in {:.2}ms",
//                         i + 1,
//                         symbols.len(),
//                         result.side,
//                         symbol,
//                         result.leaves_qty,
//                         trade_time.as_secs_f64() * 1000.0
//                     );

//                     if i < symbols.len() - 1 {
//                         current_amount = result.leaves_qty;
//                         if current_amount <= 0.0 {
//                             error!("Invalid leaves_qty for next trade: {:.8}", current_amount);
//                             all_successful = false;
//                             break;
//                         }
//                     }
//                 }
//                 Err(e) => {
//                     error!("Trade {}/{} failed: {}", i + 1, symbols.len(), e);
//                     all_successful = false;
//                     break;
//                 }
//             }
//         }

//         Ok(all_successful)
//     }

//     fn execute_mock_triangular_trades_safe(
//         opportunity: &ArbitrageOpportunity,
//         client: &mut MockFixClient,
//         symbol_map: &Arc<SymbolMap>
//     ) -> Result<bool> {
//         let path = &opportunity.path;
//         let symbols = [&path.first_symbol, &path.second_symbol, &path.third_symbol];
//         let directions = [
//             path.first_is_base_to_quote,
//             path.second_is_base_to_quote,
//             path.third_is_base_to_quote,
//         ];

//         let mut current_amount = opportunity.start_amount;
//         let mut all_successful = true;

//         for (i, &symbol) in symbols.iter().enumerate() {
//             let precision_info = symbol_map.get(symbol);
//             let (qty_precision, price_precision) = if let Some(info) = precision_info {
//                 (info.qty_precision, info.price_precision)
//             } else {
//                 (8, 8)
//             };

//             let order = if directions[i] {
//                 MarketOrder {
//                     symbol: symbol.to_string(),
//                     side: OrderSide::Sell,
//                     quantity: current_amount,
//                     quote_quantity: None,
//                     qty_precision: Some(qty_precision),
//                     price_precision: Some(price_precision),
//                 }
//             } else {
//                 MarketOrder {
//                     symbol: symbol.to_string(),
//                     side: OrderSide::Buy,
//                     quantity: 0.0,
//                     quote_quantity: Some(current_amount),
//                     qty_precision: Some(qty_precision),
//                     price_precision: Some(price_precision),
//                 }
//             };

//             match client.place_market_order(order) {
//                 Ok(result) => {
//                     debug!(
//                         "Mock trade {}/{}: {} @ {:.8}",
//                         i + 1,
//                         symbols.len(),
//                         result.filled_qty,
//                         result.avg_price
//                     );

//                     if i < symbols.len() - 1 {
//                         current_amount = if directions[i] {
//                             result.cum_quote_qty
//                         } else {
//                             result.filled_qty
//                         };
//                     }
//                 }
//                 Err(e) => {
//                     error!("Mock trade {}/{} failed: {}", i + 1, symbols.len(), e);
//                     all_successful = false;
//                     break;
//                 }
//             }

//             thread::sleep(Duration::from_millis(50));
//         }

//         Ok(all_successful)
//     }

//     pub fn connect(&self) -> Result<()> {
//         if self.is_debug_mode {
//             if let Some(mock_client) = &self.mock_fix_client {
//                 let mut client = mock_client.lock().unwrap();
//                 client.connect()?;
//                 info!("Connected to Mock FIX API for debug mode");
//             }
//         } else if let Some(real_client) = &self.real_fix_client {
//             let mut client = real_client.lock().unwrap();
//             client.connect()?;
//             self.connection_health.store(true, Ordering::Relaxed);
//             info!("Connected to Real FIX API for production trading");
//         }
//         Ok(())
//     }

//     pub fn disconnect(&self) -> Result<()> {
//         info!("Disconnecting FIX arbitrage executor...");

//         self.shutdown.store(true, Ordering::Relaxed);
//         self.connection_health.store(false, Ordering::Relaxed);

//         if self.is_debug_mode {
//             if let Some(mock_client) = &self.mock_fix_client {
//                 let mut client = mock_client.lock().unwrap();
//                 client.disconnect()?;
//             }
//         } else if let Some(real_client) = &self.real_fix_client {
//             let mut client = real_client.lock().unwrap();
//             client.disconnect()?;
//         }

//         info!("FIX arbitrage executor disconnected gracefully");
//         Ok(())
//     }
// }

use std::sync::{ Arc, Mutex };
use std::time::{ Instant, Duration };
use std::sync::atomic::{ AtomicBool, Ordering };
use std::collections::HashSet;
use std::thread;

use anyhow::{ Result, Context };
use tracing::{ info, warn, debug, error };
use crate::arbitrage::detector::{ ArbitrageOpportunity, ExecutionCallback };
use crate::exchange::fix_api::{ FixClient, MarketOrder, OrderSide };
use crate::exchange::mock_fix_api::MockFixClient;
use crate::models::symbol_map::SymbolMap;

// Removed ExecutionStats struct - only used for performance testing

pub struct FixArbitrageExecutor {
    real_fix_client: Option<Arc<Mutex<FixClient>>>,
    mock_fix_client: Option<Arc<Mutex<MockFixClient>>>,
    symbol_map: Arc<SymbolMap>,
    is_debug_mode: bool,
    // Removed: execution_counter, successful_executions, failed_executions, rejected_executions, total_execution_time_us
    // These stats are only used for performance testing
    shutdown: Arc<AtomicBool>,
    active_symbols: Arc<Mutex<HashSet<u16>>>, // Keep - used for collision detection
    global_execution_lock: Arc<AtomicBool>, // Keep - used for throttling
    last_execution_time: Arc<Mutex<Option<Instant>>>, // Keep - used for rate limiting
    execution_delay_ms: u64, // Keep - used for rate limiting
    connection_health: Arc<AtomicBool>, // Keep - used for connection monitoring
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
        } else if let Some(config) = fix_config {
            let real_client = FixClient::new(config).context("Failed to create FIX client")?;

            // Update symbol precisions for all symbols
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
        };

        Ok(Self {
            real_fix_client,
            mock_fix_client,
            symbol_map,
            is_debug_mode,
            // Removed performance tracking stats
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
        let symbol_map = Arc::clone(&self.symbol_map);
        // Removed: performance counters
        let active_symbols = Arc::clone(&self.active_symbols);
        let global_lock = Arc::clone(&self.global_execution_lock);
        let last_exec_time = Arc::clone(&self.last_execution_time);
        let delay_ms = self.execution_delay_ms;
        let connection_health = Arc::clone(&self.connection_health);

        Arc::new(move |opportunity: ArbitrageOpportunity| {
            // Check connection health
            if !connection_health.load(Ordering::Relaxed) {
                debug!("Skipping execution due to unhealthy connection");
                return;
            }

            // Global execution lock for throttling
            if
                global_lock
                    .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
            {
                // Removed: performance counter increment
                debug!("Rejected opportunity due to global execution lock");
                return;
            }

            // Rate limiting logic
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

            // Symbol collision detection
            let has_conflict = {
                let active = active_symbols.lock().unwrap();
                symbols.iter().any(|&sym| active.contains(&sym))
            };

            if has_conflict {
                global_lock.store(false, Ordering::Release);
                // Removed: performance counter increment
                debug!("Rejected opportunity due to symbol conflict: {:?}", symbols);
                return;
            }

            // Lock symbols for execution
            {
                let mut active = active_symbols.lock().unwrap();
                for &symbol in symbols {
                    active.insert(symbol);
                }
            }

            info!("🔄 Starting triangular arbitrage execution: {}", opportunity.display());

            // Execute the arbitrage
            let success = Self::execute_triangular_arbitrage_safe(
                &opportunity,
                &real_client,
                &mock_client,
                is_debug,
                &symbol_map,
                &connection_health
            );

            let execution_time = execution_start.elapsed().as_micros() as u64;

            // Unlock symbols
            {
                let mut active = active_symbols.lock().unwrap();
                for &symbol in symbols {
                    active.remove(&symbol);
                }
            }

            // Update last execution time
            {
                let mut last_time_guard = last_exec_time.lock().unwrap();
                *last_time_guard = Some(Instant::now());
            }

            global_lock.store(false, Ordering::Release);

            // Removed: performance counter updates
            // Log results without tracking stats
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

                    // Update connection health on connection errors
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
        } else if let Some(real_client) = real_fix_client {
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

    fn execute_real_triangular_trades_safe(
        opportunity: &ArbitrageOpportunity,
        client: &mut FixClient,
        symbol_map: &Arc<SymbolMap>,
        connection_health: &Arc<AtomicBool>
    ) -> Result<bool> {
        // Check connection before starting
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

        // Execute each trade in the triangular path
        for (i, &symbol) in symbols.iter().enumerate() {
            if !client.is_connected() {
                error!("FIX client disconnected during execution");
                connection_health.store(false, Ordering::Relaxed);
                return Err(anyhow::anyhow!("FIX client disconnected"));
            }

            // Get symbol precision info
            let precision_info = symbol_map.get(symbol);
            let (qty_precision, price_precision) = if let Some(info) = precision_info {
                (info.qty_precision, info.price_precision)
            } else {
                (8, 8) // Default precision
            };

            // Create market order based on trade direction
            let order = if directions[i] {
                // Sell base asset
                MarketOrder {
                    symbol: symbol.to_string(),
                    side: OrderSide::Sell,
                    quantity: current_amount,
                    quote_quantity: None,
                    qty_precision: Some(qty_precision),
                    price_precision: Some(price_precision),
                }
            } else {
                // Buy with quote currency
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

            // Execute the trade
            match client.place_market_order(order) {
                Ok(result) => {
                    let trade_time = trade_start.elapsed();

                    // Check for rejection
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

                    // Update amount for next trade using leaves_qty
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

        // Execute mock trades
        for (i, &symbol) in symbols.iter().enumerate() {
            // Get symbol precision
            let precision_info = symbol_map.get(symbol);
            let (qty_precision, price_precision) = if let Some(info) = precision_info {
                (info.qty_precision, info.price_precision)
            } else {
                (8, 8)
            };

            // Create mock order
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

            // Execute mock trade
            match client.place_market_order(order) {
                Ok(result) => {
                    debug!(
                        "Mock trade {}/{}: {} @ {:.8}",
                        i + 1,
                        symbols.len(),
                        result.filled_qty,
                        result.avg_price
                    );

                    // Update amount for next trade
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

            // Small delay between mock trades
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
        } else if let Some(real_client) = &self.real_fix_client {
            let mut client = real_client.lock().unwrap();
            client.connect()?;
            self.connection_health.store(true, Ordering::Relaxed);
            info!("Connected to Real FIX API for production trading");
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
        } else if let Some(real_client) = &self.real_fix_client {
            let mut client = real_client.lock().unwrap();
            client.disconnect()?;
        }

        info!("FIX arbitrage executor disconnected gracefully");
        Ok(())
    }
}
