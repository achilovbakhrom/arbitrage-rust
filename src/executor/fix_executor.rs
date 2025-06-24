use std::sync::{ Arc, Mutex };
use std::time::Instant;
use std::sync::atomic::{ AtomicU64, Ordering };
use std::collections::HashSet;
use std::thread;
use anyhow::{ Result, Context };
use tracing::{ info, warn, error, debug };
use crossbeam_channel::{ bounded, Receiver, Sender };

use crate::arbitrage::detector::{ ArbitrageOpportunity, ExecutionCallback };
use crate::exchange::fix_api::{ FixClient, MarketOrder, OrderSide, ExecutionResult };
use crate::exchange::mock_fix_api::MockFixClient;
use crate::orderbook::manager::OrderBookManager;
use crate::models::symbol_map::SymbolMap;

#[derive(Debug)]
pub struct TriangularExecutionResult {
    pub success: bool,
    pub actual_profit: f64,
    pub trades_executed: u8,
    pub error: Option<String>,
}

#[derive(Debug)]
struct ExecutionRequest {
    opportunity: ArbitrageOpportunity,
    timestamp: Instant,
    priority: u8,
}

pub struct FixArbitrageExecutor {
    real_fix_client: Option<Arc<Mutex<FixClient>>>,
    mock_fix_client: Option<Arc<Mutex<MockFixClient>>>,

    symbol_map: Arc<SymbolMap>,

    is_debug_mode: bool,

    orderbook_manager: Arc<OrderBookManager>,

    // Callback-based execution
    execution_sender: Option<Sender<ExecutionRequest>>,
    execution_handles: Vec<thread::JoinHandle<()>>,

    execution_counter: Arc<AtomicU64>,
    successful_executions: Arc<AtomicU64>,
    total_execution_time_us: Arc<AtomicU64>,
    rejected_counter: Arc<AtomicU64>,

    min_profit_threshold: f64,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl FixArbitrageExecutor {
    pub fn new(
        orderbook_manager: Arc<OrderBookManager>,
        is_debug_mode: bool,
        fix_config: Option<crate::exchange::fix_api::FixConfig>,
        min_profit_threshold: f64,
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
            orderbook_manager,
            execution_sender: None,
            execution_handles: Vec::new(),
            execution_counter: Arc::new(AtomicU64::new(0)),
            successful_executions: Arc::new(AtomicU64::new(0)),
            total_execution_time_us: Arc::new(AtomicU64::new(0)),
            rejected_counter: Arc::new(AtomicU64::new(0)),
            min_profit_threshold,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    pub fn enable_callback_execution(&mut self, num_threads: usize) -> ExecutionCallback {
        let (execution_sender, execution_receiver) = bounded(10000);
        self.execution_sender = Some(execution_sender.clone());

        // Clear any existing handles
        self.execution_handles.clear();

        for thread_id in 0..num_threads {
            let receiver = execution_receiver.clone();
            let real_client = self.real_fix_client.clone();
            let mock_client = self.mock_fix_client.clone();
            let is_debug = self.is_debug_mode;
            let shutdown = self.shutdown.clone();
            let symbol_map = self.symbol_map.clone();

            // Clone the Arc<AtomicU64> counters for each thread
            let executed_counter = self.execution_counter.clone();
            let successful_counter = self.successful_executions.clone();
            let time_counter = self.total_execution_time_us.clone();
            let rejected_counter = self.rejected_counter.clone();

            let handle = thread::Builder
                ::new()
                .name(format!("fix-executor-{}", thread_id))
                .spawn(move || {
                    Self::execution_thread_worker(
                        receiver,
                        real_client,
                        mock_client,
                        is_debug,
                        shutdown,
                        symbol_map,
                        executed_counter,
                        successful_counter,
                        time_counter,
                        rejected_counter,
                        thread_id
                    );
                })
                .expect("Failed to spawn execution thread");

            self.execution_handles.push(handle);
        }

        info!("Started {} FIX execution threads with callback support", num_threads);

        let sender = execution_sender;
        Arc::new(move |opportunity: ArbitrageOpportunity| {
            let priority = ((opportunity.profit_ratio - 1.0) * 1000.0) as u8;

            let exec_request = ExecutionRequest {
                opportunity,
                timestamp: Instant::now(),
                priority,
            };

            match sender.try_send(exec_request) {
                Ok(_) => {}
                Err(crossbeam_channel::TrySendError::Full(_)) => {
                    debug!("FIX execution queue full, dropping opportunity");
                }
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                    error!("FIX execution channel disconnected");
                }
            }
        })
    }

    fn execution_thread_worker(
        receiver: Receiver<ExecutionRequest>,
        real_fix_client: Option<Arc<Mutex<FixClient>>>,
        mock_fix_client: Option<Arc<Mutex<MockFixClient>>>,
        is_debug_mode: bool,
        shutdown: Arc<std::sync::atomic::AtomicBool>,
        symbol_map: Arc<SymbolMap>,
        executed_counter: Arc<AtomicU64>,
        successful_counter: Arc<AtomicU64>,
        time_counter: Arc<AtomicU64>,
        rejected_counter: Arc<AtomicU64>,
        thread_id: usize
    ) {
        #[cfg(target_os = "linux")]
        {
            let mut cpu_set = nix::sched::CpuSet::new();
            cpu_set.set((thread_id + 2) % 4).ok();
            nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &cpu_set).ok();
        }

        let mut active_symbols: HashSet<u16> = HashSet::with_capacity(10);

        info!("FIX execution thread {} started with callback support", thread_id);

        while !shutdown.load(Ordering::Relaxed) {
            match receiver.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(exec_request) => {
                    let execution_start = Instant::now();

                    let symbols = &exec_request.opportunity.symbols_involved;
                    let has_conflict = symbols.iter().any(|&sym| active_symbols.contains(&sym));

                    if has_conflict {
                        rejected_counter.fetch_add(1, Ordering::Relaxed);
                        debug!("Rejected opportunity due to symbol conflict: {:?}", symbols);
                        continue;
                    }

                    for &symbol in symbols {
                        active_symbols.insert(symbol);
                    }

                    let success = Self::execute_triangular_arbitrage_optimized(
                        &exec_request.opportunity,
                        &real_fix_client,
                        &mock_fix_client,
                        is_debug_mode,
                        &symbol_map
                    );

                    let execution_time = execution_start.elapsed().as_micros() as u64;

                    for &symbol in symbols {
                        active_symbols.remove(&symbol);
                    }

                    executed_counter.fetch_add(1, Ordering::Relaxed);
                    time_counter.fetch_add(execution_time, Ordering::Relaxed);

                    if success {
                        successful_counter.fetch_add(1, Ordering::Relaxed);

                        info!(
                            "FIX Thread {} executed: {:.4}% profit in {}μs",
                            thread_id,
                            exec_request.opportunity.profit_percentage(),
                            execution_time / 1000
                        );
                    } else {
                        warn!(
                            "FIX Thread {} execution failed: {}",
                            thread_id,
                            exec_request.opportunity.display()
                        );
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    info!("FIX execution thread {} receiver disconnected", thread_id);
                    break;
                }
            }
        }

        info!("FIX execution thread {} shutting down", thread_id);
    }

    fn execute_triangular_arbitrage_optimized(
        opportunity: &ArbitrageOpportunity,
        real_fix_client: &Option<Arc<Mutex<FixClient>>>,
        mock_fix_client: &Option<Arc<Mutex<MockFixClient>>>,
        is_debug_mode: bool,
        symbol_map: &Arc<SymbolMap>
    ) -> bool {
        if is_debug_mode {
            if let Some(mock_client) = mock_fix_client {
                let mut client = mock_client.lock().unwrap();
                let result = Self::execute_mock_triangular_trades(
                    opportunity,
                    &mut client,
                    symbol_map
                );
                result.is_ok()
            } else {
                false
            }
        } else {
            if let Some(real_client) = real_fix_client {
                let mut client = real_client.lock().unwrap();
                let result = Self::execute_real_triangular_trades(
                    opportunity,
                    &mut client,
                    symbol_map
                );
                result.is_ok()
            } else {
                false
            }
        }
    }

    fn execute_mock_triangular_trades(
        opportunity: &ArbitrageOpportunity,
        client: &mut MockFixClient,
        symbol_map: &Arc<SymbolMap>
    ) -> Result<()> {
        let path = &opportunity.path;
        let mut current_amount = opportunity.start_amount;

        let symbols = [&path.first_symbol, &path.second_symbol, &path.third_symbol];
        let is_base_to_quote = [
            path.first_is_base_to_quote,
            path.second_is_base_to_quote,
            path.third_is_base_to_quote,
        ];

        for (i, &symbol) in symbols.iter().enumerate() {
            let (qty_precision, price_precision) = symbol_map
                .get(symbol)
                .map(|s| (s.qty_precision, s.price_precision))
                .unwrap_or((8, 8));

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

            let result = client.place_market_order(order)?;

            current_amount = if is_base_to_quote[i] {
                result.cum_quote_qty
            } else {
                result.filled_qty
            };
        }

        Ok(())
    }

    fn execute_real_triangular_trades(
        opportunity: &ArbitrageOpportunity,
        client: &mut FixClient,
        symbol_map: &Arc<SymbolMap>
    ) -> Result<()> {
        let path = &opportunity.path;
        let mut current_amount = opportunity.start_amount;

        let symbols = [&path.first_symbol, &path.second_symbol, &path.third_symbol];
        let is_base_to_quote = [
            path.first_is_base_to_quote,
            path.second_is_base_to_quote,
            path.third_is_base_to_quote,
        ];

        for (i, &symbol) in symbols.iter().enumerate() {
            let (qty_precision, price_precision) = symbol_map
                .get(symbol)
                .map(|s| (s.qty_precision, s.price_precision))
                .unwrap_or((8, 8));

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

            let result = client.place_market_order(order)?;

            current_amount = if is_base_to_quote[i] {
                result.cum_quote_qty
            } else {
                result.filled_qty
            };
        }

        Ok(())
    }

    // Legacy method for backward compatibility
    pub fn execute_triangular_arbitrage(
        &self,
        opportunity: &ArbitrageOpportunity
    ) -> TriangularExecutionResult {
        let start_time = Instant::now();
        let execution_id = self.execution_counter.fetch_add(1, Ordering::Relaxed);

        debug!(
            execution_id = execution_id,
            path = %opportunity.display(),
            expected_profit = %opportunity.profit_percentage(),
            "Starting triangular arbitrage execution"
        );

        if !self.validate_opportunity(opportunity) {
            return TriangularExecutionResult {
                success: false,
                actual_profit: 0.0,
                trades_executed: 0,
                error: Some("Opportunity no longer profitable".to_string()),
            };
        }

        let trade_results = self.execute_triangular_trades(opportunity);

        let execution_time_us = start_time.elapsed().as_micros() as u64;
        self.total_execution_time_us.fetch_add(execution_time_us, Ordering::Relaxed);

        let (success, actual_profit, trades_executed) = self.calculate_execution_result(
            &trade_results,
            opportunity
        );

        if success {
            self.successful_executions.fetch_add(1, Ordering::Relaxed);
            info!(
                execution_id = execution_id,
                actual_profit = actual_profit,
                execution_time_us = execution_time_us,
                "Triangular arbitrage executed successfully"
            );
        } else {
            warn!(
                execution_id = execution_id,
                trades_executed = trades_executed,
                "Triangular arbitrage execution failed"
            );
        }

        TriangularExecutionResult {
            success,
            actual_profit,
            trades_executed,
            error: if success {
                None
            } else {
                Some("Execution incomplete".to_string())
            },
        }
    }

    fn validate_opportunity(&self, opportunity: &ArbitrageOpportunity) -> bool {
        let first_book = self.orderbook_manager.get_top_of_book_internal(
            &opportunity.path.first_symbol
        );
        let second_book = self.orderbook_manager.get_top_of_book_internal(
            &opportunity.path.second_symbol
        );
        let third_book = self.orderbook_manager.get_top_of_book_internal(
            &opportunity.path.third_symbol
        );

        if
            let (
                Some((first_bid, first_ask)),
                Some((second_bid, second_ask)),
                Some((third_bid, third_ask)),
            ) = (first_book, second_book, third_book)
        {
            let current_profit = self.calculate_current_profit(
                opportunity,
                &first_bid,
                &first_ask,
                &second_bid,
                &second_ask,
                &third_bid,
                &third_ask
            );

            current_profit > self.min_profit_threshold
        } else {
            false
        }
    }

    fn calculate_current_profit(
        &self,
        opportunity: &ArbitrageOpportunity,
        first_bid: &Option<crate::models::level::Level>,
        first_ask: &Option<crate::models::level::Level>,
        second_bid: &Option<crate::models::level::Level>,
        second_ask: &Option<crate::models::level::Level>,
        third_bid: &Option<crate::models::level::Level>,
        third_ask: &Option<crate::models::level::Level>
    ) -> f64 {
        let path = &opportunity.path;
        let start_amount = opportunity.start_amount;
        let mut amount = start_amount;

        let first_price = if path.first_is_base_to_quote {
            first_bid.as_ref().unwrap().price
        } else {
            first_ask.as_ref().unwrap().price
        };

        amount = if path.first_is_base_to_quote {
            amount * first_price
        } else {
            amount / first_price
        };

        let second_price = if path.second_is_base_to_quote {
            second_bid.as_ref().unwrap().price
        } else {
            second_ask.as_ref().unwrap().price
        };

        amount = if path.second_is_base_to_quote {
            amount * second_price
        } else {
            amount / second_price
        };

        let third_price = if path.third_is_base_to_quote {
            third_bid.as_ref().unwrap().price
        } else {
            third_ask.as_ref().unwrap().price
        };

        amount = if path.third_is_base_to_quote {
            amount * third_price
        } else {
            amount / third_price
        };

        ((amount - start_amount) / start_amount) * 100.0
    }

    fn execute_triangular_trades(
        &self,
        opportunity: &ArbitrageOpportunity
    ) -> Vec<ExecutionResult> {
        let mut results = Vec::with_capacity(3);
        let path = &opportunity.path;
        let mut current_amount = opportunity.start_amount;

        if
            let Some(result) = self.execute_single_trade(
                &path.first_symbol,
                if path.first_is_base_to_quote {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                },
                current_amount,
                path.first_is_base_to_quote
            )
        {
            if result.order_status == "FILLED" {
                current_amount = if path.first_is_base_to_quote {
                    result.cum_quote_qty
                } else {
                    result.filled_qty
                };
                results.push(result);
            } else {
                results.push(result);
                return results;
            }
        } else {
            return results;
        }

        if
            let Some(result) = self.execute_single_trade(
                &path.second_symbol,
                if path.second_is_base_to_quote {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                },
                current_amount,
                path.second_is_base_to_quote
            )
        {
            if result.order_status == "FILLED" {
                current_amount = if path.second_is_base_to_quote {
                    result.cum_quote_qty
                } else {
                    result.filled_qty
                };
                results.push(result);
            } else {
                results.push(result);
                return results;
            }
        } else {
            return results;
        }

        if
            let Some(result) = self.execute_single_trade(
                &path.third_symbol,
                if path.third_is_base_to_quote {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                },
                current_amount,
                path.third_is_base_to_quote
            )
        {
            results.push(result);
        }

        results
    }

    fn execute_single_trade(
        &self,
        symbol: &str,
        side: OrderSide,
        amount: f64,
        is_base_to_quote: bool
    ) -> Option<ExecutionResult> {
        let (qty_precision, price_precision) = self.get_symbol_precision(symbol).unwrap_or((8, 8));

        let precise_amount = Self::apply_precision(amount, qty_precision);

        let order = MarketOrder {
            symbol: symbol.to_string(),
            side,
            quantity: if is_base_to_quote {
                precise_amount
            } else {
                0.0
            },
            quote_quantity: if is_base_to_quote {
                None
            } else {
                Some(precise_amount)
            },
            qty_precision: Some(qty_precision),
            price_precision: Some(price_precision),
        };

        debug!(
            symbol = symbol,
            side = ?side,
            quantity = precise_amount,
            original_quantity = amount,
            qty_precision = qty_precision,
            is_base_to_quote = is_base_to_quote,
            "Executing single trade with precision"
        );

        if self.is_debug_mode {
            if let Some(mock_client) = &self.mock_fix_client {
                let mut client = mock_client.lock().unwrap();
                match client.place_market_order(order) {
                    Ok(result) => Some(result),
                    Err(e) => {
                        error!("Mock trade execution failed: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            if let Some(real_client) = &self.real_fix_client {
                let mut client = real_client.lock().unwrap();
                match client.place_market_order(order) {
                    Ok(result) => Some(result),
                    Err(e) => {
                        error!("Real trade execution failed: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        }
    }

    fn get_symbol_precision(&self, symbol: &str) -> Option<(u8, u8)> {
        self.symbol_map.get(symbol).map(|s| (s.qty_precision, s.price_precision))
    }

    fn apply_precision(value: f64, precision: u8) -> f64 {
        let factor = (10f64).powi(precision as i32);
        (value * factor).floor() / factor
    }

    fn calculate_execution_result(
        &self,
        trade_results: &[ExecutionResult],
        opportunity: &ArbitrageOpportunity
    ) -> (bool, f64, u8) {
        let trades_executed = trade_results.len() as u8;

        let all_successful =
            trade_results.len() == 3 && trade_results.iter().all(|r| r.order_status == "FILLED");

        if !all_successful {
            return (false, 0.0, trades_executed);
        }

        let start_amount = opportunity.start_amount;

        let final_amount = if let Some(last_trade) = trade_results.last() {
            if opportunity.path.third_is_base_to_quote {
                last_trade.cum_quote_qty
            } else {
                last_trade.filled_qty
            }
        } else {
            start_amount
        };

        let actual_profit = final_amount - start_amount;
        (true, actual_profit, trades_executed)
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

    pub fn get_execution_stats(&self) -> ExecutionStats {
        let total_executions = self.execution_counter.load(Ordering::Relaxed);
        let successful_executions = self.successful_executions.load(Ordering::Relaxed);
        let total_time_us = self.total_execution_time_us.load(Ordering::Relaxed);
        let rejected = self.rejected_counter.load(Ordering::Relaxed);

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
            rejected_executions: rejected,
            success_rate,
            avg_execution_time_us,
            total_execution_time_us: total_time_us,
        }
    }
}

#[derive(Debug)]
pub struct ExecutionStats {
    pub total_executions: u64,
    pub successful_executions: u64,
    pub rejected_executions: u64,
    pub success_rate: f64,
    pub avg_execution_time_us: u64,
    pub total_execution_time_us: u64,
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
