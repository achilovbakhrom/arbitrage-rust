// src/executor/fix_executor.rs
use std::sync::{ Arc, Mutex };
use std::time::Instant;
use std::sync::atomic::{ AtomicU64, Ordering };
use anyhow::{ Result, Context };
use tracing::{ info, warn, error, debug };

use crate::arbitrage::detector::ArbitrageOpportunity;
use crate::exchange::fix_api::{ FixClient, MarketOrder, OrderSide, ExecutionResult };
use crate::exchange::mock_fix_api::MockFixClient;
use crate::orderbook::manager::OrderBookManager;

/// Execution result for triangular arbitrage
#[derive(Debug)]
pub struct TriangularExecutionResult {
    pub opportunity: ArbitrageOpportunity,
    pub success: bool,
    pub actual_profit: f64,
    pub execution_time_us: u64,
    pub trades_executed: u8, // Number of successful trades (0-3)
    pub trade_results: Vec<ExecutionResult>,
    pub error: Option<String>,
}

/// Enhanced arbitrage executor that uses FIX API for real trading
pub struct FixArbitrageExecutor {
    // FIX clients (only one will be used based on debug mode)
    real_fix_client: Option<Arc<Mutex<FixClient>>>,
    mock_fix_client: Option<Arc<Mutex<MockFixClient>>>,

    // Debug mode flag
    is_debug_mode: bool,

    // Dependencies
    orderbook_manager: Arc<OrderBookManager>,

    // Performance tracking
    execution_counter: AtomicU64,
    successful_executions: AtomicU64,
    total_execution_time_us: AtomicU64,

    // Configuration
    max_slippage_percentage: f64,
    min_profit_threshold: f64,
}

impl FixArbitrageExecutor {
    /// Create a new FIX arbitrage executor
    pub fn new(
        orderbook_manager: Arc<OrderBookManager>,
        is_debug_mode: bool,
        fix_config: Option<crate::exchange::fix_api::FixConfig>,
        max_slippage_percentage: f64,
        min_profit_threshold: f64
    ) -> Result<Self> {
        let (real_fix_client, mock_fix_client) = if is_debug_mode {
            // Debug mode: use mock client
            let mock_client = MockFixClient::new();
            (None, Some(Arc::new(Mutex::new(mock_client))))
        } else {
            // Production mode: use real FIX client
            if let Some(config) = fix_config {
                let real_client = FixClient::new(config).context("Failed to create FIX client")?;
                (Some(Arc::new(Mutex::new(real_client))), None)
            } else {
                return Err(anyhow::anyhow!("FIX configuration required for production mode"));
            }
        };

        Ok(Self {
            real_fix_client,
            mock_fix_client,
            is_debug_mode,
            orderbook_manager,
            execution_counter: AtomicU64::new(0),
            successful_executions: AtomicU64::new(0),
            total_execution_time_us: AtomicU64::new(0),
            max_slippage_percentage,
            min_profit_threshold,
        })
    }

    /// Connect to the FIX API (real or mock)
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

    /// Disconnect from the FIX API
    pub fn disconnect(&self) -> Result<()> {
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
        Ok(())
    }

    /// Execute a triangular arbitrage opportunity
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

        // Validate opportunity is still profitable
        if !self.validate_opportunity(opportunity) {
            return TriangularExecutionResult {
                opportunity: opportunity.clone(),
                success: false,
                actual_profit: 0.0,
                execution_time_us: start_time.elapsed().as_micros() as u64,
                trades_executed: 0,
                trade_results: Vec::new(),
                error: Some("Opportunity no longer profitable".to_string()),
            };
        }

        // Execute the three trades in sequence
        let trade_results = self.execute_triangular_trades(opportunity);

        let execution_time_us = start_time.elapsed().as_micros() as u64;
        self.total_execution_time_us.fetch_add(execution_time_us, Ordering::Relaxed);

        // Calculate actual profit/loss
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
            opportunity: opportunity.clone(),
            success,
            actual_profit,
            execution_time_us,
            trades_executed,
            trade_results,
            error: if success {
                None
            } else {
                Some("Execution incomplete".to_string())
            },
        }
    }

    /// Validate that the opportunity is still profitable with current prices
    fn validate_opportunity(&self, opportunity: &ArbitrageOpportunity) -> bool {
        // Get current top of book for all three symbols
        let first_book = self.orderbook_manager.get_top_of_book(&opportunity.path.first_symbol);
        let second_book = self.orderbook_manager.get_top_of_book(&opportunity.path.second_symbol);
        let third_book = self.orderbook_manager.get_top_of_book(&opportunity.path.third_symbol);

        if
            let (
                Some((first_bid, first_ask)),
                Some((second_bid, second_ask)),
                Some((third_bid, third_ask)),
            ) = (first_book, second_book, third_book)
        {
            // Calculate current profit with latest prices
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
            false // Missing price data
        }
    }

    /// Calculate current profit percentage with latest orderbook data
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

        // First trade
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

        // Second trade
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

        // Third trade
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

        // Calculate profit percentage
        ((amount - start_amount) / start_amount) * 100.0
    }

    /// Execute the three trades that make up the triangular arbitrage
    fn execute_triangular_trades(
        &self,
        opportunity: &ArbitrageOpportunity
    ) -> Vec<ExecutionResult> {
        let mut results = Vec::with_capacity(3);
        let path = &opportunity.path;
        let mut current_amount = opportunity.start_amount;

        // Trade 1: Start asset -> Intermediate asset 1
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
                return results; // Stop if first trade fails
            }
        } else {
            return results; // Stop if first trade fails
        }

        // Trade 2: Intermediate asset 1 -> Intermediate asset 2
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
                return results; // Stop if second trade fails
            }
        } else {
            return results; // Stop if second trade fails
        }

        // Trade 3: Intermediate asset 2 -> End asset (should be same as start)
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

    /// Execute a single market order
    fn execute_single_trade(
        &self,
        symbol: &str,
        side: OrderSide,
        amount: f64,
        is_base_to_quote: bool
    ) -> Option<ExecutionResult> {
        let order = MarketOrder {
            symbol: symbol.to_string(),
            side,
            quantity: if is_base_to_quote {
                amount
            } else {
                0.0
            },
            quote_quantity: if is_base_to_quote {
                None
            } else {
                Some(amount)
            },
        };

        debug!(
            symbol = symbol,
            side = ?side,
            quantity = amount,
            is_base_to_quote = is_base_to_quote,
            "Executing single trade"
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

    /// Calculate the final execution result
    fn calculate_execution_result(
        &self,
        trade_results: &[ExecutionResult],
        opportunity: &ArbitrageOpportunity
    ) -> (bool, f64, u8) {
        let trades_executed = trade_results.len() as u8;

        // Check if all trades were successful
        let all_successful =
            trade_results.len() == 3 && trade_results.iter().all(|r| r.order_status == "FILLED");

        if !all_successful {
            return (false, 0.0, trades_executed);
        }

        // Calculate actual profit from trade results
        let start_amount = opportunity.start_amount;

        // For triangular arbitrage, the final amount should be in the same asset as start
        // This is a simplified calculation - in reality you'd need to track the exact asset flow
        let final_amount = if let Some(last_trade) = trade_results.last() {
            // This depends on the direction of the last trade
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

    /// Get execution statistics
    pub fn get_execution_stats(&self) -> ExecutionStats {
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

    /// Check if the executor is connected
    pub fn is_connected(&self) -> bool {
        if self.is_debug_mode {
            if let Some(mock_client) = &self.mock_fix_client {
                mock_client.lock().unwrap().is_connected()
            } else {
                false
            }
        } else {
            if let Some(real_client) = &self.real_fix_client {
                real_client.lock().unwrap().is_connected()
            } else {
                false
            }
        }
    }
}

/// Execution statistics
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
            "FIX Executions: {}, Success Rate: {:.2}%, Avg Time: {}μs, Total Time: {}μs",
            self.total_executions,
            self.success_rate * 100.0,
            self.avg_execution_time_us,
            self.total_execution_time_us
        )
    }
}
