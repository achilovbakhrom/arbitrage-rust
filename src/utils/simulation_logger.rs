use std::sync::Arc;
use tracing::{ info, warn, error };
use crate::arbitrage::detector::ArbitrageOpportunity;
use crate::models::symbol_map::SymbolMap;
use crate::orderbook_manager::manager::OrderBookManager;

#[derive(Debug, Clone)]
pub struct SimulationResult {
    pub opportunity_id: String,
    pub path: String,
    pub simulated_profit_ratio: f64,
    pub simulated_profit_percentage: f64,
    pub simulated_final_amount: f64,
    pub start_amount: f64,
    pub symbol_prices: [(String, f64); 3],
    pub timestamp: chrono::DateTime<chrono::Local>,
}

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub opportunity_id: String,
    pub path: String,
    pub actual_profit_ratio: f64,
    pub actual_profit_percentage: f64,
    pub actual_final_amount: f64,
    pub start_amount: f64,
    pub executed_prices: [(String, f64); 3],
    pub execution_time_ms: f64,
    pub success: bool,
    pub timestamp: chrono::DateTime<chrono::Local>,
}

#[derive(Debug, Clone)]
pub struct ComparisonResult {
    pub opportunity_id: String,
    pub simulation: SimulationResult,
    pub execution: ExecutionResult,
    pub profit_difference: f64,
    pub profit_percentage_difference: f64,
    pub price_slippage: [(String, f64); 3],
}

pub struct SimulationLogger {
    orderbook_manager: Arc<OrderBookManager>,
    symbol_map: Arc<SymbolMap>,
}

impl SimulationLogger {
    pub fn new(orderbook_manager: Arc<OrderBookManager>, symbol_map: Arc<SymbolMap>) -> Self {
        Self {
            orderbook_manager,
            symbol_map,
        }
    }

    pub fn simulate_opportunity(
        &self,
        opportunity: &ArbitrageOpportunity
    ) -> Option<SimulationResult> {
        let opportunity_id = self.generate_opportunity_id(opportunity);
        let path = opportunity.display();
        let start_amount = opportunity.start_amount;

        info!("🔮 SIMULATION: Starting simulation for opportunity: {}", path);

        let path_ref = &opportunity.path;
        let symbols = [
            path_ref.first_symbol.to_string(),
            path_ref.second_symbol.to_string(),
            path_ref.third_symbol.to_string(),
        ];

        let directions = [
            path_ref.first_is_base_to_quote,
            path_ref.second_is_base_to_quote,
            path_ref.third_is_base_to_quote,
        ];

        let mut current_amount = start_amount;
        let mut symbol_prices = [
            (String::new(), 0.0),
            (String::new(), 0.0),
            (String::new(), 0.0),
        ];

        for (i, symbol) in symbols.iter().enumerate() {
            let symbol_id = match crate::utils::symbol_interner::global::get_id(symbol) {
                Some(id) => id,
                None => {
                    warn!("🔮 SIMULATION: Symbol not found: {}", symbol);
                    return None;
                }
            };

            let book = self.orderbook_manager.get_book_by_id(symbol_id)?;

            if !book.is_synced() {
                warn!("🔮 SIMULATION: OrderBook not synced for {}", symbol);
                return None;
            }

            let effective_price = book.get_effective_price(current_amount, directions[i])?;

            symbol_prices[i] = (symbol.clone(), effective_price);

            current_amount = if directions[i] {
                current_amount * effective_price
            } else {
                current_amount / effective_price
            };

            info!(
                "🔮 SIMULATION: Step {} - {} {} at price {:.8} -> amount: {:.8}",
                i + 1,
                if directions[i] {
                    "SELL"
                } else {
                    "BUY"
                },
                symbol,
                effective_price,
                current_amount
            );
        }

        let profit_ratio = current_amount / start_amount;
        let profit_percentage = (profit_ratio - 1.0) * 100.0;

        let simulation_result = SimulationResult {
            opportunity_id: opportunity_id.clone(),
            path: path.clone(),
            simulated_profit_ratio: profit_ratio,
            simulated_profit_percentage: profit_percentage,
            simulated_final_amount: current_amount,
            start_amount,
            symbol_prices,
            timestamp: chrono::Local::now(),
        };

        info!(
            "🔮 SIMULATION COMPLETE: {} | Simulated profit: {:.4}% | Final amount: {:.8}",
            path,
            profit_percentage,
            current_amount
        );

        Some(simulation_result)
    }

    pub fn log_execution_result(
        &self,
        opportunity: &ArbitrageOpportunity,
        simulation: Option<SimulationResult>,
        executed_trades: &[(String, f64, f64)],
        execution_time: std::time::Duration,
        success: bool
    ) {
        let opportunity_id = self.generate_opportunity_id(opportunity);
        let path = opportunity.display();
        let start_amount = opportunity.start_amount;

        let mut executed_prices = [
            (String::new(), 0.0),
            (String::new(), 0.0),
            (String::new(), 0.0),
        ];

        let final_amount = if executed_trades.len() >= 3 {
            for (i, (symbol, price, _)) in executed_trades.iter().take(3).enumerate() {
                executed_prices[i] = (symbol.clone(), *price);
            }
            executed_trades
                .last()
                .map(|(_, _, amount)| *amount)
                .unwrap_or(start_amount)
        } else {
            start_amount
        };

        let actual_profit_ratio = final_amount / start_amount;
        let actual_profit_percentage = (actual_profit_ratio - 1.0) * 100.0;

        let execution_result = ExecutionResult {
            opportunity_id: opportunity_id.clone(),
            path: path.clone(),
            actual_profit_ratio,
            actual_profit_percentage,
            actual_final_amount: final_amount,
            start_amount,
            executed_prices,
            execution_time_ms: execution_time.as_secs_f64() * 1000.0,
            success,
            timestamp: chrono::Local::now(),
        };

        if success {
            info!(
                "🎯 EXECUTION COMPLETE: {} | Actual profit: {:.4}% | Final amount: {:.8} | Time: {:.2}ms",
                path,
                actual_profit_percentage,
                final_amount,
                execution_result.execution_time_ms
            );
        } else {
            error!(
                "❌ EXECUTION FAILED: {} | Partial result: {:.4}% | Time: {:.2}ms",
                path,
                actual_profit_percentage,
                execution_result.execution_time_ms
            );
        }

        if let Some(sim) = simulation {
            self.log_comparison(&sim, &execution_result);
        }
    }

    fn log_comparison(&self, simulation: &SimulationResult, execution: &ExecutionResult) {
        let profit_difference =
            execution.actual_profit_percentage - simulation.simulated_profit_percentage;
        let profit_percentage_difference = if simulation.simulated_profit_percentage != 0.0 {
            (profit_difference / simulation.simulated_profit_percentage) * 100.0
        } else {
            0.0
        };

        let mut price_slippage = [
            (String::new(), 0.0),
            (String::new(), 0.0),
            (String::new(), 0.0),
        ];

        for i in 0..3 {
            let sim_price = simulation.symbol_prices[i].1;
            let exec_price = execution.executed_prices[i].1;

            if sim_price > 0.0 && exec_price > 0.0 {
                let slippage = ((exec_price - sim_price) / sim_price) * 100.0;
                price_slippage[i] = (simulation.symbol_prices[i].0.clone(), slippage);
            }
        }

        let comparison = ComparisonResult {
            opportunity_id: simulation.opportunity_id.clone(),
            simulation: simulation.clone(),
            execution: execution.clone(),
            profit_difference,
            profit_percentage_difference,
            price_slippage: price_slippage.clone(),
        };

        info!("📊 SIMULATION vs EXECUTION COMPARISON:");
        info!("   Path: {}", comparison.simulation.path);
        info!("   Simulated profit: {:.4}%", comparison.simulation.simulated_profit_percentage);
        info!("   Actual profit: {:.4}%", comparison.execution.actual_profit_percentage);
        info!(
            "   Difference: {:.4}% ({:.2}% relative)",
            profit_difference,
            profit_percentage_difference
        );

        info!("   Price Slippage:");
        for (symbol, slippage) in &price_slippage {
            if !symbol.is_empty() {
                info!("     {}: {:.4}%", symbol, slippage);
            }
        }

        if profit_difference.abs() > 0.5 {
            warn!("⚠️  Large profit difference detected: {:.4}%", profit_difference);
        }

        if price_slippage.iter().any(|(_, slippage)| slippage.abs() > 0.1) {
            warn!("⚠️  Significant price slippage detected");
        }

        if comparison.execution.success {
            info!("✅ Execution successful - simulation was reasonably accurate");
        } else {
            warn!("❌ Execution failed - simulation may have been too optimistic");
        }
    }

    fn generate_opportunity_id(&self, opportunity: &ArbitrageOpportunity) -> String {
        format!(
            "{}_{}_{}",
            chrono::Local::now().format("%H%M%S%3f"),
            opportunity.path.first_symbol,
            opportunity.profit_percentage() as u32
        )
    }
}
