use std::sync::{ Arc, Mutex };
use std::time::{ Duration, Instant };
use anyhow::Result;
use tracing::{ error, info, warn };
use crate::config::Config;
use crate::exchange::fix_api::{ FixClient, ExecutionResult, MarketOrder };
use crate::exchange::mock_fix_api::MockFixClient;
use crate::models::symbol_map::SymbolMap;
use crate::arbitrage::detector::ArbitrageOpportunity;
use crate::orderbook_manager::manager::OrderBookManager;
use crate::utils::simulation_logger::SimulationLogger;
use crate::utils::order_ops::{
    TriangularTradeData,
    create_order_with_precision,
    get_symbol_precision,
    calculate_next_amount,
    validate_trade_amount,
};

#[derive(Clone)]
pub enum FixClientType {
    Real(Arc<Mutex<FixClient>>),
    Mock(Arc<Mutex<MockFixClient>>),
}

impl FixClientType {
    #[inline]
    pub fn connect(&self) -> Result<()> {
        match self {
            FixClientType::Real(client) => {
                let mut client = client.lock().unwrap();
                client.connect()?;
                info!("Connected to Real FIX API");
            }
            FixClientType::Mock(client) => {
                let mut client = client.lock().unwrap();
                client.connect()?;
                info!("Connected to Mock FIX API");
            }
        }
        Ok(())
    }

    #[inline]
    pub fn disconnect(&self) -> Result<()> {
        match self {
            FixClientType::Real(client) => {
                let mut client = client.lock().unwrap();
                client.disconnect()?;
                info!("Disconnected Real FIX client");
            }
            FixClientType::Mock(client) => {
                let mut client = client.lock().unwrap();
                client.disconnect()?;
                info!("Disconnected Mock FIX client");
            }
        }
        Ok(())
    }

    #[inline]
    pub fn is_connected(&self) -> bool {
        match self {
            FixClientType::Real(client) => client.lock().unwrap().is_connected(),
            FixClientType::Mock(_) => true,
        }
    }

    #[inline]
    pub fn place_market_order(&self, order: MarketOrder) -> Result<ExecutionResult> {
        match self {
            FixClientType::Real(client) => {
                let mut client = client.lock().unwrap();
                client.place_market_order(order)
            }
            FixClientType::Mock(client) => {
                let mut client = client.lock().unwrap();
                client.place_market_order(order)
            }
        }
    }
}

fn execute_trade_step(
    client: &FixClientType,
    symbol: &str,
    is_base_to_quote: bool,
    amount: f64,
    symbol_map: &Arc<SymbolMap>,
    step_number: usize,
    total_steps: usize
) -> Result<ExecutionResult> {
    validate_trade_amount(amount, symbol).expect("Error while executing validate_trade_amount");

    if !client.is_connected() {
        return Err(
            anyhow::anyhow!("FIX client disconnected during execution at step {}", step_number)
        );
    }

    let (qty_precision, price_precision) = get_symbol_precision(symbol_map, symbol);

    let order = create_order_with_precision(
        symbol.to_string(),
        is_base_to_quote,
        amount,
        qty_precision,
        price_precision
    );

    let trade_start = Instant::now();
    info!(
        "📤 Executing trade {}/{}: {} {} {:.8}",
        step_number,
        total_steps,
        if is_base_to_quote {
            "SELL"
        } else {
            "BUY"
        },
        symbol,
        amount
    );

    let result = client.place_market_order(order)?;
    let trade_time = trade_start.elapsed();

    if result.order_status != "FILLED" {
        return Err(
            anyhow::anyhow!("Unexpected order status: {} (expected FILLED)", result.order_status)
        );
    }

    if result.filled_qty <= 0.0 {
        return Err(
            anyhow::anyhow!(
                "No quantity filled for trade {}/{}: {}",
                step_number,
                total_steps,
                symbol
            )
        );
    }

    info!(
        "✅ Trade {}/{} COMPLETED: {} {} - Filled: {:.8} @ {:.8} in {:.2}ms",
        step_number,
        total_steps,
        result.side,
        symbol,
        result.filled_qty,
        result.avg_price,
        trade_time.as_secs_f64() * 1000.0
    );

    Ok(result)
}

fn execute_triangular_sequence(
    trade_data: &TriangularTradeData,
    client: &FixClientType,
    symbol_map: &Arc<SymbolMap>
) -> Result<(bool, Vec<(String, f64, f64)>)> {
    let mut current_amount = trade_data.start_amount;
    let mut executed_trades = Vec::with_capacity(3);
    let execution_start = Instant::now();

    info!(
        "🔄 Starting triangular arbitrage execution: {} → {} → {} (Start: {:.8})",
        trade_data.symbols[0],
        trade_data.symbols[1],
        trade_data.symbols[2],
        current_amount
    );

    for (i, symbol) in trade_data.symbols.iter().enumerate() {
        let result = execute_trade_step(
            client,
            symbol,
            trade_data.directions[i],
            current_amount,
            symbol_map,
            i + 1,
            trade_data.symbols.len()
        )?;

        executed_trades.push((
            symbol.clone(),
            result.avg_price,
            if i == trade_data.symbols.len() - 1 {
                if trade_data.directions[i] { result.cum_quote_qty } else { result.filled_qty }
            } else {
                current_amount
            },
        ));

        if i < trade_data.symbols.len() - 1 {
            let next_symbol = &trade_data.symbols[i + 1];
            let (next_qty_precision, next_price_precision) = get_symbol_precision(
                symbol_map,
                next_symbol
            );

            current_amount = calculate_next_amount(
                result.filled_qty,
                result.cum_quote_qty,
                trade_data.directions[i],
                trade_data.directions[i + 1],
                next_qty_precision,
                next_price_precision
            );

            if current_amount <= 0.0 {
                return Err(anyhow::anyhow!("Invalid amount for next trade: {:.8}", current_amount));
            }

            info!("💰 Amount for next trade: {:.8}", current_amount);
        } else {
            current_amount = if trade_data.directions[i] {
                result.cum_quote_qty
            } else {
                result.filled_qty
            };
        }

        if matches!(client, FixClientType::Mock(_)) && i < trade_data.symbols.len() - 1 {
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    let total_execution_time = execution_start.elapsed();
    let profit = current_amount - trade_data.start_amount;
    let profit_percentage = (profit / trade_data.start_amount) * 100.0;

    info!(
        "🎉 Triangular arbitrage COMPLETED in {:.2}ms! Final amount: {:.8}",
        total_execution_time.as_secs_f64() * 1000.0,
        current_amount
    );

    if profit > 0.0 {
        info!("💰 Profit: {:.8} ({:.4}%)", profit, profit_percentage);
    } else {
        warn!("📉 Loss: {:.8} ({:.4}%)", profit.abs(), profit_percentage.abs());
    }

    Ok((true, executed_trades))
}

pub fn execute_triangular_trades_with_simulation(
    opportunity: &ArbitrageOpportunity,
    client: &FixClientType,
    symbol_map: &Arc<SymbolMap>,
    orderbook_manager: &Arc<OrderBookManager>
) -> Result<bool> {
    let simulation_logger = SimulationLogger::new(
        Arc::clone(orderbook_manager),
        Arc::clone(symbol_map)
    );

    let simulation_result = simulation_logger.simulate_opportunity(opportunity);

    if let Some(ref sim) = simulation_result {
        info!(
            "🔮 Pre-execution simulation: {:.4}% profit expected",
            sim.simulated_profit_percentage
        );
    }

    let trade_data = TriangularTradeData::from_opportunity(opportunity);
    let execution_start = Instant::now();

    let result = execute_triangular_sequence(&trade_data, client, symbol_map);
    let execution_time = execution_start.elapsed();

    match result {
        Ok((success, executed_trades)) => {
            simulation_logger.log_execution_result(
                opportunity,
                simulation_result,
                &executed_trades,
                execution_time,
                success
            );
            Ok(success)
        }
        Err(e) => {
            simulation_logger.log_execution_result(
                opportunity,
                simulation_result,
                &[],
                execution_time,
                false
            );
            Err(e)
        }
    }
}

pub fn execute_triangular_trades(
    trade_data: &TriangularTradeData,
    client: &FixClientType,
    symbol_map: &Arc<SymbolMap>
) -> Result<bool> {
    let (success, _) = execute_triangular_sequence(trade_data, client, symbol_map)?;
    Ok(success)
}

pub fn create_fix_client(
    is_debug_mode: bool,
    fix_config: Option<crate::exchange::fix_api::FixConfig>,
    config: Config,
    symbol_map: &Arc<SymbolMap>
) -> Result<Option<FixClientType>> {
    if is_debug_mode {
        let mock_client = MockFixClient::new();
        Ok(Some(FixClientType::Mock(Arc::new(Mutex::new(mock_client)))))
    } else if let Some(fix_config) = fix_config {
        let real_client = FixClient::new(fix_config, config)?;

        for symbol in symbol_map.get_all_symbols() {
            real_client.update_symbol_precision(
                &symbol.symbol,
                symbol.qty_precision,
                symbol.price_precision
            );
        }

        Ok(Some(FixClientType::Real(Arc::new(Mutex::new(real_client)))))
    } else {
        Ok(None)
    }
}
