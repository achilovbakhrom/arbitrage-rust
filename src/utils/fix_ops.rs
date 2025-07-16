use std::sync::{ Arc, Mutex };
use std::time::{ Duration, Instant };
use anyhow::Result;
use tracing::{ error, info, warn };
use crate::exchange::fix_api::{ FixClient, ExecutionResult, MarketOrder };
use crate::exchange::mock_fix_api::MockFixClient;
use crate::models::symbol_map::SymbolMap;

use crate::utils::order_ops::{ self, TriangularTradeData };

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
            FixClientType::Real(client) => { client.lock().unwrap().is_connected() }
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

pub fn execute_triangular_trades(
    trade_data: &TriangularTradeData,
    client: &FixClientType,
    symbol_map: &Arc<SymbolMap>
) -> Result<bool> {
    let mut current_amount = trade_data.start_amount;
    let mut all_successful = true;
    let execution_start = Instant::now();

    info!(
        "🔄 Starting triangular arbitrage execution: {} → {} → {} (Start: {:.8})",
        trade_data.symbols[0],
        trade_data.symbols[1],
        trade_data.symbols[2],
        current_amount
    );

    for (i, symbol) in trade_data.symbols.iter().enumerate() {
        if !client.is_connected() {
            error!("FIX client disconnected during execution at step {}", i + 1);
            return Err(anyhow::anyhow!("FIX client disconnected"));
        }

        let (qty_precision, price_precision) = order_ops::get_symbol_precision(symbol_map, symbol);

        let order = order_ops::create_order_with_precision(
            symbol.to_string(),
            trade_data.directions[i],
            current_amount,
            qty_precision,
            price_precision
        );

        let trade_start = Instant::now();
        info!(
            "📤 Executing trade {}/{}: {} {} {:.8}",
            i + 1,
            trade_data.symbols.len(),
            if trade_data.directions[i] {
                "SELL"
            } else {
                "BUY"
            },
            symbol,
            current_amount
        );

        match client.place_market_order(order) {
            Ok(result) => {
                let trade_time = trade_start.elapsed();

                if result.order_status != "FILLED" {
                    error!("❌ Unexpected order status: {} (expected FILLED)", result.order_status);
                    all_successful = false;
                    break;
                }

                if result.filled_qty <= 0.0 {
                    error!(
                        "❌ No quantity filled for trade {}/{}: {}",
                        i + 1,
                        trade_data.symbols.len(),
                        symbol
                    );
                    all_successful = false;
                    break;
                }

                info!(
                    "✅ Trade {}/{} COMPLETED: {} {} - Filled: {:.8} @ {:.8} in {:.2}ms",
                    i + 1,
                    trade_data.symbols.len(),
                    result.side,
                    symbol,
                    result.filled_qty,
                    result.avg_price,
                    trade_time.as_secs_f64() * 1000.0
                );

                if i < trade_data.symbols.len() - 1 {
                    current_amount = if trade_data.directions[i] {
                        result.cum_quote_qty
                    } else {
                        result.filled_qty
                    };

                    if current_amount <= 0.0 {
                        error!(
                            "❌ Invalid amount for next trade: {:.8} (from trade {} result)",
                            current_amount,
                            i + 1
                        );
                        all_successful = false;
                        break;
                    }

                    info!("💰 Amount for next trade: {:.8}", current_amount);
                }
            }
            Err(e) => {
                error!(
                    "❌ Trade {}/{} failed: {} - Error: {}",
                    i + 1,
                    trade_data.symbols.len(),
                    symbol,
                    e
                );
                all_successful = false;
                break;
            }
        }

        if matches!(client, FixClientType::Mock(_)) && i < trade_data.symbols.len() - 1 {
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    let total_execution_time = execution_start.elapsed();

    if all_successful {
        info!(
            "🎉 Triangular arbitrage COMPLETED successfully in {:.2}ms! Final amount: {:.8}",
            total_execution_time.as_secs_f64() * 1000.0,
            current_amount
        );

        let profit = current_amount - trade_data.start_amount;
        let profit_percentage = (profit / trade_data.start_amount) * 100.0;

        if profit > 0.0 {
            info!("💰 Actual profit: {:.8} ({:.4}%)", profit, profit_percentage);
        } else {
            warn!("📉 Loss: {:.8} ({:.4}%)", profit.abs(), profit_percentage.abs());
        }
    } else {
        error!(
            "❌ Triangular arbitrage failed after {:.2}ms",
            total_execution_time.as_secs_f64() * 1000.0
        );
    }

    Ok(all_successful)
}

pub fn create_fix_client(
    is_debug_mode: bool,
    fix_config: Option<crate::exchange::fix_api::FixConfig>,
    symbol_map: &Arc<SymbolMap>
) -> Result<Option<FixClientType>> {
    if is_debug_mode {
        let mock_client = MockFixClient::new();
        Ok(Some(FixClientType::Mock(Arc::new(Mutex::new(mock_client)))))
    } else if let Some(config) = fix_config {
        let mut real_client = FixClient::new(config)?;

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
