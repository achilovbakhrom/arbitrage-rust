use std::sync::{ Arc, Mutex };
use std::time::{ Duration, Instant };
use anyhow::Result;
use tracing::{ info, error, debug };
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

    for (i, symbol) in trade_data.symbols.iter().enumerate() {
        if !client.is_connected() {
            error!("FIX client disconnected during execution");
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

        match client.place_market_order(order) {
            Ok(result) => {
                let trade_time = trade_start.elapsed();

                if result.order_status == "REJECTED" || result.exec_type == "REJECTED" {
                    error!("Order rejected: {:?}", result.reject_reason);
                    all_successful = false;
                    break;
                }

                debug!(
                    "Trade {}/{} executed: {} {} in {:.2}ms",
                    i + 1,
                    trade_data.symbols.len(),
                    result.side,
                    symbol,
                    trade_time.as_secs_f64() * 1000.0
                );

                if i < trade_data.symbols.len() - 1 {
                    current_amount = match client {
                        FixClientType::Real(_) => result.leaves_qty,
                        FixClientType::Mock(_) => {
                            if trade_data.directions[i] {
                                result.cum_quote_qty
                            } else {
                                result.filled_qty
                            }
                        }
                    };

                    if current_amount <= 0.0 {
                        error!("Invalid amount for next trade: {:.8}", current_amount);
                        all_successful = false;
                        break;
                    }
                }
            }
            Err(e) => {
                error!("Trade {}/{} failed: {}", i + 1, trade_data.symbols.len(), e);
                all_successful = false;
                break;
            }
        }

        if matches!(client, FixClientType::Mock(_)) {
            std::thread::sleep(Duration::from_millis(50));
        }
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
