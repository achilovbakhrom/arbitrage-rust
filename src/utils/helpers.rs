use std::{ rc::Rc, sync::Arc };

use anyhow::{ anyhow, Context };
use tokio::{ runtime::Runtime };
use tracing::{ error, info, warn };

use crate::{
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient },
    executor::fix_executor::FixArbitrageExecutor,
    models::{ command::Command, symbol::Symbol, symbol_map::SymbolMap },
    orderbook::manager::OrderBookManager,
    API_TIMEOUT,
};

pub fn get_run_command() -> Command {
    if std::env::args().nth(1).as_deref() == Some("perf-test") {
        Command::PerformanceTest
    } else {
        Command::Run
    }
}

pub fn build_tokio_async_runtime() -> Result<Runtime, anyhow::Error> {
    tokio::runtime::Builder
        ::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .context("Failed to create Tokio runtime")
}

pub fn is_operational(runtime: &Runtime, client: Arc<BinanceClient>) -> bool {
    let is_operational = runtime.block_on(async {
        tokio::time::timeout(API_TIMEOUT, client.is_operational()).await
    });

    match is_operational {
        Ok(Ok(true)) => true,
        _ => false,
    }
}

pub fn get_all_symbols(
    runtime: &Runtime,
    client: Arc<BinanceClient>
) -> Result<Vec<Symbol>, anyhow::Error> {
    let symbols = runtime.block_on(async {
        tokio::time::timeout(API_TIMEOUT, client.get_active_spot_symbols()).await
    });

    match symbols {
        Ok(symbols) => symbols,
        Err(_) => {
            error!("Timed out while fetching symbols");
            return Err(anyhow!("Timed out while fetching symbols"));
        }
    }
}

pub fn build_symbol_map(
    symbols: Vec<Symbol>,
    config: &Config
) -> Result<Arc<SymbolMap>, anyhow::Error> {
    let mut symbol_map = SymbolMap::from_symbols(symbols);

    info!("Finding triangular arbitrage paths starting with {}...", config.base_asset);
    symbol_map.find_targeted_triangular_paths(
        &config.base_asset,
        config.max_triangles,
        &config.excluded_coins
    );

    let total_paths = symbol_map.get_triangular_paths().len();

    info!("Found {} potential triangular arbitrage paths", total_paths);

    if total_paths > 0 {
        // Log a sample of paths (limit to avoid excessive logging)
        let examples = symbol_map
            .get_triangular_paths()
            .iter()
            .take(5) // Only show first 5 examples
            .enumerate()
            .map(|(i, p)|
                format!(
                    "{:3}. {} → {} → {}",
                    i + 1,
                    p.first_symbol,
                    p.second_symbol,
                    p.third_symbol
                )
            )
            .collect::<Vec<_>>()
            .join("\n");

        info!("Sample paths: \n{}", examples);
    } else {
        warn!("No triangular paths found. Check base_asset and excluded_fiats configuration.");
        return Err(anyhow!("No triangular paths found. Cannot continue."));
    }

    Ok(Arc::new(symbol_map))
}

pub fn build_execultor(
    manager: Arc<OrderBookManager>,
    symbol_map: Arc<SymbolMap>,
    config: &Config
) -> Result<Option<Arc<FixArbitrageExecutor>>, anyhow::Error> {
    if config.should_enable_real_trading() || config.should_enable_mock_trading() {
        info!("Creating FIX executor ({}) with precision support", if config.debug {
            "Mock"
        } else {
            "Real"
        });

        let fix_config = if config.debug {
            None // Mock client doesn't need FIX config
        } else {
            Some(config.get_fix_client_config())
        };

        // Create FIX executor with symbol map for precision support
        match
            FixArbitrageExecutor::new(
                manager,
                config.debug, // Use debug mode flag
                fix_config,
                config.max_slippage_percentage,
                symbol_map.clone() // Pass symbol map for precision
            )
        {
            Ok(executor) => {
                let executor = Arc::new(executor);

                // Connect to FIX API
                if let Err(e) = executor.connect() {
                    error!("Failed to connect FIX executor: {}", e);
                    return Err(anyhow!("Failed to connect FIX executor: {}", e));
                }

                info!("✓ FIX executor connected successfully with precision support");
                Ok(Some(executor))
            }
            Err(e) => {
                error!("Failed to create FIX executor: {}", e);
                return Err(anyhow!("Failed to create FIX executor: {}", e));
            }
        }
    } else {
        info!("Trading disabled, using simulation mode only");
        Ok(None)
    }
}
