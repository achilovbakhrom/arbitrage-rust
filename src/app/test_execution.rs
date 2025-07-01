use std::sync::Arc;
use crate::{
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient },
    executor::fix_executor::FixArbitrageExecutor,
    models::{ triangular_path::TriangularPath },
    utils::{
        console::print_config,
        helpers::{ build_symbol_map, build_tokio_async_runtime, run_api_with_timeout },
        symbol_interner::global,
    },
    arbitrage::detector::ArbitrageOpportunity,
};
use anyhow::{ anyhow, Context, Result };
use tracing::{ error, info };

pub fn run_test_execution(config: Config) -> Result<()> {
    print_config(&config);
    info!("Starting test execution mode - Single Path Test");

    let api_client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    let rt = build_tokio_async_runtime()?;

    if run_api_with_timeout(&rt, api_client.is_operational())? {
        info!("✓ Exchange is operational");
    } else {
        error!("❌ Exchange is not operational or timed out");
        return Err(anyhow!("Exchange is not operational"));
    }

    let symbols = run_api_with_timeout(&rt, api_client.get_active_spot_symbols())?;
    info!("✓ Fetched {} symbols from exchange", symbols.len());

    let symbol_map = build_symbol_map(symbols.clone(), &config)?;

    let fix_executor = create_fix_executor(symbol_map.clone(), &config)?;

    if let Some(executor) = fix_executor {
        info!("Testing single triangular arbitrage execution...");

        let test_opportunity = create_single_test_opportunity(&symbol_map, &config)?;

        info!("Executing test opportunity: {}", test_opportunity.display());

        let success = executor.force_execute_opportunity(test_opportunity.clone());

        if success {
            info!("✓ Test execution completed successfully!");
            info!("FIX API is working correctly");
        } else {
            error!("❌ Test execution failed");
            error!("FIX API may have issues - check configuration");
        }

        if let Err(e) = executor.disconnect() {
            error!("Error disconnecting FIX executor: {}", e);
        } else {
            info!("FIX executor disconnected successfully");
        }

        info!("Single path test execution completed!");
    } else {
        error!("No FIX executor available for testing");
        return Err(anyhow!("FIX executor not configured"));
    }

    Ok(())
}

fn create_fix_executor(
    symbol_map: Arc<crate::models::symbol_map::SymbolMap>,
    config: &Config
) -> Result<Option<Arc<FixArbitrageExecutor>>> {
    if config.should_enable_real_trading() || config.should_enable_mock_trading() {
        info!("Creating FIX executor ({}) for test execution", if config.debug {
            "Mock"
        } else {
            "Real"
        });

        let fix_config = if config.debug { None } else { Some(config.get_fix_client_config()) };

        match FixArbitrageExecutor::new(config.debug, fix_config, symbol_map.clone()) {
            Ok(executor) => {
                if let Err(e) = executor.connect() {
                    error!("Failed to connect FIX executor: {}", e);
                    return Err(anyhow!("Failed to connect FIX executor: {}", e));
                }

                let executor_arc = Arc::new(executor);

                info!("✓ FIX executor connected successfully for test execution");
                Ok(Some(executor_arc))
            }
            Err(e) => {
                error!("Failed to create FIX executor: {}", e);
                Err(anyhow!("Failed to create FIX executor: {}", e))
            }
        }
    } else {
        error!("Trading must be enabled for test execution");
        Err(anyhow!("Trading not enabled in configuration"))
    }
}

fn create_single_test_opportunity(
    symbol_map: &Arc<crate::models::symbol_map::SymbolMap>,
    config: &Config
) -> Result<ArbitrageOpportunity> {
    let test_path = ("BTCUSDT", "ETHBTC", "ETHUSDT", false, true, true);
    let (first_symbol, second_symbol, third_symbol, first_dir, second_dir, third_dir) = test_path;

    if
        let (Some(_), Some(_), Some(_)) = (
            symbol_map.get(first_symbol),
            symbol_map.get(second_symbol),
            symbol_map.get(third_symbol),
        )
    {
        let path = TriangularPath {
            first_symbol: first_symbol.into(),
            second_symbol: second_symbol.into(),
            third_symbol: third_symbol.into(),
            first_is_base_to_quote: first_dir,
            second_is_base_to_quote: second_dir,
            third_is_base_to_quote: third_dir,
            start_asset: config.base_asset.clone().into(),
            end_asset: config.base_asset.clone().into(),
        };

        let first_symbol_id = global::intern(first_symbol);
        let second_symbol_id = global::intern(second_symbol);
        let third_symbol_id = global::intern(third_symbol);

        let opportunity = ArbitrageOpportunity {
            path: Arc::new(path),
            profit_ratio: 1.01,
            start_amount: config.trade_amount,
            symbols_involved: [first_symbol_id, second_symbol_id, third_symbol_id],
        };

        info!(
            "Created test opportunity: {} → {} → {} (Expected 1% profit)",
            first_symbol,
            second_symbol,
            third_symbol
        );

        Ok(opportunity)
    } else {
        Err(
            anyhow!(
                "Test symbols not available: {} → {} → {}",
                first_symbol,
                second_symbol,
                third_symbol
            )
        )
    }
}
