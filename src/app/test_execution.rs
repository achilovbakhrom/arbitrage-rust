use std::{ sync::Arc, thread, time::Duration };
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
    info!("Starting test execution mode");

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
        info!("Testing triangular arbitrage execution...");

        let test_opportunities = create_hardcoded_opportunities(&symbol_map, &config);

        if test_opportunities.is_empty() {
            error!("No test opportunities created");
            return Err(anyhow!("No test opportunities available"));
        }

        info!("Created {} test opportunities", test_opportunities.len());

        for (i, opportunity) in test_opportunities.iter().enumerate() {
            info!(
                "Executing test opportunity {}/{}: {}",
                i + 1,
                test_opportunities.len(),
                opportunity.display()
            );

            let success = executor.force_execute_opportunity(opportunity.clone());

            if success {
                info!("✓ Test execution {} completed successfully", i + 1);
            } else {
                error!("❌ Test execution {} failed", i + 1);
            }

            if i < test_opportunities.len() - 1 {
                info!("Waiting 4 seconds before next execution...");
                thread::sleep(Duration::from_secs(4));
            }
        }

        if let Err(e) = executor.disconnect() {
            error!("Error disconnecting FIX executor: {}", e);
        } else {
            info!("FIX executor disconnected successfully");
        }

        info!("Test execution completed successfully!");
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

fn create_hardcoded_opportunities(
    symbol_map: &Arc<crate::models::symbol_map::SymbolMap>,
    config: &Config
) -> Vec<ArbitrageOpportunity> {
    let mut opportunities = Vec::new();

    let hardcoded_paths = vec![
        ("BTCUSDT", "ETHBTC", "ETHUSDT", false, true, true),
        ("ADAUSDT", "ADABTC", "BTCUSDT", false, true, true),
        ("BNBUSDT", "BNBBTC", "BTCUSDT", false, true, true),
        ("DOTUSDT", "DOTBTC", "BTCUSDT", false, true, true),
        ("LINKUSDT", "LINKBTC", "BTCUSDT", false, true, true)
    ];

    for (
        first_symbol,
        second_symbol,
        third_symbol,
        first_dir,
        second_dir,
        third_dir,
    ) in hardcoded_paths {
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

            opportunities.push(opportunity);
            info!(
                "Created test opportunity: {} → {} → {}",
                first_symbol,
                second_symbol,
                third_symbol
            );
        } else {
            info!(
                "Skipping unavailable symbols: {} → {} → {}",
                first_symbol,
                second_symbol,
                third_symbol
            );
        }
    }

    opportunities
}
