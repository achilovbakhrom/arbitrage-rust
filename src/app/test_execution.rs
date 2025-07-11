use std::sync::Arc;
use std::thread;
use std::time::Duration;
use crate::{
    arbitrage::detector::ArbitrageOpportunity,
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient, fix_api::MarketOrder },
    executor::fix_executor::FixArbitrageExecutor,
    models::triangular_path::TriangularPath,
    utils::{
        console::print_config,
        helpers::{ build_symbol_map, build_tokio_async_runtime, run_api_with_timeout },
        symbol_interner::global,
    },
};
use anyhow::{ anyhow, Context, Result };
use tracing::{ error, info, warn };

pub fn run_test_execution(config: Config) -> Result<()> {
    print_config(&config);
    info!("🧪 Starting enhanced test execution mode - Single Path Test");

    if !config.should_enable_real_trading() && !config.should_enable_mock_trading() {
        error!("❌ Trading must be enabled for test execution");
        error!("   Set TAA_ENABLE_TRADING=true in your .env file");
        return Err(anyhow!("Trading not enabled in configuration"));
    }

    let api_client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    let rt = build_tokio_async_runtime()?;

    info!("🔍 Testing exchange connectivity...");
    if run_api_with_timeout(&rt, api_client.is_operational())? {
        info!("✅ Exchange is operational");
    } else {
        error!("❌ Exchange is not operational or timed out");
        return Err(anyhow!("Exchange is not operational"));
    }

    info!("📊 Fetching symbol information...");
    let symbols = run_api_with_timeout(&rt, api_client.get_active_spot_symbols())?;
    info!("✅ Fetched {} symbols from exchange", symbols.len());

    let symbol_map = build_symbol_map(symbols.clone(), &config)?;

    info!("🔧 Creating FIX executor...");
    let fix_executor = create_fix_executor(symbol_map.clone(), &config)?;

    if let Some(executor) = fix_executor {
        info!("🚀 Starting triangular arbitrage test execution...");

        if !executor.is_healthy() {
            warn!("⚠️  Connection health check failed, attempting to reset...");
            executor.reset_connection_health();
        }

        let test_opportunities = create_test_opportunities(&symbol_map, &config)?;

        if test_opportunities.is_empty() {
            error!("❌ No test opportunities could be created");
            return Err(anyhow!("No suitable trading pairs found for test"));
        }

        info!("📋 Created {} test opportunities", test_opportunities.len());

        let mut successful_tests = 0;
        let mut failed_tests = 0;

        // for (i, test_opportunity) in test_opportunities.iter().enumerate() {
        //     info!("");
        //     info!("🎯 Test {}/{}: {}", i + 1, test_opportunities.len(), test_opportunity.display());

        //     info!("   💰 Expected profit: {:.4}%", test_opportunity.profit_percentage());
        //     info!("   💵 Start amount: {:.8} {}", test_opportunity.start_amount, config.base_asset);

        //     let success = executor.force_execute_opportunity(test_opportunity.clone());

        //     if success {
        //         successful_tests += 1;
        //         info!("✅ Test {}/{} completed successfully!", i + 1, test_opportunities.len());
        //     } else {
        //         failed_tests += 1;
        //         error!("❌ Test {}/{} failed", i + 1, test_opportunities.len());
        //     }

        //     if i < test_opportunities.len() - 1 {
        //         info!("⏳ Waiting 10 seconds before next test to respect rate limits...");
        //         thread::sleep(Duration::from_secs(10));
        //     }
        // }

        let symbol = "BTCUSDT".to_string();

        let precision_info = symbol_map.get(&symbol);

        let (qty_precision, price_precision) = if let Some(info) = precision_info {
            (info.qty_precision, info.price_precision)
        } else {
            (8, 8)
        };

        executor.place_single_order(MarketOrder {
            symbol: symbol,
            side: crate::exchange::fix_api::OrderSide::Buy,
            quantity: 0.0,
            quote_quantity: Some(10.0),
            qty_precision: Some(qty_precision),
            price_precision: Some(price_precision),
        });

        info!("");
        info!("📊 Test Execution Summary:");
        info!("   ✅ Successful tests: {}", successful_tests);
        info!("   ❌ Failed tests: {}", failed_tests);
        info!(
            "   📈 Success rate: {:.1}%",
            ((successful_tests as f64) / (test_opportunities.len() as f64)) * 100.0
        );

        let stats = executor.get_execution_stats();
        info!("📈 Detailed Statistics: {}", stats);

        if let Err(e) = executor.disconnect() {
            error!("Error disconnecting FIX executor: {}", e);
        } else {
            info!("✅ FIX executor disconnected successfully");
        }

        if successful_tests > 0 {
            info!("🎉 Test execution completed successfully!");
            info!("   Your FIX API configuration is working correctly");
            info!("   You can now run the full arbitrage system");
        } else {
            error!("💥 All tests failed!");
            error!("   Please check your FIX API configuration");
            error!("   Common issues:");
            error!("   - Incorrect API key or private key");
            error!("   - IP not whitelisted on Binance");
            error!("   - Insufficient account permissions");
            error!("   - Rate limits exceeded");
        }

        Ok(())
    } else {
        error!("❌ No FIX executor available for testing");
        Err(anyhow!("FIX executor not configured"))
    }
}

fn create_fix_executor(
    symbol_map: Arc<crate::models::symbol_map::SymbolMap>,
    config: &Config
) -> Result<Option<Arc<FixArbitrageExecutor>>> {
    if config.should_enable_real_trading() || config.should_enable_mock_trading() {
        let mode = if config.debug { "Mock" } else { "Real" };
        info!("🔧 Creating FIX executor ({}) for test execution", mode);

        let fix_config = if config.debug { None } else { Some(config.get_fix_client_config()) };

        match FixArbitrageExecutor::new(config.debug, fix_config, symbol_map.clone()) {
            Ok(executor) => {
                info!("🔗 Connecting to FIX API...");
                if let Err(e) = executor.connect() {
                    error!("❌ Failed to connect FIX executor: {}", e);
                    error!("   Please check your FIX API configuration:");
                    error!("   - TAA_FIX_API (your API key)");
                    error!("   - TAA_FIX_PRIVATE_KEY_BASE64 (your base64 encoded private key)");
                    error!("   - TAA_FIX_ENDPOINT (should be fix-oe.binance.com:9000)");
                    return Err(anyhow!("Failed to connect FIX executor: {}", e));
                }

                let executor_arc = Arc::new(executor);
                info!("✅ FIX executor connected successfully for test execution");

                Ok(Some(executor_arc))
            }
            Err(e) => {
                error!("❌ Failed to create FIX executor: {}", e);
                Err(anyhow!("Failed to create FIX executor: {}", e))
            }
        }
    } else {
        error!("❌ Trading must be enabled for test execution");
        error!("   Set TAA_ENABLE_TRADING=true in your .env file");
        Err(anyhow!("Trading not enabled in configuration"))
    }
}

fn create_test_opportunities(
    symbol_map: &Arc<crate::models::symbol_map::SymbolMap>,
    config: &Config
) -> Result<Vec<ArbitrageOpportunity>> {
    let mut opportunities = Vec::new();

    let test_paths = [("BTCUSDT", "ETHBTC", "ETHUSDT", false, false, true)];

    info!("🔍 Creating test opportunities from predefined paths...");

    for (
        i,
        &(first_symbol, second_symbol, third_symbol, first_dir, second_dir, third_dir),
    ) in test_paths.iter().enumerate() {
        let first_exists = symbol_map.get(first_symbol).is_some();
        let second_exists = symbol_map.get(second_symbol).is_some();
        let third_exists = symbol_map.get(third_symbol).is_some();

        if !first_exists || !second_exists || !third_exists {
            warn!(
                "⚠️  Skipping test path {}: {} → {} → {} (some symbols not available)",
                i + 1,
                first_symbol,
                second_symbol,
                third_symbol
            );
            continue;
        }

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

        let test_amount = (config.trade_amount * 0.1).max(10.0); // 10% of normal amount, minimum $10

        let opportunity = ArbitrageOpportunity {
            path: Arc::new(path),
            profit_ratio: 1.005, // Simulate 0.5% profit for testing
            start_amount: test_amount,
            symbols_involved: [first_symbol_id, second_symbol_id, third_symbol_id],
        };

        info!(
            "✅ Created test opportunity {}: {} → {} → {} (amount: {:.8})",
            i + 1,
            first_symbol,
            second_symbol,
            third_symbol,
            test_amount
        );

        opportunities.push(opportunity);
    }

    if opportunities.is_empty() {
        error!("❌ Could not create any test opportunities");
        error!("   Available symbols might not include the required trading pairs");
        error!("   Check if your account has access to spot trading");

        let all_symbols = symbol_map.get_all_symbols();

        let available_usdt_pairs: Vec<_> = all_symbols
            .iter()
            .filter(|s| s.symbol.ends_with("USDT") && s.is_trading)
            .take(10)
            .map(|s| s.symbol.as_ref())
            .collect();

        if !available_usdt_pairs.is_empty() {
            info!("💡 Available USDT pairs for reference: {:?}", available_usdt_pairs);
        }
    }

    Ok(opportunities)
}
