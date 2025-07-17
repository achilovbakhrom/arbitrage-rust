use std::sync::Arc;
use std::sync::atomic::{ AtomicBool, Ordering };
use crate::{
    arbitrage::detector::ArbitrageOpportunity,
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient },
    utils::{
        console::print_config,
        helpers::{ build_symbol_map, build_tokio_async_runtime, run_api_with_timeout },
        validation::{ validate_test_execution_config, print_trading_mode_info },
    },
    utils::{
        fix_ops::{ create_fix_client, execute_triangular_trades },
        order_ops::TriangularTradeData,
    },
};
use anyhow::{ anyhow, Context, Result };
use tracing::{ error, info, warn };

pub fn run(config: Config, shutdown: Option<Arc<AtomicBool>>) -> Result<()> {
    print_config(&config);
    info!("🧪 Starting triangular arbitrage test execution");

    validate_test_execution_config(&config)?;

    let shutdown = shutdown.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

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

    let fix_config = if config.debug { None } else { Some(config.get_fix_client_config()) };

    match create_fix_client(config.debug, fix_config, config.clone(), &symbol_map)? {
        Some(client) => {
            info!("🔧 Creating {} FIX client for testing...", if config.debug {
                "Mock"
            } else {
                "Real"
            });

            client.connect()?;
            info!("✅ FIX client connected successfully");

            print_trading_mode_info(true, config.debug);

            let opportunity = create_test_opportunity(&config, &symbol_map)?;
            info!("📋 Created test triangular arbitrage:");
            info!("   Path: {}", opportunity.display());

            if shutdown.load(Ordering::Relaxed) {
                info!("🛑 Test cancelled by user");
                client.disconnect()?;
                return Ok(());
            }

            if !config.debug {
                warn!("⚠️  REAL MONEY WARNING: This will execute actual trades!");
            }

            info!("🚀 Executing triangular arbitrage...");
            let trade_data = TriangularTradeData::from_opportunity(&opportunity);
            let success = execute_triangular_trades(&trade_data, &client, &symbol_map)?;

            if success {
                info!("🎉 {} triangular arbitrage completed successfully!", if config.debug {
                    "Mock"
                } else {
                    "Real"
                });
                info!("   All three orders executed correctly");
                if !config.debug {
                    info!("   Your FIX API is working for triangular arbitrage");
                }
            } else {
                warn!("⚠️  {} triangular arbitrage partially failed", if config.debug {
                    "Mock"
                } else {
                    "Real"
                });
            }

            info!("🔌 Disconnecting FIX client...");
            client.disconnect()?;
            info!("✅ FIX client disconnected successfully");

            Ok(())
        }
        None => Err(anyhow!("Failed to create FIX client")),
    }
}

fn create_test_opportunity(
    config: &Config,
    symbol_map: &Arc<crate::models::symbol_map::SymbolMap>
) -> Result<ArbitrageOpportunity> {
    let paths = symbol_map.get_triangular_paths();
    if paths.is_empty() {
        return Err(anyhow!("No triangular paths available for testing"));
    }

    let test_path = &paths[0];
    let test_amount = (config.trade_amount * 0.01).max(10.0);

    info!("📝 Test details:");
    info!(
        "   Path: {} → {} → {}",
        test_path.first_symbol,
        test_path.second_symbol,
        test_path.third_symbol
    );
    info!("   Amount: ${:.2}", test_amount);

    let opportunity = ArbitrageOpportunity {
        path: Arc::new(test_path.clone()),
        profit_ratio: 1.005,
        start_amount: test_amount,
        symbols_involved: [0, 1, 2],
    };

    Ok(opportunity)
}
