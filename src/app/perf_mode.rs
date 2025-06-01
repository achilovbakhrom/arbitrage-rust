// src/app/perf_mode.rs - Updated with FIX integration
use std::sync::Arc;

use crate::{
    arbitrage::{ self },
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient },
    executor::{
        executor::{ ArbitrageExecutor, ExecutionStrategy },
        fix_executor::FixArbitrageExecutor,
    },
    models::symbol_map::SymbolMap,
    orderbook::manager::OrderBookManager,
    performance,
    utils::console::print_config,
    API_TIMEOUT,
};
use anyhow::{ anyhow, Context, Result };
use tracing::{ error, info };

pub fn create_output_file() -> Result<String> {
    // Create output directory
    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let output_dir = format!("./performance_results/{}", timestamp);
    std::fs
        ::create_dir_all(&output_dir)
        .context(format!("Failed to create output directory: {}", output_dir))?;

    let output_file = format!("{}/performance_results.csv", output_dir);
    info!("Results will be saved to: {}", output_file);

    Ok(output_file)
}

pub fn run_performance_test(config: Config) -> Result<()> {
    // Output information
    print_config(&config);
    info!("Starting optimized performance test for triangular arbitrage system");
    info!("Test duration: 120 seconds");

    let output_file = create_output_file()?;

    // Initialize exchange client
    let client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.fix_secret.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    // Create a runtime for async operations
    let rt = tokio::runtime::Builder
        ::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .context("Failed to create Tokio runtime")?;

    // Verify exchange connectivity
    let is_operational = rt.block_on(async {
        tokio::time::timeout(API_TIMEOUT, client.is_operational()).await
    });

    match is_operational {
        Ok(Ok(true)) => {
            info!("✓ Exchange is operational");
        }
        _ => {
            error!("❌ Exchange is not operational or timed out");
            return Err(anyhow!("Exchange is not operational"));
        }
    }

    // Fetch symbols
    let symbols = rt.block_on(async {
        tokio::time::timeout(API_TIMEOUT, client.get_active_spot_symbols()).await
    });

    let symbols = match symbols {
        Ok(Ok(symbols)) => symbols,
        Ok(Err(e)) => {
            error!("Failed to fetch symbols: {}", e);
            return Err(anyhow!("Failed to fetch symbols: {}", e));
        }
        Err(_) => {
            error!("Timed out while fetching symbols");
            return Err(anyhow!("Timed out while fetching symbols"));
        }
    };

    info!("✓ Fetched {} symbols from exchange", symbols.len());

    // Create symbol map and find triangular paths
    let mut symbol_map = SymbolMap::from_symbols(symbols.clone());

    info!("Finding triangular arbitrage paths starting with {}...", config.base_asset);
    symbol_map.find_targeted_triangular_paths(
        &config.base_asset,
        config.max_triangles,
        &config.excluded_coins
    );

    let total_paths = symbol_map.get_triangular_paths().len();

    if total_paths == 0 {
        error!("No triangular paths found. Cannot continue.");
        return Err(anyhow!("No triangular paths found. Cannot continue."));
    }

    info!("Found {} potential triangular arbitrage paths", total_paths);

    // Get unique symbols for market data
    let unique_symbols = symbol_map.get_unique_symbols();

    // Create orderbook manager
    let orderbook_manager = Arc::new(OrderBookManager::new(config.depth, client.clone()));

    // Create fallback executor for compatibility
    let fallback_executor = Arc::new(
        ArbitrageExecutor::new(
            orderbook_manager.clone(),
            ExecutionStrategy::FastSequential // Maximum speed
        )
    );

    // Create FIX executor for performance testing if trading is enabled
    let fix_executor = if
        config.should_enable_real_trading() ||
        config.should_enable_mock_trading()
    {
        info!("Creating FIX executor for performance test ({})", if config.debug {
            "Mock"
        } else {
            "Real"
        });

        let fix_config = if config.debug {
            None // Mock client doesn't need FIX config
        } else {
            Some(config.get_fix_client_config())
        };

        match
            FixArbitrageExecutor::new(
                orderbook_manager.clone(),
                config.debug, // Use debug mode flag
                fix_config,
                config.max_slippage_percentage,
                config.min_execution_profit_threshold
            )
        {
            Ok(executor) => {
                let executor = Arc::new(executor);

                // Connect to FIX API
                if let Err(e) = executor.connect() {
                    error!("Failed to connect FIX executor: {}", e);
                    return Err(anyhow!("Failed to connect FIX executor: {}", e));
                }

                info!("✓ FIX executor connected for performance test");
                Some(executor)
            }
            Err(e) => {
                error!("Failed to create FIX executor: {}", e);
                return Err(anyhow!("Failed to create FIX executor: {}", e));
            }
        }
    } else {
        info!("Trading disabled for performance test, using simulation mode only");
        None
    };

    // Convert paths to Arc for zero-copy sharing
    let triangular_paths: Vec<Arc<_>> = symbol_map
        .get_triangular_paths()
        .iter()
        .map(|p| Arc::new(p.clone()))
        .collect();

    // Create the enhanced arbitrage detector with FIX integration
    let detector = arbitrage::detector::create_detector_with_fix(
        orderbook_manager.clone(),
        config.fee, // 0.1% fee
        config.threshold, // Configured minimum profit threshold
        triangular_paths.clone(),
        config.trade_amount, // Start with configured amount
        fallback_executor.clone(), // Pass the fallback executor
        fix_executor.clone(), // Pass the FIX executor
        config.min_volume_multiplier, // Require 2x trade amount in liquidity
        true // is_perf flag
    );

    let detector_mode = if fix_executor.is_some() {
        if config.debug { "FIX Mock Trading" } else { "FIX Real Trading" }
    } else {
        "Simulation Only"
    };

    info!(
        "Created optimized ultra-fast arbitrage detector with synchronous execution, threshold: {:.2}% (Mode: {})",
        config.threshold * 100.0,
        detector_mode
    );

    info!("Created arbitrage detector. Starting optimized performance test...");

    // Run the optimized performance test
    let test_result = performance::run_performance_test(
        config.sbe_api_key,
        unique_symbols,
        orderbook_manager.clone(),
        detector.clone(),
        300, // 5 minutes
        output_file,
        triangular_paths // Pass the triangular paths
    );

    // Cleanup: Disconnect FIX executor if it exists
    if let Some(fix_exec) = fix_executor {
        if let Err(e) = fix_exec.disconnect() {
            error!("Error disconnecting FIX executor: {}", e);
        } else {
            info!("FIX executor disconnected successfully");
        }
    }

    match test_result {
        Ok(_) => {
            info!("Optimized performance test completed successfully!");

            // Print final FIX executor stats if available
            if let Some(fix_stats) = detector.get_fix_execution_stats() {
                info!("Final FIX Execution Stats: {}", fix_stats);
            }
        }
        Err(e) => {
            error!("Optimized performance test failed: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
