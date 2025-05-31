use std::sync::Arc;

use crate::{
    arbitrage::{ self, executor::{ ArbitrageExecutor, ExecutionStrategy } },
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient },
    models::symbol_map::SymbolMap,
    orderbook::manager::OrderBookManager,
    performance,
    API_TIMEOUT,
};
use anyhow::{ anyhow, Context, Result };
use tracing::{ error, info };

pub fn run_performance_test(config: Config) -> Result<()> {
    // Output information
    info!("Starting optimized performance test for triangular arbitrage system");
    info!("Test duration: 120 seconds");

    // Create output directory
    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let output_dir = format!("./performance_results/{}", timestamp);
    std::fs
        ::create_dir_all(&output_dir)
        .context(format!("Failed to create output directory: {}", output_dir))?;

    let output_file = format!("{}/performance_results.csv", output_dir);
    info!("Results will be saved to: {}", output_file);

    // Initialize exchange client
    let client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.fix_secret.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    // Create a runtime for async operations
    let rt = tokio::runtime::Builder
        ::new_multi_thread()
        .worker_threads(2)
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

    let executor = Arc::new(
        ArbitrageExecutor::new(
            orderbook_manager.clone(),
            ExecutionStrategy::FastSequential // Maximum speed
        )
    );

    // Convert paths to Arc for zero-copy sharing
    let triangular_paths: Vec<Arc<_>> = symbol_map
        .get_triangular_paths()
        .iter()
        .map(|p| Arc::new(p.clone()))
        .collect();

    // Create the enhanced arbitrage detector
    let detector = arbitrage::detector::create_detector(
        orderbook_manager.clone(),
        config.fee, // 0.1% fee
        config.threshold, // Configured minimum profit threshold
        triangular_paths.clone(),
        config.trade_amount, // Start with configured amount
        executor.clone(), // Pass the executor
        config.min_volume_multiplier, // Require 2x trade amount in liquidity
        config.depth, // Check top 5 price levels
        false // is_perf flag
    );

    info!(
        "Created optimized ultra-fast arbitrage detector with synchronous execution, threshold: {:.2}%",
        config.threshold * 100.0
    );

    info!("Created arbitrage detector. Starting optimized performance test...");

    // Run the optimized performance test
    let test_result = performance::run_performance_test(
        config.sbe_api_key,
        unique_symbols,
        orderbook_manager.clone(),
        detector.clone(),
        120, // 2 minutes
        output_file,
        triangular_paths // Pass the triangular paths
    );

    match test_result {
        Ok(_) => {
            info!("Optimized performance test completed successfully!");
        }
        Err(e) => {
            error!("Optimized performance test failed: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
