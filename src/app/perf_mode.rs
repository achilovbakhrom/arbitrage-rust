// src/app/perf_mode.rs - Updated to use optimizations
use std::sync::Arc;

use crate::{
    arbitrage::{ self },
    config::Config,
    exchange::{ binance::BinanceClient },
    executor::executor::{ ArbitrageExecutor, ExecutionStrategy },
    orderbook::manager::OrderBookManager,
    performance,
    utils::{
        console::print_config,
        helpers::{
            build_execultor,
            build_symbol_map,
            build_tokio_async_runtime,
            get_all_symbols,
            is_operational,
        },
    },
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
    info!("Starting ULTRA-OPTIMIZED performance test for triangular arbitrage system");
    info!("Test duration: 120 seconds");

    let output_file = create_output_file()?;

    // Initialize exchange client
    let client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.fix_secret.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    // Create a runtime for async operations
    let rt = build_tokio_async_runtime()?;

    // Verify exchange connectivity
    if is_operational(&rt, client.clone()) {
        info!("✓ Exchange is operational");
    } else {
        error!("❌ Exchange is not operational or timed out");
        return Err(anyhow!("Exchange is not operational"));
    }

    // Fetch symbols
    let symbols = get_all_symbols(&rt, client.clone())?;

    info!("✓ Fetched {} symbols from exchange", symbols.len());

    // Create symbol map and find triangular paths
    let symbol_map = build_symbol_map(symbols.clone(), &config)?;

    // Get unique symbols for market data
    let unique_symbols = symbol_map.get_unique_symbols();

    // Create OPTIMIZED orderbook manager
    let orderbook_manager = Arc::new(OrderBookManager::new(config.depth, client.clone()));

    // Preload symbols for maximum optimization
    orderbook_manager.preload_symbols(&unique_symbols);

    // Create fallback executor for compatibility
    let fallback_executor = Arc::new(
        ArbitrageExecutor::new(
            orderbook_manager.clone(),
            ExecutionStrategy::FastSequential // Maximum speed
        )
    );

    // Create FIX executor for performance testing if trading is enabled
    let fix_executor = build_execultor(orderbook_manager.clone(), symbol_map.clone(), &config)?;

    // Convert paths to Arc for zero-copy sharing
    let triangular_paths: Vec<Arc<_>> = symbol_map
        .get_triangular_paths()
        .iter()
        .map(|p| Arc::new(p.clone()))
        .collect();

    // Create the ULTRA-OPTIMIZED arbitrage detector with FIX integration
    let detector = arbitrage::detector::create_detector(
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
        if config.debug {
            "FIX Mock Trading (Ultra-Optimized)"
        } else {
            "FIX Real Trading (Ultra-Optimized)"
        }
    } else {
        "Simulation Only (Ultra-Optimized)"
    };

    info!(
        "Created ULTRA-OPTIMIZED arbitrage detector with threshold: {:.2}% (Mode: {})",
        config.threshold * 100.0,
        detector_mode
    );

    // Print optimization stats
    let opt_stats = orderbook_manager.get_optimization_stats();
    let detector_opt_stats = detector.get_optimization_stats();
    info!(
        "Optimization stats - OrderBook: {} books, {} interner entries",
        opt_stats.total_books,
        opt_stats.interner_string_entries
    );
    info!(
        "Optimization stats - Detector: {} paths, {} symbol map entries",
        detector_opt_stats.total_paths,
        detector_opt_stats.symbol_map_entries
    );

    info!("Starting ULTRA-OPTIMIZED performance test...");

    // Run the optimized performance test
    let test_result = performance::run_performance_test(
        config.sbe_api_key,
        unique_symbols,
        orderbook_manager.clone(),
        detector.clone(),
        600, // 10 minutes
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
            info!("ULTRA-OPTIMIZED performance test completed successfully!");

            // Print final optimization stats
            let final_opt_stats = orderbook_manager.get_optimization_stats();
            let final_detector_stats = detector.get_optimization_stats();

            info!(
                "Final optimization stats: {} books, {} interner entries, {} hash cache",
                final_opt_stats.total_books,
                final_opt_stats.interner_string_entries,
                final_opt_stats.symbol_hash_cache_size
            );

            // Print final FIX executor stats if available
            if let Some(fix_stats) = detector.get_fix_execution_stats() {
                info!("Final FIX Execution Stats: {}", fix_stats);
            }

            // Print final execution stats
            let exec_stats = detector.get_execution_stats();
            info!(
                "Final execution stats: {} opportunities, {} executions, {} profitable",
                exec_stats.0,
                exec_stats.1,
                exec_stats.2
            );
        }
        Err(e) => {
            error!("ULTRA-OPTIMIZED performance test failed: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
