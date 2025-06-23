use std::sync::Arc;

use crate::{
    arbitrage::{ self },
    config::Config,
    exchange::binance::BinanceClient,
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
            prepare_symbol_mappings,
        },
    },
};
use anyhow::{ anyhow, Context, Result };
use tracing::{ error, info };

pub fn create_output_file() -> Result<String> {
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
    print_config(&config);
    info!("Starting ZERO-CLONING performance test for triangular arbitrage system");
    info!("Test duration: 120 seconds");

    let output_file = create_output_file()?;

    let client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    let rt = build_tokio_async_runtime()?;

    if is_operational(&rt, client.clone()) {
        info!("✓ Exchange is operational");
    } else {
        error!("❌ Exchange is not operational or timed out");
        return Err(anyhow!("Exchange is not operational"));
    }

    let symbols = get_all_symbols(&rt, client.clone())?;
    info!("✓ Fetched {} symbols from exchange", symbols.len());

    let symbol_map = build_symbol_map(symbols.clone(), &config)?;
    let unique_symbols = symbol_map.get_unique_symbols();

    let orderbook_manager = Arc::new(OrderBookManager::new(config.depth, client.clone()));

    prepare_symbol_mappings(&unique_symbols);

    let fallback_executor = Arc::new(ArbitrageExecutor::new(ExecutionStrategy::FastSequential));

    let fix_executor = build_execultor(orderbook_manager.clone(), symbol_map.clone(), &config)?;

    let triangular_paths = symbol_map.get_trangular_paths_as_arc();

    let detector = arbitrage::detector::create_detector(
        orderbook_manager.clone(),
        config.fee,
        config.threshold,
        triangular_paths.clone(),
        config.trade_amount,
        fallback_executor.clone(),
        fix_executor.clone(),
        config.min_volume_multiplier,
        true
    );

    let detector_mode = if fix_executor.is_some() {
        if config.debug {
            "FIX Mock Trading (Zero-Cloning)"
        } else {
            "FIX Real Trading (Zero-Cloning)"
        }
    } else {
        "Simulation Only (Zero-Cloning)"
    };

    info!(
        "Created ZERO-CLONING arbitrage detector with threshold: {:.2}% (Mode: {})",
        config.threshold * 100.0,
        detector_mode
    );

    let _detection_handle = arbitrage::detector::start_detection_thread(detector.clone());

    let opt_stats = orderbook_manager.get_optimization_stats();
    let detector_opt_stats = detector.get_optimization_stats();
    info!("Optimization stats - OrderBook: {} books", opt_stats.total_books);
    info!(
        "Optimization stats - Detector: {} paths, {} symbol map entries",
        detector_opt_stats.total_paths,
        detector_opt_stats.symbol_map_entries
    );

    info!("Starting ZERO-CLONING performance test...");

    let test_result = performance::run_performance_test(
        config.sbe_api_key,
        unique_symbols,
        orderbook_manager.clone(),
        detector.clone(),
        120,
        output_file,
        triangular_paths
    );

    if let Some(fix_exec) = fix_executor {
        if let Err(e) = fix_exec.disconnect() {
            error!("Error disconnecting FIX executor: {}", e);
        } else {
            info!("FIX executor disconnected successfully");
        }
    }

    match test_result {
        Ok(_) => {
            info!("ZERO-CLONING performance test completed successfully!");

            let final_opt_stats = orderbook_manager.get_optimization_stats();

            info!(
                "Final optimization stats: {} books, {} hash cache",
                final_opt_stats.total_books,
                final_opt_stats.symbol_hash_cache_size
            );

            if let Some(fix_stats) = detector.get_fix_execution_stats() {
                info!("Final FIX Execution Stats: {}", fix_stats);
            }

            let exec_stats = detector.get_execution_stats();
            info!(
                "Final execution stats: {} opportunities, {} executions, {} profitable",
                exec_stats.0,
                exec_stats.1,
                exec_stats.2
            );

            info!("ZERO-CLONING OPTIMIZATIONS APPLIED:");
            info!("  ✓ Symbol ID system eliminates string operations");
            info!("  ✓ Direct slice references eliminate array cloning");
            info!("  ✓ OrderBook references eliminate Arc cloning");
            info!("  ✓ Precomputed path data eliminates runtime allocations");
            info!("  ✓ Fixed-size arrays eliminate Vec reallocations");
            info!("  ✓ Cache-aligned structures optimize memory access");
            info!("  ✓ Separate arbitrage detection thread eliminates blocking");
        }
        Err(e) => {
            error!("ZERO-CLONING performance test failed: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
