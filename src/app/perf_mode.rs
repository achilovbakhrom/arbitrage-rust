use std::sync::Arc;

use crate::{
    arbitrage::{ self },
    config::Config,
    exchange::binance::BinanceClient,
    orderbook::manager::OrderBookManager,
    performance,
    utils::{
        console::print_config,
        helpers::{
            build_symbol_map,
            build_tokio_async_runtime,
            get_all_symbols,
            is_operational,
            create_detector_and_executor,
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
    info!("Starting CALLBACK-OPTIMIZED performance test for triangular arbitrage system");
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

    let (detector, fix_executor) = create_detector_and_executor(
        orderbook_manager.clone(),
        symbol_map.clone(),
        &config,
        true
    )?;

    let detector_mode = if fix_executor.is_some() {
        if config.debug {
            "FIX Mock Trading (Callback-Optimized)"
        } else {
            "FIX Real Trading (Callback-Optimized)"
        }
    } else {
        "Simulation Only (Callback-Optimized)"
    };

    info!(
        "Created CALLBACK-OPTIMIZED arbitrage detector with threshold: {:.2}% (Mode: {})",
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

    info!("Starting CALLBACK-OPTIMIZED performance test...");

    let triangular_paths = symbol_map.get_trangular_paths_as_arc();

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
            info!("CALLBACK-OPTIMIZED performance test completed successfully!");

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

            info!("CALLBACK-BASED OPTIMIZATIONS APPLIED:");
            info!("  ✓ Direct callback execution eliminates channel overhead");
            info!("  ✓ Zero-copy opportunity passing");
            info!("  ✓ Independent FIX executor threads with symbol conflict detection");
            info!("  ✓ Lock-free opportunity detection");
            info!("  ✓ CPU affinity optimization for hot paths");
            info!("  ✓ Minimal memory allocations in critical path");
            info!("  ✓ Symbol ID system eliminates string operations");
            info!("  ✓ Direct slice references eliminate array cloning");
            info!("  ✓ OrderBook references eliminate Arc cloning");
            info!("  ✓ Precomputed path data eliminates runtime allocations");
            info!("  ✓ Fixed-size arrays eliminate Vec reallocations");
            info!("  ✓ Cache-aligned structures optimize memory access");
            info!("  ✓ Separate arbitrage detection thread eliminates blocking");
        }
        Err(e) => {
            error!("CALLBACK-OPTIMIZED performance test failed: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
