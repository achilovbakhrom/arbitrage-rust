use std::{ sync::{ atomic::{ AtomicBool, Ordering }, Arc }, thread, time::Duration };
use crate::{
    arbitrage::{ self, detector::ArbitrageDetector },
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient },
    executor::fix_executor::FixArbitrageExecutor,
    orderbook::manager::OrderBookManager,
    performance,
    utils::{
        console::print_config,
        helpers::{ build_symbol_map, build_tokio_async_runtime, run_api_with_timeout },
        symbol_interner::global,
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
    info!("Starting performance test for triangular arbitrage system");
    info!("Test duration: 120 seconds");

    let output_file = create_output_file()?;

    let client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    let rt = build_tokio_async_runtime()?;

    if run_api_with_timeout(&rt, client.is_operational())? {
        info!("✓ Exchange is operational");
    } else {
        error!("❌ Exchange is not operational or timed out");
        return Err(anyhow!("Exchange is not operational"));
    }

    let symbols = run_api_with_timeout(&rt, client.get_active_spot_symbols())?;
    info!("✓ Fetched {} symbols from exchange", symbols.len());

    let symbol_map = build_symbol_map(symbols.clone(), &config)?;
    let unique_symbols = symbol_map.get_unique_symbols();

    let orderbook_manager = Arc::new(OrderBookManager::new(config.depth, client.clone()));
    prepare_symbol_mappings(&unique_symbols);

    let triangular_paths = symbol_map.get_trangular_paths_as_arc();

    let detector = arbitrage::detector::create_detector(
        orderbook_manager.clone(),
        config.fee,
        config.threshold,
        triangular_paths.clone(),
        config.trade_amount,
        config.min_volume_multiplier
    );

    let fix_executor = create_fix_executor(orderbook_manager.clone(), symbol_map.clone(), &config)?;

    let execution_handler = start_execution_thread(detector.clone(), fix_executor.clone());

    let detector_mode = if fix_executor.is_some() {
        if config.debug {
            "FIX Mock Trading (Performance Test)"
        } else {
            "FIX Real Trading (Performance Test)"
        }
    } else {
        "Simulation Only (Performance Test)"
    };

    info!(
        "Created arbitrage detector with threshold: {:.2}% (Mode: {})",
        config.threshold * 100.0,
        detector_mode
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let detection_handle = start_detection_thread(
        detector.clone(),
        orderbook_manager.clone(),
        shutdown.clone()
    );

    let opt_stats = orderbook_manager.get_optimization_stats();
    let detector_opt_stats = detector.get_optimization_stats();
    info!("Optimization stats - OrderBook: {} books", opt_stats.total_books);
    info!(
        "Optimization stats - Detector: {} paths, {} symbol map entries",
        detector_opt_stats.total_paths,
        detector_opt_stats.symbol_map_entries
    );

    info!("Starting performance test...");

    let test_result = performance::run_performance_test(
        config.sbe_api_key,
        unique_symbols,
        orderbook_manager.clone(),
        detector.clone(),
        600,
        output_file,
        triangular_paths
    );

    shutdown.store(true, Ordering::Relaxed);
    detector.shutdown();

    if let Some(fix_exec) = fix_executor {
        if let Err(e) = fix_exec.disconnect() {
            error!("Error disconnecting FIX executor: {}", e);
        } else {
            info!("FIX executor disconnected successfully");
        }
    }

    if let Err(e) = detection_handle.join() {
        error!("Error joining detection thread: {:?}", e);
    }

    if let Err(e) = execution_handler.join() {
        error!("Error joining execution thread: {:?}", e);
    }

    match test_result {
        Ok(_) => {
            info!("Performance test completed successfully!");

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
        }
        Err(e) => {
            error!("Performance test failed: {}", e);
            return Err(e);
        }
    }

    Ok(())
}

fn create_fix_executor(
    orderbook_manager: Arc<OrderBookManager>,
    symbol_map: Arc<crate::models::symbol_map::SymbolMap>,
    config: &Config
) -> Result<Option<Arc<FixArbitrageExecutor>>> {
    if config.should_enable_real_trading() || config.should_enable_mock_trading() {
        info!("Creating FIX executor ({}) for performance test", if config.debug {
            "Mock"
        } else {
            "Real"
        });

        let fix_config = if config.debug { None } else { Some(config.get_fix_client_config()) };

        match
            FixArbitrageExecutor::new(
                orderbook_manager,
                config.debug,
                fix_config,
                config.min_execution_profit_threshold,
                symbol_map.clone()
            )
        {
            Ok(mut executor) => {
                if let Err(e) = executor.connect() {
                    error!("Failed to connect FIX executor: {}", e);
                    return Err(anyhow!("Failed to connect FIX executor: {}", e));
                }

                executor.enable_callback_execution(3);
                let executor_arc = Arc::new(executor);

                info!("✓ FIX executor connected successfully for performance test");
                Ok(Some(executor_arc))
            }
            Err(e) => {
                error!("Failed to create FIX executor: {}", e);
                Err(anyhow!("Failed to create FIX executor: {}", e))
            }
        }
    } else {
        info!("Trading disabled, using simulation mode only");
        Ok(None)
    }
}

fn start_detection_thread(
    detector: Arc<arbitrage::detector::ArbitrageDetector>,
    orderbook_manager: Arc<OrderBookManager>,
    shutdown: Arc<AtomicBool>
) -> thread::JoinHandle<()> {
    thread::Builder
        ::new()
        .name("perf-arbitrage-detector".to_string())
        .spawn(move || {
            info!("Performance arbitrage detection thread started");

            let update_callback = {
                let detector = detector.clone();
                Arc::new(move |symbol_id: u16, _book: &crate::orderbook::orderbook::OrderBook| {
                    detector.process_symbol_update(symbol_id);
                })
            };

            orderbook_manager.register_update_callback(update_callback);

            let stats_start = std::time::Instant::now();

            while !shutdown.load(Ordering::Relaxed) && !detector.is_shutdown() {
                thread::sleep(Duration::from_millis(100));

                if stats_start.elapsed() > Duration::from_secs(30) {
                    let stats = detector.get_performance_stats();
                    info!(
                        "Performance stats: {} symbols processed, {} paths checked, {} opportunities found",
                        stats.symbols_processed,
                        stats.paths_checked,
                        stats.opportunities_found
                    );
                }
            }

            info!("Performance arbitrage detection thread stopped");
        })
        .expect("Failed to spawn performance arbitrage detection thread")
}

fn prepare_symbol_mappings(symbols: &[String]) -> Vec<(String, u16)> {
    global::intern_batch(symbols);
    global::get_all_mappings()
}

fn start_execution_thread(
    detector: Arc<ArbitrageDetector>,
    fix_executor: Option<Arc<FixArbitrageExecutor>>
) -> thread::JoinHandle<()> {
    thread::Builder
        ::new()
        .name("execution".to_string())
        .spawn(move || {
            if let Some(ref _executor) = fix_executor {
                let execution_callback = Arc::new(
                    move |opportunity: arbitrage::detector::ArbitrageOpportunity| {
                        info!(
                            "Execution opportunity: {} with {:.4}% profit",
                            opportunity.display(),
                            opportunity.profit_percentage()
                        );
                    }
                );
                detector.register_execution_callback(execution_callback);
            }
        })
        .expect("Failed to spawn execution thread")
}
