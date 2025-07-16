use std::{ sync::{ atomic::{ AtomicBool, Ordering }, Arc }, thread, time::{ Duration, Instant } };
use crate::{
    arbitrage::{ self, detector::ArbitrageDetector },
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient },
    executor::fix_executor::FixArbitrageExecutor,
    orderbook_manager::manager::OrderBookManager,
    performance,
    utils::{
        console::print_config,
        helpers::{
            build_symbol_map,
            build_tokio_async_runtime,
            create_output_file,
            print_executor_info,
            run_api_with_timeout,
        },
        symbol_interner::global,
        types::FastHashMap,
    },
};
use ahash::HashMapExt;
use anyhow::{ anyhow, Context, Result };
use parking_lot::RwLock;
use tracing::{ error, info };

pub fn run_performance_test(config: Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    print_config(&config);
    info!("Starting performance test for triangular arbitrage system");

    let output_file = create_output_file()?;

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
    let triangular_paths = symbol_map.get_trangular_paths_as_arc();
    let unique_symbols = symbol_map.get_unique_symbols();

    let orderbook_manager = Arc::new(
        OrderBookManager::new(config.depth, Arc::<BinanceClient>::clone(&api_client))
    );
    global::intern_batch(&unique_symbols);

    let detection_times: Arc<RwLock<FastHashMap<u16, Duration>>> = Arc::new(
        RwLock::new(FastHashMap::with_capacity(2048))
    );

    let detector = arbitrage::detector::create_detector(
        Arc::clone(&orderbook_manager),
        triangular_paths.clone(),
        Arc::clone(&shutdown),
        config.fee,
        config.threshold,
        config.trade_amount,
        config.min_volume_multiplier
    );

    let detection_handle = start_detection_thread(
        Arc::clone(&orderbook_manager),
        Arc::clone(&detector),
        Arc::clone(&shutdown),
        Arc::clone(&detection_times)
    );

    let fix_executor = create_fix_executor(Arc::clone(&symbol_map), &config)?;
    print_executor_info(fix_executor.is_some(), config.debug, config.threshold);

    let execution_handler = start_execution_thread(Arc::clone(&detector), Arc::clone(&shutdown));

    info!("Starting performance test...");
    let test_result = performance::run_performance_test(
        config.sbe_api_key,
        unique_symbols,
        Arc::clone(&orderbook_manager),
        config.performance_test_duration_secs,
        output_file,
        shutdown,
        detection_times
    );

    if let Some(fix_exec) = fix_executor {
        if let Err(e) = fix_exec.disconnect() {
            error!("Error disconnecting FIX executor: {}", e);
        } else {
            info!("FIX executor disconnected successfully");
        }
    }

    info!("Waiting for threads to complete...");

    if let Err(e) = detection_handle.join() {
        error!("Error joining detection thread: {:?}", e);
    } else {
        info!("Detection thread completed");
    }

    if let Err(e) = execution_handler.join() {
        error!("Error joining execution thread: {:?}", e);
    } else {
        info!("Execution thread completed");
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
        }
        Err(e) => {
            error!("Performance test failed: {}", e);
            return Err(e);
        }
    }

    Ok(())
}

fn start_detection_thread(
    orderbook_manager: Arc<OrderBookManager>,
    detector: Arc<arbitrage::detector::ArbitrageDetector>,
    shutdown: Arc<AtomicBool>,
    detection_times: Arc<RwLock<FastHashMap<u16, Duration>>>
) -> thread::JoinHandle<()> {
    thread::Builder
        ::new()
        .name("perf-arbitrage-detector".to_string())
        .spawn(move || {
            info!("Performance arbitrage detection thread started");

            orderbook_manager.register_update_callback({
                let detector = Arc::clone(&detector);
                Arc::new(
                    move |symbol_id: u16, _book: &crate::orderbook_manager::orderbook::OrderBook| {
                        let detection_start = Instant::now();
                        detector.process_symbol_update(symbol_id);
                        let detection_time = detection_start.elapsed();

                        detection_times.write().insert(symbol_id, detection_time);
                    }
                )
            });

            while !shutdown.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

            info!("Performance arbitrage detection thread stopped");
        })
        .expect("Failed to spawn performance arbitrage detection thread")
}

fn start_execution_thread(
    detector: Arc<ArbitrageDetector>,
    shutdown: Arc<AtomicBool>
) -> thread::JoinHandle<()> {
    thread::Builder
        ::new()
        .name("execution".to_string())
        .spawn(move || {
            detector.register_execution_callback(
                Arc::new(move |opportunity: arbitrage::detector::ArbitrageOpportunity| {
                    info!(
                        "Execution opportunity: {} with {:.4}% profit",
                        opportunity.display(),
                        opportunity.profit_percentage()
                    );
                })
            );

            while !shutdown.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

            info!("Execution thread stopped");
        })
        .expect("Failed to spawn execution thread")
}

fn create_fix_executor(
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
                config.debug,
                fix_config,
                config.clone(),
                Arc::clone(&symbol_map)
            )
        {
            Ok(executor) => {
                if let Err(e) = executor.connect() {
                    error!("Failed to connect FIX executor: {}", e);
                    return Err(anyhow!("Failed to connect FIX executor: {}", e));
                }

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
