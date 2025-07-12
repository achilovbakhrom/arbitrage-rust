use std::{ sync::{ atomic::{ AtomicBool, Ordering }, Arc }, thread, time::Duration };
use crate::{
    arbitrage::{ self, detector::ArbitrageDetector },
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient, sbe_client::BinanceSbeClient },
    executor::fix_executor::FixArbitrageExecutor,
    orderbook_manager::manager::OrderBookManager,
    utils::{
        console::{ print_app_started, print_app_starting, print_config },
        helpers::{ build_symbol_map, build_tokio_async_runtime, run_api_with_timeout },
        symbol_interner::global,
    },
    WEBSOCKET_BUFFER_SIZE,
};
use anyhow::{ anyhow, Context, Result };
use tracing::{ error, info, warn };

pub fn run_normal_mode(config: Config) -> Result<()> {
    print_app_starting();
    print_config(&config);

    let client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    info!("Connected to exchange: {}", client.name());

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
    let triangular_paths = symbol_map.get_trangular_paths_as_arc();
    let unique_symbols = symbol_map.get_unique_symbols();
    global::intern_batch(&unique_symbols);

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);

    let orderbook_manager = Arc::new(
        OrderBookManager::new(config.depth, Arc::<BinanceClient>::clone(&client))
    );
    global::intern_batch(&unique_symbols);

    let detector = arbitrage::detector::create_detector(
        Arc::clone(&orderbook_manager),
        triangular_paths,
        Arc::clone(&shutdown),
        config.fee,
        config.threshold,
        config.trade_amount,
        config.min_volume_multiplier
    );

    let detection_handle = start_detection_thread(
        Arc::clone(&detector),
        Arc::clone(&orderbook_manager),
        Arc::clone(&shutdown)
    );

    let fix_executor = create_fix_executor(Arc::clone(&symbol_map), &config)?;

    let execution_handler = start_execution_thread(
        Arc::clone(&detector),
        fix_executor.clone(),
        Arc::clone(&shutdown)
    );

    ctrlc
        ::set_handler(move || {
            info!("Received Ctrl+C, shutting down...");
            shutdown_clone.store(true, Ordering::Relaxed);
        })
        .context("Error setting Ctrl-C handler")?;

    let symbol_mappings = orderbook_manager.get_symbol_mappings();
    let ws_handle = start_websocket_thread(
        config.sbe_api_key.clone(),
        symbol_mappings.clone(),
        Arc::clone(&orderbook_manager),
        Arc::clone(&shutdown)
    )?;

    print_app_started();
    info!("\nPress Ctrl+C to exit");

    if fix_executor.is_some() {
        if config.debug {
            info!("🔵 FIX mock trading mode enabled");
        } else {
            info!("🔴 FIX REAL TRADING MODE ENABLED");
            info!("⚠️  Make sure you understand the risks!");
        }
    } else {
        info!("🟡 simulation mode");
    }

    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }

    if let Some(fix_exec) = fix_executor {
        if let Err(e) = fix_exec.disconnect() {
            error!("Error disconnecting FIX executor: {}", e);
        } else {
            info!("FIX executor disconnected successfully");
        }
    }

    if let Err(e) = ws_handle.join() {
        error!("Error joining WebSocket thread: {:?}", e);
    }

    if let Err(e) = detection_handle.join() {
        error!("Error joining detection thread: {:?}", e);
    }

    if let Err(e) = execution_handler.join() {
        error!("Error joining execution thread: {:?}", e);
    }

    thread::sleep(Duration::from_millis(200));

    info!("Triangular arbitrage system stopped");
    Ok(())
}

fn start_websocket_thread(
    sbe_api_key: String,
    symbol_mappings: Vec<(String, u16)>,
    orderbook_manager: Arc<OrderBookManager>,
    shutdown: Arc<AtomicBool>
) -> Result<thread::JoinHandle<()>> {
    let handle = thread::Builder
        ::new()
        .name("websocket-thread".to_string())
        .spawn(move || {
            let client = BinanceSbeClient::new(sbe_api_key);
            client.preload_symbol_mappings(&symbol_mappings);
            client.disable_timing();

            let mut ws_stream = loop {
                if shutdown.load(Ordering::Relaxed) {
                    return;
                }

                match client.connect() {
                    Ok(stream) => {
                        break stream;
                    }
                    Err(err) => {
                        warn!("Connection failed: {}, retrying in 5 seconds...", err);
                        thread::sleep(Duration::from_secs(5));
                    }
                }
            };

            info!("Connected to Binance SBE WebSocket");

            let channels = ["depth".to_string()];
            let subscription_symbols = symbol_mappings
                .iter()
                .take(WEBSOCKET_BUFFER_SIZE)
                .map(|(symbol, _)| symbol.clone())
                .collect::<Vec<String>>();

            match
                client.subscribe(&mut ws_stream, &subscription_symbols, channels.iter().as_slice())
            {
                Ok(_) => info!("Successfully subscribed to {} symbols", subscription_symbols.len()),
                Err(e) => {
                    error!("Failed to subscribe: {}", e);
                    return;
                }
            }

            client.set_depth_callback(
                move |
                    symbol_id: u16,
                    bids: &[(f64, f64); 5],
                    asks: &[(f64, f64); 5],
                    first_update_id: u64,
                    last_update_id: u64,
                    _
                | {
                    orderbook_manager.apply_depth_update(
                        symbol_id,
                        bids,
                        asks,
                        first_update_id,
                        last_update_id
                    );
                }
            );

            if let Err(e) = client.process_messages(&mut ws_stream, &shutdown) {
                error!("WebSocket processing error: {}", e);
            }
        })?;

    Ok(handle)
}

fn create_fix_executor(
    symbol_map: Arc<crate::models::symbol_map::SymbolMap>,
    config: &Config
) -> Result<Option<Arc<FixArbitrageExecutor>>> {
    if config.should_enable_real_trading() || config.should_enable_mock_trading() {
        info!("Creating FIX executor ({}) with precision support", if config.debug {
            "Mock"
        } else {
            "Real"
        });

        let fix_config = if config.debug { None } else { Some(config.get_fix_client_config()) };

        match FixArbitrageExecutor::new(config.debug, fix_config, Arc::clone(&symbol_map)) {
            Ok(executor) => {
                if let Err(e) = executor.connect() {
                    error!("Failed to connect FIX executor: {}", e);
                    return Err(anyhow!("Failed to connect FIX executor: {}", e));
                }

                let executor_arc = Arc::new(executor);

                info!("✓ FIX executor connected successfully with precision support");
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
        .name("arbitrage-detector".to_string())
        .spawn(move || {
            info!("Arbitrage detection thread started");

            let update_callback = {
                let detector = Arc::clone(&detector);
                Arc::new(
                    move |symbol_id: u16, _book: &crate::orderbook_manager::orderbook::OrderBook| {
                        detector.process_symbol_update(symbol_id);
                    }
                )
            };

            orderbook_manager.register_update_callback(update_callback);

            while !shutdown.load(Ordering::Relaxed) && !detector.is_shutdown() {
                thread::sleep(Duration::from_millis(100));
            }

            info!("Arbitrage detection thread stopped");
        })
        .expect("Failed to spawn arbitrage detection thread")
}

fn start_execution_thread(
    detector: Arc<ArbitrageDetector>,
    fix_executor: Option<Arc<FixArbitrageExecutor>>,
    shutdown: Arc<AtomicBool>
) -> thread::JoinHandle<()> {
    thread::Builder
        ::new()
        .name("execution".to_string())
        .spawn(move || {
            if let Some(executor) = fix_executor {
                let callback = executor.get_callback();
                detector.register_execution_callback(
                    Arc::new(move |opportunity: arbitrage::detector::ArbitrageOpportunity| {
                        info!(
                            "Execution opportunity: {} with {:.4}% profit",
                            opportunity.display(),
                            opportunity.profit_percentage()
                        );
                        callback(opportunity);
                    })
                );

                while !shutdown.load(Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        })
        .expect("Failed to spawn execution thread")
}
