use std::{ sync::{ atomic::{ AtomicBool, Ordering }, Arc }, thread, time::Duration };

use crate::{
    arbitrage::{ self, detector::ArbitrageDetector },
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient, sbe_client::BinanceSbeClient },
    executor::{ executor::ArbitrageExecutor, fix_executor::FixArbitrageExecutor },
    orderbook::manager::OrderBookManager,
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
    let unique_symbols = symbol_map.get_unique_symbols();

    let orderbook_manager = Arc::new(OrderBookManager::new(config.depth, client.clone()));
    orderbook_manager.preload_symbols(&unique_symbols);

    let triangular_paths = symbol_map.get_trangular_paths_as_arc();
    let fallback_executor = Arc::new(
        ArbitrageExecutor::new(crate::executor::executor::ExecutionStrategy::FastSequential)
    );

    let detector = arbitrage::detector::create_detector(
        orderbook_manager.clone(),
        config.fee,
        config.threshold,
        triangular_paths,
        config.trade_amount,
        config.min_volume_multiplier
    );

    let fix_executor = create_fix_executor(orderbook_manager.clone(), symbol_map.clone(), &config)?;

    if let Some(ref executor) = fix_executor {
        let execution_callback = create_execution_callback(executor.clone());
        detector.register_execution_callback(execution_callback);
    }

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
        "Created arbitrage detector with threshold: {:.2}% (Mode: {})",
        config.threshold * 100.0,
        detector_mode
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    ctrlc
        ::set_handler(move || {
            info!("Received Ctrl+C, shutting down...");
            shutdown_clone.store(true, Ordering::Relaxed);
        })
        .context("Error setting Ctrl-C handler")?;

    let detection_handle = start_detection_thread(
        detector.clone(),
        orderbook_manager.clone(),
        shutdown.clone()
    );

    let symbol_mappings = orderbook_manager.get_symbol_mappings();
    let ws_handle = start_websocket_thread(
        config.sbe_api_key.clone(),
        symbol_mappings,
        orderbook_manager.clone(),
        shutdown.clone()
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

    detector.shutdown();

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

    thread::sleep(Duration::from_millis(200));

    info!("Triangular arbitrage system stopped");
    Ok(())
}

fn create_fix_executor(
    orderbook_manager: Arc<OrderBookManager>,
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

                let execution_callback = executor.enable_callback_execution(3);
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

fn create_execution_callback(
    fix_executor: Arc<FixArbitrageExecutor>
) -> arbitrage::detector::ExecutionCallback {
    Arc::new(move |opportunity: arbitrage::detector::ArbitrageOpportunity| {
        info!(
            "Execution opportunity: {} with {:.4}% profit",
            opportunity.display(),
            opportunity.profit_percentage()
        );
    })
}

fn start_detection_thread(
    detector: Arc<ArbitrageDetector>,
    orderbook_manager: Arc<OrderBookManager>,
    shutdown: Arc<AtomicBool>
) -> thread::JoinHandle<()> {
    thread::Builder
        ::new()
        .name("arbitrage-detector".to_string())
        .spawn(move || {
            info!("Arbitrage detection thread started");

            let update_callback = {
                let detector = detector.clone();
                Arc::new(move |symbol_id: u16, _book: &crate::orderbook::orderbook::OrderBook| {
                    detector.process_symbol_update(symbol_id);
                })
            };

            orderbook_manager.register_update_callback(update_callback);

            while !shutdown.load(Ordering::Relaxed) && !detector.is_shutdown() {
                thread::sleep(Duration::from_millis(100));
            }

            info!("Arbitrage detection thread stopped");
        })
        .expect("Failed to spawn arbitrage detection thread")
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

            let channels = vec!["depth".to_string()];
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
