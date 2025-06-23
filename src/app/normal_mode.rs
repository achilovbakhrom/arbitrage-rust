// use std::{ sync::{ atomic::{ AtomicBool, Ordering }, Arc }, thread, time::Duration };
// use crossbeam_channel::{ bounded, Receiver };

// use crate::{
//     arbitrage::{ self },
//     config::Config,
//     exchange::{ binance::BinanceClient, client::ExchangeClient, sbe_client::BinanceSbeClient },
//     executor::executor::{ ArbitrageExecutor, ExecutionStrategy },
//     orderbook::manager::{ OrderBookManager, UpdateBatch },
//     utils::{
//         console::{ print_app_started, print_app_starting, print_config },
//         helpers::{
//             build_execultor,
//             build_symbol_map,
//             build_tokio_async_runtime,
//             get_all_symbols,
//             is_operational,
//         },
//     },
//     WEBSOCKET_BUFFER_SIZE,
// };
// use anyhow::{ anyhow, Context, Result };
// use tracing::{ error, info, warn };

// const ARBITRAGE_CHANNEL_CAPACITY: usize = 512;

// pub fn run_normal_mode(config: Config) -> Result<()> {
//     print_app_starting();
//     print_config(&config);

//     let client = Arc::new(
//         BinanceClient::new(config.fix_api.clone(), config.debug).context(
//             "Failed to create Binance client"
//         )?
//     );

//     info!("Connected to exchange: {}", client.name());

//     let rt = build_tokio_async_runtime()?;

//     if is_operational(&rt, client.clone()) {
//         info!("✓ Exchange is operational");
//     } else {
//         error!("❌ Exchange is not operational or timed out");
//         return Err(anyhow!("Exchange is not operational"));
//     }

//     let symbols = get_all_symbols(&rt, client.clone())?;
//     info!("✓ Fetched {} symbols from exchange", symbols.len());

//     let symbol_map = build_symbol_map(symbols.clone(), &config)?;
//     let unique_symbols = symbol_map.get_unique_symbols();

//     let (arbitrage_sender, arbitrage_receiver) = bounded::<UpdateBatch>(ARBITRAGE_CHANNEL_CAPACITY);

//     let mut orderbook_manager = OrderBookManager::new(config.depth, client.clone());
//     orderbook_manager.set_arbitrage_sender(arbitrage_sender);
//     let orderbook_manager = Arc::new(orderbook_manager);

//     orderbook_manager.preload_symbols(&unique_symbols);

//     let executor_cb = Arc::new(ArbitrageExecutor::new(ExecutionStrategy::FastSequential));
//     let fix_executor = build_execultor(orderbook_manager.clone(), symbol_map.clone(), &config)?;

//     let triangular_paths = symbol_map.get_trangular_paths_as_arc();

//     let detector = arbitrage::detector::create_detector(
//         orderbook_manager.clone(),
//         config.fee,
//         config.threshold,
//         triangular_paths,
//         config.trade_amount,
//         executor_cb.clone(),
//         fix_executor.clone(),
//         config.min_volume_multiplier,
//         false
//     );

//     let detector_mode = if fix_executor.is_some() {
//         if config.debug {
//             "FIX Mock Trading (Ultra-Optimized+Batched)"
//         } else {
//             "FIX Real Trading (Ultra-Optimized+Batched)"
//         }
//     } else {
//         "Simulation Only (Ultra-Optimized+Batched)"
//     };

//     info!(
//         "Created ULTRA-OPTIMIZED+BATCHED arbitrage detector with threshold: {:.2}% (Mode: {})",
//         config.threshold * 100.0,
//         detector_mode
//     );

//     let arbitrage_thread = start_ultra_fast_arbitrage_thread(
//         arbitrage_receiver,
//         detector.clone(),
//         orderbook_manager.clone()
//     );

//     info!("Starting ULTRA-OPTIMIZED WebSocket connection...");

//     let shutdown = Arc::new(AtomicBool::new(false));
//     let shutdown_clone = shutdown.clone();

//     ctrlc
//         ::set_handler(move || {
//             info!("Received Ctrl+C, shutting down...");
//             shutdown_clone.store(true, Ordering::Relaxed);
//         })
//         .context("Error setting Ctrl-C handler")?;

//     let symbol_mappings = orderbook_manager.get_symbol_mappings();

//     let orderbook_manager_thread = orderbook_manager.clone();
//     let sbe_api_key = config.sbe_api_key.clone();
//     let shutdown_for_ws = shutdown.clone();
//     let fix_executor_for_cleanup = fix_executor.clone();

//     let ws_handle = thread::Builder
//         ::new()
//         .name("websocket-thread".to_string())
//         .spawn(move || {
//             #[cfg(target_os = "linux")]
//             {
//                 let mut cpu_set = nix::sched::CpuSet::new();
//                 cpu_set.set(0).ok();
//                 nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &cpu_set).ok();
//             }

//             let client = BinanceSbeClient::new(sbe_api_key);
//             client.preload_symbol_mappings(&symbol_mappings);
//             client.disable_timing();

//             let mut ws_stream = loop {
//                 if shutdown_for_ws.load(Ordering::Relaxed) {
//                     return;
//                 }

//                 match client.connect() {
//                     Ok(stream) => {
//                         break stream;
//                     }
//                     Err(err) => {
//                         warn!("Connection failed: {}, retrying in 5 seconds...", err);
//                         thread::sleep(Duration::from_secs(5));
//                     }
//                 }
//             };

//             info!("Connected to Binance SBE WebSocket (ULTRA-OPTIMIZED+BATCHED)");

//             let channels = vec!["depth".to_string()];

//             let subscription_symbols = symbol_mappings
//                 .iter()
//                 .take(WEBSOCKET_BUFFER_SIZE)
//                 .map(|(symbol, _)| symbol.clone())
//                 .collect::<Vec<String>>();

//             match
//                 client.subscribe(&mut ws_stream, &subscription_symbols, channels.iter().as_slice())
//             {
//                 Ok(_) => info!("Successfully subscribed to {} symbols", subscription_symbols.len()),
//                 Err(e) => {
//                     error!("Failed to subscribe: {}", e);
//                     return;
//                 }
//             }

//             client.set_depth_callback(
//                 move |
//                     symbol_id: u16,
//                     bids: &[(f64, f64); 5],
//                     asks: &[(f64, f64); 5],
//                     first_update_id: u64,
//                     last_update_id: u64,
//                     _
//                 | {
//                     orderbook_manager_thread.apply_depth_update(
//                         symbol_id,
//                         bids,
//                         asks,
//                         first_update_id,
//                         last_update_id
//                     );
//                 }
//             );

//             if let Err(e) = client.process_messages_with_shutdown(&mut ws_stream, &shutdown_for_ws) {
//                 error!("WebSocket processing error: {}", e);
//             }
//         })?;

//     print_app_started();
//     info!("\nPress Ctrl+C to exit");

//     if fix_executor.is_some() {
//         if config.debug {
//             info!("🔵 ULTRA-OPTIMIZED+BATCHED mock trading mode enabled");
//         } else {
//             info!("🔴 ULTRA-OPTIMIZED+BATCHED REAL TRADING MODE ENABLED");
//             info!("⚠️  Make sure you understand the risks!");
//         }
//     } else {
//         info!("🟡 ULTRA-OPTIMIZED+BATCHED simulation mode");
//     }

//     let flush_manager = orderbook_manager.clone();
//     let flush_shutdown = shutdown.clone();
//     let flush_handle = thread::spawn(move || {
//         while !flush_shutdown.load(Ordering::Relaxed) {
//             thread::sleep(Duration::from_micros(100));
//             flush_manager.flush_pending_batch();
//         }
//     });

//     while !shutdown.load(Ordering::Relaxed) {
//         thread::sleep(Duration::from_millis(100));
//     }

//     if let Some(fix_exec) = fix_executor_for_cleanup {
//         if let Err(e) = fix_exec.disconnect() {
//             error!("Error disconnecting FIX executor: {}", e);
//         } else {
//             info!("FIX executor disconnected successfully");
//         }
//     }

//     if let Err(e) = ws_handle.join() {
//         error!("Error joining WebSocket thread: {:?}", e);
//     }

//     if let Err(e) = arbitrage_thread.join() {
//         error!("Error joining arbitrage detection thread: {:?}", e);
//     }

//     if let Err(e) = flush_handle.join() {
//         error!("Error joining flush thread: {:?}", e);
//     }

//     thread::sleep(Duration::from_millis(200));

//     info!("ULTRA-OPTIMIZED+BATCHED triangular arbitrage system stopped");
//     Ok(())
// }

// fn start_ultra_fast_arbitrage_thread(
//     receiver: Receiver<UpdateBatch>,
//     detector: Arc<crate::arbitrage::detector::ArbitrageDetectorState>,
//     orderbook_manager: Arc<OrderBookManager>
// ) -> thread::JoinHandle<()> {
//     thread::Builder
//         ::new()
//         .name("ultra-arbitrage".to_string())
//         .spawn(move || {
//             #[cfg(target_os = "linux")]
//             {
//                 let mut cpu_set = nix::sched::CpuSet::new();
//                 cpu_set.set(1).ok();
//                 nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &cpu_set).ok();
//             }

//             info!("Ultra-fast arbitrage detection thread started with CPU affinity");

//             let mut processed_count = 0u64;

//             while let Ok(batch) = receiver.recv() {
//                 for symbol_id in batch.iter() {
//                     if let Some(book) = orderbook_manager.get_book_by_id(symbol_id) {
//                         detector.handle_update_by_id(symbol_id, &book);
//                         processed_count += 1;
//                     }
//                 }

//                 if processed_count % 1024 == 0 {
//                     thread::yield_now();
//                 }
//             }

//             info!("Ultra-fast arbitrage detection thread stopped (processed {} updates)", processed_count);
//         })
//         .expect("Failed to spawn ultra-fast arbitrage detection thread")
// }

use std::{ sync::{ atomic::{ AtomicBool, Ordering }, Arc }, thread, time::Duration };

use crate::{
    arbitrage::{ self },
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient, sbe_client::BinanceSbeClient },
    executor::executor::{ ArbitrageExecutor, ExecutionStrategy },
    orderbook::manager::OrderBookManager,
    utils::{
        console::{ print_app_started, print_app_starting, print_config },
        helpers::{
            build_execultor,
            build_symbol_map,
            build_tokio_async_runtime,
            get_all_symbols,
            is_operational,
        },
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
    orderbook_manager.preload_symbols(&unique_symbols);

    let executor_cb = Arc::new(ArbitrageExecutor::new(ExecutionStrategy::FastSequential));
    let fix_executor = build_execultor(orderbook_manager.clone(), symbol_map.clone(), &config)?;

    let triangular_paths = symbol_map.get_trangular_paths_as_arc();

    let detector = arbitrage::detector::create_detector(
        orderbook_manager.clone(),
        config.fee,
        config.threshold,
        triangular_paths,
        config.trade_amount,
        executor_cb.clone(),
        fix_executor.clone(),
        config.min_volume_multiplier,
        false
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

    let detection_handle = arbitrage::detector::start_detection_thread(detector.clone());

    info!("Starting ULTRA-OPTIMIZED WebSocket connection...");

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    ctrlc
        ::set_handler(move || {
            info!("Received Ctrl+C, shutting down...");
            shutdown_clone.store(true, Ordering::Relaxed);
        })
        .context("Error setting Ctrl-C handler")?;

    let symbol_mappings = orderbook_manager.get_symbol_mappings();

    let orderbook_manager_thread = orderbook_manager.clone();
    let sbe_api_key = config.sbe_api_key.clone();
    let shutdown_for_ws = shutdown.clone();
    let fix_executor_for_cleanup = fix_executor.clone();

    let ws_handle = thread::Builder
        ::new()
        .name("websocket-thread".to_string())
        .spawn(move || {
            #[cfg(target_os = "linux")]
            {
                let mut cpu_set = nix::sched::CpuSet::new();
                cpu_set.set(0).ok();
                nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &cpu_set).ok();
            }

            let client = BinanceSbeClient::new(sbe_api_key);
            client.preload_symbol_mappings(&symbol_mappings);
            client.disable_timing();

            let mut ws_stream = loop {
                if shutdown_for_ws.load(Ordering::Relaxed) {
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

            info!("Connected to Binance SBE WebSocket (ULTRA-OPTIMIZED)");

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
                    orderbook_manager_thread.apply_depth_update(
                        symbol_id,
                        bids,
                        asks,
                        first_update_id,
                        last_update_id
                    );
                }
            );

            if let Err(e) = client.process_messages_with_shutdown(&mut ws_stream, &shutdown_for_ws) {
                error!("WebSocket processing error: {}", e);
            }
        })?;

    print_app_started();
    info!("\nPress Ctrl+C to exit");

    if fix_executor.is_some() {
        if config.debug {
            info!("🔵 ULTRA-OPTIMIZED mock trading mode enabled");
        } else {
            info!("🔴 ULTRA-OPTIMIZED REAL TRADING MODE ENABLED");
            info!("⚠️  Make sure you understand the risks!");
        }
    } else {
        info!("🟡 ULTRA-OPTIMIZED simulation mode");
    }

    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }

    if let Some(fix_exec) = fix_executor_for_cleanup {
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

    info!("ULTRA-OPTIMIZED triangular arbitrage system stopped");
    Ok(())
}
