use std::{ sync::{ atomic::{ AtomicBool, Ordering }, Arc }, thread, time::Duration };

use crate::{
    arbitrage::{ self },
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient, sbe_client::BinanceSbeClient },
    orderbook::manager::OrderBookManager,
    utils::{
        console::{ print_app_started, print_app_starting, print_config },
        helpers::{
            build_symbol_map,
            build_tokio_async_runtime,
            get_all_symbols,
            is_operational,
            create_detector_and_executor,
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

    let (detector, fix_executor) = create_detector_and_executor(
        orderbook_manager.clone(),
        symbol_map.clone(),
        &config,
        false
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

    let detection_handle = arbitrage::detector::start_detection_thread(detector.clone());

    info!("Starting CALLBACK-OPTIMIZED WebSocket connection...");

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

            info!("Connected to Binance SBE WebSocket (CALLBACK-OPTIMIZED)");

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
            info!("🔵 CALLBACK-OPTIMIZED FIX mock trading mode enabled");
        } else {
            info!("🔴 CALLBACK-OPTIMIZED FIX REAL TRADING MODE ENABLED");
            info!("⚠️  Make sure you understand the risks!");
        }
    } else {
        info!("🟡 CALLBACK-OPTIMIZED simulation mode");
    }

    info!("CALLBACK-BASED OPTIMIZATIONS APPLIED:");
    info!("  ✓ Direct callback execution eliminates channel overhead");
    info!("  ✓ Zero-copy opportunity passing");
    info!("  ✓ Independent FIX executor threads with symbol conflict detection");
    info!("  ✓ Lock-free opportunity detection");
    info!("  ✓ CPU affinity optimization for hot paths");
    info!("  ✓ Minimal memory allocations in critical path");

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

    info!("CALLBACK-OPTIMIZED triangular arbitrage system stopped");
    Ok(())
}
