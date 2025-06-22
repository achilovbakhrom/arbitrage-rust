// src/app/normal_mode.rs - Ultra-optimized to eliminate all cloning in hot path
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
    // Display startup information
    print_app_starting();
    print_config(&config);

    // Initialize exchange api client with proper error handling
    let client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    info!("Connected to exchange: {}", client.name());

    // Create a runtime for async operations
    let rt = build_tokio_async_runtime()?;

    // Verify exchange connectivity with timeout
    if is_operational(&rt, client.clone()) {
        info!("✓ Exchange is operational");
    } else {
        error!("❌ Exchange is not operational or timed out");
        return Err(anyhow!("Exchange is not operational"));
    }

    // Fetch all active spot trading symbols (with timeout)
    let symbols = get_all_symbols(&rt, client.clone())?;

    info!("✓ Fetched {} symbols from exchange", symbols.len());

    // Create symbol map and find triangular paths
    let symbol_map = build_symbol_map(symbols.clone(), &config)?;

    // Get unique symbols for market data
    let unique_symbols = symbol_map.get_unique_symbols();

    // Create the shared orderbook manager with optimal depth and optimizations
    let orderbook_manager = Arc::new(OrderBookManager::new(config.depth, client.clone()));

    // Preload symbols for optimization
    orderbook_manager.preload_symbols(&unique_symbols);

    // Create fallback executor for simulation compatibility
    let executor_cb = Arc::new(ArbitrageExecutor::new(ExecutionStrategy::FastSequential));

    // Create FIX executor based on configuration with symbol map for precision
    let fix_executor = build_execultor(orderbook_manager.clone(), symbol_map.clone(), &config)?;

    // OPTIMIZATION: Convert paths from Arc to owned data to eliminate cloning
    let triangular_paths = symbol_map.get_trangular_paths_as_arc();

    // Create the arbitrage detector with FIX integration and optimizations
    arbitrage::detector::create_detector(
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

    // Start WebSocket connection for real-time market data
    info!("Starting ULTRA-OPTIMIZED WebSocket connection...");

    // Create shutdown signal
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    // Set up Ctrl+C handler
    ctrlc
        ::set_handler(move || {
            info!("Received Ctrl+C, shutting down...");
            shutdown_clone.store(true, Ordering::Relaxed);
        })
        .context("Error setting Ctrl-C handler")?;

    // CRITICAL OPTIMIZATION: Create symbol mappings once for SBE client
    let symbol_mappings = orderbook_manager.get_symbol_mappings();

    // Create clones for the WebSocket thread
    let orderbook_manager_thread = orderbook_manager.clone();
    let sbe_api_key = config.sbe_api_key.clone();
    let shutdown_for_ws = shutdown.clone();
    let fix_executor_for_cleanup = fix_executor.clone();

    // Start WebSocket processing in a separate thread
    let ws_handle = thread::spawn(move || {
        let mut client = BinanceSbeClient::new(sbe_api_key);

        // CRITICAL: Preload symbol mappings to avoid runtime string operations
        client.preload_symbol_mappings(&symbol_mappings);

        // Keep timing disabled for production mode to maximize performance
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

        // Subscribe to depth stream for each symbol
        let channels = vec!["depth".to_string()];

        // Limit to WEBSOCKET_BUFFER_SIZE symbols per connection to avoid overwhelming
        let subscription_symbols = symbol_mappings
            .iter()
            .take(WEBSOCKET_BUFFER_SIZE)
            .map(|(symbol, _)| symbol.clone())
            .collect::<Vec<String>>();

        match client.subscribe(&mut ws_stream, &subscription_symbols, channels.iter().as_slice()) {
            Ok(_) => info!("Successfully subscribed to {} symbols", subscription_symbols.len()),
            Err(e) => {
                error!("Failed to subscribe: {}", e);
                return;
            }
        }

        // ULTRA-OPTIMIZED callback with ZERO cloning and ZERO string operations
        client.set_depth_callback(
            move |
                symbol_id: u16,
                bids: &[(f64, f64); 5],
                asks: &[(f64, f64); 5],
                first_update_id: u64,
                last_update_id: u64,
                _
            | {
                // CRITICAL OPTIMIZATION: Use symbol_id directly, pass slice references
                orderbook_manager_thread.apply_depth_update_by_id(
                    symbol_id,
                    bids, // Pass slice reference - ZERO cloning
                    asks, // Pass slice reference - ZERO cloning
                    first_update_id,
                    last_update_id
                );
            }
        );

        // Process WebSocket messages with shutdown support
        if let Err(e) = client.process_messages_with_shutdown(&mut ws_stream, &shutdown_for_ws) {
            error!("WebSocket processing error: {}", e);
        }
    });

    // Print startup complete message
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

    // Wait for shutdown signal
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }

    // Cleanup: Disconnect FIX executor
    if let Some(fix_exec) = fix_executor_for_cleanup {
        if let Err(e) = fix_exec.disconnect() {
            error!("Error disconnecting FIX executor: {}", e);
        } else {
            info!("FIX executor disconnected successfully");
        }
    }

    // Wait for WebSocket thread to finish
    if let Err(e) = ws_handle.join() {
        error!("Error joining WebSocket thread: {:?}", e);
    }

    // Give threads a moment to clean up
    thread::sleep(Duration::from_millis(200));

    info!("ULTRA-OPTIMIZED triangular arbitrage system stopped");
    Ok(())
}
