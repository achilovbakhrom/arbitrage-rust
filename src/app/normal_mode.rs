use std::{ sync::{ atomic::{ AtomicBool, Ordering }, Arc }, thread, time::Duration };

use crate::{
    arbitrage::{ self },
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient, sbe_client::BinanceSbeClient },
    executor::{
        executor::{ ArbitrageExecutor, ExecutionStrategy },
        fix_executor::FixArbitrageExecutor,
    },
    models::symbol_map::SymbolMap,
    orderbook::manager::OrderBookManager,
    utils::console::{ print_app_started, print_app_starting, print_config },
    API_TIMEOUT,
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
        BinanceClient::new(config.fix_api.clone(), config.fix_secret.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    info!("Connected to exchange: {}", client.name());

    // Create a runtime for async operations
    let rt = tokio::runtime::Builder
        ::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .context("Failed to create Tokio runtime")?;

    // Verify exchange connectivity with timeout
    let is_operational = rt.block_on(async {
        tokio::time::timeout(API_TIMEOUT, client.is_operational()).await
    });

    match is_operational {
        Ok(Ok(true)) => {
            info!("✓ Exchange is operational");
        }
        _ => {
            error!("❌ Exchange is not operational or timed out");
            return Err(anyhow!("Exchange is not operational"));
        }
    }

    // Fetch all active spot trading symbols (with timeout)
    let symbols = rt.block_on(async {
        tokio::time::timeout(API_TIMEOUT, client.get_active_spot_symbols()).await
    });

    let symbols = match symbols {
        Ok(Ok(symbols)) => symbols,
        Ok(Err(e)) => {
            error!("Failed to fetch symbols: {}", e);
            return Err(anyhow!("Failed to fetch symbols: {}", e));
        }
        Err(_) => {
            error!("Timed out while fetching symbols");
            return Err(anyhow!("Timed out while fetching symbols"));
        }
    };

    info!("✓ Fetched {} symbols from exchange", symbols.len());

    // Create symbol map and find triangular paths
    let mut symbol_map = SymbolMap::from_symbols(symbols.clone());

    info!("Finding triangular arbitrage paths starting with {}...", config.base_asset);
    symbol_map.find_targeted_triangular_paths(
        &config.base_asset,
        config.max_triangles,
        &config.excluded_coins
    );

    let total_paths = symbol_map.get_triangular_paths().len();

    info!("Found {} potential triangular arbitrage paths", total_paths);

    if total_paths > 0 {
        // Log a sample of paths (limit to avoid excessive logging)
        let examples = symbol_map
            .get_triangular_paths()
            .iter()
            .take(5) // Only show first 5 examples
            .enumerate()
            .map(|(i, p)|
                format!(
                    "{:3}. {} → {} → {}",
                    i + 1,
                    p.first_symbol,
                    p.second_symbol,
                    p.third_symbol
                )
            )
            .collect::<Vec<_>>()
            .join("\n");

        info!("Sample paths: \n{}", examples);
    } else {
        warn!("No triangular paths found. Check base_asset and excluded_fiats configuration.");
        return Err(anyhow!("No triangular paths found. Cannot continue."));
    }

    // Get unique symbols for market data
    let unique_symbols = symbol_map.get_unique_symbols();

    // Create the shared orderbook manager with optimal depth
    let orderbook_manager = Arc::new(OrderBookManager::new(config.depth, client.clone()));

    // Create fallback executor for simulation compatibility
    let executor_cb = Arc::new(
        ArbitrageExecutor::new(
            orderbook_manager.clone(),
            ExecutionStrategy::FastSequential // Maximum speed
        )
    );

    // Create FIX executor based on configuration
    let fix_executor = if
        config.should_enable_real_trading() ||
        config.should_enable_mock_trading()
    {
        info!("Creating FIX executor ({})", if config.debug { "Mock" } else { "Real" });

        let fix_config = if config.debug {
            None // Mock client doesn't need FIX config
        } else {
            Some(config.get_fix_client_config())
        };

        match
            FixArbitrageExecutor::new(
                orderbook_manager.clone(),
                config.debug, // Use debug mode flag
                fix_config,
                config.max_slippage_percentage,
                config.min_execution_profit_threshold
            )
        {
            Ok(executor) => {
                let executor = Arc::new(executor);

                // Connect to FIX API
                if let Err(e) = executor.connect() {
                    error!("Failed to connect FIX executor: {}", e);
                    return Err(anyhow!("Failed to connect FIX executor: {}", e));
                }

                info!("✓ FIX executor connected successfully");
                Some(executor)
            }
            Err(e) => {
                error!("Failed to create FIX executor: {}", e);
                return Err(anyhow!("Failed to create FIX executor: {}", e));
            }
        }
    } else {
        info!("Trading disabled, using simulation mode only");
        None
    };

    // Convert paths to Arc for zero-copy sharing
    let triangular_paths: Vec<Arc<_>> = symbol_map
        .get_triangular_paths()
        .iter()
        .map(|p| Arc::new(p.clone()))
        .collect();

    // Create the arbitrage detector with FIX integration
    let _arbitrage_detector = arbitrage::detector::create_detector_with_fix(
        orderbook_manager.clone(),
        config.fee,
        config.threshold,
        triangular_paths,
        config.trade_amount,
        executor_cb.clone(),
        fix_executor.clone(),
        config.min_volume_multiplier,
        false // is_perf flag
    );

    let detector_mode = if fix_executor.is_some() {
        if config.debug { "FIX Mock Trading" } else { "FIX Real Trading" }
    } else {
        "Simulation Only"
    };

    info!(
        "Created high-performance event-driven arbitrage detector with threshold: {:.2}% (Mode: {})",
        config.threshold * 100.0,
        detector_mode
    );

    // Start WebSocket connection for real-time market data
    info!("Starting WebSocket connection for real-time market data...");

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

    // Create clones for the WebSocket thread
    let orderbook_manager_thread = orderbook_manager.clone();
    let unique_symbols_cloned = Arc::new(unique_symbols.clone());
    let sbe_api_key = config.sbe_api_key.clone();
    let shutdown_for_ws = shutdown.clone();
    let fix_executor_for_cleanup = fix_executor.clone();

    // Start WebSocket processing in a separate thread
    let ws_handle = thread::spawn(move || {
        let client = BinanceSbeClient::new(sbe_api_key);

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

        info!("Connected to Binance SBE WebSocket");

        // Subscribe to depth stream for each symbol
        let channels = vec!["depth".to_string()];

        // Limit to WEBSOCKET_BUFFER_SIZE symbols per connection to avoid overwhelming
        let subscription_symbols = unique_symbols_cloned
            .iter()
            .take(WEBSOCKET_BUFFER_SIZE)
            .cloned()
            .collect::<Vec<String>>();

        match client.subscribe(&mut ws_stream, &subscription_symbols, channels.iter().as_slice()) {
            Ok(_) => info!("Successfully subscribed to {} symbols", subscription_symbols.len()),
            Err(e) => {
                error!("Failed to subscribe: {}", e);
                return;
            }
        }

        // Set up ultra-fast depth update callback for production
        client.set_depth_callback(
            move |symbol, bids, asks, first_update_id, last_update_id, _receive_time_us| {
                // Clone the data to avoid lifetime issues
                let symbol_arc: Arc<str> = Arc::from(symbol);

                // Fast path - no timing overhead in production
                orderbook_manager_thread.apply_depth_update(
                    &symbol_arc,
                    &bids,
                    &asks,
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
            info!("🔵 Mock trading mode enabled - simulating FIX API trades");
        } else {
            info!("🔴 REAL TRADING MODE ENABLED - executing actual trades via FIX API");
            info!("⚠️  Make sure you understand the risks!");
        }
    } else {
        info!("🟡 Simulation mode - no actual trades will be executed");
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

    info!("Triangular arbitrage system stopped");
    Ok(())
}
