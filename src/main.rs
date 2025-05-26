mod config;
mod constants;
mod enums;
mod exchange;
mod utils;
mod models;
mod orderbook;
mod arbitrage;
mod performance;

use std::time::Duration;
use std::sync::Arc;
use models::symbol_map::SymbolMap;
use tokio::time::{ sleep, timeout };

use config::Config;
use anyhow::{ anyhow, Context, Result };

use exchange::{ binance::BinanceClient, client::ExchangeClient, sbe_client::BinanceSbeClient };
use tracing::{ error, info, warn };
use utils::{ console::{ print_app_started, print_app_starting, print_config }, logging };
use orderbook::manager::OrderBookManager;

const API_TIMEOUT: Duration = Duration::from_secs(5);
const WEBSOCKET_BUFFER_SIZE: usize = 100;

// Define command line arguments enum
#[derive(Debug)]
enum Command {
    Run,
    PerformanceTest,
}

fn main() -> Result<()> {
    // Parse command line arguments
    let command = if std::env::args().nth(1).as_deref() == Some("perf-test") {
        Command::PerformanceTest
    } else {
        Command::Run
    };

    // Load configuration with helpful error messages
    let mut config = Config::from_env().context(
        "Failed to load configuration from environment. Make sure you have a .env file with required variables."
    )?;

    // Override debug mode for performance tests
    if matches!(command, Command::PerformanceTest) {
        config.debug = false; // Force disable console logging for performance tests
        config.max_triangles = 50;
    }

    // Initialize logging system
    logging
        ::init_logging(config.log_level, config.debug, &config.log_config)
        .context("Failed to initialize logging system")?;

    let runtime = tokio::runtime::Builder
        ::new_multi_thread()
        .worker_threads(4) // Limit the number of worker threads
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");

    match command {
        Command::Run => {
            runtime.block_on(async { run_normal_mode(config).await }).unwrap();
        }
        Command::PerformanceTest => {
            runtime.block_on(async { run_performance_test(config).await }).unwrap();
        }
    }
    Ok(())
}

async fn run_normal_mode(config: Config) -> Result<()> {
    // Display startup information
    print_app_starting();
    print_config(&config);

    // Initialize exchange client with proper error handling
    let client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.fix_secret.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    info!("Connected to exchange: {}", client.name());

    // Verify exchange connectivity with timeout
    match timeout(API_TIMEOUT, client.is_operational()).await {
        Ok(Ok(true)) => {
            info!("✓ Exchange is operational");
        }
        _ => {
            error!("❌ Exchange is not operational or timed out");
            return Err(anyhow!("Exchange is not operational"));
        }
    }

    // Fetch all active spot trading symbols (with timeout)
    let symbols = match timeout(API_TIMEOUT, client.get_active_spot_symbols()).await {
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

    // Convert paths to Arc for zero-copy sharing
    let triangular_paths: Vec<Arc<_>> = symbol_map
        .get_triangular_paths()
        .iter()
        .map(|p| Arc::new(p.clone()))
        .collect();

    // Create the event-driven arbitrage detector
    let _arbitrage_detector = arbitrage::detector::create_event_driven_detector(
        orderbook_manager.clone(),
        0.001, // 0.1% fee
        config.threshold, // Configured minimum profit threshold
        triangular_paths,
        100.0 // Start with 100 USDT
    );

    info!(
        "Created high-performance event-driven arbitrage detector with threshold: {:.2}%",
        config.threshold * 100.0
    );

    // Start WebSocket connection for real-time market data
    info!("Starting WebSocket connection for real-time market data...");
    let message_task = {
        // Create clones for the WebSocket task
        let manager_for_msg_task = orderbook_manager.clone();
        let paths_for_msg_task = Arc::new(unique_symbols.clone());
        let sbe_api_key = config.sbe_api_key.clone();

        tokio::spawn(async move {
            let client = BinanceSbeClient::new(sbe_api_key);

            let mut ws_stream = loop {
                match client.connect().await {
                    Ok(stream) => {
                        break stream;
                    }
                    Err(err) => {
                        warn!("Connection failed: {}", err);
                    }
                }
            };

            info!("Connected to Binance SBE WebSocket");

            // Subscribe to depth stream for each symbol
            let channels = vec!["depth".to_string()];

            // Limit to WEBSOCKET_BUFFER_SIZE symbols per connection to avoid overwhelming
            let subscription_symbols = paths_for_msg_task
                .iter()
                .take(WEBSOCKET_BUFFER_SIZE)
                .cloned()
                .collect::<Vec<String>>();

            match
                client.subscribe(
                    &mut ws_stream,
                    &subscription_symbols,
                    channels.iter().as_slice()
                ).await
            {
                Ok(_) => info!("Successfully subscribed to {} symbols", subscription_symbols.len()),
                Err(e) => {
                    error!("Failed to subscribe: {}", e);
                    return;
                }
            }

            // Set up depth update callback
            client.set_depth_callback(move |symbol, bids, asks, first_update_id, last_update_id| {
                // Clone the data to avoid lifetime issues with the spawned task
                let symbol_arc: Arc<str> = Arc::from(symbol.to_string());
                let manager = manager_for_msg_task.clone();

                manager.apply_depth_update(
                    &symbol_arc,
                    &bids,
                    &asks,
                    first_update_id,
                    last_update_id
                );
            });

            // Process WebSocket messages
            if let Err(e) = client.process_messages(&mut ws_stream).await {
                error!("WebSocket processing error: {}", e);
            }
        })
    };

    // Print startup complete message
    print_app_started();
    info!("\n Press Ctrl+C to exit");

    // Wait for CTRL+C signal for graceful shutdown
    tokio::signal::ctrl_c().await?;
    info!("Received Ctrl+C, shutting down...");

    // Clean up and exit
    message_task.abort();

    // Give tasks a moment to clean up
    sleep(Duration::from_millis(200)).await;

    info!("Triangular arbitrage system stopped");
    Ok(())
}

async fn run_performance_test(config: Config) -> Result<()> {
    // Output information
    info!("Starting performance test for triangular arbitrage system");
    info!("Test duration: 300 seconds");

    // Create output directory
    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let output_dir = format!("./performance_results/{}", timestamp);
    std::fs
        ::create_dir_all(&output_dir)
        .context(format!("Failed to create output directory: {}", output_dir))?;

    let output_file = format!("{}/performance_results.csv", output_dir);
    info!("Results will be saved to: {}", output_file);

    // Initialize exchange client
    let client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.fix_secret.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    // Verify exchange connectivity
    match timeout(API_TIMEOUT, client.is_operational()).await {
        Ok(Ok(true)) => {
            info!("✓ Exchange is operational");
        }
        _ => {
            error!("❌ Exchange is not operational or timed out");
            return Err(anyhow!("Exchange is not operational"));
        }
    }

    // Fetch symbols
    let symbols = match timeout(API_TIMEOUT, client.get_active_spot_symbols()).await {
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

    if total_paths == 0 {
        error!("No triangular paths found. Cannot continue.");
        return Err(anyhow!("No triangular paths found. Cannot continue."));
    }

    info!("Found {} potential triangular arbitrage paths", total_paths);

    // Get unique symbols for market data
    let unique_symbols = symbol_map.get_unique_symbols();

    // Create orderbook manager
    let orderbook_manager = Arc::new(OrderBookManager::new(config.depth, client.clone()));

    // Convert paths to Arc for zero-copy sharing
    let triangular_paths: Vec<Arc<_>> = symbol_map
        .get_triangular_paths()
        .iter()
        .map(|p| Arc::new(p.clone()))
        .collect();

    // Create arbitrage detector
    let detector = arbitrage::detector::create_event_driven_detector(
        orderbook_manager.clone(),
        0.001, // 0.1% fee
        config.threshold,
        triangular_paths,
        100.0 // Start with 100 USDT
    );

    info!("Created arbitrage detector. Starting performance test...");

    // Run the performance test for 300 seconds
    let test_result = performance::run_performance_test(
        config.sbe_api_key,
        unique_symbols,
        orderbook_manager.clone(),
        detector.clone(),
        120, // 5 minutes
        output_file
    ).await;

    match test_result {
        Ok(_) => {
            info!("Performance test completed successfully!");
        }
        Err(e) => {
            error!("Performance test failed: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
