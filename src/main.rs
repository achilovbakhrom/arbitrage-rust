// src/main.rs
mod config;
mod constants;
mod enums;
mod exchange;
mod utils;
mod models;
mod orderbook;
mod arbitrage;

use std::time::Duration;
use std::sync::Arc;
use colored::Colorize;
use models::symbol_map::SymbolMap;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::time::{ sleep, timeout };
// use rust_decimal_macros::dec;
use tokio::signal;

use config::Config;
use anyhow::{ anyhow, Context, Result };

use exchange::{ binance::BinanceClient, client::ExchangeClient, sbe_client::BinanceSbeClient };
use tracing::{ debug, error, info, warn };
use utils::{ console::{ print_app_started, print_app_starting, print_config }, logging };
use orderbook::manager::OrderBookManager;
use arbitrage::detector::{ ArbitrageDetector, ArbitrageOpportunity };

const API_TIMEOUT: Duration = Duration::from_secs(5);
const WEBSOCKET_BUFFER_SIZE: usize = 100;
const SCAN_INTERVAL_MS: u64 = 250; // Milliseconds between scans
const STATS_INTERVAL_SECS: u64 = 60; // Seconds between printing stats

#[tokio::main]
async fn main() -> Result<()> {
    // Load configuration with helpful error messages
    let config = Config::from_env().context(
        "Failed to load configuration from environment. Make sure you have a .env file with required variables."
    )?;

    // Initialize logging system
    logging
        ::init_logging(config.log_level, config.debug, &config.log_config)
        .context("Failed to initialize logging system")?;

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

    // Convert paths to Arc for zero-copy sharing
    let triangular_paths: Vec<Arc<_>> = symbol_map
        .get_triangular_paths()
        .iter()
        .map(|p| Arc::new(p.clone()))
        .collect();

    // Create the shared orderbook manager with optimal depth
    let orderbook_manager = Arc::new(OrderBookManager::new(config.depth, client.clone()));

    // Create the arbitrage detector with specified thresholds
    let arbitrage_detector = Arc::new(
        ArbitrageDetector::new(
            orderbook_manager.clone(),
            dec!(0.001), // 0.1% fee
            Decimal::from_f64(config.threshold).unwrap() // Configured minimum profit threshold
        )
    );

    info!("Created arbitrage detector with threshold: {:.2}%", config.threshold * 100.0);

    // Get unique symbols for market data
    let unique_symbols = symbol_map.get_unique_symbols();

    // Initialize orderbooks with snapshots for faster startup
    info!("Pre-loading orderbook snapshots for {} symbols...", unique_symbols.len());
    let mut success_count = 0;

    // Use a semaphore to limit concurrent requests to avoid rate limiting
    let semaphore = Arc::new(tokio::sync::Semaphore::new(5));

    // Create tasks to fetch initial snapshots
    let mut snapshot_tasks = Vec::with_capacity(unique_symbols.len());

    for symbol in &unique_symbols {
        let symbol_arc: Arc<str> = Arc::from(symbol.clone());
        let manager = orderbook_manager.clone();
        let client = client.clone();
        let semaphore = semaphore.clone();

        let task = tokio::spawn(async move {
            // Acquire permit from semaphore (limits concurrent requests)
            let _permit = semaphore.acquire().await.unwrap();

            match client.fetch_depth(symbol_arc.as_ref(), config.max_triangles).await {
                Ok(depth) => {
                    // Convert to the format expected by apply_snapshot
                    let bids = depth.bids
                        .iter()
                        .map(|l| (l.price, l.quantity))
                        .collect::<Vec<_>>();

                    let asks = depth.asks
                        .iter()
                        .map(|l| (l.price, l.quantity))
                        .collect::<Vec<_>>();

                    // Apply snapshot to orderbook
                    manager.apply_snapshot(
                        symbol_arc.clone(),
                        &bids,
                        &asks,
                        depth.last_update_id
                    ).await;

                    (symbol_arc, true)
                }
                Err(e) => {
                    warn!("Failed to fetch snapshot for {}: {}", symbol_arc, e);
                    (symbol_arc, false)
                }
            }
        });

        snapshot_tasks.push(task);

        // Add small delay between task spawns to smooth out request rate
        sleep(Duration::from_millis(10)).await;
    }

    // Wait for all snapshot tasks to complete
    for task in snapshot_tasks {
        if let Ok((symbol, success)) = task.await {
            if success {
                success_count += 1;
                debug!("Fetched initial snapshot for {}", symbol);
            }
        }
    }

    // Simple success message without colored formatting to avoid issues
    info!(
        "Initial orderbook loading: {} succeeded, {} failed",
        success_count,
        unique_symbols.len() - success_count
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

            // Attempt to connect with exponential backoff retry
            let mut retry_count = 0;
            let max_retries = 5;
            let mut ws_stream = loop {
                match client.connect().await {
                    Ok(stream) => {
                        break stream;
                    }
                    Err(err) => {
                        retry_count += 1;
                        if retry_count >= max_retries {
                            error!("Failed to connect after {} retries: {}", max_retries, err);
                            return;
                        }

                        let backoff = Duration::from_secs((2u64).pow(retry_count as u32));
                        warn!("Connection failed, retrying in {:?}: {}", backoff, err);
                        sleep(backoff).await;
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
            client.set_depth_callback(
                Box::new(move |symbol, bids, asks, first_update_id, last_update_id| {
                    if symbol.trim().is_empty() {
                        error!("Received empty symbol in depth update");
                        return;
                    }

                    // Clone the data to avoid lifetime issues with the spawned task
                    let symbol_arc: Arc<str> = Arc::from(symbol.to_string());
                    let bids_cloned = bids.to_vec();
                    let asks_cloned = asks.to_vec();
                    let manager = manager_for_msg_task.clone();

                    // Spawn a task to update the orderbook without blocking the WebSocket
                    tokio::spawn(async move {
                        // Apply depth update
                        manager.apply_depth_update(
                            &symbol_arc,
                            &bids_cloned,
                            &asks_cloned,
                            first_update_id,
                            last_update_id
                        ).await;
                    });
                })
            ).await;

            // Process WebSocket messages
            if let Err(e) = client.process_messages(&mut ws_stream).await {
                error!("WebSocket processing error: {}", e);
            }
        })
    };

    // Share the paths between tasks
    let arbitrage_paths = Arc::new(triangular_paths);
    let detector_for_task = arbitrage_detector.clone();

    // Start monitoring for arbitrage opportunities
    info!("Starting arbitrage monitoring...");
    print_app_started();

    // Create a task to scan for arbitrage opportunities
    let arbitrage_task = tokio::spawn(async move {
        // Track statistics
        let mut scan_count = 0u64;
        let mut last_stat_time = std::time::Instant::now();
        let mut total_opportunities_found = 0u64;
        let mut total_scan_time_ms = 0u64;

        // Use a sliding window interval to avoid lock contention
        let mut interval = tokio::time::interval(Duration::from_millis(SCAN_INTERVAL_MS));

        loop {
            interval.tick().await;

            // Use standard scan_paths method for now (we'll update this later)
            let start_time = std::time::Instant::now();
            let opportunities = detector_for_task.scan_paths(
                &arbitrage_paths,
                dec!(100.0) // Start with 100 USDT
            ).await;
            let scan_time = start_time.elapsed();

            // Update stats
            scan_count += 1;
            total_scan_time_ms += scan_time.as_millis() as u64;
            total_opportunities_found += opportunities.len() as u64;

            // Print arbitrage opportunities that exceed the threshold
            if !opportunities.is_empty() {
                print_arbitrage_opportunities(&opportunities);
            }

            // Log performance stats periodically
            if last_stat_time.elapsed() > Duration::from_secs(STATS_INTERVAL_SECS) {
                let elapsed_secs = last_stat_time.elapsed().as_secs_f64();
                let scans_per_second = (scan_count as f64) / elapsed_secs;
                let avg_scan_time = (total_scan_time_ms as f64) / (scan_count as f64);

                info!(
                    "Stats: {} scans ({:.2}/sec), avg time {:.2}ms, found {} opportunities",
                    scan_count,
                    scans_per_second,
                    avg_scan_time,
                    total_opportunities_found
                );

                // Reset counters
                scan_count = 0;
                total_scan_time_ms = 0;
                total_opportunities_found = 0;
                last_stat_time = std::time::Instant::now();
            }
        }
    });

    // Wait for CTRL+C signal for graceful shutdown
    info!("\n Press Ctrl+C to exit");

    // Use tokio's ctrl_c signal handler
    tokio::signal::ctrl_c().await?;
    info!("Received Ctrl+C, shutting down...");

    // Clean up and exit
    message_task.abort();
    arbitrage_task.abort();

    // Give tasks a moment to clean up
    sleep(Duration::from_millis(200)).await;

    info!("Triangular arbitrage system stopped");
    Ok(())
}

/// Helper function to print detected arbitrage opportunities in a nice format
fn print_arbitrage_opportunities(opportunities: &[ArbitrageOpportunity]) {
    if opportunities.is_empty() {
        return;
    }

    info!("\n=== ARBITRAGE OPPORTUNITIES ===");

    for (i, opp) in opportunities.iter().enumerate() {
        // Calculate profit percentage for display
        let profit_pct = (opp.profit_ratio - dec!(1.0)) * dec!(100.0);

        info!(
            "#{}: {} → {} → {} | Profit: {}% | Start: {} {} | End: {} {}",
            i + 1,
            opp.path.first_symbol,
            opp.path.second_symbol,
            opp.path.third_symbol,
            profit_pct,
            opp.start_amount,
            opp.path.start_asset,
            opp.end_amount,
            opp.path.end_asset
        );
    }

    println!("================================\n");
}
