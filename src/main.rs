// src/main.rs
mod config;
mod constants;
mod enums;
mod exchange;
mod helpers;
mod utils;
mod models;
mod orderbook;
mod arbitrage;

use std::time::Duration;
use std::sync::Arc;
use colored::Colorize;
use models::symbol_map::SymbolMap;
use tokio::time::timeout;
use rust_decimal_macros::dec;

use config::Config;
use anyhow::{ anyhow, Context, Result };

use exchange::{ binance::BinanceClient, client::ExchangeClient, sbe_client::BinanceSbeClient };
use tracing::{ error, info, warn };
use utils::{ console::{ print_app_started, print_app_starting, print_config }, logging };
use orderbook::manager::OrderBookManager;
use arbitrage::detector::ArbitrageDetector;

const API_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env().context("Failed to load configuration from environment")?;

    logging
        ::init_logging(config.log_level, config.debug, &config.log_config)
        .context("Failed to initialize logging system")?;

    print_app_starting();
    print_config(&config);

    print_app_started();

    // Initialize exchange client
    let client = BinanceClient::new(
        config.fix_api.clone(),
        config.fix_secret.clone(),
        config.debug
    )?;

    info!("Connected to exchange: {}", client.name());

    // Verify exchange connectivity
    match timeout(API_TIMEOUT, client.is_operational()).await {
        Ok(Ok(true)) => {
            info!("Exchange is operational");
        }
        _ => {
            error!("Exchange is not operational or timed out");
            anyhow::bail!("Exchange is not operational");
        }
    }

    // Fetch all active spot trading symbols (with timeout)
    let symbols = match timeout(API_TIMEOUT, client.get_active_spot_symbols()).await {
        Ok(Ok(symbols)) => symbols,
        Ok(Err(e)) => {
            error!("Failed to fetch symbols: {}", e);
            anyhow::bail!("Failed to fetch symbols: {}", e);
        }
        Err(_) => {
            error!("Timed out while fetching symbols");
            anyhow::bail!("Timed out while fetching symbols");
        }
    };

    // Create symbol map and find triangular paths
    let mut symbol_map = SymbolMap::from_symbols(symbols);

    symbol_map.find_targeted_triangular_paths(
        &config.base_asset,
        config.max_triangles,
        &config.excluded_fiats
    );

    let total_paths = symbol_map.get_triangular_paths().len();

    info!("Found {} potential triangular arbitrage paths", total_paths);

    if total_paths > 0 {
        // Log a sample of paths (limit to avoid excessive logging)
        let examples = symbol_map
            .get_triangular_paths()
            .iter()
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

        info!("Example paths: \n{}", examples.magenta());
    } else {
        warn!("No triangular paths found. Check base_asset and excluded_fiats configuration.");
        anyhow::bail!("No triangular paths found. Cannot continue.");
    }

    // Convert paths to Arc for zero-copy sharing
    let triangular_paths: Vec<Arc<_>> = symbol_map
        .get_triangular_paths()
        .iter()
        .map(|p| Arc::new(p.clone()))
        .collect();

    // Create the shared orderbook manager
    let orderbook_manager = Arc::new(OrderBookManager::new(20)); // Keep 20 levels per side

    // Create the arbitrage detector with specified thresholds
    let arbitrage_detector = Arc::new(
        ArbitrageDetector::new(
            orderbook_manager.clone(),
            dec!(0.001), // 0.1% fee
            dec!(0.002) // 0.2% minimum profit threshold
        )
    );

    // Create a clone for the message task
    let manager_for_msg_task = orderbook_manager.clone();
    let paths_for_msg_task = Arc::new(symbol_map.get_unique_symbols());

    // Start WebSocket data stream task
    info!("Starting WebSocket connection for real-time market data...");
    let message_task = tokio::spawn(async move {
        let client = BinanceSbeClient::new(config.sbe_api_key);

        // Attempt to connect, with retry logic
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
                    tokio::time::sleep(backoff).await;
                }
            }
        };

        info!("Connected to Binance SBE WebSocket");

        // Subscribe to depth stream for each symbol
        let channels: Vec<String> = vec!["depth".to_string()];

        // Get symbols for subscription (limit to 100 to avoid exceeding connection limits)
        let subscription_symbols = paths_for_msg_task
            .iter()
            .take(100) // Binance recommends no more than 100-200 symbols per connection
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

        // Set depth callback for orderbook updates
        client.set_depth_callback(
            Box::new(move |symbol, bids, asks, first_update_id, last_update_id| {
                // Clone the data to avoid lifetime issues with the spawned task
                let symbol_arc = Arc::from(symbol.to_string());
                let bids_cloned: Vec<(f64, f64)> = bids.to_vec();
                let asks_cloned: Vec<(f64, f64)> = asks.to_vec();
                let manager = manager_for_msg_task.clone();

                // Create a task to update the orderbook without blocking the WebSocket
                tokio::spawn(async move {
                    // Update orderbook with cloned data
                    manager.apply_snapshot(
                        symbol_arc,
                        &bids_cloned,
                        &asks_cloned,
                        last_update_id
                    ).await;

                    // Log updates occasionally (to reduce allocations)
                    if last_update_id % 1000 == 0 {
                        info!("Orderbook update");
                    }
                });
            })
        ).await;

        // Process WebSocket messages
        if let Err(e) = client.process_messages(&mut ws_stream).await {
            error!("WebSocket processing error: {}", e);
        }
    });

    // Wait a moment for initial data to populate orderbooks
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("Starting arbitrage scanner...");

    // Share the paths between tasks
    let arbitrage_paths = Arc::new(triangular_paths);

    // Create a task to scan for arbitrage opportunities
    // Clone detector before moving into tokio task
    let detector_for_task = arbitrage_detector.clone();

    let arbitrage_task = tokio::spawn(async move {
        // Use a sliding window interval to avoid lock contention
        let mut interval = tokio::time::interval(Duration::from_millis(250));

        // Performance stats
        let mut scan_count = 0u64;
        let mut last_stat_time = std::time::Instant::now();
        let mut opportunities_found = 0u64;

        loop {
            interval.tick().await;

            // Scan with minimal allocations
            let start_time = std::time::Instant::now();
            // Use the cloned detector here
            let opportunities = detector_for_task.scan_paths(
                &arbitrage_paths,
                dec!(100.0) // Start with 100 USDT
            ).await;
            let scan_time = start_time.elapsed();

            scan_count += 1;
            opportunities_found += opportunities.len() as u64;

            // Log top opportunities if any found
            if !opportunities.is_empty() {
                // Log only a limited number to reduce allocations
                for (i, opp) in opportunities.iter().take(3).enumerate() {
                    info!(
                        rank = i + 1,
                        path = format!(
                            "{} → {} → {}",
                            opp.path.first_symbol,
                            opp.path.second_symbol,
                            opp.path.third_symbol
                        ),
                        profit_pct = format!("{:.4}%", opp.profit_percentage()),
                        start = format!("{:.2} {}", opp.start_amount, opp.path.start_asset),
                        end = format!("{:.2} {}", opp.end_amount, opp.path.end_asset),
                        "Arbitrage opportunity"
                    );
                }
            }

            // Log performance stats periodically
            if last_stat_time.elapsed() > Duration::from_secs(60) {
                info!(
                    scans_per_second = format!(
                        "{:.2}",
                        (scan_count as f64) / last_stat_time.elapsed().as_secs_f64()
                    ),
                    avg_scan_time_ms = format!("{:.2}", (scan_time.as_micros() as f64) / 1000.0),
                    total_opportunities = opportunities_found,
                    "Scanner performance"
                );

                scan_count = 0;
                opportunities_found = 0;
                last_stat_time = std::time::Instant::now();
            }
        }
    });

    // Wait for CTRL+C signal for graceful shutdown
    info!("\n Press Ctrl+C to exit");
    tokio::signal::ctrl_c().await.map_err(|e| anyhow!("Failed to listen for Ctrl+C: {}", e))?;

    info!("Received Ctrl+C, shutting down...");

    // Clean up and exit
    message_task.abort();
    arbitrage_task.abort();

    // Give tasks a moment to clean up
    tokio::time::sleep(Duration::from_millis(200)).await;

    info!("Triangular arbitrage system stopped");
    Ok(())
}
