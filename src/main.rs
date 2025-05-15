mod config;
mod constants;
mod enums;
mod exchange;
mod helpers;
mod utils;
mod models;
mod orderbook;

use std::time::Duration;
use colored::Colorize;
use models::symbol_map::SymbolMap;
use tokio::time::timeout;

use config::Config;
use anyhow::{ anyhow, Context, Result };

use exchange::{ binance::BinanceClient, client::ExchangeClient, sbe_client::BinanceSbeClient };
use tracing::{ error, info };
use utils::{ console::{ print_app_started, print_app_starting, print_config }, logging };

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

    let client = BinanceClient::new(
        config.fix_api.clone(),
        config.fix_secret.clone(),
        config.debug
    )?;

    info!("Connected to exchange: {}", client.name());

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

    // Create symbol map
    let mut symbol_map = SymbolMap::from_symbols(symbols);

    symbol_map.find_targeted_triangular_paths(
        &config.base_asset,
        config.max_triangles,
        &config.excluded_fiats
    );

    let total_paths = symbol_map.get_triangular_paths().len();

    info!("total_paths: {}", total_paths);

    if total_paths > 0 {
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
    }

    let message_task = tokio::spawn(async move {
        let client = BinanceSbeClient::new(config.sbe_api_key);
        let mut ws_stream = client.connect().await.unwrap();

        let symbols: Vec<String> = symbol_map.get_unique_symbols();

        let channels: Vec<String> = vec!["depth".to_string()];
        // &v.push("depth".to_string());

        client
            .subscribe(&mut ws_stream, symbols.iter().as_slice(), channels.iter().as_slice()).await
            .unwrap();

        client.process_messages(&mut ws_stream).await.unwrap();
    });
    // while let Some(msg_result) = read.next().await {
    //     match msg_result {
    //         Ok(msg) => {
    //             match msg {
    //                 Message::Text(text) => {
    //                     println!("Received: {}", text);
    //                 }
    //                 Message::Binary(bin) => {
    //                     println!("Received binary data: {} bytes", bin.len());
    //                 }
    //                 Message::Ping(data) => {
    //                     println!("Received ping");
    //                     // You might want to respond with a pong
    //                     // if let Err(e) = write.send(Message::Pong(data)).await {
    //                     //     eprintln!("Failed to send pong: {}", e);
    //                     // }
    //                 }
    //                 Message::Pong(_) => {
    //                     println!("Received pong");
    //                 }
    //                 Message::Close(frame) => {
    //                     println!("Connection closed: {:?}", frame);
    //                     break;
    //                 }
    //                 _ => {
    //                     println!("Received other message type");
    //                 }
    //             }
    //         }
    //         Err(e) => {
    //             eprintln!("Error receiving message: {}", e);
    //             break;
    //         }
    //     }
    // }

    info!("\n Press Ctrl+C to exit");
    tokio::signal::ctrl_c().await.map_err(|e| anyhow!("Failed to listen for Ctrl+C: {}", e))?;

    println!("Received Ctrl+C, shutting down...");

    // Clean up and exit
    message_task.abort();

    Ok(())
}
