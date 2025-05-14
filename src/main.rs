mod config;
mod constants;
mod enums;
mod exchange;
mod helpers;
mod utils;
mod sbe;
mod models;
mod orderbook;

use std::time::Duration;
use colored::Colorize;
use models::symbol_map::SymbolMap;
use tokio::time::timeout;

use config::Config;
use anyhow::{ Context, Result };

use exchange::{ binance::BinanceClient, client::ExchangeClient };
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
            .take(config.max_triangles)
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

    // This would scan prices for each path and execute trades when profitable

    info!("Application completed successfully");

    // // Check if the exchange is operational (with timeout)
    // match timeout(API_TIMEOUT, client.is_operational()).await {
    //     Ok(Ok(true)) => {
    //         info!("Exchange is operational");
    //     }
    //     _ => {
    //         error!("Exchange is not operational or timed out");
    //         anyhow::bail!("Exchange is not operational");
    //     }
    // }

    // config.print();

    // Logger::init(config.log_level.as_ref());

    // let client = RestApiClient::new().unwrap();

    // let symbols = match client.fetch_all_trading_spot_symbols().await {
    //     Result::Ok(symbols) => symbols,
    //     Result::Err(e) => {
    //         error!("Error fetching symbols: {}", e);
    //         process::exit(1);
    //     }
    // };

    // info!("symbols len: {}", symbols.len());

    // let (triangles, used) = create_triangles_from_symbols(&symbols, config.pairs_count);

    // info!("triangles: {}", triangles.len());
    // info!("used: {}", used.len());

    // println!("Press Ctrl+C to close the app...");
    // tokio::signal::ctrl_c().await.expect("failed to listen for Ctrl+C");
    // info!("Shutting down");
    Ok(())
}
