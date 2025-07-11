use std::sync::Arc;
use anyhow::{ anyhow, Context };
use tokio::runtime::Runtime;
use tracing::{ error, info, warn };
use crate::{
    config::Config,
    models::{ command::Command, symbol::Symbol, symbol_map::SymbolMap },
    utils::symbol_interner::global,
    API_TIMEOUT,
};

pub fn get_run_command() -> Command {
    match std::env::args().nth(1).as_deref() {
        Some("perf-test") => Command::PerformanceTest,
        Some("test-exec") => Command::TestExecution,
        _ => Command::Run,
    }
}

pub fn build_tokio_async_runtime() -> Result<Runtime, anyhow::Error> {
    tokio::runtime::Builder
        ::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .context("Failed to create Tokio runtime")
}

pub fn run_api_with_timeout<T, F>(runtime: &Runtime, handler: F) -> Result<T, anyhow::Error>
    where F: std::future::Future<Output = Result<T, anyhow::Error>>
{
    let result = runtime.block_on(async { tokio::time::timeout(API_TIMEOUT, handler).await });

    match result {
        Ok(symbols) => symbols,
        Err(_) => {
            error!("Timed out while fetching symbols");
            Err(anyhow!("Timed out while fetching symbols"))
        }
    }
}

pub fn build_symbol_map(
    symbols: Vec<Symbol>,
    config: &Config
) -> Result<Arc<SymbolMap>, anyhow::Error> {
    let symbol_names: Vec<String> = symbols
        .iter()
        .map(|s| s.symbol.to_string())
        .collect();

    global::intern_batch(&symbol_names);
    info!("Preloaded {} symbols into global interner", symbol_names.len());

    let mut symbol_map = SymbolMap::from_symbols(symbols);

    info!("Finding triangular arbitrage paths starting with {}...", config.base_asset);
    symbol_map.find_targeted_triangular_paths(
        &config.base_asset,
        config.max_triangles,
        &config.excluded_coins
    );

    let paths = symbol_map.get_triangular_paths();
    let total_paths = paths.len();

    info!("Found {} potential triangular arbitrage paths", total_paths);

    if total_paths > 0 {
        let examples = paths
            .iter()
            .take(5)
            .enumerate()
            .map(|(i, p)| {
                format!(
                    "{:3}. {} → {} → {}",
                    i + 1,
                    p.first_symbol,
                    p.second_symbol,
                    p.third_symbol
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        info!("Sample paths: \n{}", examples);
    } else {
        warn!("No triangular paths found. Check base_asset and excluded_fiats configuration.");
        return Err(anyhow!("No triangular paths found. Cannot continue."));
    }

    Ok(Arc::new(symbol_map))
}

pub fn create_output_file() -> anyhow::Result<String> {
    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let output_dir = format!("./performance_results/{timestamp}");
    std::fs
        ::create_dir_all(&output_dir)
        .context(format!("Failed to create output directory: {output_dir}"))?;

    let output_file = format!("{output_dir}/performance_results.csv");
    info!("Results will be saved to: {}", output_file);

    Ok(output_file)
}

pub fn print_executor_info(has_exec: bool, is_debug: bool, threshold: f64) {
    let mode = if has_exec {
        if is_debug {
            "FIX Mock Trading (Performance Test)"
        } else {
            "FIX Real Trading (Performance Test)"
        }
    } else {
        "Simulation Only (Performance Test)"
    };

    info!("Created arbitrage detector with threshold: {:.2}% (Mode: {})", threshold * 100.0, mode);
}
