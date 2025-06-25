use std::sync::Arc;

use anyhow::{ anyhow, Context };
use tokio::{ runtime::Runtime };
use tracing::{ error, info, warn };

use crate::{
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient },
    executor::fix_executor::FixArbitrageExecutor,
    models::{ command::Command, symbol::Symbol, symbol_map::SymbolMap },
    orderbook::manager::OrderBookManager,
    utils::symbol_interner::global,
    API_TIMEOUT,
};

pub fn get_run_command() -> Command {
    if std::env::args().nth(1).as_deref() == Some("perf-test") {
        Command::PerformanceTest
    } else {
        Command::Run
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

pub fn is_operational(runtime: &Runtime, client: Arc<BinanceClient>) -> bool {
    let is_operational = runtime.block_on(async {
        tokio::time::timeout(API_TIMEOUT, client.is_operational()).await
    });

    match is_operational {
        Ok(Ok(true)) => true,
        _ => false,
    }
}

pub fn get_all_symbols(
    runtime: &Runtime,
    client: Arc<BinanceClient>
) -> Result<Vec<Symbol>, anyhow::Error> {
    let symbols = runtime.block_on(async {
        tokio::time::timeout(API_TIMEOUT, client.get_active_spot_symbols()).await
    });

    match symbols {
        Ok(symbols) => symbols,
        Err(_) => {
            error!("Timed out while fetching symbols");
            return Err(anyhow!("Timed out while fetching symbols"));
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

    Ok(Arc::new(symbol_map))
}

pub fn build_execultor(
    manager: Arc<OrderBookManager>,
    symbol_map: Arc<SymbolMap>,
    config: &Config
) -> Result<Option<Arc<FixArbitrageExecutor>>, anyhow::Error> {
    if config.should_enable_real_trading() || config.should_enable_mock_trading() {
        info!("Creating FIX executor ({}) with precision support", if config.debug {
            "Mock"
        } else {
            "Real"
        });

        let fix_config = if config.debug { None } else { Some(config.get_fix_client_config()) };

        match
            FixArbitrageExecutor::new(
                manager,
                config.debug,
                fix_config,
                config.max_slippage_percentage,
                symbol_map.clone()
            )
        {
            Ok(executor) => {
                let executor = Arc::new(executor);

                if let Err(e) = executor.connect() {
                    error!("Failed to connect FIX executor: {}", e);
                    return Err(anyhow!("Failed to connect FIX executor: {}", e));
                }

                info!("✓ FIX executor connected successfully with precision support");
                Ok(Some(executor))
            }
            Err(e) => {
                error!("Failed to create FIX executor: {}", e);
                return Err(anyhow!("Failed to create FIX executor: {}", e));
            }
        }
    } else {
        info!("Trading disabled, using simulation mode only");
        Ok(None)
    }
}

pub fn create_detector_and_executor(
    orderbook_manager: Arc<OrderBookManager>,
    symbol_map: Arc<SymbolMap>,
    config: &Config,
    is_perf: bool
) -> Result<
    (Arc<crate::arbitrage::detector::ArbitrageDetectorState>, Option<Arc<FixArbitrageExecutor>>),
    anyhow::Error
> {
    let triangular_paths = symbol_map.get_trangular_paths_as_arc();

    let fallback_executor = Arc::new(
        crate::executor::executor::ArbitrageExecutor::new(
            crate::executor::executor::ExecutionStrategy::FastSequential
        )
    );

    let mut fix_executor_opt = None;
    let mut execution_callback_opt = None;

    if config.should_enable_real_trading() || config.should_enable_mock_trading() {
        info!("Creating CALLBACK-ENABLED FIX executor ({}) with zero-latency execution", if
            config.debug
        {
            "Mock"
        } else {
            "Real"
        });

        let fix_config = if config.debug { None } else { Some(config.get_fix_client_config()) };

        match
            FixArbitrageExecutor::new(
                orderbook_manager.clone(),
                config.debug,
                fix_config,
                config.min_execution_profit_threshold,
                symbol_map.clone()
            )
        {
            Ok(mut executor) => {
                if let Err(e) = executor.connect() {
                    error!("Failed to connect callback-enabled FIX executor: {}", e);
                    return Err(anyhow!("Failed to connect callback-enabled FIX executor: {}", e));
                }

                let execution_callback = executor.enable_callback_execution(3);
                let executor_arc = Arc::new(executor);

                info!(
                    "✓ Callback-enabled FIX executor connected successfully with zero-latency execution"
                );

                fix_executor_opt = Some(executor_arc);
                execution_callback_opt = Some(execution_callback);
            }
            Err(e) => {
                error!("Failed to create callback-enabled FIX executor: {}", e);
                return Err(anyhow!("Failed to create callback-enabled FIX executor: {}", e));
            }
        }
    } else {
        info!("Trading disabled, using simulation mode only");
    }

    let detector = crate::arbitrage::detector::create_detector(
        orderbook_manager.clone(),
        config.fee,
        config.threshold,
        triangular_paths,
        config.trade_amount,
        fallback_executor,
        fix_executor_opt.clone(),
        config.min_volume_multiplier,
        is_perf
    );

    if let Some(execution_callback) = execution_callback_opt {
        detector.register_execution_callback(execution_callback);
        info!("✓ Connected detector to FIX executor via direct callbacks");
        info!("✓ Zero-latency execution path established");
    } else {
        info!("No callback execution configured, using simulation mode");
    }

    Ok((detector, fix_executor_opt))
}

pub fn prepare_symbol_mappings(symbols: &[String]) -> Vec<(String, u16)> {
    global::intern_batch(symbols);
    global::get_all_mappings()
}
