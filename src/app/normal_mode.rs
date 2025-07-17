use std::sync::{ atomic::{ AtomicBool, Ordering }, Arc };
use std::thread;
use std::time::Duration;
use crate::{
    arbitrage::{ self },
    config::Config,
    exchange::{ binance::BinanceClient, client::ExchangeClient },
    orderbook_manager::manager::OrderBookManager,
    utils::{
        console::{ print_app_started, print_app_starting, print_config },
        helpers::{ build_symbol_map, build_tokio_async_runtime, run_api_with_timeout },
        symbol_interner::global,
        validation::{ print_trading_mode_info, validate_startup_config },
    },
    utils::{
        fix_ops::{ create_fix_client, FixClientType, execute_triangular_trades_with_simulation },
        thread_ops::{ start_detection_thread, start_execution_thread, ThreadManager },
        websocket_ops::{ start_websocket_thread, create_depth_update_callback, WebSocketConfig },
    },
};
use anyhow::{ anyhow, Context, Result };
use tracing::{ error, info };

pub fn run(config: Config, shutdown: Arc<AtomicBool>) -> Result<()> {
    print_app_starting();
    print_config(&config);

    validate_startup_config(&config)?;

    let client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    let rt = build_tokio_async_runtime()?;

    if run_api_with_timeout(&rt, client.is_operational())? {
        info!("✓ Exchange is operational");
    } else {
        error!("❌ Exchange is not operational or timed out");
        return Err(anyhow!("Exchange is not operational"));
    }

    let symbols = run_api_with_timeout(&rt, client.get_active_spot_symbols())?;
    info!("✓ Fetched {} symbols from exchange", symbols.len());

    let symbol_map = build_symbol_map(symbols.clone(), &config)?;
    let triangular_paths = symbol_map.get_trangular_paths_as_arc();
    let unique_symbols = symbol_map.get_unique_symbols();
    global::intern_batch(&unique_symbols);

    let orderbook_manager = Arc::new(
        OrderBookManager::new(config.depth, Arc::<BinanceClient>::clone(&client))
    );

    let detector = arbitrage::detector::create_detector(
        Arc::clone(&orderbook_manager),
        triangular_paths,
        Arc::clone(&shutdown),
        config.fee,
        config.threshold,
        config.trade_amount,
        &symbol_map
    );

    let fix_client = create_fix_executor(Arc::clone(&symbol_map), &config)?;
    let mut thread_manager = ThreadManager::new();

    // 1. Detection handling
    let detection_handle = start_detection_thread(
        Arc::clone(&detector),
        Arc::clone(&orderbook_manager),
        Arc::clone(&shutdown)
    );
    thread_manager.add_handle(detection_handle);

    // 2. Execution handling (fix socket)
    let execution_callback = fix_client
        .as_ref()
        .map(|client| {
            create_execution_callback(
                client.clone(),
                Arc::clone(&symbol_map),
                Arc::clone(&orderbook_manager)
            )
        });

    let execution_handle = start_execution_thread(
        Arc::clone(&detector),
        execution_callback,
        Arc::clone(&shutdown)
    );
    thread_manager.add_handle(execution_handle);

    // 3. depth update handling (sbe socket)
    let ws_config = WebSocketConfig::new(config.sbe_api_key.clone(), unique_symbols.clone());
    let depth_callback = create_depth_update_callback(Arc::clone(&orderbook_manager));
    let ws_handle = start_websocket_thread(
        ws_config,
        Arc::clone(&orderbook_manager),
        Arc::clone(&shutdown),
        depth_callback
    )?;
    thread_manager.add_handle(ws_handle);

    print_app_started();

    info!("\nPress Ctrl+C to exit");

    print_trading_mode_info(fix_client.is_some(), config.debug);

    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }

    info!("Shutting down triangular arbitrage system...");

    if let Some(fix_exec) = fix_client {
        if let Err(e) = fix_exec.disconnect() {
            error!("Error disconnecting FIX executor: {}", e);
        } else {
            info!("FIX executor disconnected successfully");
        }
    }

    thread_manager.join_all();

    thread::sleep(Duration::from_millis(200));

    info!("Triangular arbitrage system stopped");
    Ok(())
}

fn create_fix_executor(
    symbol_map: Arc<crate::models::symbol_map::SymbolMap>,
    config: &Config
) -> Result<Option<FixClientType>> {
    if config.should_enable_real_trading() || config.should_enable_mock_trading() {
        info!("Creating FIX executor ({}) with precision support", if config.debug {
            "Mock"
        } else {
            "Real"
        });

        let fix_config = if config.debug {
            None
        } else {
            Some(config.clone().get_fix_client_config())
        };

        match create_fix_client(config.debug, fix_config, config.clone(), &symbol_map)? {
            Some(executor) => {
                executor.connect()?;
                info!("✓ FIX executor connected successfully with precision support");
                Ok(Some(executor))
            }
            None => {
                error!("Failed to create FIX executor");
                Err(anyhow!("Failed to create FIX executor"))
            }
        }
    } else {
        info!("Trading disabled, using simulation mode only");
        Ok(None)
    }
}

fn create_execution_callback(
    fix_client: FixClientType,
    symbol_map: Arc<crate::models::symbol_map::SymbolMap>,
    orderbook_manager: Arc<OrderBookManager>
) -> crate::arbitrage::detector::ExecutionCallback {
    Arc::new(move |opportunity| {
        info!("🚀 ENHANCED EXECUTION: Starting arbitrage with simulation logging");

        match
            execute_triangular_trades_with_simulation(
                &opportunity,
                &fix_client,
                &symbol_map,
                &orderbook_manager
            )
        {
            Ok(true) => {
                info!(
                    "✅ Enhanced triangular arbitrage completed successfully: {}",
                    opportunity.display()
                );
            }
            Ok(false) => {
                error!(
                    "⚠️ Enhanced triangular arbitrage partially failed: {}",
                    opportunity.display()
                );
            }
            Err(e) => {
                error!(
                    "❌ Enhanced triangular arbitrage failed: {} - Error: {}",
                    opportunity.display(),
                    e
                );
            }
        }
    })
}
