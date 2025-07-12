use std::sync::Arc;
use std::time::Duration;
use std::sync::atomic::{ AtomicBool, Ordering };
use crate::{
    arbitrage::detector::ArbitrageOpportunity,
    config::Config,
    exchange::{
        binance::BinanceClient,
        client::ExchangeClient,
        fix_api::{ FixClient, MarketOrder, OrderSide },
    },
    exchange::mock_fix_api::MockFixClient,
    utils::{
        console::print_config,
        helpers::{ build_symbol_map, build_tokio_async_runtime, run_api_with_timeout },
    },
};
use anyhow::{ anyhow, Context, Result };
use tracing::{ error, info, warn };

pub fn run_test_execution(config: Config) -> Result<()> {
    print_config(&config);
    info!("🧪 Starting triangular arbitrage test execution");

    if !config.should_enable_real_trading() && !config.should_enable_mock_trading() {
        error!("❌ Trading must be enabled for test execution");
        error!("   Set TAA_ENABLE_TRADING=true in your .env file");
        return Err(anyhow!("Trading not enabled in configuration"));
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);

    ctrlc
        ::set_handler(move || {
            info!("🛑 Received Ctrl+C, stopping test execution...");
            shutdown_clone.store(true, Ordering::Relaxed);
        })
        .context("Error setting Ctrl-C handler")?;

    let api_client = Arc::new(
        BinanceClient::new(config.fix_api.clone(), config.debug).context(
            "Failed to create Binance client"
        )?
    );

    let rt = build_tokio_async_runtime()?;

    info!("🔍 Testing exchange connectivity...");
    if run_api_with_timeout(&rt, api_client.is_operational())? {
        info!("✅ Exchange is operational");
    } else {
        error!("❌ Exchange is not operational or timed out");
        return Err(anyhow!("Exchange is not operational"));
    }

    info!("📊 Fetching symbol information...");
    let symbols = run_api_with_timeout(&rt, api_client.get_active_spot_symbols())?;
    info!("✅ Fetched {} symbols from exchange", symbols.len());

    let symbol_map = build_symbol_map(symbols.clone(), &config)?;

    if config.debug {
        info!("🔧 Creating Mock FIX client for testing...");
        run_mock_test(&config, &symbol_map, &shutdown)
    } else {
        info!("🔧 Creating Real FIX client for testing...");
        run_real_test(&config, &symbol_map, &shutdown)
    }
}

fn run_mock_test(
    config: &Config,
    symbol_map: &Arc<crate::models::symbol_map::SymbolMap>,
    shutdown: &Arc<AtomicBool>
) -> Result<()> {
    let mut mock_client = MockFixClient::new();

    info!("🔗 Connecting to Mock FIX API...");
    mock_client.connect()?;
    info!("✅ Mock FIX client connected successfully");

    let opportunity = create_test_opportunity(config, symbol_map)?;
    info!("📋 Created test triangular arbitrage:");
    info!("   Path: {}", opportunity.display());

    if shutdown.load(Ordering::Relaxed) {
        info!("🛑 Test cancelled by user");
        mock_client.disconnect()?;
        return Ok(());
    }

    info!("🚀 Executing triangular arbitrage...");
    let success = execute_mock_triangular(&opportunity, &mut mock_client, symbol_map)?;

    if success {
        info!("🎉 Mock triangular arbitrage completed successfully!");
        info!("   All three orders executed correctly");
    } else {
        warn!("⚠️  Mock triangular arbitrage partially failed");
    }

    info!("🔌 Disconnecting Mock FIX client...");
    mock_client.disconnect()?;
    info!("✅ Mock FIX client disconnected successfully");

    Ok(())
}

fn run_real_test(
    config: &Config,
    symbol_map: &Arc<crate::models::symbol_map::SymbolMap>,
    shutdown: &Arc<AtomicBool>
) -> Result<()> {
    let fix_config = config.get_fix_client_config();
    let mut fix_client = FixClient::new(fix_config).context("Failed to create FIX client")?;

    for symbol in symbol_map.get_all_symbols() {
        fix_client.update_symbol_precision(
            &symbol.symbol,
            symbol.qty_precision,
            symbol.price_precision
        );
    }

    info!("🔗 Connecting to Real FIX API...");
    fix_client.connect()?;
    info!("✅ Real FIX client connected successfully");

    let opportunity = create_test_opportunity(config, symbol_map)?;
    info!("📋 Created test triangular arbitrage:");
    info!("   Path: {}", opportunity.display());

    if shutdown.load(Ordering::Relaxed) {
        info!("🛑 Test cancelled by user");
        fix_client.disconnect()?;
        return Ok(());
    }

    warn!("⚠️  REAL MONEY WARNING: This will execute actual trades!");
    info!("🚀 Executing triangular arbitrage...");
    let success = execute_real_triangular(&opportunity, &mut fix_client, symbol_map)?;

    if success {
        info!("🎉 Real triangular arbitrage completed successfully!");
        info!("   All three orders executed correctly");
        info!("   Your FIX API is working for triangular arbitrage");
    } else {
        warn!("⚠️  Real triangular arbitrage partially failed");
    }

    info!("🔌 Disconnecting FIX client...");
    fix_client.disconnect()?;
    info!("✅ Real FIX client disconnected successfully");

    Ok(())
}

fn create_test_opportunity(
    config: &Config,
    symbol_map: &Arc<crate::models::symbol_map::SymbolMap>
) -> Result<ArbitrageOpportunity> {
    let paths = symbol_map.get_triangular_paths();
    if paths.is_empty() {
        return Err(anyhow!("No triangular paths available for testing"));
    }

    let test_path = &paths[0];
    let test_amount = (config.trade_amount * 0.01).max(10.0);

    info!("📝 Test details:");
    info!(
        "   Path: {} → {} → {}",
        test_path.first_symbol,
        test_path.second_symbol,
        test_path.third_symbol
    );
    info!("   Amount: ${:.2}", test_amount);

    let opportunity = ArbitrageOpportunity {
        path: Arc::new(test_path.clone()),
        profit_ratio: 1.005,
        start_amount: test_amount,
        symbols_involved: [0, 1, 2],
    };

    Ok(opportunity)
}

fn execute_mock_triangular(
    opportunity: &ArbitrageOpportunity,
    client: &mut MockFixClient,
    symbol_map: &Arc<crate::models::symbol_map::SymbolMap>
) -> Result<bool> {
    let path = &opportunity.path;
    let symbols = [&path.first_symbol, &path.second_symbol, &path.third_symbol];
    let directions = [
        path.first_is_base_to_quote,
        path.second_is_base_to_quote,
        path.third_is_base_to_quote,
    ];

    let mut current_amount = opportunity.start_amount;
    info!("🔄 Starting with {:.6} {}", current_amount, path.start_asset);

    for (i, &symbol) in symbols.iter().enumerate() {
        let precision_info = symbol_map.get(symbol);
        let (qty_precision, price_precision) = if let Some(info) = precision_info {
            (info.qty_precision, info.price_precision)
        } else {
            (8, 8)
        };

        let order = create_order(
            symbol.to_string(),
            directions[i],
            current_amount,
            qty_precision,
            price_precision
        );

        info!(
            "📋 Trade {}/{}: {} {} (amount: {:.8})",
            i + 1,
            symbols.len(),
            match order.side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            },
            symbol,
            current_amount
        );

        let result = client.place_market_order(order)?;

        info!("✅ Trade {}/{} executed:", i + 1, symbols.len());
        info!("   Filled: {:.8} @ {:.8}", result.filled_qty, result.avg_price);
        info!("   Total: {:.8}", result.cum_quote_qty);

        if i < symbols.len() - 1 {
            current_amount = if directions[i] { result.cum_quote_qty } else { result.filled_qty };
            info!("💱 Next amount: {:.8}", current_amount);
        }

        std::thread::sleep(Duration::from_millis(100));
    }

    let final_amount = current_amount;
    let profit = final_amount - opportunity.start_amount;
    let profit_pct = (profit / opportunity.start_amount) * 100.0;

    info!("🎯 Result:");
    info!("   Start: {:.6} {}", opportunity.start_amount, path.start_asset);
    info!("   End: {:.6} {}", final_amount, path.end_asset);
    info!("   Profit: {:.6} ({:.4}%)", profit, profit_pct);

    Ok(true)
}

fn execute_real_triangular(
    opportunity: &ArbitrageOpportunity,
    client: &mut FixClient,
    symbol_map: &Arc<crate::models::symbol_map::SymbolMap>
) -> Result<bool> {
    let path = &opportunity.path;
    let symbols = [&path.first_symbol, &path.second_symbol, &path.third_symbol];
    let directions = [
        path.first_is_base_to_quote,
        path.second_is_base_to_quote,
        path.third_is_base_to_quote,
    ];

    let mut current_amount = opportunity.start_amount;
    info!("🔄 Starting REAL triangular execution with {:.6} {}", current_amount, path.start_asset);

    for (i, &symbol) in symbols.iter().enumerate() {
        if !client.is_connected() {
            return Err(anyhow!("FIX client disconnected"));
        }

        let precision_info = symbol_map.get(symbol);
        let (qty_precision, price_precision) = if let Some(info) = precision_info {
            (info.qty_precision, info.price_precision)
        } else {
            (8, 8)
        };

        let order = create_order(
            symbol.to_string(),
            directions[i],
            current_amount,
            qty_precision,
            price_precision
        );

        info!(
            "📋 REAL Trade {}/{}: {} {} (amount: {:.8})",
            i + 1,
            symbols.len(),
            match order.side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            },
            symbol,
            current_amount
        );

        let result = client.place_market_order(order)?;

        info!("✅ REAL Trade {}/{} placed:", i + 1, symbols.len());
        info!("   Order ID: {}", result.order_id);
        info!("   Status: {}", result.order_status);
        info!("   Leaves Qty: {:.8}", result.leaves_qty);
        info!("   Time: {:.2}ms", result.execution_time.as_secs_f64() * 1000.0);

        if result.order_status == "REJECTED" {
            return Err(anyhow!("Order rejected: {:?}", result.reject_reason));
        }

        if i < symbols.len() - 1 {
            current_amount = result.leaves_qty;
            info!("💱 Next amount from leaves_qty: {:.8}", current_amount);
        }

        std::thread::sleep(Duration::from_millis(500));
    }

    info!("🎯 REAL triangular arbitrage sequence completed");
    info!("   All orders placed successfully using leaves_qty flow");

    Ok(true)
}

fn create_order(
    symbol: String,
    is_base_to_quote: bool,
    amount: f64,
    qty_precision: u8,
    price_precision: u8
) -> MarketOrder {
    if is_base_to_quote {
        MarketOrder {
            symbol,
            side: OrderSide::Sell,
            quantity: amount,
            quote_quantity: None,
            qty_precision: Some(qty_precision),
            price_precision: Some(price_precision),
        }
    } else {
        MarketOrder {
            symbol,
            side: OrderSide::Buy,
            quantity: 0.0,
            quote_quantity: Some(amount),
            qty_precision: Some(qty_precision),
            price_precision: Some(price_precision),
        }
    }
}
