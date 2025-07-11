use std::sync::Arc;
use std::time::Duration;
use std::sync::atomic::{ AtomicBool, Ordering };
use crate::{
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
    info!("🧪 Starting enhanced test execution mode - Single Order Test");

    if !config.should_enable_real_trading() && !config.should_enable_mock_trading() {
        error!("❌ Trading must be enabled for test execution");
        error!("   Set TAA_ENABLE_TRADING=true in your .env file");
        return Err(anyhow!("Trading not enabled in configuration"));
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

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

    let test_order = create_simple_test_order(config, symbol_map)?;
    info!(
        "📋 Created test order: {} {} {}",
        match test_order.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        },
        test_order.symbol,
        if test_order.quote_quantity.is_some() {
            format!("quote_qty: {:.8}", test_order.quote_quantity.unwrap())
        } else {
            format!("qty: {:.8}", test_order.quantity)
        }
    );

    if shutdown.load(Ordering::Relaxed) {
        info!("🛑 Test cancelled by user");
        mock_client.disconnect()?;
        return Ok(());
    }

    info!("🚀 Placing test order...");
    match mock_client.place_market_order(test_order) {
        Ok(result) => {
            info!("✅ Test order executed successfully!");
            info!("   Order ID: {}", result.order_id);
            info!("   Filled Quantity: {:.8}", result.filled_qty);
            info!("   Average Price: {:.8}", result.avg_price);
            info!("   Total Value: {:.8}", result.cum_quote_qty);
            info!("   Execution Time: {:.2}ms", result.execution_time.as_secs_f64() * 1000.0);

            info!("🎉 Mock test completed successfully!");
            info!("   Your configuration is working correctly");
        }
        Err(e) => {
            error!("❌ Test order failed: {}", e);
            mock_client.disconnect()?;
            return Err(anyhow!("Test order execution failed: {}", e));
        }
    }

    info!("🔌 Disconnecting FIX client...");
    std::thread::sleep(Duration::from_millis(200));
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

    let test_order = create_simple_test_order(config, symbol_map)?;
    info!(
        "📋 Created test order: {} {} {}",
        match test_order.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        },
        test_order.symbol,
        if test_order.quote_quantity.is_some() {
            format!("quote_qty: {:.8}", test_order.quote_quantity.unwrap())
        } else {
            format!("qty: {:.8}", test_order.quantity)
        }
    );

    if shutdown.load(Ordering::Relaxed) {
        info!("🛑 Test cancelled by user");
        fix_client.disconnect()?;
        return Ok(());
    }

    info!("🚀 Placing test order...");
    match fix_client.place_market_order(test_order) {
        Ok(result) => {
            info!("✅ Test order placed successfully!");
            info!("   Order ID: {}", result.order_id);
            info!("   Symbol: {}", result.symbol);
            info!("   Side: {}", result.side);
            info!("   Order Status: {}", result.order_status);
            info!("   Placement Time: {:.2}ms", result.execution_time.as_secs_f64() * 1000.0);

            info!("🎉 Real test completed successfully!");
            info!("   Your FIX API configuration is working correctly");
            info!("   Market order was accepted by the exchange");
            info!("   You can now run the full arbitrage system");
        }
        Err(e) => {
            error!("❌ Test order failed: {}", e);
            info!("🔌 Disconnecting FIX client...");
            fix_client.disconnect()?;
            return Err(anyhow!("Test order execution failed: {}", e));
        }
    }

    info!("🔌 Disconnecting FIX client...");
    std::thread::sleep(Duration::from_millis(200));
    fix_client.disconnect()?;

    info!("✅ Real FIX client disconnected successfully");

    Ok(())
}

fn create_simple_test_order(
    config: &Config,
    symbol_map: &Arc<crate::models::symbol_map::SymbolMap>
) -> Result<MarketOrder> {
    let test_symbol = "BTCUSDT";

    let symbol_info = symbol_map
        .get(test_symbol)
        .ok_or_else(|| anyhow!("Test symbol {} not found", test_symbol))?;

    let test_amount = (config.trade_amount * 0.01).max(10.0); // 1% of normal amount, minimum $10

    let order = MarketOrder {
        symbol: test_symbol.to_string(),
        side: OrderSide::Buy, // Buy a small amount of BTC with USDT
        quantity: 0.0, // Will be calculated from quote_quantity
        quote_quantity: Some(test_amount), // Buy $10-$100 worth
        qty_precision: Some(symbol_info.qty_precision),
        price_precision: Some(symbol_info.price_precision),
    };

    info!("📝 Test order details:");
    info!("   Symbol: {}", order.symbol);
    info!("   Side: BUY");
    info!("   Quote Amount: ${:.2}", test_amount);
    info!("   Quantity Precision: {}", symbol_info.qty_precision);
    info!("   Price Precision: {}", symbol_info.price_precision);

    Ok(order)
}
