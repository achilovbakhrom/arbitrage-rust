use std::time::{ Duration, Instant };
use anyhow::Result;
use tracing::info;
use std::collections::HashMap;
use super::fix_api::{ ExecutionResult, MarketOrder, OrderSide };

pub struct MockFixClient {
    is_connected: bool,
    simulated_latency: Duration,
    success_rate: f64,
    symbol_precisions: HashMap<String, (u8, u8)>,
}

impl MockFixClient {
    pub fn new() -> Self {
        Self {
            is_connected: false,
            simulated_latency: Duration::from_millis(50),
            success_rate: 0.98,
            symbol_precisions: HashMap::new(),
        }
    }

    pub fn connect(&mut self) -> Result<()> {
        info!("Mock FIX client: Simulating connection...");
        std::thread::sleep(Duration::from_millis(200));
        self.is_connected = true;
        info!("Mock FIX client: Connected successfully");
        Ok(())
    }

    pub fn disconnect(&mut self) -> Result<()> {
        info!("Mock FIX client: Disconnecting...");
        std::thread::sleep(Duration::from_millis(50));
        self.is_connected = false;
        info!("Mock FIX client: Disconnected successfully");
        Ok(())
    }

    fn format_quantity(&self, quantity: f64, precision: Option<u8>) -> String {
        match precision {
            Some(p) => format!("{:.prec$}", quantity, prec = p as usize),
            None => format!("{:.8}", quantity),
        }
    }

    fn apply_precision(value: f64, precision: u8) -> f64 {
        let factor = (10f64).powi(precision as i32);
        (value * factor).floor() / factor
    }

    pub fn place_market_order(&mut self, order: MarketOrder) -> Result<ExecutionResult> {
        if !self.is_connected {
            return Err(anyhow::anyhow!("Mock FIX client not connected"));
        }

        let start_time = Instant::now();
        info!("Mock FIX: Processing order for {} {}", order.symbol, match order.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        });

        std::thread::sleep(self.simulated_latency);

        let success = rand::random::<f64>() < self.success_rate;
        if !success {
            return Err(anyhow::anyhow!("Mock order execution failed (simulated failure)"));
        }

        let client_order_id = format!(
            "MOCK_{}",
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );

        let precision = order.qty_precision
            .or_else(|| self.symbol_precisions.get(&order.symbol).map(|(q, _)| *q))
            .unwrap_or(8);

        let price_precision = order.price_precision
            .or_else(|| self.symbol_precisions.get(&order.symbol).map(|(_, p)| *p))
            .unwrap_or(8);

        let simulated_price = match order.symbol.as_str() {
            "BTCUSDT" => 50000.0,
            "ETHUSDT" => 3000.0,
            "ADAUSDT" => 0.5,
            _ => 1.0,
        };

        let slippage_factor = match order.side {
            OrderSide::Buy => 1.0 + (rand::random::<f64>() * 0.0004 + 0.0001),
            OrderSide::Sell => 1.0 - (rand::random::<f64>() * 0.0004 + 0.0001),
        };

        let final_price = Self::apply_precision(simulated_price * slippage_factor, price_precision);

        let (filled_qty, cum_quote_qty) = if let Some(quote_qty) = order.quote_quantity {
            let precise_quote = Self::apply_precision(quote_qty, price_precision);
            let qty = precise_quote / final_price;
            let precise_qty = Self::apply_precision(qty, precision);
            (precise_qty, precise_quote)
        } else {
            let precise_qty = Self::apply_precision(order.quantity, precision);
            let quote_value = precise_qty * final_price;
            (precise_qty, quote_value)
        };

        info!(
            "Mock FIX: Order executed - {} {} {} @ {:.8} (Total: {:.8})",
            match order.side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            },
            order.symbol,
            self.format_quantity(filled_qty, order.qty_precision),
            final_price,
            cum_quote_qty
        );

        Ok(ExecutionResult {
            client_order_id,
            order_id: format!("MOCK_ORDER_{}", rand::random::<u64>()),
            symbol: order.symbol,
            side: match order.side {
                OrderSide::Buy => "BUY".to_string(),
                OrderSide::Sell => "SELL".to_string(),
            },
            filled_qty,
            avg_price: final_price,
            cum_quote_qty,
            leaves_qty: 0.0,
            order_status: "FILLED".to_string(),
            exec_type: "FILL".to_string(),
            last_px: final_price,
            last_qty: filled_qty,
            execution_time: start_time.elapsed(),
            reject_reason: None,
        })
    }
}
