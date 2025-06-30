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
            simulated_latency: Duration::from_micros(250),
            success_rate: 0.95,
            symbol_precisions: HashMap::new(),
        }
    }

    pub fn connect(&mut self) -> Result<()> {
        info!("Mock FIX client: Simulating connection...");
        std::thread::sleep(Duration::from_millis(100));
        self.is_connected = true;
        info!("Mock FIX client: Connected successfully");
        Ok(())
    }

    pub fn disconnect(&mut self) -> Result<()> {
        info!("Mock FIX client: Disconnecting...");
        self.is_connected = false;
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
        std::thread::sleep(self.simulated_latency);

        let success = rand::random::<f64>() < self.success_rate;
        if !success {
            return Err(anyhow::anyhow!("Mock order execution failed"));
        }

        let client_order_id = format!(
            "MOCK_{}",
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );

        let precision = order.qty_precision
            .or_else(|| self.symbol_precisions.get(&order.symbol).map(|(q, _)| *q))
            .unwrap_or(8);

        let precise_quantity = if let Some(quote_qty) = order.quote_quantity {
            Self::apply_precision(quote_qty, precision)
        } else {
            Self::apply_precision(order.quantity, precision)
        };

        let slippage_factor = 1.0 + (rand::random::<f64>() * 0.0004 + 0.0001);
        let simulated_price = match order.side {
            OrderSide::Buy => 50000.0 * slippage_factor,
            OrderSide::Sell => 50000.0 / slippage_factor,
        };

        let filled_qty = if order.quote_quantity.is_some() {
            precise_quantity / simulated_price
        } else {
            precise_quantity
        };

        let avg_price = simulated_price;
        let cum_quote_qty = filled_qty * avg_price;

        info!(
            "Mock FIX: Order executed - {} {} {} @ {:.8} (precision: {})",
            match order.side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            },
            order.symbol,
            self.format_quantity(filled_qty, order.qty_precision),
            avg_price,
            precision
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
            avg_price,
            cum_quote_qty,
            leaves_qty: 0.0,
            order_status: "FILLED".to_string(),
            exec_type: "TRADE".to_string(),
            last_px: avg_price,
            last_qty: filled_qty,
            execution_time: start_time.elapsed(),
        })
    }
}
