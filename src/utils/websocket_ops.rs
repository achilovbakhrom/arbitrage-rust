use std::sync::{ Arc, atomic::{ AtomicBool, Ordering } };
use std::thread::{ self, JoinHandle };
use std::time::Duration;
use anyhow::Result;
use tracing::{ info, warn, error };
use crate::exchange::sbe_client::BinanceSbeClient;
use crate::orderbook_manager::manager::OrderBookManager;
use crate::WEBSOCKET_BUFFER_SIZE;

pub struct WebSocketConfig {
    pub api_key: String,
    pub symbols: Vec<String>,
    pub buffer_size: Option<usize>,
    pub enable_timing: bool,
}

impl WebSocketConfig {
    pub fn new(api_key: String, symbols: Vec<String>) -> Self {
        Self {
            api_key,
            symbols,
            buffer_size: Some(WEBSOCKET_BUFFER_SIZE),
            enable_timing: false,
        }
    }
}

pub fn start_websocket_thread<F>(
    config: WebSocketConfig,
    orderbook_manager: Arc<OrderBookManager>,
    shutdown: Arc<AtomicBool>,
    depth_callback: F
) -> Result<JoinHandle<()>>
    where F: Fn(u16, &[(f64, f64); 5], &[(f64, f64); 5], u64, u64, u64) + Send + Sync + 'static
{
    let symbol_mappings = orderbook_manager.get_symbol_mappings();

    let handle = thread::Builder
        ::new()
        .name("websocket-thread".to_string())
        .spawn(move || {
            let client = BinanceSbeClient::new(config.api_key);
            client.preload_symbol_mappings(&symbol_mappings);

            if config.enable_timing {
                client.enable_timing();
            } else {
                client.disable_timing();
            }

            let mut ws_stream = loop {
                if shutdown.load(Ordering::Relaxed) {
                    return;
                }

                match client.connect() {
                    Ok(stream) => {
                        break stream;
                    }
                    Err(err) => {
                        warn!("Connection failed: {}, retrying in 5 seconds...", err);
                        thread::sleep(Duration::from_secs(5));
                    }
                }
            };

            info!("Connected to Binance SBE WebSocket");

            let channels = ["depth".to_string()];
            let subscription_symbols = config.symbols
                .iter()
                .take(config.buffer_size.unwrap_or(WEBSOCKET_BUFFER_SIZE))
                .map(|symbol| symbol.clone())
                .collect::<Vec<String>>();

            match
                client.subscribe(&mut ws_stream, &subscription_symbols, channels.iter().as_slice())
            {
                Ok(_) => info!("Successfully subscribed to {} symbols", subscription_symbols.len()),
                Err(e) => {
                    error!("Failed to subscribe: {}", e);
                    return;
                }
            }

            client.set_depth_callback(depth_callback);

            if let Err(e) = client.process_messages(&mut ws_stream, &shutdown) {
                error!("WebSocket processing error: {}", e);
            }
        })?;

    Ok(handle)
}

pub fn create_standard_depth_callback(
    orderbook_manager: Arc<OrderBookManager>
) -> impl Fn(u16, &[(f64, f64); 5], &[(f64, f64); 5], u64, u64, u64) + Send + Sync {
    move |
        symbol_id: u16,
        bids: &[(f64, f64); 5],
        asks: &[(f64, f64); 5],
        first_update_id: u64,
        last_update_id: u64,
        _: u64
    | {
        orderbook_manager.apply_depth_update(
            symbol_id,
            bids,
            asks,
            first_update_id,
            last_update_id
        );
    }
}
