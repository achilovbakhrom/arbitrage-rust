// src/exchange/sbe_client.rs
use anyhow::{ Context, Result };
use bytes::BytesMut;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::protocol::Message,
    MaybeTlsStream,
    WebSocketStream,
};
use futures_util::{ SinkExt, StreamExt };
use std::collections::HashMap;
use std::time::{ Duration, Instant };
use tracing::{ debug, error, info, warn };

// use crate::models::{ orderbook::OrderBook, OrderBookManager, OrderBookSide };
use crate::models::orderbook::OrderBook;
use crate::sbe::{ self, SbeGoMarshaller, SbeGoMessageHeader };

// Template IDs for different message types
const TRADE_TEMPLATE_ID: i32 = 10000;
const BEST_BID_ASK_TEMPLATE_ID: i32 = 10001;
const DEPTH_TEMPLATE_ID: i32 = 10003;

// Binance WebSocket URL for SBE
const BINANCE_SBE_URL: &str = "wss://stream-sbe.binance.com:9443/ws";

/// SBE client for receiving binary-encoded market data
pub struct SbeClient {
    /// API key for authentication
    api_key: Arc<str>,

    /// SBE marshaller for decoding messages
    marshaller: SbeGoMarshaller,

    /// Orderbook manager for storing and accessing orderbooks
    orderbook_manager: Arc<OrderBookManager>,

    /// WebSocket connection
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,

    /// Last heartbeat time
    last_heartbeat: Instant,

    /// Connected symbols
    connected_symbols: HashMap<String, bool>,

    /// ID counter for WebSocket subscriptions
    id_counter: usize,
}

/// Subscription message for WebSocket API
#[derive(serde::Serialize)]
struct SubscriptionRequest {
    method: String,
    params: Vec<String>,
    id: usize,
}

impl SbeClient {
    /// Create a new SBE client
    pub fn new(api_key: String, orderbook_manager: Arc<OrderBookManager>) -> Self {
        Self {
            api_key: api_key.into(),
            marshaller: SbeGoMarshaller::default(),
            orderbook_manager,
            ws_stream: None,
            last_heartbeat: Instant::now(),
            connected_symbols: HashMap::new(),
            id_counter: 0,
        }
    }

    /// Get next ID for subscription requests
    fn next_id(&mut self) -> usize {
        self.id_counter += 1;
        self.id_counter
    }

    /// Connect to Binance WebSocket and start processing messages
    pub async fn connect_and_subscribe(&mut self, symbols: &[Arc<str>]) -> Result<()> {
        // Set up HTTP headers for connection
        let mut request = http::Request
            ::builder()
            .uri(BINANCE_SBE_URL)
            .header("User-Agent", "Rust Triangular Arbitrage")
            .header("X-MBX-APIKEY", self.api_key.as_ref())
            .body(())?;

        // Connect to WebSocket
        info!("Connecting to Binance SBE WebSocket");
        let (ws_stream, _) = connect_async(request).await.context(
            "Failed to connect to Binance SBE WebSocket"
        )?;

        info!("Connected to Binance SBE WebSocket");
        self.ws_stream = Some(ws_stream);

        // Subscribe to depth updates for all symbols
        self.subscribe_depth(symbols).await?;

        // Start message processing
        self.process_messages().await?;

        Ok(())
    }

    /// Subscribe to depth updates for symbols
    async fn subscribe_depth(&mut self, symbols: &[Arc<str>]) -> Result<()> {
        if self.ws_stream.is_none() {
            anyhow::bail!("Not connected to WebSocket");
        }

        // Create stream names for depth updates
        let mut streams = Vec::with_capacity(symbols.len());
        for symbol in symbols {
            let stream_name = format!("{}@depth", symbol.to_lowercase());
            streams.push(stream_name);
            self.connected_symbols.insert(symbol.to_string(), true);
        }

        // Create subscription message
        let subscription = SubscriptionRequest {
            method: "SUBSCRIBE".to_string(),
            params: streams,
            id: self.next_id(),
        };

        // Send subscription request
        let payload = serde_json::to_string(&subscription)?;
        let stream = self.ws_stream.as_mut().unwrap();
        stream.send(Message::Text(payload)).await?;

        info!("Subscribed to depth updates for {} symbols", symbols.len());

        Ok(())
    }

    /// Process incoming WebSocket messages
    pub async fn process_messages(&mut self) -> Result<()> {
        if self.ws_stream.is_none() {
            anyhow::bail!("Not connected to WebSocket");
        }

        let mut stream = self.ws_stream.take().unwrap();

        // Process messages in a loop
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    // Process binary message (SBE encoded)
                    self.process_binary_message(&data).await?;
                }
                Ok(Message::Ping(data)) => {
                    // Respond to ping with pong
                    stream.send(Message::Pong(data)).await?;
                    self.last_heartbeat = Instant::now();
                }
                Ok(Message::Pong(_)) => {
                    // Update heartbeat time
                    self.last_heartbeat = Instant::now();
                }
                Ok(Message::Text(text)) => {
                    // Process text message (likely a subscription response)
                    debug!("Received text message: {}", text);
                }
                Ok(Message::Close(frame)) => {
                    info!("Received close frame: {:?}", frame);
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
            }
        }

        self.ws_stream = Some(stream);
        Ok(())
    }

    /// Process binary message containing SBE-encoded data
    async fn process_binary_message(&self, data: &[u8]) -> Result<()> {
        // Create a buffer from the data
        let mut buf = BytesMut::from(data);

        // Decode the message header
        let mut header = SbeGoMessageHeader::default();
        header.decode(&self.marshaller, &mut buf)?;

        // Process based on template ID
        match header.template_id {
            DEPTH_TEMPLATE_ID => {
                self.process_depth_update(buf, header).await?;
            }
            BEST_BID_ASK_TEMPLATE_ID => {
                self.process_best_bid_ask(buf, header).await?;
            }
            TRADE_TEMPLATE_ID => {
                // Process trade events if needed
            }
            _ => {
                debug!("Unknown template ID: {}", header.template_id);
            }
        }

        Ok(())
    }

    /// Process depth update message
    async fn process_depth_update(
        &self,
        mut buf: BytesMut,
        header: SbeGoMessageHeader
    ) -> Result<()> {
        // Decode depth update message
        let mut depth_event = sbe::DepthDiffStreamEvent::default();
        depth_event.decode(&self.marshaller, &mut buf, header.version, header.block_length, true)?;

        // Extract symbol
        let symbol = String::from_utf8(depth_event.symbol.to_vec())?;

        // Process bids
        let mut bids = Vec::with_capacity(depth_event.bids.len());
        for bid in &depth_event.bids {
            // Convert price and quantity using exponents
            let price = convert_decimal(bid.price, depth_event.price_exponent);
            let quantity = convert_decimal(bid.qty, depth_event.qty_exponent);

            bids.push(crate::models::Level {
                price,
                quantity,
            });
        }

        // Process asks
        let mut asks = Vec::with_capacity(depth_event.asks.len());
        for ask in &depth_event.asks {
            // Convert price and quantity using exponents
            let price = convert_decimal(ask.price, depth_event.price_exponent);
            let quantity = convert_decimal(ask.qty, depth_event.qty_exponent);

            asks.push(crate::models::Level {
                price,
                quantity,
            });
        }

        // Create orderbook update
        let orderbook = OrderBook {
            event_type: "depth".to_string(),
            event_time: depth_event.event_time,
            symbol: symbol.clone(),
            first_update_id: depth_event.first_book_update_id as u64,
            last_update_id: depth_event.last_book_update_id as u64,
            bids,
            asks,
        };

        // Update the orderbook
        let symbol_arc: Arc<str> = symbol.into();
        self.orderbook_manager.apply_depth_update(symbol_arc, orderbook).await;

        Ok(())
    }

    /// Process best bid/ask message (bookTicker)
    async fn process_best_bid_ask(
        &self,
        mut buf: BytesMut,
        header: SbeGoMessageHeader
    ) -> Result<()> {
        // Decode best bid/ask message
        let mut ticker = sbe::BestBidAskStreamEvent::default();
        ticker.decode(&self.marshaller, &mut buf, header.version, header.block_length, true)?;

        // Extract symbol
        let symbol = String::from_utf8(ticker.symbol.to_vec())?;

        // Get or create orderbook
        let symbol_arc: Arc<str> = symbol.clone().into();
        let orderbook = self.orderbook_manager.get_or_create_orderbook(symbol_arc.clone()).await;

        // Update best bid and ask
        let mut ob = orderbook.write().await;

        // Convert price and quantity using exponents
        let bid_price = convert_decimal(ticker.bid_price, ticker.price_exponent);
        let bid_qty = convert_decimal(ticker.bid_qty, ticker.qty_exponent);
        let ask_price = convert_decimal(ticker.ask_price, ticker.price_exponent);
        let ask_qty = convert_decimal(ticker.ask_qty, ticker.qty_exponent);

        // Update orderbook
        ob.update_price_level(OrderBookSide::Bids, bid_price, bid_qty);
        ob.update_price_level(OrderBookSide::Asks, ask_price, ask_qty);
        ob.last_update_id = ticker.book_update_id as u64;

        Ok(())
    }

    /// Send ping to keep connection alive
    pub async fn send_ping(&mut self) -> Result<()> {
        if self.ws_stream.is_none() {
            anyhow::bail!("Not connected to WebSocket");
        }

        let stream = self.ws_stream.as_mut().unwrap();
        stream.send(Message::Ping(vec![])).await?;

        Ok(())
    }

    /// Check if connection is healthy
    pub fn is_healthy(&self) -> bool {
        if self.ws_stream.is_none() {
            return false;
        }

        // Check if we've received a heartbeat recently
        self.last_heartbeat.elapsed() < Duration::from_secs(60)
    }
}

/// Convert a decimal value using its exponent
fn convert_decimal(value: i64, exponent: i8) -> f64 {
    (value as f64) * (10f64).powi(exponent as i32)
}
