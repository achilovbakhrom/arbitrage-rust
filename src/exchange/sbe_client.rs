use tokio_tungstenite::{ connect_async, WebSocketStream };
use futures::{ SinkExt, StreamExt };
use tokio::{ net::TcpStream, sync::RwLock, time::{ interval, Duration } };
use tungstenite::{ client::IntoClientRequest, handshake::client::Request, protocol::Message };
use anyhow::{ Result, Context };
use tracing::{ debug, error, info };
use std::{ sync::Arc, time::{ SystemTime, UNIX_EPOCH } };
use tungstenite::handshake::client::generate_key;

use crate::models::sbe::{
    depth_diff_stream_event_codec::DepthDiffStreamEventDecoder,
    message_header_codec::{ self, MessageHeaderDecoder },
    Decoder,
    ReadBuf,
};

// Use the official endpoint as per Binance documentation
const BINANCE_SBE_URL: &str = "wss://stream-sbe.binance.com:9443/ws";

pub struct BinanceSbeClient {
    api_key: Arc<str>,
    endpoint: String,
    depth_callback: RwLock<
        Option<Box<dyn Fn(&str, &[(f64, f64)], &[(f64, f64)], u64, u64) + Send + Sync>>
    >,
}

impl BinanceSbeClient {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key: api_key.into(),
            endpoint: BINANCE_SBE_URL.into(),
            depth_callback: RwLock::new(None),
        }
    }

    pub async fn connect(
        &self
    ) -> Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>> {
        info!("Connecting to Binance SBE WebSocket: {}", self.endpoint);

        // Create a request with proper headers as per Binance documentation
        let request = Request::builder()
            .uri(self.endpoint.as_str())
            .header("X-MBX-APIKEY", self.api_key.as_ref())
            .header("Sec-WebSocket-Key", generate_key())
            .header("Sec-WebSocket-Version", "13")
            .header("Host", "stream-sbe.binance.com")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .body(())
            .context("Failed to build request")?;

        // Convert to a proper WebSocket request
        let client_request = request
            .into_client_request()
            .context("Failed to convert to WebSocket request")?;

        debug!("Connecting with request: {:?}", &client_request);

        // Connect using the prepared WebSocket request
        let (ws_stream, response) = connect_async(client_request).await.context(
            "Failed to connect to Binance SBE WebSocket"
        )?;

        info!("Connected to Binance SBE WebSocket with response: {:?}", response);

        Ok(ws_stream)
    }

    pub async fn subscribe(
        &self,
        ws_stream: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
        symbols: &[String],
        channels: &[String]
    ) -> Result<()> {
        // Prepare subscription message
        // Format is: <symbol>@<channel>
        let params = symbols
            .iter()
            .flat_map(|symbol| {
                channels
                    .iter()
                    .map(move |channel| { format!("{}@{}", symbol.to_lowercase(), channel) })
            })
            .collect::<Vec<String>>();

        let subscription =
            serde_json::json!({
            "method": "SUBSCRIBE",
            "params": params,
            "id": 1
        }).to_string();

        debug!("Subscription request: {}", &subscription);

        // Send subscription message
        let message = Message::Text(subscription.into());
        ws_stream.send(message).await?;

        // Process subscription response
        if let Some(message_result) = ws_stream.next().await {
            match message_result {
                Ok(message) => {
                    match message {
                        Message::Text(text) => {
                            debug!("Subscription response: {}", text);
                            // Check for successful subscription (result: null indicates success)
                            if !text.contains("\"result\":null") {
                                error!("Failed to subscribe: {}", text);
                                anyhow::bail!("Failed to subscribe: {}", text);
                            }
                            info!("Successfully subscribed to {} streams", params.len());
                        }
                        Message::Binary(_) => {
                            // The subscription response should be a text message
                            error!("Unexpected binary response to subscription");
                            anyhow::bail!("Unexpected binary response to subscription");
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    error!("Error receiving subscription response: {}", e);
                    anyhow::bail!("Failed to receive subscription response: {}", e);
                }
            }
        }

        Ok(())
    }

    // Start a ping-pong keepalive loop in a separate task
    pub fn start_heartbeat(
        ws_stream: Arc<tokio::sync::Mutex<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>>>
    ) {
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(30));

            loop {
                interval.tick().await;

                // Send ping with current timestamp
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs()
                    .to_string();

                let ping_msg = Message::Ping(now.into_bytes().into());

                let mut locked_stream = ws_stream.lock().await;
                if let Err(e) = locked_stream.send(ping_msg).await {
                    error!("Failed to send ping: {}", e);
                    break;
                }

                debug!("Sent heartbeat ping");
            }
        });
    }

    pub async fn process_messages(
        &self,
        ws_stream: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>
    ) -> Result<()> {
        // Create a shared websocket for the heartbeat
        let shared_ws = Arc::new(tokio::sync::Mutex::new(ws_stream.get_ref().clone()));

        // Start heartbeat in the background
        // Self::start_heartbeat(shared_ws);

        while let Some(message_result) = ws_stream.next().await {
            match message_result {
                Ok(message) => {
                    match message {
                        Message::Binary(data) => {
                            // Handle the binary message with proper error handling
                            if let Err(e) = self.handle_binary_message(&data).await {
                                debug!("Error handling binary message: {}", e);
                                // Continue processing messages instead of failing completely
                            }
                        }
                        Message::Text(text) => {
                            debug!("Received text message: {}", text);
                        }
                        Message::Ping(data) => {
                            // Respond to ping with pong as required by the WebSocket protocol
                            debug!("Received ping, responding with pong");
                            if let Err(e) = ws_stream.send(Message::Pong(data.clone())).await {
                                error!("Failed to send pong: {}", e);
                                return Err(anyhow::anyhow!("WebSocket connection error: {}", e));
                            }
                        }
                        Message::Pong(data) => {
                            // Log pong responses
                            debug!("Received pong response: {:?}", data);
                        }
                        Message::Close(frame) => {
                            info!("WebSocket closed: {:?}", frame);
                            return Ok(());
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    error!("WebSocket message error: {}", e);
                    return Err(anyhow::anyhow!("WebSocket error: {}", e));
                }
            }
        }

        info!("WebSocket stream ended");
        Ok(())
    }

    #[inline]
    pub async fn set_depth_callback(
        &self,
        callback: Box<dyn Fn(&str, &[(f64, f64)], &[(f64, f64)], u64, u64) + Send + Sync>
    ) {
        let mut cb = self.depth_callback.write().await;
        *cb = Some(callback);
    }

    #[inline]
    async fn handle_binary_message(&self, data: &[u8]) -> Result<()> {
        // Create a buffer for SBE decoding
        let buf_reader = ReadBuf::new(data);

        // Safety check for minimum header size
        if data.len() < message_header_codec::ENCODED_LENGTH {
            debug!(
                "Received binary message with insufficient length for header: {} bytes",
                data.len()
            );
            return Ok(());
        }

        // Read the SBE message header
        let mut header = MessageHeaderDecoder::default().wrap(buf_reader, 0);
        let block_length = header.block_length();
        let template_id = header.template_id();
        let version = header.version();

        // Validate message size
        let expected_min_size = message_header_codec::ENCODED_LENGTH + (block_length as usize);
        if data.len() < expected_min_size {
            debug!(
                "Message too small: expected at least {} bytes but got {} bytes (template_id: {})",
                expected_min_size,
                data.len(),
                template_id
            );
            return Ok(());
        }

        // Debug output for message details
        debug!(
            "SBE Message: template_id={}, block_length={}, version={}, data_len={}",
            template_id,
            block_length,
            version,
            data.len()
        );

        // Fast path for depth messages (10003)
        if template_id == 10003 {
            // Stack-allocated arrays to avoid heap allocations in hot path
            let mut bids = [(0.0, 0.0); 50]; // Reasonable max size
            let mut asks = [(0.0, 0.0); 50]; // Reasonable max size

            let mut depth_decoder = DepthDiffStreamEventDecoder::default();

            // Properly position the decoder
            let buf_reader = header.parent()?;
            depth_decoder = depth_decoder.wrap(
                buf_reader,
                message_header_codec::ENCODED_LENGTH,
                block_length,
                version
            );

            let event_time = depth_decoder.event_time();
            let first_update_id = depth_decoder.first_book_update_id();
            let last_update_id = depth_decoder.last_book_update_id();
            let price_exponent = depth_decoder.price_exponent();
            let qty_exponent = depth_decoder.qty_exponent();

            // Safety check for reasonable update IDs
            if first_update_id > last_update_id || last_update_id - first_update_id > 10000 {
                debug!(
                    "Suspicious update IDs: first={}, last={}, diff={}",
                    first_update_id,
                    last_update_id,
                    last_update_id - first_update_id
                );
                // Still continue processing as this might be a valid case
            }

            // Process the bids without allocating a Vec
            let mut bids_decoder = depth_decoder.bids_decoder();
            let bids_count = bids_decoder.count();

            // Safety check for reasonable bids count - convert to usize safely
            let bids_count_usize = bids_count as u32 as usize; // First convert to u32, then to usize
            if bids_count_usize > 500 {
                debug!("Unreasonable number of bids ({}), skipping message", bids_count_usize);
                return Ok(());
            }

            let mut actual_bids_count = 0;
            while let Ok(Some(_)) = bids_decoder.advance() {
                if actual_bids_count < bids.len() {
                    let price = bids_decoder.price();
                    let qty = bids_decoder.qty();

                    // Convert using exponents
                    let real_price = (price as f64) * (10f64).powi(price_exponent as i32);
                    let real_qty = (qty as f64) * (10f64).powi(qty_exponent as i32);

                    bids[actual_bids_count] = (real_price, real_qty);
                    actual_bids_count += 1;
                } else {
                    break;
                }
            }

            // Get the parent back to process asks
            let depth_decoder = bids_decoder.parent()?;

            // Process the asks without allocating a Vec
            let mut asks_decoder = depth_decoder.asks_decoder();
            let asks_count = asks_decoder.count();

            // Safety check for reasonable asks count - convert to usize safely
            let asks_count_usize = asks_count as u32 as usize; // First convert to u32, then to usize
            if asks_count_usize > 500 {
                debug!("Unreasonable number of asks ({}), skipping message", asks_count_usize);
                return Ok(());
            }

            let mut actual_asks_count = 0;
            while let Ok(Some(_)) = asks_decoder.advance() {
                if actual_asks_count < asks.len() {
                    let price = asks_decoder.price();
                    let qty = asks_decoder.qty();

                    // Convert using exponents
                    let real_price = (price as f64) * (10f64).powi(price_exponent as i32);
                    let real_qty = (qty as f64) * (10f64).powi(qty_exponent as i32);

                    asks[actual_asks_count] = (real_price, real_qty);
                    actual_asks_count += 1;
                } else {
                    break;
                }
            }

            // Get the parent back to process symbol
            let mut depth_decoder = asks_decoder.parent()?;

            // Get symbol without allocation if possible
            let symbol_coordinates = depth_decoder.symbol_decoder();

            // Verify symbol coordinates are within buffer bounds
            if depth_decoder.get_limit() < symbol_coordinates.0 + symbol_coordinates.1 {
                debug!(
                    "Symbol coordinates out of bounds: offset={}, length={}, limit={}",
                    symbol_coordinates.0,
                    symbol_coordinates.1,
                    depth_decoder.get_limit()
                );
                return Ok(());
            }

            let symbol_bytes = depth_decoder.symbol_slice(symbol_coordinates);

            // Extract the symbol with minimal allocations
            let symbol = if let Ok(s) = std::str::from_utf8(symbol_bytes) {
                // Zero-copy if valid UTF-8
                s.trim_end_matches('\0')
            } else {
                // Fallback with allocation only if necessary
                debug!("Invalid UTF-8 in symbol");
                return Ok(());
            };

            // Call the callback if set, using only slices of our arrays
            let cb_guard = self.depth_callback.read().await;
            if let Some(cb) = cb_guard.as_ref() {
                cb(
                    symbol,
                    &bids[0..actual_bids_count],
                    &asks[0..actual_asks_count],
                    first_update_id as u64,
                    last_update_id as u64
                );
            }

            return Ok(());
        } else {
            // Log other SBE message types
            debug!("Received unsupported SBE message type: {}", template_id);
        }

        Ok(())
    }
}

// Helper functions to convert numeric values with exponents
fn convert_price(value: i64, exponent: i8) -> f64 {
    (value as f64) * (10f64).powi(exponent as i32)
}

fn convert_quantity(value: i64, exponent: i8) -> f64 {
    (value as f64) * (10f64).powi(exponent as i32)
}
