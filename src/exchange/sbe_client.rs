use tokio_tungstenite::{ connect_async, WebSocketStream };
use futures::{ SinkExt, StreamExt };
use tokio::{ net::TcpStream, sync::RwLock };
use tungstenite::{
    client::IntoClientRequest,
    handshake::client::Request,
    protocol::Message,
    Utf8Bytes,
};
use anyhow::{ Result, Context };
use tracing::{ debug, error, info };
use std::{ ops::Deref, sync::Arc, time::{ Instant, SystemTime, UNIX_EPOCH } };
use tungstenite::handshake::client::generate_key;

use crate::models::sbe::{
    depth_diff_stream_event_codec::DepthDiffStreamEventDecoder,
    message_header_codec::{ self, MessageHeaderDecoder },
    ReadBuf,
    Reader,
};

const BINANCE_SBE_URL: &str = "wss://stream-sbe.binance.com:9443/ws";

// const BINANCE_SBE_URL: &str = "wss://stream.binance.com:9443";

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

    // pub async fn connect(
    //     &self
    // ) -> Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>> {
    //     info!("Connecting to Binance SBE WebSocket: {}", self.endpoint);

    //     // Create a request and convert it to a client request
    //     let request = Request::builder()
    //         .uri(self.endpoint.as_str())
    //         .header("X-MBX-APIKEY", self.api_key.as_ref())
    //         .body(())
    //         .context("Failed to build request")?;

    //     // Convert to a proper WebSocket request (this handles the WebSocket headers)
    //     let client_request = request
    //         .into_client_request()
    //         .context("Failed to convert to WebSocket request")?;

    //     // Connect using the prepared WebSocket request
    //     let (ws_stream, _) = connect_async(client_request).await.context(
    //         "Failed to connect to Binance SBE WebSocket"
    //     )?;

    //     info!("Connected to Binance SBE WebSocket");

    //     Ok(ws_stream)
    // }

    pub async fn connect(
        &self
    ) -> Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>> {
        info!("Connecting to Binance SBE WebSocket: {}", self.endpoint);

        // Create a request and convert it to a client request
        let request = Request::builder()
            .uri(self.endpoint.as_str())
            .header("X-MBX-APIKEY", self.api_key.as_ref())
            .header("Sec-WebSocket-Key", generate_key())
            .header("Sec-WebSocket-Version", "13")
            .header("Host", "binance.com")
            .header("Connection", "keep-alive, Upgrade")
            .header("Upgrade", "websocket")
            .body(())
            .context("Failed to build request")?;

        // Convert to a proper WebSocket request (this handles the WebSocket headers)
        let client_request = request
            .into_client_request()
            .context("Failed to convert to WebSocket request")?;

        println!("clientReq: {:?}", &client_request);

        // Connect using the prepared WebSocket request
        let (ws_stream, _) = connect_async(client_request).await.context(
            "Failed to connect to Binance SBE WebSocket"
        )?;

        info!("Connected to Binance SBE WebSocket");

        Ok(ws_stream)
    }

    pub async fn subscribe(
        &self,
        ws_stream: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
        symbols: &[String],
        channels: &[String]
    ) -> Result<()> {
        // Prepare subscription message
        let subscription =
            serde_json::json!({
            "method": "SUBSCRIBE",
            "params": symbols.iter()
            .take(1)
                .flat_map(|symbol| {
                    channels.iter().map(move |channel| {
                        format!("{}@{}", symbol.to_lowercase(), channel)
                    })
                })
                .collect::<Vec<String>>(),
            "id": 1
        }).to_string();

        println!("subscription {}", &subscription);

        let utf8_bytes = Utf8Bytes::from(subscription);

        // Send subscription message
        let message = Message::Text(utf8_bytes);
        ws_stream.send(message).await?;

        // Process subscription response
        if let Some(Ok(message)) = ws_stream.next().await {
            match message {
                Message::Text(text) => {
                    debug!("Subscription response: {}", text);
                    if !text.contains("\"result\":null") {
                        error!("Failed to subscribe: {}", text);
                        anyhow::bail!("Failed to subscribe: {}", text);
                    }
                    info!("Successfully subscribed to {} symbols", symbols.len());
                }
                Message::Binary(_) => {
                    // The subscription response should be a text message
                    error!("Unexpected binary response to subscription");
                    anyhow::bail!("Unexpected binary response to subscription");
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub async fn process_messages(
        &self,
        ws_stream: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>
    ) -> Result<()> {
        while let Some(Ok(message)) = ws_stream.next().await {
            // let start = SystemTime::now();
            // let since_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");

            // Convert to microseconds
            // let micros = since_epoch.as_millis();

            match message {
                Message::Binary(data) => {
                    self.handle_binary_message(&data)?;
                }
                Message::Text(text) => {
                    debug!("Received text message: {}", text);
                }
                Message::Ping(data) => {
                    ws_stream.send(Message::Pong(data)).await?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    #[inline]
    pub async fn set_depth_callback(
        &self,
        callback: Box<dyn Fn(&str, &[(f64, f64)], &[(f64, f64)], u64, u64) + Send + Sync>
    ) {
        let mut cb = self.depth_callback.write().await.unwrap();
        cb = callback;
    }

    #[inline]
    fn handle_binary_message(&self, data: &[u8]) -> Result<()> {
        // Create a buffer for SBE decoding
        let buf_reader = ReadBuf::new(data);

        // Read the SBE message header
        let mut header = crate::models::sbe::message_header_codec::MessageHeaderDecoder
            ::default()
            .wrap(buf_reader, 0);
        let block_length = header.block_length();
        let version = header.version();

        // Fast path for depth messages (10003)
        if header.template_id() == 10003 {
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

            // Process the bids without allocating a Vec
            let mut bids_decoder = depth_decoder.bids_decoder();
            let bids_count = bids_decoder.count() as usize;
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
            let asks_count = asks_decoder.count() as usize;
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
            let symbol_bytes = depth_decoder.symbol_slice(symbol_coordinates);

            // Extract the symbol with minimal allocations
            let symbol = if let Ok(s) = std::str::from_utf8(symbol_bytes) {
                // Zero-copy if valid UTF-8
                s.trim_end_matches('\0')
            } else {
                // Fallback with allocation only if necessary
                return Ok(()); // Skip invalid symbols
            };

            // Call the callback if set, using only slices of our arrays
            if let Some(cb) = self.depth_callback.read().await.unwrap().deref() {
                cb(
                    symbol,
                    &bids[0..actual_bids_count],
                    &asks[0..actual_asks_count],
                    first_update_id as u64,
                    last_update_id as u64
                );
            }

            return Ok(());
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
