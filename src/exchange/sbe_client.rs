use http::header;
use tokio_tungstenite::{ connect_async, WebSocketStream };
use futures::{ SinkExt, StreamExt };
use tokio::net::TcpStream;
use tungstenite::{
    client::IntoClientRequest,
    handshake::client::Request,
    protocol::Message,
    Utf8Bytes,
};
use anyhow::{ Result, Context };
use tracing::{ debug, error, info };
use std::{ sync::Arc, time::{ Instant, SystemTime, UNIX_EPOCH } };
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
}

impl BinanceSbeClient {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key: api_key.into(),
            endpoint: BINANCE_SBE_URL.into(),
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

    fn handle_binary_message(&self, data: &[u8]) -> Result<()> {
        // Create a buffer for SBE decodingc

        let buf_reader = ReadBuf::new(data);

        // // Read the SBE message header
        let mut header = crate::models::sbe::message_header_codec::MessageHeaderDecoder
            ::default()
            .wrap(buf_reader, 0);
        let block_length = header.block_length();
        let version = header.version();
        // header.wrap(buf_reader, 0);

        // let h = crate::models::sbe::message_header_codec::MessageHeaderDecoder::default();

        // header.decode(&self.marshaller, buf.clone())?;

        // // Process different message types based on template ID
        match header.template_id() {
            10000 => {
                // Trade message
                let trade_msg =
                    crate::models::sbe::trades_stream_event_codec::TradesStreamEventDecoder::default();
                let trade = trade_msg.wrap(buf_reader, 0, header.version(), header.block_length());
                info!("Trade: {:?}", trade);
                // Process trade message...
            }
            10001 => {
                // Best bid/ask message
                let bba_msg =
                    crate::models::sbe::best_bid_ask_stream_event_codec::BestBidAskStreamEventDecoder::default();
                let bb = bba_msg.wrap(buf_reader, 0, header.version(), header.block_length());
                info!("Best Bid: {:?}", bb);
                // Process best bid/ask message...
            }
            10003 => {
                let mut depth_decoder = DepthDiffStreamEventDecoder::default();

                // Important: Use the header function which extracts information from the header
                // and properly positions the decoder
                let buf_reader = header.parent()?;
                depth_decoder = depth_decoder.wrap(
                    buf_reader,
                    message_header_codec::ENCODED_LENGTH, // Start after the header
                    block_length,
                    version
                );

                let event_time = depth_decoder.event_time();
                let first_update_id = depth_decoder.first_book_update_id();
                let last_update_id = depth_decoder.last_book_update_id();
                let price_exponent = depth_decoder.price_exponent();
                let qty_exponent = depth_decoder.qty_exponent();

                debug!(
                    "Depth Message - Event Time: {}, First Update ID: {}, Last Update ID: {}, Price Exponent: {}, Qty Exponent: {}",
                    event_time,
                    first_update_id,
                    last_update_id,
                    price_exponent,
                    qty_exponent
                );

                // Process the bids
                let mut bids_decoder = depth_decoder.bids_decoder();
                let bids_count = bids_decoder.count();

                debug!("Bids count: {}", bids_count);

                // Collect bid prices and quantities
                let mut bids = Vec::with_capacity(bids_count as usize);

                while let Ok(Some(_)) = bids_decoder.advance() {
                    let price = bids_decoder.price();
                    let qty = bids_decoder.qty();

                    // Convert using exponents
                    let real_price = (price as f64) * (10f64).powi(price_exponent as i32);
                    let real_qty = (qty as f64) * (10f64).powi(qty_exponent as i32);

                    bids.push((real_price, real_qty));

                    if bids.len() <= 5 {
                        // Log only first 5 bids to avoid excessive logging
                        println!("Bid: {} @ {}", real_qty, real_price);
                    }
                }

                // Important: Get the parent back to process asks
                let depth_decoder = bids_decoder.parent()?;

                // Process the asks
                let mut asks_decoder = depth_decoder.asks_decoder();
                let asks_count = asks_decoder.count();

                debug!("Asks count: {}", asks_count);

                // Collect ask prices and quantities
                let mut asks = Vec::with_capacity(asks_count as usize);

                while let Ok(Some(_)) = asks_decoder.advance() {
                    let price = asks_decoder.price();
                    let qty = asks_decoder.qty();

                    // Convert using exponents
                    let real_price = (price as f64) * (10f64).powi(price_exponent as i32);
                    let real_qty = (qty as f64) * (10f64).powi(qty_exponent as i32);

                    asks.push((real_price, real_qty));

                    if asks.len() <= 5 {
                        // Log only first 5 asks
                        println!("Ask: {} @ {}", real_qty, real_price);
                    }
                }

                // Important: Get the parent back to process symbol
                let mut depth_decoder = asks_decoder.parent()?;

                let symbol_coordinates = depth_decoder.symbol_decoder();
                let symbol_bytes = depth_decoder.symbol_slice(symbol_coordinates);

                debug!("Symbol bytes (len={}): {:?}", symbol_bytes.len(), symbol_bytes);

                // Try multiple approaches to extract the symbol

                // 1. Direct UTF-8 conversion (might fail but we'll handle it)
                let symbol = match std::str::from_utf8(symbol_bytes) {
                    Ok(s) => s.trim_end_matches('\0').to_string(),
                    Err(_) => {
                        // 2. Fallback to lossy conversion if UTF-8 fails
                        String::from_utf8_lossy(symbol_bytes).trim_end_matches('\0').to_string()
                    }
                };

                // 3. If still problematic, extract only valid ASCII characters
                let symbol_filtered = symbol_bytes
                    .iter()
                    .filter_map(|&b| {
                        if b >= 32 && b <= 126 {
                            // Printable ASCII
                            Some(b as char)
                        } else {
                            None
                        }
                    })
                    .collect::<String>();

                info!("Symbol (direct): '{}', Symbol (filtered): '{}'", symbol, symbol_filtered);

                // Use the filtered version if direct conversion looks suspicious
                let final_symbol = if symbol.chars().any(|c| (!c.is_ascii() || c.is_control())) {
                    symbol_filtered
                } else {
                    symbol
                };

                info!(
                    "Final symbol: '{}', Depth: {} bids, {} asks, Range: {}-{}",
                    final_symbol,
                    bids.len(),
                    asks.len(),
                    first_update_id,
                    last_update_id
                );

                // Process depth update message...
            }
            _ => {
                debug!("Unknown template ID: {}", header.template_id());
            }
        }

        Ok(())
    }
}

// impl BinanceSbeClient {
//     fn handle_depth_message(&self, msg: crate::models::sbe::DepthDiffStreamEvent) -> Result<()> {
//         // Extract symbol
//         // let symbol = std::str::from_utf8(&msg.symbol)?;

//         // // Convert to internal format
//         // let bids = msg.bids
//         //     .iter()
//         //     .map(|b| Level {
//         //         price: convert_price(b.price, msg.price_exponent),
//         //         quantity: convert_quantity(b.qty, msg.qty_exponent),
//         //     })
//         //     .collect();

//         // let asks = msg.asks
//         //     .iter()
//         //     .map(|a| Level {
//         //         price: convert_price(a.price, msg.price_exponent),
//         //         quantity: convert_quantity(a.qty, msg.qty_exponent),
//         //     })
//         //     .collect();

//         // let update = crate::models::OrderBookUpdate {
//         //     symbol: symbol.to_string(),
//         //     event_time: msg.event_time,
//         //     first_update_id: msg.first_book_update_id,
//         //     last_update_id: msg.last_book_update_id,
//         //     bids,
//         //     asks,
//         // };

//         // Forward to order book manager
//         // order_book_manager.apply_update(update);

//         Ok(())
//     }

//     // Similarly implement handlers for trades and best bid/ask messages
// }

// Helper functions to convert numeric values with exponents
fn convert_price(value: i64, exponent: i8) -> f64 {
    (value as f64) * (10f64).powi(exponent as i32)
}

fn convert_quantity(value: i64, exponent: i8) -> f64 {
    (value as f64) * (10f64).powi(exponent as i32)
}
