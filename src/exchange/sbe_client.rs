// use tokio_tungstenite::{ connect_async, WebSocketStream };
// use futures::{ SinkExt, StreamExt };
// use tokio::{ net::TcpStream, sync::RwLock };
// use tungstenite::{ client::IntoClientRequest, handshake::client::Request, protocol::Message };
// use anyhow::{ Result, Context };
// use tracing::{ debug, error, info };
// use std::sync::Arc;
// use tungstenite::handshake::client::generate_key;

// use crate::models::sbe::{
//     depth_diff_stream_event_codec::DepthDiffStreamEventDecoder,
//     message_header_codec::{ self, MessageHeaderDecoder },
//     Decoder,
//     ReadBuf,
// };

// // Use the official endpoint as per Binance documentation
// const BINANCE_SBE_URL: &str = "wss://stream-sbe.binance.com:9443/ws";

// pub struct BinanceSbeClient<F>
//     where F: Fn(&str, [(f64, f64); 100], [(f64, f64); 100], u64, u64) + Send + Sync + 'static {
//     api_key: Arc<str>,
//     endpoint: String,
//     depth_callback: RwLock<Option<F>>,
// }

// impl<F> BinanceSbeClient<F>
//     where F: Fn(&str, [(f64, f64); 100], [(f64, f64); 100], u64, u64) + Send + Sync + 'static
// {
//     pub fn new(api_key: String) -> Self {
//         Self {
//             api_key: api_key.into(),
//             endpoint: BINANCE_SBE_URL.into(),
//             depth_callback: RwLock::new(None),
//         }
//     }

//     pub async fn connect(
//         &self
//     ) -> Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>> {
//         info!("Connecting to Binance SBE WebSocket: {}", self.endpoint);

//         // Create a request with proper headers as per Binance documentation
//         let request = Request::builder()
//             .uri(self.endpoint.as_str())
//             .header("X-MBX-APIKEY", self.api_key.as_ref())
//             .header("Sec-WebSocket-Key", generate_key())
//             .header("Sec-WebSocket-Version", "13")
//             .header("Host", "stream-sbe.binance.com")
//             .header("Connection", "Upgrade")
//             .header("Upgrade", "websocket")
//             .body(())
//             .context("Failed to build request")?;

//         // Convert to a proper WebSocket request
//         let client_request = request
//             .into_client_request()
//             .context("Failed to convert to WebSocket request")?;

//         debug!("Connecting with request: {:?}", &client_request);

//         // Connect using the prepared WebSocket request
//         let (ws_stream, response) = connect_async(client_request).await.context(
//             "Failed to connect to Binance SBE WebSocket"
//         )?;

//         info!("Connected to Binance SBE WebSocket with response: {:?}", response);

//         Ok(ws_stream)
//     }

//     pub async fn subscribe(
//         &self,
//         ws_stream: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
//         symbols: &[String],
//         channels: &[String]
//     ) -> Result<()> {
//         // Prepare subscription message
//         // Format is: <symbol>@<channel>
//         let params = symbols
//             .iter()
//             .flat_map(|symbol| {
//                 channels
//                     .iter()
//                     .map(move |channel| { format!("{}@{}", symbol.to_lowercase(), channel) })
//             })
//             .collect::<Vec<String>>();

//         let subscription =
//             serde_json::json!({
//             "method": "SUBSCRIBE",
//             "params": params,
//             "id": 1
//         }).to_string();

//         debug!("Subscription request: {}", &subscription);

//         // Send subscription message
//         let message = Message::Text(subscription.into());
//         ws_stream.send(message).await?;

//         Ok(())
//     }

//     pub async fn process_messages(
//         &self,
//         ws_stream: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>
//     ) -> Result<()> {
//         while let Some(message_result) = ws_stream.next().await {
//             match message_result {
//                 Ok(message) => {
//                     match message {
//                         Message::Binary(data) => {
//                             // Handle the binary message with proper error handling
//                             if let Err(e) = self.handle_binary_message(&data).await {
//                                 debug!("Error handling binary message: {}", e);
//                                 // Continue processing messages instead of failing completely
//                             }
//                         }
//                         Message::Text(text) => {
//                             debug!("Received text message: {}", text);
//                         }
//                         Message::Ping(data) => {
//                             // Respond to ping with pong as required by the WebSocket protocol
//                             debug!("Received ping, responding with pong");
//                             if let Err(e) = ws_stream.send(Message::Pong(data.clone())).await {
//                                 error!("Failed to send pong: {}", e);
//                                 return Err(anyhow::anyhow!("WebSocket connection error: {}", e));
//                             }
//                         }
//                         Message::Pong(data) => {
//                             // Log pong responses
//                             debug!("Received pong response: {:?}", data);
//                         }
//                         Message::Close(frame) => {
//                             info!("WebSocket closed: {:?}", frame);
//                             return Ok(());
//                         }
//                         _ => {}
//                     }
//                 }
//                 Err(e) => {
//                     error!("WebSocket message error: {}", e);
//                     return Err(anyhow::anyhow!("WebSocket error: {}", e));
//                 }
//             }
//         }

//         info!("WebSocket stream ended");
//         Ok(())
//     }

//     #[inline]
//     pub async fn set_depth_callback(&self, callback: F) {
//         let mut cb = self.depth_callback.write().await;
//         *cb = Some(callback);
//     }

//     // Private implementation that can fail
//     async fn handle_binary_message_inner(&self, data: &[u8]) -> Result<()> {
//         // Safety check for minimum header size first
//         if data.len() < message_header_codec::ENCODED_LENGTH {
//             debug!("Message too small for SBE header: {} bytes", data.len());
//             return Ok(()); // Return ok but don't process further
//         }

//         // Create a buffer for SBE decoding
//         let buf_reader = ReadBuf::new(data);

//         // Try to read the SBE message header with safety checks
//         let mut header = MessageHeaderDecoder::default().wrap(buf_reader, 0);
//         let block_length = header.block_length();
//         let template_id = header.template_id();
//         let version = header.version();

//         // Sanity check on block_length to avoid huge allocations or out-of-bounds
//         if block_length > 10000 {
//             debug!("Suspicious block_length: {}, ignoring message", block_length);
//             return Ok(());
//         }

//         // Validate expected message size before proceeding
//         let expected_min_size = message_header_codec::ENCODED_LENGTH + (block_length as usize);
//         if data.len() < expected_min_size {
//             debug!(
//                 "Message too small: expected at least {} bytes but got {} bytes (template_id: {})",
//                 expected_min_size,
//                 data.len(),
//                 template_id
//             );
//             return Ok(());
//         }

//         // Debug output for message details (keep this as is - useful for debugging)
//         debug!(
//             "SBE Message: template_id={}, block_length={}, version={}, data_len={}",
//             template_id,
//             block_length,
//             version,
//             data.len()
//         );

//         // Fast path for depth messages (10003)
//         if template_id == 10003 {
//             // Use fixed-size arrays to avoid heap allocations in hot path
//             // Increase max size for safety (based on your mention of the 54946 issue)
//             let mut bids = [(0.0, 0.0); 100]; // Increased max size
//             let mut asks = [(0.0, 0.0); 100]; // Increased max size

//             let mut depth_decoder = DepthDiffStreamEventDecoder::default();

//             // Try to get parent buffer with error handling
//             let parent_result = header.parent();
//             if parent_result.is_err() {
//                 debug!("Failed to get parent buffer from header");
//                 return Ok(());
//             }

//             let buf_reader = parent_result.unwrap();

//             // Wrap depth decoder with careful positioning
//             depth_decoder = depth_decoder.wrap(
//                 buf_reader,
//                 message_header_codec::ENCODED_LENGTH,
//                 block_length,
//                 version
//             );

//             // Get basic message fields
//             let event_time = depth_decoder.event_time();
//             let first_update_id = depth_decoder.first_book_update_id();
//             let last_update_id = depth_decoder.last_book_update_id();
//             let price_exponent = depth_decoder.price_exponent();
//             let qty_exponent = depth_decoder.qty_exponent();

//             // Sanity check on exponents
//             if
//                 price_exponent < -10 ||
//                 price_exponent > 10 ||
//                 qty_exponent < -10 ||
//                 qty_exponent > 10
//             {
//                 debug!(
//                     "Suspicious exponents: price_exp={}, qty_exp={}",
//                     price_exponent,
//                     qty_exponent
//                 );
//                 return Ok(());
//             }

//             // Safety check for reasonable update IDs
//             if first_update_id > last_update_id || last_update_id - first_update_id > 10000 {
//                 debug!(
//                     "Suspicious update IDs: first={}, last={}, diff={}",
//                     first_update_id,
//                     last_update_id,
//                     last_update_id - first_update_id
//                 );
//                 // We'll continue but with caution
//             }

//             // Process the bids with careful error handling
//             let mut bids_decoder = depth_decoder.bids_decoder();
//             let bids_count = bids_decoder.count();

//             // Sanity check on bid count
//             let bids_count_usize = bids_count as u32 as usize; // Convert via u32 for safety
//             if bids_count_usize > bids.len() || bids_count_usize > 1000 {
//                 debug!("Unreasonable number of bids ({}), truncating or skipping", bids_count_usize);
//                 if bids_count_usize > 1000 {
//                     return Ok(());
//                 }
//                 // Otherwise we'll process within our array limits
//             }

//             let mut actual_bids_count = 0;
//             while actual_bids_count < bids.len() {
//                 match bids_decoder.advance() {
//                     Ok(Some(_)) => {
//                         // Get price and quantity with error checking
//                         let price = bids_decoder.price();
//                         let qty = bids_decoder.qty();

//                         // Convert using exponents with basic overflow protection
//                         let real_price = if price_exponent.abs() > 10 {
//                             debug!("Extreme price exponent: {}", price_exponent);
//                             0.0 // Protect against extreme exponents
//                         } else {
//                             (price as f64) * (10f64).powi(price_exponent as i32)
//                         };

//                         let real_qty = if qty_exponent.abs() > 10 {
//                             debug!("Extreme quantity exponent: {}", qty_exponent);
//                             0.0 // Protect against extreme exponents
//                         } else {
//                             (qty as f64) * (10f64).powi(qty_exponent as i32)
//                         };

//                         // Check for unreasonable values (NaN, infinity, etc.)
//                         if !real_price.is_finite() || !real_qty.is_finite() {
//                             debug!(
//                                 "Non-finite price or quantity calculated: price={}, qty={}",
//                                 real_price,
//                                 real_qty
//                             );
//                             continue; // Skip this level
//                         }

//                         // Store in our pre-allocated array
//                         bids[actual_bids_count] = (real_price, real_qty);
//                         actual_bids_count += 1;
//                     }
//                     Ok(None) => {
//                         break;
//                     } // No more bids
//                     Err(e) => {
//                         debug!("Error advancing bids decoder: {:?}", e);
//                         break;
//                     }
//                 }
//             }

//             // Try to get parent for asks processing
//             let parent_result = bids_decoder.parent();
//             if parent_result.is_err() {
//                 debug!("Failed to get parent from bids decoder");
//                 return Ok(());
//             }

//             let depth_decoder = parent_result.unwrap();

//             // Process the asks with careful error handling
//             let mut asks_decoder = depth_decoder.asks_decoder();
//             let asks_count = asks_decoder.count();

//             // Sanity check on ask count
//             let asks_count_usize = asks_count as u32 as usize; // Convert via u32 for safety
//             if asks_count_usize > asks.len() || asks_count_usize > 1000 {
//                 debug!("Unreasonable number of asks ({}), truncating or skipping", asks_count_usize);
//                 if asks_count_usize > 1000 {
//                     return Ok(());
//                 }
//                 // Otherwise we'll process within our array limits
//             }

//             let mut actual_asks_count = 0;
//             while actual_asks_count < asks.len() {
//                 match asks_decoder.advance() {
//                     Ok(Some(_)) => {
//                         // Get price and quantity with error checking
//                         let price = asks_decoder.price();
//                         let qty = asks_decoder.qty();

//                         // Convert using exponents with basic overflow protection
//                         let real_price = if price_exponent.abs() > 10 {
//                             debug!("Extreme price exponent: {}", price_exponent);
//                             0.0 // Protect against extreme exponents
//                         } else {
//                             (price as f64) * (10f64).powi(price_exponent as i32)
//                         };

//                         let real_qty = if qty_exponent.abs() > 10 {
//                             debug!("Extreme quantity exponent: {}", qty_exponent);
//                             0.0 // Protect against extreme exponents
//                         } else {
//                             (qty as f64) * (10f64).powi(qty_exponent as i32)
//                         };

//                         // Check for unreasonable values (NaN, infinity, etc.)
//                         if !real_price.is_finite() || !real_qty.is_finite() {
//                             debug!(
//                                 "Non-finite price or quantity calculated: price={}, qty={}",
//                                 real_price,
//                                 real_qty
//                             );
//                             continue; // Skip this level
//                         }

//                         // Store in our pre-allocated array
//                         asks[actual_asks_count] = (real_price, real_qty);
//                         actual_asks_count += 1;
//                     }
//                     Ok(None) => {
//                         break;
//                     } // No more asks
//                     Err(e) => {
//                         debug!("Error advancing asks decoder: {:?}", e);
//                         break;
//                     }
//                 }
//             }

//             // Try to get parent for symbol processing
//             let parent_result = asks_decoder.parent();
//             if parent_result.is_err() {
//                 debug!("Failed to get parent from asks decoder");
//                 return Ok(());
//             }

//             let mut depth_decoder = parent_result.unwrap();

//             // Carefully extract symbol
//             let symbol_coordinates = depth_decoder.symbol_decoder();

//             // Verify symbol coordinates are within buffer bounds
//             if depth_decoder.get_limit() < symbol_coordinates.0 + symbol_coordinates.1 {
//                 debug!(
//                     "Symbol coordinates out of bounds: offset={}, length={}, limit={}",
//                     symbol_coordinates.0,
//                     symbol_coordinates.1,
//                     depth_decoder.get_limit()
//                 );
//                 return Ok(());
//             }

//             // Sanity check on symbol length
//             if symbol_coordinates.1 > 20 {
//                 debug!("Symbol length suspiciously long: {}", symbol_coordinates.1);
//                 return Ok(());
//             }

//             // Get symbol bytes with bound checking
//             let symbol_bytes = depth_decoder.symbol_slice(symbol_coordinates);

//             // Extract symbol string with error handling
//             let symbol = match std::str::from_utf8(symbol_bytes) {
//                 Ok(s) => s.trim_end_matches('\0'),
//                 Err(_) => {
//                     debug!("Invalid UTF-8 in symbol bytes");
//                     return Ok(());
//                 }
//             };

//             // Finally, call the callback if set
//             let cb_guard = self.depth_callback.read().await;

//             if let Some(cb) = cb_guard.as_ref() {
//                 cb(
//                     symbol,
//                     // bids[0..actual_bids_count.min(bids.len())], // Ensure we don't exceed array bounds
//                     // asks[0..actual_asks_count.min(asks.len())], // Ensure we don't exceed array bounds
//                     bids,
//                     asks,
//                     first_update_id as u64,
//                     last_update_id as u64
//                 );
//             }

//             return Ok(());
//         } else {
//             // Just log other message types
//             debug!("Received unsupported SBE message type: {}", template_id);
//         }

//         Ok(())
//     }

//     #[inline]
//     async fn handle_binary_message(&self, data: &[u8]) -> Result<()> {
//         match self.handle_binary_message_inner(data).await {
//             Ok(_) => Ok(()),
//             Err(e) => {
//                 // Log the error but don't propagate it
//                 debug!("Error in message handling, continuing: {:?}", e);
//                 Ok(())
//             }
//         }
//     }
// }
use tokio_tungstenite::{ connect_async, WebSocketStream };
use futures::{ SinkExt, StreamExt };
use tokio::{ net::TcpStream, sync::RwLock };
use tungstenite::{ client::IntoClientRequest, handshake::client::Request, protocol::Message };
use anyhow::{ Result, Context };
use tracing::{ debug, error, info };
use std::sync::Arc;
use tungstenite::handshake::client::generate_key;
use dashmap::DashMap;
use std::cell::UnsafeCell;

const BINANCE_SBE_URL: &str = "wss://stream-sbe.binance.com:9443/ws";

// Thread-safe buffer wrapper
struct BufferCell {
    bids: UnsafeCell<[(f64, f64); 100]>,
    asks: UnsafeCell<[(f64, f64); 100]>,
}

unsafe impl Sync for BufferCell {}
unsafe impl Send for BufferCell {}

impl BufferCell {
    fn new() -> Self {
        Self {
            bids: UnsafeCell::new([(0.0, 0.0); 100]),
            asks: UnsafeCell::new([(0.0, 0.0); 100]),
        }
    }
}

pub struct BinanceSbeClient<F>
    where F: Fn(&str, [(f64, f64); 100], [(f64, f64); 100], u64, u64) + Send + Sync + 'static {
    api_key: Arc<str>,
    endpoint: String,
    depth_callback: RwLock<Option<F>>,

    // Thread-local buffers using UnsafeCell
    buffers: BufferCell,

    // Symbol intern pool to avoid string allocations
    symbol_pool: Arc<DashMap<Box<[u8]>, Arc<str>>>,
}

impl<F> BinanceSbeClient<F>
    where F: Fn(&str, [(f64, f64); 100], [(f64, f64); 100], u64, u64) + Send + Sync + 'static
{
    pub fn new(api_key: String) -> Self {
        Self {
            api_key: api_key.into(),
            endpoint: BINANCE_SBE_URL.into(),
            depth_callback: RwLock::new(None),
            buffers: BufferCell::new(),
            symbol_pool: Arc::new(DashMap::with_capacity(1000)),
        }
    }

    pub async fn connect(
        &self
    ) -> Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>> {
        info!("Connecting to Binance SBE WebSocket: {}", self.endpoint);

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

        let client_request = request
            .into_client_request()
            .context("Failed to convert to WebSocket request")?;

        debug!("Connecting with request: {:?}", &client_request);

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
        let params = symbols
            .iter()
            .flat_map(|symbol| {
                channels.iter().map(move |channel| format!("{}@{}", symbol.to_lowercase(), channel))
            })
            .collect::<Vec<String>>();

        let subscription =
            serde_json::json!({
            "method": "SUBSCRIBE",
            "params": params,
            "id": 1
        }).to_string();

        debug!("Subscription request: {}", &subscription);

        let message = Message::Text(subscription.into());
        ws_stream.send(message).await?;

        Ok(())
    }

    pub async fn process_messages(
        &self,
        ws_stream: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>
    ) -> Result<()> {
        while let Some(message_result) = ws_stream.next().await {
            match message_result {
                Ok(message) => {
                    match message {
                        Message::Binary(data) => {
                            if let Err(e) = self.handle_binary_message(&data).await {
                                debug!("Error handling binary message: {}", e);
                            }
                        }
                        Message::Text(text) => {
                            debug!("Received text message: {}", text);
                        }
                        Message::Ping(data) => {
                            debug!("Received ping, responding with pong");
                            if let Err(e) = ws_stream.send(Message::Pong(data.clone())).await {
                                error!("Failed to send pong: {}", e);
                                return Err(anyhow::anyhow!("WebSocket connection error: {}", e));
                            }
                        }
                        Message::Pong(data) => {
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
    pub async fn set_depth_callback(&self, callback: F) {
        let mut cb = self.depth_callback.write().await;
        *cb = Some(callback);
    }

    #[inline(always)]
    async fn handle_binary_message_inner(&self, data: &[u8]) -> Result<()> {
        // Fast bounds check
        if data.len() < 32 {
            return Ok(());
        }

        // Safe byte extraction with bounds checking
        let template_id = u16::from_le_bytes([
            *data.get(2).ok_or_else(|| anyhow::anyhow!("Invalid data"))?,
            *data.get(3).ok_or_else(|| anyhow::anyhow!("Invalid data"))?,
        ]);

        // Fast path for depth messages only
        if template_id != 10003 {
            return Ok(());
        }

        self.parse_depth_message_fast(data).await
    }

    #[inline(always)]
    async fn parse_depth_message_fast(&self, data: &[u8]) -> Result<()> {
        let mut offset = 8; // Skip header

        // Safe read functions
        let read_i64 = |data: &[u8], offset: usize| -> Result<i64> {
            if offset + 8 > data.len() {
                return Err(anyhow::anyhow!("Buffer overflow"));
            }
            let bytes = &data[offset..offset + 8];
            Ok(
                i64::from_le_bytes([
                    bytes[0],
                    bytes[1],
                    bytes[2],
                    bytes[3],
                    bytes[4],
                    bytes[5],
                    bytes[6],
                    bytes[7],
                ])
            )
        };

        let read_u16 = |data: &[u8], offset: usize| -> Result<u16> {
            if offset + 2 > data.len() {
                return Err(anyhow::anyhow!("Buffer overflow"));
            }
            Ok(u16::from_le_bytes([data[offset], data[offset + 1]]))
        };

        let read_i8 = |data: &[u8], offset: usize| -> Result<i8> {
            if offset >= data.len() {
                return Err(anyhow::anyhow!("Buffer overflow"));
            }
            Ok(data[offset] as i8)
        };

        // Read fixed fields
        let event_time = read_i64(data, offset)?;
        offset += 8;
        let first_update_id = read_i64(data, offset)? as u64;
        offset += 8;
        let last_update_id = read_i64(data, offset)? as u64;
        offset += 8;

        let price_exp = read_i8(data, offset)?;
        offset += 1;
        let qty_exp = read_i8(data, offset)?;
        offset += 1;

        // Pre-calculate multipliers
        let price_mult = (10f64).powi(price_exp as i32);
        let qty_mult = (10f64).powi(qty_exp as i32);

        // Read bid count
        offset += 2; // Skip block length
        let bid_count = read_u16(data, offset)? as usize;
        offset += 2;

        // Get mutable access to buffers through UnsafeCell
        let (bid_buffer, ask_buffer) = unsafe {
            let bids = &mut *self.buffers.bids.get();
            let asks = &mut *self.buffers.asks.get();
            (bids, asks)
        };

        // Clear buffers
        for i in 0..100 {
            bid_buffer[i] = (0.0, 0.0);
            ask_buffer[i] = (0.0, 0.0);
        }

        // Parse bids directly into buffer
        let bid_limit = bid_count.min(100);
        for i in 0..bid_limit {
            let price_raw = read_i64(data, offset)? as f64;
            offset += 8;
            let qty_raw = read_i64(data, offset)? as f64;
            offset += 8;

            bid_buffer[i] = (price_raw * price_mult, qty_raw * qty_mult);
        }

        // Skip remaining bids if any
        if bid_count > 100 {
            offset += (bid_count - 100) * 16;
        }

        // Read ask count
        offset += 2; // Skip block length
        let ask_count = read_u16(data, offset)? as usize;
        offset += 2;

        // Parse asks directly into buffer
        let ask_limit = ask_count.min(100);
        for i in 0..ask_limit {
            let price_raw = read_i64(data, offset)? as f64;
            offset += 8;
            let qty_raw = read_i64(data, offset)? as f64;
            offset += 8;

            ask_buffer[i] = (price_raw * price_mult, qty_raw * qty_mult);
        }

        // Skip remaining asks if any
        if ask_count > 100 {
            offset += (ask_count - 100) * 16;
        }

        // Extract symbol length
        let symbol_len = *data
            .get(offset)
            .ok_or_else(|| anyhow::anyhow!("Invalid symbol length"))? as usize;
        offset += 1;

        // Bounds check for symbol
        if offset + symbol_len > data.len() {
            return Err(anyhow::anyhow!("Invalid symbol length"));
        }

        // Get symbol slice
        let symbol_bytes = &data[offset..offset + symbol_len];

        // Intern the symbol
        let symbol = self.intern_symbol(symbol_bytes);

        // Call the callback
        let cb_guard = self.depth_callback.read().await;
        if let Some(cb) = cb_guard.as_ref() {
            cb(&symbol, *bid_buffer, *ask_buffer, first_update_id, last_update_id);
        }

        Ok(())
    }

    #[inline(always)]
    fn intern_symbol(&self, bytes: &[u8]) -> Arc<str> {
        // Remove trailing nulls
        let trimmed = bytes
            .iter()
            .rposition(|&b| b != 0)
            .map(|i| &bytes[..=i])
            .unwrap_or(bytes);

        if let Some(interned) = self.symbol_pool.get(trimmed) {
            interned.clone()
        } else {
            let symbol = std::str::from_utf8(trimmed).unwrap_or("").to_string();
            let arc_str: Arc<str> = Arc::from(symbol);
            self.symbol_pool.insert(trimmed.to_vec().into_boxed_slice(), arc_str.clone());
            arc_str
        }
    }

    #[inline]
    async fn handle_binary_message(&self, data: &[u8]) -> Result<()> {
        match self.handle_binary_message_inner(data).await {
            Ok(_) => Ok(()),
            Err(e) => {
                debug!("Error in message handling, continuing: {:?}", e);
                Ok(())
            }
        }
    }
}
