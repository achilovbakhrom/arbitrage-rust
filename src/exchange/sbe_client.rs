use tokio_tungstenite::{ connect_async, WebSocketStream };
use futures::{ SinkExt, StreamExt };
use tokio::net::TcpStream;
use tungstenite::{ client::IntoClientRequest, handshake::client::Request, protocol::Message };
use anyhow::{ Result, Context };
use tracing::{ debug, error, info };
use std::{ sync::Arc, time::Instant };
use tungstenite::handshake::client::generate_key;
use dashmap::DashMap;
use std::cell::UnsafeCell;
use parking_lot::RwLock;

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
    // CHANGED: tokio::RwLock -> parking_lot::RwLock
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
                            if let Err(e) = self.handle_binary_message(&data) {
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

    // CHANGED: Removed async - now synchronous
    #[inline]
    pub fn set_depth_callback(&self, callback: F) {
        let mut cb = self.depth_callback.write();
        *cb = Some(callback);
    }

    #[inline(always)]
    fn handle_binary_message_inner(&self, data: &[u8]) -> Result<()> {
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

        self.parse_depth_message_fast(data)
    }

    #[inline(always)]
    fn parse_depth_message_fast(&self, data: &[u8]) -> Result<()> {
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
        // CHANGED: Synchronous callback access
        let cb_guard = self.depth_callback.read();
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
    fn handle_binary_message(&self, data: &[u8]) -> Result<()> {
        match self.handle_binary_message_inner(data) {
            Ok(_) => Ok(()),
            Err(e) => {
                debug!("Error in message handling, continuing: {:?}", e);
                Ok(())
            }
        }
    }
}
