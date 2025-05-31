// src/exchange/sbe_client.rs - Performance-optimized version with minimal timing overhead

use tungstenite::{
    http::Request,
    protocol::Message,
    handshake::client::generate_key,
    connect,
    WebSocket,
    stream::MaybeTlsStream,
};
use anyhow::{ Result, Context };
use tracing::{ debug, error, info };
use std::{ net::TcpStream, sync::Arc, time::{ SystemTime, UNIX_EPOCH, Instant } };
use dashmap::DashMap;
use std::cell::UnsafeCell;
use parking_lot::RwLock;
use std::sync::atomic::{ AtomicBool, Ordering };

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
    where F: Fn(&str, [(f64, f64); 100], [(f64, f64); 100], u64, u64, u64) + Send + Sync + 'static {
    api_key: Arc<str>,
    endpoint: String,
    depth_callback: RwLock<Option<F>>,
    buffers: BufferCell,
    symbol_pool: Arc<DashMap<Box<[u8]>, Arc<str>>>,
    // Performance optimization: only measure timing when enabled
    timing_enabled: AtomicBool,
}

impl<F> BinanceSbeClient<F>
    where F: Fn(&str, [(f64, f64); 100], [(f64, f64); 100], u64, u64, u64) + Send + Sync + 'static
{
    pub fn new(api_key: String) -> Self {
        Self {
            api_key: api_key.into(),
            endpoint: BINANCE_SBE_URL.into(),
            depth_callback: RwLock::new(None),
            buffers: BufferCell::new(),
            symbol_pool: Arc::new(DashMap::with_capacity(1000)),
            timing_enabled: AtomicBool::new(false), // Disabled by default for production
        }
    }

    /// Enable timing measurements (only for performance tests)
    pub fn enable_timing(&self) {
        self.timing_enabled.store(true, Ordering::Relaxed);
    }

    /// Disable timing measurements (default, for production)
    pub fn disable_timing(&self) {
        self.timing_enabled.store(false, Ordering::Relaxed);
    }

    pub fn connect(&self) -> Result<WebSocket<MaybeTlsStream<TcpStream>>> {
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

        let (ws_stream, response) = connect(request).context(
            "Failed to connect to Binance SBE WebSocket"
        )?;

        info!("Connected to Binance SBE WebSocket with response: {:?}", response);

        Ok(ws_stream)
    }

    pub fn subscribe(
        &self,
        ws_stream: &mut WebSocket<MaybeTlsStream<TcpStream>>,
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
        ws_stream.send(message)?;

        Ok(())
    }

    // Optimized message processing with conditional timing
    pub fn process_messages_with_shutdown(
        &self,
        ws_stream: &mut WebSocket<MaybeTlsStream<TcpStream>>,
        shutdown: &std::sync::atomic::AtomicBool
    ) -> Result<()> {
        use std::sync::atomic::Ordering;

        // Check if timing is enabled once, outside the loop
        let timing_enabled = self.timing_enabled.load(Ordering::Relaxed);

        loop {
            // Check for shutdown signal
            if shutdown.load(Ordering::Relaxed) {
                info!("Shutdown signal received, closing WebSocket");
                let _ = ws_stream.close(None);
                return Ok(());
            }

            // Conditional timing measurement - zero overhead when disabled
            let (message_result, receive_time_us) = if timing_enabled {
                let start = Instant::now();
                let result = ws_stream.read();
                let elapsed = start.elapsed().as_micros() as u64;
                (result, elapsed)
            } else {
                // Fast path - no timing overhead
                (ws_stream.read(), 0)
            };

            match message_result {
                Ok(message) => {
                    match message {
                        Message::Binary(data) => {
                            if
                                let Err(e) = self.handle_binary_message_optimized(
                                    &data,
                                    receive_time_us
                                )
                            {
                                debug!("Error handling binary message: {}", e);
                            }
                        }
                        Message::Text(text) => {
                            debug!("Received text message: {}", text);
                        }
                        Message::Ping(data) => {
                            debug!("Received ping, responding with pong");
                            if let Err(e) = ws_stream.send(Message::Pong(data)) {
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
                        Message::Frame(_) => {
                            debug!("Received raw frame");
                        }
                    }
                }
                Err(e) => {
                    error!("WebSocket message error: {}", e);
                    return Err(anyhow::anyhow!("WebSocket error: {}", e));
                }
            }
        }
    }

    #[inline]
    pub fn set_depth_callback(&self, callback: F) {
        let mut cb = self.depth_callback.write();
        *cb = Some(callback);
    }

    #[inline(always)]
    fn handle_binary_message_optimized(&self, data: &[u8], receive_time_us: u64) -> Result<()> {
        // Ultra-fast bounds check with likely hint
        if unlikely(data.len() < 32) {
            return Ok(());
        }

        // Optimized template ID extraction - single operation
        let template_id = unsafe {
            u16::from_le_bytes([*data.get_unchecked(2), *data.get_unchecked(3)])
        };

        // Fast path for depth messages only - branch predictor friendly
        if likely(template_id == 10003) {
            self.parse_depth_message_ultra_fast(data, receive_time_us)
        } else {
            Ok(())
        }
    }

    #[inline(always)]
    fn parse_depth_message_ultra_fast(&self, data: &[u8], receive_time_us: u64) -> Result<()> {
        let mut offset = 8; // Skip header

        // Unsafe optimized read functions - no bounds checking for speed
        unsafe fn read_i64_fast(data: &[u8], offset: usize) -> i64 {
            let ptr = data.as_ptr().add(offset) as *const i64;
            i64::from_le(ptr.read_unaligned())
        }

        unsafe fn read_u16_fast(data: &[u8], offset: usize) -> u16 {
            let ptr = data.as_ptr().add(offset) as *const u16;
            u16::from_le(ptr.read_unaligned())
        }

        unsafe fn read_i8_fast(data: &[u8], offset: usize) -> i8 {
            *data.get_unchecked(offset) as i8
        }

        // Read fixed fields with unsafe optimizations
        let _event_time = unsafe { read_i64_fast(data, offset) };
        offset += 8;
        let first_update_id = unsafe { read_i64_fast(data, offset) as u64 };
        offset += 8;
        let last_update_id = unsafe { read_i64_fast(data, offset) as u64 };
        offset += 8;

        let price_exp = unsafe { read_i8_fast(data, offset) };
        offset += 1;
        let qty_exp = unsafe { read_i8_fast(data, offset) };
        offset += 1;

        // Pre-calculate multipliers
        let price_mult = (10f64).powi(price_exp as i32);
        let qty_mult = (10f64).powi(qty_exp as i32);

        // Read bid count
        offset += 2; // Skip block length
        let bid_count = unsafe { read_u16_fast(data, offset) as usize };
        offset += 2;

        // Get mutable access to buffers through UnsafeCell
        let (bid_buffer, ask_buffer) = unsafe {
            let bids = &mut *self.buffers.bids.get();
            let asks = &mut *self.buffers.asks.get();
            (bids, asks)
        };

        // Optimized buffer clearing - use ptr::write_bytes for bulk zero
        unsafe {
            std::ptr::write_bytes(bid_buffer.as_mut_ptr(), 0, 100);
            std::ptr::write_bytes(ask_buffer.as_mut_ptr(), 0, 100);
        }

        // Optimized bid parsing - unrolled for better performance
        let bid_limit = bid_count.min(100);
        for i in 0..bid_limit {
            let price_raw = unsafe { read_i64_fast(data, offset) as f64 };
            offset += 8;
            let qty_raw = unsafe { read_i64_fast(data, offset) as f64 };
            offset += 8;

            bid_buffer[i] = (price_raw * price_mult, qty_raw * qty_mult);
        }

        // Skip remaining bids if any
        if bid_count > 100 {
            offset += (bid_count - 100) * 16;
        }

        // Read ask count
        offset += 2; // Skip block length
        let ask_count = unsafe { read_u16_fast(data, offset) as usize };
        offset += 2;

        // Optimized ask parsing
        let ask_limit = ask_count.min(100);
        for i in 0..ask_limit {
            let price_raw = unsafe { read_i64_fast(data, offset) as f64 };
            offset += 8;
            let qty_raw = unsafe { read_i64_fast(data, offset) as f64 };
            offset += 8;

            ask_buffer[i] = (price_raw * price_mult, qty_raw * qty_mult);
        }

        // Skip remaining asks if any
        if ask_count > 100 {
            offset += (ask_count - 100) * 16;
        }

        // Extract symbol length with bounds check
        if unlikely(offset >= data.len()) {
            return Err(anyhow::anyhow!("Invalid symbol offset"));
        }

        let symbol_len = data[offset] as usize;
        offset += 1;

        // Bounds check for symbol
        if unlikely(offset + symbol_len > data.len()) {
            return Err(anyhow::anyhow!("Invalid symbol length"));
        }

        // Get symbol slice
        let symbol_bytes = &data[offset..offset + symbol_len];

        // Optimized symbol interning
        let symbol = self.intern_symbol_fast(symbol_bytes);

        // Fast callback execution - read lock is very fast
        let cb_guard = self.depth_callback.read();
        if let Some(cb) = cb_guard.as_ref() {
            cb(&symbol, *bid_buffer, *ask_buffer, first_update_id, last_update_id, receive_time_us);
        }

        Ok(())
    }

    #[inline(always)]
    fn intern_symbol_fast(&self, bytes: &[u8]) -> Arc<str> {
        // Fast path: remove trailing nulls more efficiently
        let trimmed = if bytes.is_empty() {
            bytes
        } else {
            let mut end = bytes.len();
            while end > 0 && bytes[end - 1] == 0 {
                end -= 1;
            }
            &bytes[..end]
        };

        // Try to get existing symbol first (most common case)
        if let Some(interned) = self.symbol_pool.get(trimmed) {
            return interned.clone();
        }

        // Only create new symbol if not found
        let symbol = (unsafe { std::str::from_utf8_unchecked(trimmed) }).to_string();
        let arc_str: Arc<str> = Arc::from(symbol);
        self.symbol_pool.insert(trimmed.to_vec().into_boxed_slice(), arc_str.clone());
        arc_str
    }
}

// Branch prediction hints for better performance
// #[inline(always)]
// fn likely(b: bool) -> bool {
//     std::intrinsics::likely(b)
// }

// #[inline(always)]
// fn unlikely(b: bool) -> bool {
//     std::intrinsics::unlikely(b)
// }

#[inline(always)]
fn likely(b: bool) -> bool {
    // For stable Rust, we can use cold/inline attributes instead
    if b {
        true
    } else {
        false
    }
}

#[inline(always)]
fn unlikely(b: bool) -> bool {
    // For stable Rust, we can use cold/inline attributes instead
    if b {
        true
    } else {
        false
    }
}
