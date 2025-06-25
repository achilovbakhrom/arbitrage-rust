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
use std::{ net::TcpStream, sync::Arc, time::Instant };
use std::cell::UnsafeCell;
use parking_lot::RwLock;
use std::sync::atomic::{ AtomicBool, Ordering };

use crate::utils::symbol_interner::global;

const DEPTH: usize = 5;
const BINANCE_SBE_URL: &str = "wss://stream-sbe.binance.com:9443/ws";

struct BufferCell {
    bids: UnsafeCell<[(f64, f64); DEPTH]>,
    asks: UnsafeCell<[(f64, f64); DEPTH]>,
}

unsafe impl Sync for BufferCell {}
unsafe impl Send for BufferCell {}

impl BufferCell {
    fn new() -> Self {
        Self {
            bids: UnsafeCell::new([(0.0, 0.0); DEPTH]),
            asks: UnsafeCell::new([(0.0, 0.0); DEPTH]),
        }
    }
}

pub struct BinanceSbeClient<F>
    where
        F: Fn(u16, &[(f64, f64); DEPTH], &[(f64, f64); DEPTH], u64, u64, u64) +
            Send +
            Sync +
            'static {
    api_key: Arc<str>,
    endpoint: String,
    depth_callback: RwLock<Option<F>>,
    buffers: BufferCell,
    timing_enabled: AtomicBool,
}

impl<F> BinanceSbeClient<F>
    where
        F: Fn(u16, &[(f64, f64); DEPTH], &[(f64, f64); DEPTH], u64, u64, u64) +
            Send +
            Sync +
            'static
{
    pub fn new(api_key: String) -> Self {
        Self {
            api_key: api_key.into(),
            endpoint: BINANCE_SBE_URL.into(),
            depth_callback: RwLock::new(None),
            buffers: BufferCell::new(),
            timing_enabled: AtomicBool::new(false),
        }
    }

    pub fn preload_symbol_mappings(&self, symbols: &[(String, u16)]) {
        global::intern_batch(
            &symbols
                .iter()
                .map(|(s, _)| s.clone())
                .collect::<Vec<_>>()
        );
        info!("Using global symbol interner for {} symbols", symbols.len());
    }

    pub fn enable_timing(&self) {
        self.timing_enabled.store(true, Ordering::Relaxed);
    }

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

    pub fn process_messages(
        &self,
        ws_stream: &mut WebSocket<MaybeTlsStream<TcpStream>>,
        shutdown: &std::sync::atomic::AtomicBool
    ) -> Result<()> {
        use std::sync::atomic::Ordering;

        loop {
            if shutdown.load(Ordering::Relaxed) {
                info!("Shutdown signal received, closing WebSocket");
                let _ = ws_stream.close(None);
                return Ok(());
            }

            let message_result = ws_stream.read();

            match message_result {
                Ok(message) => {
                    match message {
                        Message::Binary(data) => {
                            let parse_result = self.handle_binary_message_optimized(&data);

                            if let Err(e) = parse_result {
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
    fn handle_binary_message_optimized(&self, data: &[u8]) -> Result<()> {
        if data.len() < 32 {
            return Ok(());
        }

        let template_id = unsafe {
            u16::from_le_bytes([*data.get_unchecked(2), *data.get_unchecked(3)])
        };

        if template_id == 10003 {
            self.parse_depth_message_ultra_fast(data)
        } else {
            Ok(())
        }
    }

    #[inline(always)]
    fn parse_depth_message_ultra_fast(&self, data: &[u8]) -> Result<()> {
        let timing_enabled = self.timing_enabled.load(Ordering::Relaxed);
        let parse_start = if timing_enabled { Some(Instant::now()) } else { None };

        let mut offset = 8;

        fn read_i64_fast(data: &[u8], offset: usize) -> i64 {
            unsafe {
                let ptr = data.as_ptr().add(offset) as *const i64;
                i64::from_le(ptr.read_unaligned())
            }
        }

        fn read_u16_fast(data: &[u8], offset: usize) -> u16 {
            unsafe {
                let ptr = data.as_ptr().add(offset) as *const u16;
                u16::from_le(ptr.read_unaligned())
            }
        }

        fn read_i8_fast(data: &[u8], offset: usize) -> i8 {
            unsafe { *data.get_unchecked(offset) as i8 }
        }

        let _event_time = read_i64_fast(data, offset);
        offset += 8;
        let first_update_id = read_i64_fast(data, offset) as u64;
        offset += 8;
        let last_update_id = read_i64_fast(data, offset) as u64;
        offset += 8;

        let price_exp = read_i8_fast(data, offset);
        offset += 1;
        let qty_exp = read_i8_fast(data, offset);
        offset += 1;

        let price_mult = (10f64).powi(price_exp as i32);
        let qty_mult = (10f64).powi(qty_exp as i32);

        offset += 2;
        let bid_count = read_u16_fast(data, offset) as usize;
        offset += 2;

        let (bid_buffer, ask_buffer) = unsafe {
            let bids = &mut *self.buffers.bids.get();
            let asks = &mut *self.buffers.asks.get();
            (bids, asks)
        };

        unsafe {
            std::ptr::write_bytes(bid_buffer.as_mut_ptr(), 0, DEPTH);
            std::ptr::write_bytes(ask_buffer.as_mut_ptr(), 0, DEPTH);
        }

        let bid_limit = bid_count.min(DEPTH);
        for i in 0..bid_limit {
            let price_raw = read_i64_fast(data, offset) as f64;
            offset += 8;
            let qty_raw = read_i64_fast(data, offset) as f64;
            offset += 8;

            bid_buffer[i] = (price_raw * price_mult, qty_raw * qty_mult);
        }

        if bid_count > DEPTH {
            offset += (bid_count - DEPTH) * 16;
        }

        offset += 2;
        let ask_count = read_u16_fast(data, offset) as usize;
        offset += 2;

        let ask_limit = ask_count.min(DEPTH);
        for i in 0..ask_limit {
            let price_raw = read_i64_fast(data, offset) as f64;
            offset += 8;
            let qty_raw = read_i64_fast(data, offset) as f64;
            offset += 8;

            ask_buffer[i] = (price_raw * price_mult, qty_raw * qty_mult);
        }

        if ask_count > DEPTH {
            offset += (ask_count - DEPTH) * 16;
        }

        if offset >= data.len() {
            return Err(anyhow::anyhow!("Invalid symbol offset"));
        }

        let symbol_len = data[offset] as usize;
        offset += 1;

        if offset + symbol_len > data.len() {
            return Err(anyhow::anyhow!("Invalid symbol length"));
        }

        let symbol_bytes = &data[offset..offset + symbol_len];
        let symbol_id = self.resolve_symbol_id_ultra_fast(symbol_bytes);

        let sbe_parse_time_us = if let Some(start) = parse_start {
            start.elapsed().as_micros() as u64
        } else {
            0
        };

        if let Some(symbol_id) = symbol_id {
            let cb_guard = self.depth_callback.read();
            if let Some(cb) = cb_guard.as_ref() {
                cb(
                    symbol_id,
                    bid_buffer,
                    ask_buffer,
                    first_update_id,
                    last_update_id,
                    sbe_parse_time_us
                );
            }
        }

        Ok(())
    }

    #[inline(always)]
    fn resolve_symbol_id_ultra_fast(&self, symbol_bytes: &[u8]) -> Option<u16> {
        global::get_id_from_bytes(symbol_bytes)
    }
}
