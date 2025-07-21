use base64::{ engine::general_purpose, Engine as _ };
use chrono::Utc;
use ed25519_dalek::{ Signature, Signer, SigningKey };
use native_tls::{ TlsConnector, TlsStream };
use parking_lot::Mutex;
use reqwest::Client as HttpClient;
use serde::{ self, Deserialize };
use std::collections::HashMap;
use std::io::{ BufRead, BufReader, Write, Read };
use std::net::TcpStream;
use std::sync::atomic::{ AtomicBool, AtomicU32, Ordering };
use std::sync::{ Arc, Condvar, Mutex as StdMutex };
use std::thread;
use std::time::{ Duration, Instant };
use anyhow::{ Result, Context };
use tracing::{ info, error, debug, warn };
use tokio::sync::mpsc;

use crate::config::Config;
use crate::exchange::user_data_ws::UserDataWebSocket;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderStatusResponse {
    pub symbol: String,
    pub order_id: u64,
    pub client_order_id: String,
    pub price: String,
    pub orig_qty: String,
    pub executed_qty: String,
    pub cummulative_quote_qty: String,
    pub status: String,
    pub side: String,
    pub time: u64,
    pub update_time: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum FixError {
    #[error("Connection error: {0}")] Connection(#[from] std::io::Error),
    #[error("TLS error: {0}")] Tls(#[from] native_tls::Error),
    #[error("Parse error: {0}")] Parse(String),
    #[error("Authentication error: {0}")] Auth(String),
    #[error("Order execution error: {0}")] Execution(String),
    #[error("Rate limit exceeded")]
    RateLimit,
    #[error("Order rejected: {0}")] OrderRejected(String),
    #[error("Timeout error")]
    Timeout,
}

#[derive(Debug, Clone)]
pub struct FixConfig {
    pub api_key: String,
    pub private_key_base64: String,
    pub target_comp_id: String,
    pub heartbeat_interval: u32,
    pub endpoint: String,
}

impl Default for FixConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            private_key_base64: String::new(),
            target_comp_id: "SPOT".to_string(),
            heartbeat_interval: 30,
            endpoint: "fix-oe.binance.com:9000".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub client_order_id: String,
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub filled_qty: f64,
    pub avg_price: f64,
    pub cum_quote_qty: f64,
    pub leaves_qty: f64,
    pub order_status: String,
    pub exec_type: String,
    pub last_px: f64,
    pub last_qty: f64,
    pub execution_time: Duration,
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    #[inline(always)]
    fn to_fix_side(&self) -> &'static str {
        match self {
            OrderSide::Buy => "1",
            OrderSide::Sell => "2",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketOrder {
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub quote_quantity: Option<f64>,
    pub qty_precision: Option<u8>,
    pub price_precision: Option<u8>,
}


#[derive(Debug, Clone)]
pub struct SymbolPrecision {
    pub qty_precision: u8,
    pub price_precision: u8,
}

struct RateLimiter {
    last_request: Instant,
    min_interval: Duration,
}

impl RateLimiter {
    fn new(requests_per_second: f64) -> Self {
        Self {
            last_request: Instant::now() - Duration::from_secs(1),
            min_interval: Duration::from_nanos((1_000_000_000.0 / requests_per_second) as u64),
        }
    }

    fn wait_if_needed(&mut self) {
        let elapsed = self.last_request.elapsed();
        if elapsed < self.min_interval {
            let wait_time = self.min_interval - elapsed;
            thread::sleep(wait_time);
        }
        self.last_request = Instant::now();
    }
}

pub struct FixClient {
    fix_config: FixConfig,
    sender_comp_id: String,
    seq_num: Arc<AtomicU32>,
    signing_key: SigningKey,
    is_connected: AtomicBool,
    shutdown: Arc<AtomicBool>,
    connection_handle: Option<thread::JoinHandle<()>>,
    heartbeat_handle: Option<thread::JoinHandle<()>>,
    symbol_precisions: Arc<Mutex<HashMap<String, SymbolPrecision>>>,
    stream: Arc<Mutex<Option<TlsStream<TcpStream>>>>,
    rate_limiter: Arc<Mutex<RateLimiter>>,
    logout_received: Arc<AtomicBool>,
    logout_condvar: Arc<(StdMutex<bool>, Condvar)>,
    http_client: HttpClient,
    config: Config,
    user_data_ws: Option<Arc<Mutex<UserDataWebSocket>>>,
    ws_handle: Option<thread::JoinHandle<()>>,
}

impl FixClient {
    pub fn new(fix_config: FixConfig, config: Config) -> Result<Self> {
        let key_bytes = general_purpose::STANDARD
            .decode(&fix_config.private_key_base64)
            .map_err(|e| FixError::Parse(format!("Failed to decode private key: {e}")))?;

        let signing_key = Self::parse_ed25519_key(&key_bytes)?;

        let http_client = HttpClient::builder()
            .timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .build()
            .map_err(|e|
                FixError::Connection(
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to create HTTP client: {e}")
                    )
                )
            )?;

        Ok(Self {
            config,
            http_client,
            fix_config: fix_config.clone(),
            sender_comp_id: fix_config.api_key[..8].to_string(),
            seq_num: Arc::new(AtomicU32::new(1)),
            signing_key,
            is_connected: AtomicBool::new(false),
            shutdown: Arc::new(AtomicBool::new(false)),
            connection_handle: None,
            heartbeat_handle: None,
            symbol_precisions: Arc::new(Mutex::new(HashMap::with_capacity(256))),
            stream: Arc::new(Mutex::new(None)),
            rate_limiter: Arc::new(Mutex::new(RateLimiter::new(2.0))),
            logout_received: Arc::new(AtomicBool::new(false)),
            logout_condvar: Arc::new((StdMutex::new(false), Condvar::new())),
            user_data_ws: None,
            ws_handle: None,
        })
    }

    #[inline]
    fn parse_ed25519_key(key_bytes: &[u8]) -> Result<SigningKey> {
        debug!("Parsing private key format:");
        debug!("  Key bytes length: {}", key_bytes.len());

        let key_str = String::from_utf8_lossy(key_bytes);
        if key_str.contains("-----BEGIN PRIVATE KEY-----") {
            debug!("  Detected PEM format, extracting key data...");

            let lines: Vec<&str> = key_str.lines().collect();
            let mut base64_content = String::new();
            let mut in_key = false;

            for line in lines {
                if line.contains("-----BEGIN PRIVATE KEY-----") {
                    in_key = true;
                    continue;
                }
                if line.contains("-----END PRIVATE KEY-----") {
                    break;
                }
                if in_key {
                    base64_content.push_str(line.trim());
                }
            }

            let pem_bytes = general_purpose::STANDARD
                .decode(&base64_content)
                .map_err(|e| FixError::Parse(format!("Failed to decode PEM content: {e}")))?;

            return Self::extract_ed25519_from_pkcs8(&pem_bytes);
        }

        if key_bytes.len() == 32 {
            let key_array: [u8; 32] = key_bytes
                .try_into()
                .map_err(|_| FixError::Parse("Invalid 32-byte key".to_string()))?;
            return Ok(SigningKey::from_bytes(&key_array));
        } else if key_bytes.len() > 32 {
            return Self::extract_ed25519_from_pkcs8(key_bytes);
        }

        Err(FixError::Parse(format!("Unsupported key format. Length: {}", key_bytes.len())).into())
    }

    #[inline]
    fn extract_ed25519_from_pkcs8(pkcs8_bytes: &[u8]) -> Result<SigningKey> {
        if pkcs8_bytes.len() >= 32 {
            let key_start = pkcs8_bytes.len() - 32;
            let key_slice: [u8; 32] = pkcs8_bytes[key_start..]
                .try_into()
                .map_err(|_| FixError::Parse("Invalid PKCS#8 key extraction".to_string()))?;

            Ok(SigningKey::from_bytes(&key_slice))
        } else {
            Err(FixError::Parse("PKCS#8 key too short".to_string()).into())
        }
    }

    #[inline]
    pub fn update_symbol_precision(&self, symbol: &str, qty_precision: u8, price_precision: u8) {
        let mut cache = self.symbol_precisions.lock();
        cache.insert(symbol.to_string(), SymbolPrecision {
            qty_precision,
            price_precision,
        });
    }

    #[inline]
    fn format_quantity(&self, quantity: f64, precision: Option<u8>) -> String {
        match precision {
            Some(p) => format!("{:.prec$}", quantity, prec = p as usize),
            None => format!("{quantity:.8}"),
        }
    }

    pub fn connect(&mut self) -> Result<()> {
        info!("Starting FIX connection process...");

        let connector = TlsConnector::new()?;
        let tcp_stream = TcpStream::connect(&self.fix_config.endpoint).context(
            "Failed to connect to FIX endpoint"
        )?;

        tcp_stream.set_nodelay(true)?;
        tcp_stream.set_read_timeout(Some(Duration::from_millis(1000)))?; // Shorter timeout
        tcp_stream.set_write_timeout(Some(Duration::from_secs(10)))?;

        let domain = self.fix_config.endpoint.split(':').next().unwrap();
        let tls_stream = connector.connect(domain, tcp_stream).context("TLS handshake failed")?;

        info!("✓ TLS connection established");
        *self.stream.lock() = Some(tls_stream);

        self.send_logon_and_wait_response()?;
        self.start_heartbeat()?;
        self.start_message_reader()?;
        
        // Initialize WebSocket for order status updates
        if !self.config.rest_api_secret.is_empty() {
            match self.initialize_user_data_websocket() {
                Ok(_) => info!("✓ User data WebSocket initialized"),
                Err(e) => warn!("Failed to initialize user data WebSocket: {}. Falling back to REST API polling.", e),
            }
        }

        self.is_connected.store(true, Ordering::Release);
        info!("✓ FIX client connected successfully");
        Ok(())
    }

    fn send_logon_and_wait_response(&mut self) -> Result<()> {
        let seq_num = self.seq_num.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

        let signature_payload = format!(
            "A\x01{}\x01{}\x01{}\x01{}",
            self.sender_comp_id,
            self.fix_config.target_comp_id,
            seq_num,
            sending_time
        );

        let signature: Signature = self.signing_key.sign(signature_payload.as_bytes());
        let signature_base64 = general_purpose::STANDARD.encode(signature.to_bytes());

        let sender_comp_field = format!("49={}", self.sender_comp_id);
        let target_comp_field = format!("56={}", self.fix_config.target_comp_id);
        let seq_num_field = format!("34={seq_num}");
        let sending_time_field = format!("52={sending_time}");
        let heartbeat_field = format!("108={}", self.fix_config.heartbeat_interval);
        let api_key_field = format!("553={}", self.fix_config.api_key);
        let signature_len_field = format!("95={}", signature_base64.len());
        let signature_field = format!("96={signature_base64}");

        let fields = vec![
            "35=A",
            &sender_comp_field,
            &target_comp_field,
            &seq_num_field,
            &sending_time_field,
            "98=0",
            &heartbeat_field,
            "141=Y",
            &api_key_field,
            &signature_len_field,
            &signature_field,
            "25035=1",
            "25036=1"
        ];

        let message = self.build_fix_message_fast(&fields);
        self.send_message_fast(message)?;

        let start_time = Instant::now();
        let timeout = Duration::from_secs(15);

        while start_time.elapsed() < timeout {
            match self.read_fix_message_fast() {
                Ok(response) => {
                    let fields = Self::parse_fix_message_fast(&response);
                    let msg = "Unknown".to_string();
                    let msg_type = fields.get("35").unwrap_or(&msg);

                    match msg_type.as_str() {
                        "A" => {
                            info!("✓ FIX logon successful");
                            return Ok(());
                        }
                        "3" => {
                            let msg = "Unknown".to_string();
                            let reject_code = fields.get("373").unwrap_or(&msg);
                            return Err(
                                FixError::Auth(format!("Logon rejected: {reject_code}")).into()
                            );
                        }
                        "5" => {
                            let msg = "Server initiated logout".to_string();
                            let reason = fields.get("58").unwrap_or(&msg);
                            return Err(FixError::Auth(format!("Server logout: {reason}")).into());
                        }
                        _ => {
                            continue;
                        }
                    }
                }
                Err(_) => thread::sleep(Duration::from_millis(100)),
            }
        }

        Err(FixError::Auth("Timeout waiting for logon response".to_string()).into())
    }

    pub fn place_market_order(&mut self, order: MarketOrder) -> Result<ExecutionResult> {
        {
            let mut rate_limiter = self.rate_limiter.lock();
            rate_limiter.wait_if_needed();
        }

        let client_order_id = format!(
            "{}_{}",
            self.sender_comp_id,
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );

        let start_time = Instant::now();
        
        // Determine monitoring method
        let use_websocket = self.user_data_ws.is_some();
        if use_websocket {
            info!("🚀 FAST execution mode: Placing order via FIX, monitoring via WebSocket");
        } else {
            info!("🚀 FAST execution mode: Placing order via FIX, checking status via REST");
        }

        // Send order via FIX API (fire-and-forget)
        let _sent_order_id = self.place_order_via_fix(&order, &client_order_id)?;
        
        info!(
            "✅ Order placed via FIX, waiting for status via {}...",
            if use_websocket { "WebSocket" } else { "REST API" }
        );

        let final_result = if use_websocket {
            // Try WebSocket first, fall back to REST if it fails
            match self.wait_for_order_via_websocket(
                &client_order_id,
                Duration::from_secs(30),
                start_time
            ) {
                Ok(result) => result,
                Err(e) => {
                    warn!("WebSocket order monitoring failed: {}. Falling back to REST API.", e);
                    self.check_order_status_until_filled(
                        &order.symbol,
                        "", // order_id not available since we don't wait for FIX response
                        &client_order_id,
                        Duration::from_secs(30)
                    )?
                }
            }
        } else {
            self.check_order_status_until_filled(
                &order.symbol,
                "", // order_id not available since we don't wait for FIX response
                &client_order_id,
                Duration::from_secs(30)
            )?
        };

        Ok(final_result)
    }

    fn place_order_via_fix(
        &mut self,
        order: &MarketOrder,
        client_order_id: &str
    ) -> Result<String> {
        let mut order = order.clone();
        if order.qty_precision.is_none() {
            let cache = self.symbol_precisions.lock();
            if let Some(precision_info) = cache.get(&order.symbol) {
                order.qty_precision = Some(precision_info.qty_precision);
                order.price_precision = Some(precision_info.price_precision);
            }
        }

        let seq_num = self.seq_num.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

        let seq_num_field = format!("34={seq_num}");
        let sender_comp_field = format!("49={}", self.sender_comp_id);
        let sending_time_field = format!("52={sending_time}");
        let target_comp_field = format!("56={}", self.fix_config.target_comp_id);
        let client_order_id_field = format!("11={client_order_id}");
        let symbol_field = format!("55={}", order.symbol.to_uppercase());
        let side_field = format!("54={}", order.side.to_fix_side());

        let quantity_field = if let Some(quote_qty) = order.quote_quantity {
            let formatted_qty = self.format_quantity(quote_qty, order.price_precision);
            format!("152={formatted_qty}")
        } else {
            let formatted_qty = self.format_quantity(order.quantity, order.qty_precision);
            format!("38={formatted_qty}")
        };

        let fields = vec![
            "35=D",
            &seq_num_field,
            &sender_comp_field,
            &sending_time_field,
            &target_comp_field,
            &client_order_id_field,
            &quantity_field,
            "40=1",
            &side_field,
            &symbol_field
        ];

        let message = self.build_fix_message_fast(&fields);
        self.send_message_fast(message)?;
        
        info!("📤 Order sent via FIX API: {}", client_order_id);
        Ok(client_order_id.to_string())
    }

    fn check_order_status_until_filled(
        &self,
        symbol: &str,
        _order_id: &str,
        client_order_id: &str,
        max_wait_duration: Duration
    ) -> Result<ExecutionResult> {
        let start_time = Instant::now();
        let mut poll_interval = Duration::from_millis(50);
        let max_poll_interval = Duration::from_millis(250);

        loop {
            if start_time.elapsed() > max_wait_duration {
                return Err(FixError::Timeout.into());
            }

            match self.check_order_status_rest(symbol, client_order_id) {
                Ok(status) => {
                    debug!(
                        "Order status: {} - Executed: {}/{}",
                        status.status,
                        status.executed_qty,
                        status.orig_qty
                    );

                    debug!("order: {:?}", status);

                    match status.status.as_str() {
                        "FILLED" => {
                            let executed_qty = status.executed_qty.parse::<f64>().unwrap_or(0.0);
                            let cum_quote_qty = status.cummulative_quote_qty
                                .parse::<f64>()
                                .unwrap_or(0.0);
                            let avg_price = if executed_qty > 0.0 {
                                cum_quote_qty / executed_qty
                            } else {
                                0.0
                            };

                            info!(
                                "✅ Order FILLED via REST check: {} {} @ {:.8}",
                                executed_qty,
                                symbol,
                                avg_price
                            );

                            return Ok(ExecutionResult {
                                client_order_id: status.client_order_id,
                                order_id: status.order_id.to_string(),
                                symbol: status.symbol,
                                side: status.side,
                                filled_qty: executed_qty,
                                avg_price,
                                cum_quote_qty,
                                leaves_qty: 0.0,
                                order_status: "FILLED".to_string(),
                                exec_type: "FILL".to_string(),
                                last_px: avg_price,
                                last_qty: executed_qty,
                                execution_time: start_time.elapsed(),
                                reject_reason: None,
                            });
                        }
                        "PARTIALLY_FILLED" => {
                            debug!("Order partially filled, continuing to poll...");
                        }
                        "REJECTED" | "CANCELED" | "EXPIRED" => {
                            return Err(
                                FixError::OrderRejected(
                                    format!(
                                        "Order {} with status: {}",
                                        client_order_id,
                                        status.status
                                    )
                                ).into()
                            );
                        }
                        _ => {
                            debug!("Order status: {}, continuing to poll...", status.status);
                        }
                    }
                }
                Err(e) => {
                    warn!("Error checking order status: {}", e);
                    poll_interval = (poll_interval * 2).min(max_poll_interval);
                }
            }

            std::thread::sleep(poll_interval);
            poll_interval = (poll_interval + Duration::from_millis(5)).min(max_poll_interval);
        }
    }

    fn check_order_status_rest(
        &self,
        symbol: &str,
        client_order_id: &str
    ) -> Result<OrderStatusResponse> {
        let rt = tokio::runtime::Runtime
            ::new()
            .map_err(|e|
                FixError::Connection(
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to create runtime: {e}")
                    )
                )
            )?;

        rt.block_on(async {
            let timestamp = chrono::Utc::now().timestamp_millis();
            let query_string = format!(
                "symbol={}&origClientOrderId={}&timestamp={}",
                symbol,
                client_order_id,
                timestamp
            );

            let signature = self.sign_query(&query_string);
            let url = format!(
                "https://api.binance.com/api/v3/order?{}&signature={}",
                query_string,
                signature
            );

            info!("url: {}", url);

            let response = self.http_client
                .get(&url)
                .header("X-MBX-APIKEY", &self.config.rest_api_key)
                .send().await
                .map_err(|e|
                    FixError::Connection(
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("REST API request failed: {e}")
                        )
                    )
                )?;

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(
                    FixError::Connection(
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("REST API error: {} - {}", status, text)
                        )
                    ).into()
                );
            }

            let order_status: OrderStatusResponse = response
                .json().await
                .map_err(|e| FixError::Parse(format!("Failed to parse REST response: {e}")))?;

            Ok(order_status)
        })
    }

    fn sign_query(&self, query: &str) -> String {
        use hmac::{ Hmac, Mac };
        use sha2::Sha256;

        type HmacSha256 = Hmac<Sha256>;

        let mut mac = HmacSha256::new_from_slice(self.config.rest_api_secret.as_bytes()).expect(
            "HMAC can take key of any size"
        );
        mac.update(query.as_bytes());

        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    #[inline]
    fn build_fix_message_fast(&self, fields: &[&str]) -> String {
        let mut body = String::with_capacity(512);
        for field in fields {
            body.push_str(field);
            body.push('\x01');
        }

        let header = format!("8=FIX.4.4\x019={}\x01", body.len());
        let message_without_checksum = header + &body;

        let checksum = message_without_checksum.bytes().fold(0u8, |acc, b| acc.wrapping_add(b));
        format!("{message_without_checksum}10={checksum:03}\x01")
    }

    #[inline]
    fn send_message_fast(&self, message: String) -> Result<()> {
        let mut stream_guard = self.stream.lock();
        if let Some(ref mut stream) = *stream_guard {
            stream.write_all(message.as_bytes())?;
            stream.flush()?;
            debug!("Sent FIX message: {}", message.replace('\x01', "|"));
        }
        Ok(())
    }

    #[inline]
    fn read_fix_message_fast(&self) -> Result<String> {
        let mut stream_guard = self.stream.lock();
        if let Some(ref mut stream) = *stream_guard {
            let mut reader = BufReader::with_capacity(8192, stream);

            let mut begin_buffer = Vec::with_capacity(16);
            match reader.read_until(0x01, &mut begin_buffer) {
                Ok(0) => {
                    return Err(
                        FixError::Connection(
                            std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Connection closed"
                            )
                        ).into()
                    );
                }
                Err(e) => {
                    return Err(FixError::Connection(e).into());
                }
                _ => {}
            }

            let begin_string = String::from_utf8_lossy(&begin_buffer);

            let mut body_length_buffer = Vec::with_capacity(16);
            match reader.read_until(0x01, &mut body_length_buffer) {
                Ok(0) => {
                    return Err(
                        FixError::Connection(
                            std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Connection closed"
                            )
                        ).into()
                    );
                }
                Err(e) => {
                    return Err(FixError::Connection(e).into());
                }
                _ => {}
            }

            let body_length_field = String::from_utf8_lossy(&body_length_buffer);

            if let Some(length_part) = body_length_field.trim_end_matches('\x01').split('=').nth(1) {
                if let Ok(body_length) = length_part.parse::<usize>() {
                    let mut body_buffer = vec![0u8; body_length + 7];
                    reader.read_exact(&mut body_buffer)?;

                    let full_message =
                        begin_string.to_string() +
                        &body_length_field +
                        &String::from_utf8_lossy(&body_buffer);
                    return Ok(full_message);
                }
            }

            Err(FixError::Parse("Could not parse message".to_string()).into())
        } else {
            Err(
                FixError::Connection(
                    std::io::Error::new(std::io::ErrorKind::NotConnected, "Not connected")
                ).into()
            )
        }
    }

    fn start_message_reader(&mut self) -> Result<()> {
        let shutdown = Arc::clone(&self.shutdown);
        let stream_ref = Arc::clone(&self.stream);
        let logout_received = Arc::clone(&self.logout_received);
        let logout_condvar = Arc::clone(&self.logout_condvar);

        let handle = thread::spawn(move || {
            Self::message_reader_thread(
                stream_ref,
                shutdown,
                logout_received,
                logout_condvar
            );
        });

        self.connection_handle = Some(handle);
        Ok(())
    }

    fn message_reader_thread(
        stream_ref: Arc<Mutex<Option<TlsStream<TcpStream>>>>,
        shutdown: Arc<AtomicBool>,
        logout_received: Arc<AtomicBool>,
        logout_condvar: Arc<(StdMutex<bool>, Condvar)>
    ) {
        info!("FIX message reader thread started");

        while !shutdown.load(Ordering::Relaxed) {
            match Self::read_fix_message_blocking(&stream_ref) {
                Ok(message) => {
                    debug!("Received FIX message: {}", message.replace('\x01', "|"));
                    if
                        let Err(e) = Self::process_incoming_message(
                            &message,
                            &logout_received,
                            &logout_condvar
                        )
                    {
                        error!("Error processing message: {}", e);
                    }
                }
                Err(e) => {
                    if
                        e.to_string().contains("Resource temporarily unavailable") ||
                        e.to_string().contains("timed out") ||
                        e.to_string().contains("Read timeout")
                    {
                        if shutdown.load(Ordering::Relaxed) {
                            debug!("Read timeout during shutdown, stopping message reader");
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                        continue;
                    } else {
                        error!("Error reading message: {}", e);
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        }

        info!("FIX message reader thread stopped");
    }

    fn read_fix_message_blocking(
        stream_ref: &Arc<Mutex<Option<TlsStream<TcpStream>>>>
    ) -> Result<String> {
        let mut stream_guard = stream_ref.lock();
        if let Some(ref mut stream) = *stream_guard {
            let mut reader = BufReader::with_capacity(8192, stream);

            let mut begin_buffer = Vec::with_capacity(16);
            match reader.read_until(0x01, &mut begin_buffer) {
                Ok(0) => {
                    return Err(
                        FixError::Connection(
                            std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Connection closed"
                            )
                        ).into()
                    );
                }
                Err(ref e) if
                    e.kind() == std::io::ErrorKind::WouldBlock ||
                    e.kind() == std::io::ErrorKind::TimedOut
                => {
                    return Err(
                        FixError::Connection(
                            std::io::Error::new(std::io::ErrorKind::TimedOut, "Read timeout")
                        ).into()
                    );
                }
                Err(e) => {
                    return Err(FixError::Connection(e).into());
                }
                _ => {}
            }

            let begin_string = String::from_utf8_lossy(&begin_buffer);

            let mut body_length_buffer = Vec::with_capacity(16);
            match reader.read_until(0x01, &mut body_length_buffer) {
                Ok(0) => {
                    return Err(
                        FixError::Connection(
                            std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Connection closed"
                            )
                        ).into()
                    );
                }
                Err(ref e) if
                    e.kind() == std::io::ErrorKind::WouldBlock ||
                    e.kind() == std::io::ErrorKind::TimedOut
                => {
                    return Err(
                        FixError::Connection(
                            std::io::Error::new(std::io::ErrorKind::TimedOut, "Read timeout")
                        ).into()
                    );
                }
                Err(e) => {
                    return Err(FixError::Connection(e).into());
                }
                _ => {}
            }

            let body_length_field = String::from_utf8_lossy(&body_length_buffer);

            if let Some(length_part) = body_length_field.trim_end_matches('\x01').split('=').nth(1) {
                if let Ok(body_length) = length_part.parse::<usize>() {
                    let mut body_buffer = vec![0u8; body_length + 7];
                    reader.read_exact(&mut body_buffer)?;

                    let full_message =
                        begin_string.to_string() +
                        &body_length_field +
                        &String::from_utf8_lossy(&body_buffer);
                    return Ok(full_message);
                }
            }

            Err(FixError::Parse("Could not parse message".to_string()).into())
        } else {
            Err(
                FixError::Connection(
                    std::io::Error::new(std::io::ErrorKind::NotConnected, "Not connected")
                ).into()
            )
        }
    }

    fn process_incoming_message(
        message: &str,
        logout_received: &Arc<AtomicBool>,
        logout_condvar: &Arc<(StdMutex<bool>, Condvar)>
    ) -> Result<()> {
        let fields = Self::parse_fix_message_fast(message);

        match fields.get("35").map(|s| s.as_str()) {
            Some("A") => {
                info!("Received Logon response");
            }
            Some("5") => {
                info!("Received Logout response from server");
                logout_received.store(true, Ordering::Release);
                let (lock, cvar) = &**logout_condvar;
                let mut logout_flag = lock.lock().unwrap();
                *logout_flag = true;
                cvar.notify_one();
            }
            Some("8") => {
                // Execution reports are now handled by WebSocket, just log for debugging
                if let Some(exec_result) = Self::parse_execution_report(&fields)? {
                    debug!("FIX execution report received (handled by WebSocket): {} - {}", 
                        exec_result.client_order_id, exec_result.order_status);
                }
            }
            Some("9") => {
                if let Some(reject_reason) = fields.get("58") {
                    error!("Order Cancel Reject: {}", reject_reason);
                }
            }
            Some("3") => {
                if let Some(reject_reason) = fields.get("58") {
                    error!("Session Reject: {}", reject_reason);
                }
            }
            Some("1") => {
                debug!("Received Test Request");
            }
            Some("0") => {
                debug!("Received Heartbeat");
            }
            _ => {
                debug!("Received unknown message type: {:?}", fields.get("35"));
            }
        }

        Ok(())
    }

    #[inline]
    fn parse_fix_message_fast(message: &str) -> HashMap<String, String> {
        let mut fields = HashMap::with_capacity(32);
        let mut start = 0;
        let bytes = message.as_bytes();

        while start < bytes.len() {
            if let Some(eq_pos) = bytes[start..].iter().position(|&b| b == b'=') {
                let eq_abs = start + eq_pos;
                if let Some(soh_pos) = bytes[eq_abs..].iter().position(|&b| b == 0x01) {
                    let soh_abs = eq_abs + soh_pos;

                    let key = unsafe { std::str::from_utf8_unchecked(&bytes[start..eq_abs]) };
                    let value = unsafe {
                        std::str::from_utf8_unchecked(&bytes[eq_abs + 1..soh_abs])
                    };

                    fields.insert(key.to_string(), value.to_string());
                    start = soh_abs + 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        fields
    }

    fn parse_execution_report(fields: &HashMap<String, String>) -> Result<Option<ExecutionResult>> {
        let msg_type = fields
            .get("35")
            .ok_or_else(|| FixError::Parse("Missing MsgType".to_string()))?;

        if msg_type != "8" {
            return Ok(None);
        }

        let client_order_id = fields.get("11").unwrap_or(&String::new()).clone();
        let order_id = fields.get("37").unwrap_or(&String::new()).clone();
        let symbol = fields.get("55").unwrap_or(&String::new()).clone();
        let side = fields.get("54").unwrap_or(&String::new()).clone();
        let exec_type = fields.get("150").unwrap_or(&String::new()).clone();
        let order_status = fields.get("39").unwrap_or(&String::new()).clone();

        let parse_float = |tag: &str| -> f64 {
            fields
                .get(tag)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0)
        };

        let filled_qty = parse_float("14");
        let avg_price = parse_float("6");
        let cum_quote_qty = parse_float("25017");
        let leaves_qty = parse_float("151");
        let last_px = parse_float("31");
        let last_qty = parse_float("32");

        let reject_reason = if order_status == "8" || exec_type == "8" {
            fields.get("58").cloned()
        } else {
            None
        };

        Ok(
            Some(ExecutionResult {
                client_order_id,
                order_id,
                symbol,
                side: match side.as_str() {
                    "1" => "BUY".to_string(),
                    "2" => "SELL".to_string(),
                    _ => side,
                },
                filled_qty,
                avg_price,
                cum_quote_qty,
                leaves_qty,
                order_status: match order_status.as_str() {
                    "0" => "NEW".to_string(),
                    "1" => "PARTIALLY_FILLED".to_string(),
                    "2" => "FILLED".to_string(),
                    "4" => "CANCELED".to_string(),
                    "8" => "REJECTED".to_string(),
                    _ => order_status,
                },
                exec_type: match exec_type.as_str() {
                    "0" => "NEW".to_string(),
                    "1" => "PARTIAL_FILL".to_string(),
                    "2" => "FILL".to_string(),
                    "4" => "CANCELED".to_string(),
                    "8" => "REJECTED".to_string(),
                    _ => exec_type,
                },
                last_px,
                last_qty,
                execution_time: Duration::ZERO,
                reject_reason,
            })
        )
    }

    fn start_heartbeat(&mut self) -> Result<()> {
        let interval_secs = self.fix_config.heartbeat_interval as u64;
        let shutdown = Arc::clone(&self.shutdown);
        let stream_ref = Arc::clone(&self.stream);
        let sender_comp_id = self.sender_comp_id.clone();
        let target_comp_id = self.fix_config.target_comp_id.clone();
        let seq_num = Arc::clone(&self.seq_num);

        let handle = thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                let mut remaining = Duration::from_secs(interval_secs);
                while remaining > Duration::ZERO && !shutdown.load(Ordering::Relaxed) {
                    let sleep_duration = remaining.min(Duration::from_millis(500));
                    thread::sleep(sleep_duration);
                    remaining = remaining.saturating_sub(sleep_duration);
                }

                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                let seq = seq_num.fetch_add(1, Ordering::Relaxed);
                let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

                let sender_comp_field = format!("49={sender_comp_id}");
                let target_comp_field = format!("56={target_comp_id}");
                let seq_field = format!("34={seq}");
                let sending_time_field = format!("52={sending_time}");

                let fields = [
                    "35=0",
                    &sender_comp_field,
                    &target_comp_field,
                    &seq_field,
                    &sending_time_field,
                ];

                let body = fields.join("\x01") + "\x01";
                let header = format!("8=FIX.4.4\x019={}\x01", body.len());
                let message = header + &body;
                let checksum = message.bytes().fold(0u8, |acc, b| acc.wrapping_add(b));
                let full_message = format!("{message}10={checksum:03}\x01");

                let mut stream_guard = stream_ref.lock();
                if let Some(ref mut stream) = *stream_guard {
                    if let Err(e) = stream.write_all(full_message.as_bytes()) {
                        error!("Failed to send heartbeat: {}", e);
                        break;
                    }
                    if let Err(e) = stream.flush() {
                        error!("Failed to flush heartbeat: {}", e);
                        break;
                    }
                }
            }
            debug!("Heartbeat thread stopped");
        });

        self.heartbeat_handle = Some(handle);
        Ok(())
    }

    pub fn disconnect(&mut self) -> Result<()> {
        info!("Initiating graceful FIX disconnect...");

        self.logout_received.store(false, Ordering::Release);
        {
            let (lock, _) = &*self.logout_condvar;
            let mut logout_flag = lock.lock().unwrap();
            *logout_flag = false;
        }

        let seq_num = self.seq_num.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

        let sender_comp_field = format!("49={}", self.sender_comp_id);
        let target_comp_field = format!("56={}", self.fix_config.target_comp_id);
        let seq_num_field = format!("34={seq_num}");
        let sending_time_field = format!("52={sending_time}");

        let fields = [
            "35=5",
            &sender_comp_field,
            &target_comp_field,
            &seq_num_field,
            &sending_time_field,
        ];

        let logout_message = self.build_fix_message_fast(&fields);

        if let Err(e) = self.send_message_fast(logout_message) {
            warn!("Failed to send logout message: {}", e);
        } else {
            info!("Logout message sent, waiting for server response...");

            let (lock, cvar) = &*self.logout_condvar;
            let logout_flag = lock.lock().unwrap();

            let wait_result = cvar
                .wait_timeout_while(
                    logout_flag,
                    Duration::from_secs(5),
                    |&mut logout_received| !logout_received
                )
                .unwrap();

            if wait_result.1.timed_out() {
                warn!("Timeout waiting for logout response from server");
            } else {
                info!("Logout response received from server");
            }
        }

        self.shutdown.store(true, Ordering::Release);
        self.is_connected.store(false, Ordering::Release);

        if let Some(handle) = self.heartbeat_handle.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.connection_handle.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.ws_handle.take() {
            let _ = handle.join();
        }

        {
            let mut stream_guard = self.stream.lock();
            if let Some(stream) = stream_guard.take() {
                drop(stream);
            }
        }

        info!("FIX client disconnected gracefully");
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    fn initialize_user_data_websocket(&mut self) -> Result<()> {
        if self.config.rest_api_secret.is_empty() {
            return Err(anyhow::anyhow!("REST API secret not configured"));
        }
        
        let mut ws_client = UserDataWebSocket::new(
            self.fix_config.api_key.clone(),
            self.config.rest_api_secret.clone()
        );
        
        // Create listen key using tokio runtime
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(ws_client.create_listen_key())?;
        
        // Connect to WebSocket
        let ws_stream = ws_client.connect()?;
        
        let ws_client_arc = Arc::new(Mutex::new(ws_client));
        self.user_data_ws = Some(ws_client_arc.clone());
        
        // Start WebSocket reader thread
        let shutdown = self.shutdown.clone();
        let handle = thread::spawn(move || {
            info!("Starting user data WebSocket reader thread");
            let mut ws_stream = ws_stream;
            
            while !shutdown.load(Ordering::Relaxed) {
                match ws_stream.read() {
                    Ok(msg) => {
                        if let Err(e) = ws_client_arc.lock().handle_message(msg) {
                            error!("Error handling WebSocket message: {}", e);
                        }
                    }
                    Err(e) => {
                        if !shutdown.load(Ordering::Relaxed) {
                            error!("WebSocket read error: {}", e);
                            break;
                        }
                    }
                }
            }
            
            info!("User data WebSocket reader thread stopped");
        });
        
        self.ws_handle = Some(handle);
        Ok(())
    }

    fn wait_for_order_via_websocket(
        &self,
        client_order_id: &str,
        timeout: Duration,
        start_time: Instant
    ) -> Result<ExecutionResult> {
        let (tx, mut rx) = mpsc::channel(1);
        
        // Register order with WebSocket
        if let Some(ref ws) = self.user_data_ws {
            ws.lock().register_order(client_order_id.to_string(), tx);
        } else {
            return Err(anyhow::anyhow!("WebSocket not initialized"));
        }
        
        // Wait for result with timeout
        let rt = tokio::runtime::Runtime::new()?;
        let result = rt.block_on(async {
            tokio::time::timeout(timeout, rx.recv()).await
        });
        
        match result {
            Ok(Some(mut exec_result)) => {
                exec_result.execution_time = start_time.elapsed();
                Ok(exec_result)
            }
            Ok(None) => Err(anyhow::anyhow!("Channel closed without result")),
            Err(_) => Err(FixError::Timeout.into()),
        }
    }
}
