use base64::{ engine::general_purpose, Engine as _ };
use chrono::Utc;
use ed25519_dalek::{ Signature, Signer, SigningKey };
use native_tls::{ TlsConnector, TlsStream };
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io::{ BufRead, BufReader, Write, Read };
use std::net::TcpStream;
use std::sync::atomic::{ AtomicBool, AtomicU32, Ordering };
use std::sync::{ Arc, Condvar, Mutex as StdMutex };
use std::thread;
use std::time::{ Duration, Instant };
use anyhow::{ Result, Context };
use tracing::{ info, error, debug, warn };

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

struct OrderExecution {
    result: Arc<StdMutex<Option<ExecutionResult>>>,
    condvar: Arc<Condvar>,
    start_time: Instant,
    completed: Arc<AtomicBool>,
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
    config: FixConfig,
    sender_comp_id: String,
    seq_num: Arc<AtomicU32>,
    pending_orders: Arc<Mutex<HashMap<String, OrderExecution>>>,
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
}

impl FixClient {
    pub fn new(config: FixConfig) -> Result<Self> {
        let key_bytes = general_purpose::STANDARD
            .decode(&config.private_key_base64)
            .map_err(|e| FixError::Parse(format!("Failed to decode private key: {e}")))?;

        let signing_key = Self::parse_ed25519_key(&key_bytes)?;

        Ok(Self {
            config: config.clone(),
            sender_comp_id: config.api_key[..8].to_string(),
            seq_num: Arc::new(AtomicU32::new(1)),
            pending_orders: Arc::new(Mutex::new(HashMap::with_capacity(64))),
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
        let tcp_stream = TcpStream::connect(&self.config.endpoint).context(
            "Failed to connect to FIX endpoint"
        )?;

        tcp_stream.set_nodelay(true)?;
        tcp_stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        tcp_stream.set_write_timeout(Some(Duration::from_secs(10)))?;

        let domain = self.config.endpoint.split(':').next().unwrap();
        let tls_stream = connector.connect(domain, tcp_stream).context("TLS handshake failed")?;

        info!("✓ TLS connection established");
        *self.stream.lock() = Some(tls_stream);

        self.send_logon_and_wait_response()?;
        self.start_heartbeat()?;
        self.start_message_reader()?;

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
            self.config.target_comp_id,
            seq_num,
            sending_time
        );

        let signature: Signature = self.signing_key.sign(signature_payload.as_bytes());
        let signature_base64 = general_purpose::STANDARD.encode(signature.to_bytes());

        let sender_comp_field = format!("49={}", self.sender_comp_id);
        let target_comp_field = format!("56={}", self.config.target_comp_id);
        let seq_num_field = format!("34={seq_num}");
        let sending_time_field = format!("52={sending_time}");
        let heartbeat_field = format!("108={}", self.config.heartbeat_interval);
        let api_key_field = format!("553={}", self.config.api_key);
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

        let result = Arc::new(StdMutex::new(None));
        let condvar = Arc::new(Condvar::new());
        let completed = Arc::new(AtomicBool::new(false));

        let order_exec = OrderExecution {
            result: Arc::clone(&result),
            condvar: Arc::clone(&condvar),
            start_time: Instant::now(),
            completed: Arc::clone(&completed),
        };

        {
            let mut pending = self.pending_orders.lock();
            pending.insert(client_order_id.clone(), order_exec);
        }

        let mut order = order;
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
        let target_comp_field = format!("56={}", self.config.target_comp_id);
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
        info!("Sending order: {} {} {}", order.side.to_fix_side(), order.symbol, if
            order.quote_quantity.is_some()
        {
            order.quote_quantity.unwrap()
        } else {
            order.quantity
        });

        self.send_message_fast(message)?;

        let timeout = Duration::from_secs(30);
        let start_time = Instant::now();

        info!("⏳ Waiting for order execution (market order should fill immediately)...");

        let mut order_accepted = false;
        let mut partial_fills = Vec::new();

        loop {
            if start_time.elapsed() > timeout {
                self.pending_orders.lock().remove(&client_order_id);
                return Err(FixError::Timeout.into());
            }

            if completed.load(Ordering::Relaxed) {
                let guard = result.lock().unwrap();
                if let Some(exec_result) = guard.clone() {
                    return Ok(exec_result);
                }
            }

            let guard = result.lock().unwrap();
            let (result_guard, timeout_result) = condvar
                .wait_timeout(guard, Duration::from_secs(2))
                .unwrap();

            if timeout_result.timed_out() {
                continue;
            }

            if let Some(exec_result) = result_guard.clone() {
                info!(
                    "📨 Received ExecutionReport: Status={}, ExecType={}, FilledQty={:.8}, LeavesQty={:.8}",
                    exec_result.order_status,
                    exec_result.exec_type,
                    exec_result.filled_qty,
                    exec_result.leaves_qty
                );

                if exec_result.order_status == "REJECTED" || exec_result.exec_type == "REJECTED" {
                    self.pending_orders.lock().remove(&client_order_id);
                    return Err(
                        FixError::OrderRejected(
                            exec_result.reject_reason.unwrap_or("Order rejected".to_string())
                        ).into()
                    );
                }

                if exec_result.order_status == "CANCELED" || exec_result.order_status == "EXPIRED" {
                    self.pending_orders.lock().remove(&client_order_id);
                    return Err(
                        FixError::OrderRejected(
                            format!("Order {}", exec_result.order_status.to_lowercase())
                        ).into()
                    );
                }

                if exec_result.order_status == "NEW" && !order_accepted {
                    order_accepted = true;
                    info!("✅ Market order accepted by exchange, waiting for fills...");
                    continue;
                }

                if exec_result.order_status == "PARTIALLY_FILLED" {
                    partial_fills.push((exec_result.filled_qty, exec_result.avg_price));
                    info!(
                        "🔄 Partial fill: {:.8} @ {:.8} (Total filled: {:.8}, Remaining: {:.8})",
                        exec_result.last_qty,
                        exec_result.last_px,
                        exec_result.filled_qty,
                        exec_result.leaves_qty
                    );
                    continue;
                }

                if exec_result.order_status == "FILLED" {
                    self.pending_orders.lock().remove(&client_order_id);
                    info!(
                        "🎉 Market order FULLY EXECUTED! Filled: {:.8} @ avg price {:.8} (Total value: {:.8})",
                        exec_result.filled_qty,
                        exec_result.avg_price,
                        exec_result.cum_quote_qty
                    );

                    if partial_fills.len() > 0 {
                        info!("📊 Order executed in {} fills", partial_fills.len() + 1);
                    }

                    return Ok(exec_result);
                }
            }
        }
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
        let pending_orders = Arc::clone(&self.pending_orders);
        let shutdown = Arc::clone(&self.shutdown);
        let stream_ref = Arc::clone(&self.stream);
        let logout_received = Arc::clone(&self.logout_received);
        let logout_condvar = Arc::clone(&self.logout_condvar);

        let handle = thread::spawn(move || {
            Self::message_reader_thread(
                stream_ref,
                pending_orders,
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
        pending_orders: Arc<Mutex<HashMap<String, OrderExecution>>>,
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
                            &pending_orders,
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
                        e.to_string().contains("timed out")
                    {
                        if shutdown.load(Ordering::Relaxed) {
                            debug!("Read timeout during shutdown, stopping message reader");
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(100));
                        continue;
                    } else {
                        error!("Error reading message: {}", e);
                        break;
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
            reader.read_until(0x01, &mut begin_buffer)?;
            let begin_string = String::from_utf8_lossy(&begin_buffer);

            let mut body_length_buffer = Vec::with_capacity(16);
            reader.read_until(0x01, &mut body_length_buffer)?;
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
        pending_orders: &Arc<Mutex<HashMap<String, OrderExecution>>>,
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
                if let Some(exec_result) = Self::parse_execution_report(&fields)? {
                    let client_order_id = exec_result.client_order_id.clone();

                    let orders = pending_orders.lock();
                    if let Some(order_exec) = orders.get(&client_order_id) {
                        let mut result = exec_result;
                        result.execution_time = order_exec.start_time.elapsed();

                        let is_final =
                            matches!(
                                result.order_status.as_str(),
                                "FILLED" | "REJECTED" | "CANCELED" | "EXPIRED"
                            ) || (result.leaves_qty == 0.0 && result.filled_qty > 0.0);

                        {
                            let mut guard = order_exec.result.lock().unwrap();
                            *guard = Some(result);
                        }

                        if is_final {
                            order_exec.completed.store(true, Ordering::Relaxed);
                        }

                        order_exec.condvar.notify_one();
                    }
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
        let interval_secs = self.config.heartbeat_interval as u64;
        let shutdown = Arc::clone(&self.shutdown);
        let stream_ref = Arc::clone(&self.stream);
        let sender_comp_id = self.sender_comp_id.clone();
        let target_comp_id = self.config.target_comp_id.clone();
        let seq_num = Arc::clone(&self.seq_num);

        let handle = thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(interval_secs));

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
        let target_comp_field = format!("56={}", self.config.target_comp_id);
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
}
