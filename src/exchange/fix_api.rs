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
}

#[derive(Debug, Clone)]
pub struct SymbolPrecision {
    pub qty_precision: u8,
    pub price_precision: u8,
}

pub struct FixClient {
    config: FixConfig,
    sender_comp_id: String,
    seq_num: AtomicU32,
    pending_orders: Arc<Mutex<HashMap<String, OrderExecution>>>,
    signing_key: SigningKey,
    is_connected: AtomicBool,
    shutdown: Arc<AtomicBool>,
    connection_handle: Option<thread::JoinHandle<()>>,
    heartbeat_handle: Option<thread::JoinHandle<()>>,
    symbol_precisions: Arc<Mutex<HashMap<String, SymbolPrecision>>>,
    stream: Arc<Mutex<Option<TlsStream<TcpStream>>>>,
}

impl FixClient {
    pub fn new(config: FixConfig) -> Result<Self> {
        let key_bytes = general_purpose::STANDARD
            .decode(&config.private_key_base64)
            .map_err(|e| FixError::Parse(format!("Failed to decode private key: {}", e)))?;

        let signing_key = Self::parse_ed25519_key(&key_bytes)?;

        Ok(Self {
            config: config.clone(),
            sender_comp_id: config.api_key[..8].to_string(),
            seq_num: AtomicU32::new(1),
            pending_orders: Arc::new(Mutex::new(HashMap::with_capacity(64))),
            signing_key,
            is_connected: AtomicBool::new(false),
            shutdown: Arc::new(AtomicBool::new(false)),
            connection_handle: None,
            heartbeat_handle: None,
            symbol_precisions: Arc::new(Mutex::new(HashMap::with_capacity(256))),
            stream: Arc::new(Mutex::new(None)),
        })
    }

    #[inline]
    fn parse_ed25519_key(key_bytes: &[u8]) -> Result<SigningKey> {
        info!("Debugging private key format:");
        info!("  Key bytes length: {}", key_bytes.len());
        info!("  First 16 bytes: {:?}", &key_bytes[..(16).min(key_bytes.len())]);

        let key_str = String::from_utf8_lossy(key_bytes);
        if key_str.contains("-----BEGIN PRIVATE KEY-----") {
            info!("  Detected PEM format, extracting key data...");

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

            info!("  Extracted base64 content length: {}", base64_content.len());

            let pem_bytes = general_purpose::STANDARD
                .decode(&base64_content)
                .map_err(|e| FixError::Parse(format!("Failed to decode PEM content: {}", e)))?;

            info!("  PEM decoded bytes length: {}", pem_bytes.len());
            info!("  PEM decoded first 16 bytes: {:?}", &pem_bytes[..(16).min(pem_bytes.len())]);

            return Self::extract_ed25519_from_pkcs8(&pem_bytes);
        }

        if key_bytes.len() == 32 {
            info!("  Trying as raw 32-byte Ed25519 key");
            let key_array: [u8; 32] = key_bytes
                .try_into()
                .map_err(|_| FixError::Parse("Invalid 32-byte key".to_string()))?;
            return Ok(SigningKey::from_bytes(&key_array));
        } else if key_bytes.len() == 44 {
            info!("  Trying as 44-byte base64 encoded raw key");
            let decoded = general_purpose::STANDARD
                .decode(key_bytes)
                .map_err(|e| FixError::Parse(format!("Failed to decode 44-byte key: {}", e)))?;

            if decoded.len() == 32 {
                let key_array: [u8; 32] = decoded
                    .try_into()
                    .map_err(|_| FixError::Parse("Invalid decoded 32-byte key".to_string()))?;
                return Ok(SigningKey::from_bytes(&key_array));
            }
        } else if key_bytes.len() > 32 {
            info!("  Trying as PKCS#8 format");
            return Self::extract_ed25519_from_pkcs8(key_bytes);
        }

        Err(FixError::Parse(format!("Unsupported key format. Length: {}", key_bytes.len())).into())
    }

    #[inline]
    fn extract_ed25519_from_pkcs8(pkcs8_bytes: &[u8]) -> Result<SigningKey> {
        info!("  Extracting Ed25519 key from PKCS#8 format");

        if pkcs8_bytes.len() >= 32 {
            let key_start = pkcs8_bytes.len() - 32;
            let key_slice: [u8; 32] = pkcs8_bytes[key_start..]
                .try_into()
                .map_err(|_| FixError::Parse("Invalid PKCS#8 key extraction".to_string()))?;

            info!("  Extracted 32 bytes from position {}", key_start);
            info!("  Extracted key bytes: {:?}", &key_slice[..8]);

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
            None => format!("{:.8}", quantity),
        }
    }

    pub fn validate_credentials(&self) -> Result<()> {
        info!("Validating FIX credentials...");

        if self.config.api_key.is_empty() {
            return Err(FixError::Auth("API key is empty".to_string()).into());
        }

        if self.config.api_key.len() < 8 {
            return Err(FixError::Auth("API key appears to be too short".to_string()).into());
        }

        if self.config.private_key_base64.is_empty() {
            return Err(FixError::Auth("Private key is empty".to_string()).into());
        }

        let key_bytes = general_purpose::STANDARD
            .decode(&self.config.private_key_base64)
            .map_err(|e| FixError::Auth(format!("Invalid base64 private key: {}", e)))?;

        if key_bytes.len() < 32 {
            return Err(
                FixError::Auth(format!("Private key too short: {} bytes", key_bytes.len())).into()
            );
        }

        info!("✓ Credentials format validation passed");
        info!("  API Key: {}... ({} chars)", &self.config.api_key[..8], self.config.api_key.len());
        info!("  Private Key: {} bytes", key_bytes.len());

        Ok(())
    }

    pub fn test_tcp_connection(&self) -> Result<()> {
        info!("Testing TCP connection to {}...", self.config.endpoint);

        let tcp_stream = TcpStream::connect(&self.config.endpoint).context(
            "Failed to establish TCP connection"
        )?;

        tcp_stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        tcp_stream.set_write_timeout(Some(Duration::from_secs(5)))?;

        info!("✓ TCP connection successful");

        info!("Testing TLS handshake...");
        let connector = TlsConnector::new()?;
        let domain = self.config.endpoint.split(':').next().unwrap();
        let _tls_stream = connector.connect(domain, tcp_stream).context("TLS handshake failed")?;

        info!("✓ TLS handshake successful");
        Ok(())
    }

    pub fn connect(&mut self) -> Result<()> {
        info!("Starting FIX connection process...");

        self.validate_credentials().context("Credential validation failed")?;
        self.test_tcp_connection().context("Network connectivity test failed")?;

        info!("Establishing FIX connection to {}...", self.config.endpoint);

        let connector = TlsConnector::new()?;
        let tcp_stream = TcpStream::connect(&self.config.endpoint).context(
            "Failed to connect to FIX endpoint"
        )?;

        tcp_stream.set_nodelay(true)?;
        tcp_stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        tcp_stream.set_write_timeout(Some(Duration::from_secs(10)))?;

        let domain = self.config.endpoint.split(':').next().unwrap();
        let tls_stream = connector.connect(domain, tcp_stream).context("TLS handshake failed")?;

        info!("✓ TLS connection established");
        *self.stream.lock() = Some(tls_stream);

        self.send_logon_and_wait_response().context("FIX logon failed")?;

        self.start_heartbeat().context("Failed to start heartbeat")?;

        self.start_message_reader().context("Failed to start message reader")?;

        self.is_connected.store(true, Ordering::Release);
        info!("✓ FIX client connected successfully");
        Ok(())
    }

    fn send_logon_and_wait_response(&mut self) -> Result<()> {
        let seq_num = self.seq_num.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

        let signature_payload_with_soh = format!(
            "A\x01{}\x01{}\x01{}\x01{}",
            self.sender_comp_id,
            self.config.target_comp_id,
            seq_num,
            sending_time
        );

        let signature_payload = signature_payload_with_soh;

        let signature: Signature = self.signing_key.sign(signature_payload.as_bytes());
        let signature_base64 = general_purpose::STANDARD.encode(signature.to_bytes());

        info!("Logon details:");
        info!("  API Key: {}...", &self.config.api_key[..(8).min(self.config.api_key.len())]);
        info!("  SenderCompID: {}", self.sender_comp_id);
        info!("  TargetCompID: {}", self.config.target_comp_id);
        info!("  Sequence: {}", seq_num);
        info!("  SendingTime: {}", sending_time);
        info!("  Using signature format: WITH SOH separators");
        debug!("  Signature payload: {}", signature_payload.replace('\x01', "|"));
        debug!("  Signature (base64): {}", signature_base64);
        debug!("  Signature length: {}", signature_base64.len());

        if self.sender_comp_id.len() != 8 {
            warn!("SenderCompID length is not 8 characters: {}", self.sender_comp_id.len());
        }

        let sender_comp_field = format!("49={}", self.sender_comp_id);
        let target_comp_field = format!("56={}", self.config.target_comp_id);
        let seq_num_field = format!("34={}", seq_num);
        let sending_time_field = format!("52={}", sending_time);
        let heartbeat_field = format!("108={}", self.config.heartbeat_interval);
        let api_key_field = format!("553={}", self.config.api_key);
        let signature_len_field = format!("95={}", signature_base64.len());
        let signature_field = format!("96={}", signature_base64);

        let mut fields = Vec::with_capacity(12);
        fields.push("35=A");
        fields.push(&sender_comp_field);
        fields.push(&target_comp_field);
        fields.push(&seq_num_field);
        fields.push(&sending_time_field);
        fields.push("98=0");
        fields.push(&heartbeat_field);
        fields.push("141=Y");
        fields.push(&api_key_field);
        fields.push(&signature_len_field);
        fields.push(&signature_field);
        fields.push("25035=1");

        let message = self.build_fix_message_fast(&fields);
        info!("Sending logon message ({} bytes)", message.len());
        debug!("Logon message: {}", message.replace('\x01', "|"));

        self.send_message_fast(message)?;
        info!("Logon message sent, waiting for response...");

        let start_time = Instant::now();
        let timeout = Duration::from_secs(15);

        while start_time.elapsed() < timeout {
            match self.read_fix_message_fast() {
                Ok(response) => {
                    info!("Received response: {}", response.replace('\x01', "|"));

                    let fields = Self::parse_fix_message_fast(&response);
                    let msg_type = fields.get("35").unwrap_or(&"Unknown".to_string()).clone();

                    match msg_type.as_str() {
                        "A" => {
                            if let Some(text) = fields.get("58") {
                                if !text.is_empty() {
                                    info!("Logon response text: {}", text);
                                }
                            }

                            info!("✓ FIX logon successful with SOH format");
                            return Ok(());
                        }
                        "3" => {
                            let msg = "Unknown".to_string();
                            let reject_code = fields.get("373").unwrap_or(&msg);
                            error!("SOH format failed with code {}, trying direct concatenation...", reject_code);
                        }
                        "5" => {
                            let msg = "Server initiated logout".to_string();
                            let reason = fields.get("58").unwrap_or(&msg);
                            return Err(FixError::Auth(format!("Server logout: {}", reason)).into());
                        }
                        _ => {
                            warn!("Received unexpected message type {} during logon", msg_type);
                            continue;
                        }
                    }
                }
                Err(e) => {
                    if
                        e.to_string().contains("Connection closed") ||
                        e.to_string().contains("UnexpectedEof")
                    {
                        return Err(
                            FixError::Auth(
                                "Server closed connection during logon - possible authentication failure, \
                             IP not whitelisted, or insufficient API permissions".to_string()
                            ).into()
                        );
                    }

                    return Err(e);
                }
            }
        }

        Err(FixError::Auth("Timeout waiting for logon response".to_string()).into())
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

        format!("{}10={:03}\x01", message_without_checksum, checksum)
    }

    #[inline]
    fn send_message_fast(&self, message: String) -> Result<()> {
        debug!("sending");
        let mut stream_guard = self.stream.lock();
        if let Some(ref mut stream) = *stream_guard {
            debug!("right before the sending...");
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
                                "Connection closed while reading BeginString"
                            )
                        ).into()
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    return Err(FixError::Connection(e).into());
                }
            }

            let begin_string = String::from_utf8_lossy(&begin_buffer);

            let mut body_length_buffer = Vec::with_capacity(16);
            match reader.read_until(0x01, &mut body_length_buffer) {
                Ok(0) => {
                    return Err(
                        FixError::Connection(
                            std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Connection closed while reading BodyLength"
                            )
                        ).into()
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    return Err(FixError::Connection(e).into());
                }
            }

            let body_length_field = String::from_utf8_lossy(&body_length_buffer);

            if let Some(length_part) = body_length_field.trim_end_matches('\x01').split('=').nth(1) {
                if let Ok(body_length) = length_part.parse::<usize>() {
                    let mut body_buffer = vec![0u8; body_length + 7];
                    match reader.read_exact(&mut body_buffer) {
                        Ok(_) => {}
                        Err(e) => {
                            error!(
                                "Failed to read message body: expected {} bytes, error: {}",
                                body_length + 7,
                                e
                            );
                            return Err(FixError::Connection(e).into());
                        }
                    }

                    let full_message =
                        begin_string.to_string() +
                        &body_length_field +
                        &String::from_utf8_lossy(&body_buffer);
                    return Ok(full_message.replace('\n', ""));
                }
            }

            Err(
                FixError::Parse(
                    format!("Could not parse BodyLength from: {}", body_length_field)
                ).into()
            )
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

        let handle = thread::spawn(move || {
            Self::message_reader_thread(stream_ref, pending_orders, shutdown);
        });

        self.connection_handle = Some(handle);
        Ok(())
    }

    fn message_reader_thread(
        stream_ref: Arc<Mutex<Option<TlsStream<TcpStream>>>>,
        pending_orders: Arc<Mutex<HashMap<String, OrderExecution>>>,
        shutdown: Arc<AtomicBool>
    ) {
        info!("FIX message reader thread started");

        while !shutdown.load(Ordering::Relaxed) {
            match Self::read_fix_message_blocking(&stream_ref) {
                Ok(message) => {
                    debug!("Received FIX message: {}", message.replace('\x01', "|"));
                    if let Err(e) = Self::process_incoming_message(&message, &pending_orders) {
                        error!("Error processing message: {}", e);
                    }
                }
                Err(e) => {
                    error!("Error reading message: {}", e);

                    if let Some(FixError::Connection(_)) = e.downcast_ref::<FixError>() {
                        error!("Connection lost, stopping message reader");
                        break;
                    }

                    thread::sleep(Duration::from_millis(100));
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
                                "Connection closed while reading BeginString"
                            )
                        ).into()
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    debug!("error---- {}", e);
                    return Err(FixError::Connection(e).into());
                }
            }
            let begin_string = String::from_utf8_lossy(&begin_buffer);

            let mut body_length_buffer = Vec::with_capacity(16);
            match reader.read_until(0x01, &mut body_length_buffer) {
                Ok(0) => {
                    return Err(
                        FixError::Connection(
                            std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Connection closed while reading BodyLength"
                            )
                        ).into()
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    return Err(FixError::Connection(e).into());
                }
            }
            let body_length_field = String::from_utf8_lossy(&body_length_buffer);

            if let Some(length_part) = body_length_field.trim_end_matches('\x01').split('=').nth(1) {
                if let Ok(body_length) = length_part.parse::<usize>() {
                    let mut body_buffer = vec![0u8; body_length + 7];
                    match reader.read_exact(&mut body_buffer) {
                        Ok(_) => {}
                        Err(e) => {
                            error!(
                                "Failed to read message body: expected {} bytes, error: {}",
                                body_length + 7,
                                e
                            );
                            return Err(FixError::Connection(e).into());
                        }
                    }

                    let full_message =
                        begin_string.to_string() +
                        &body_length_field +
                        &String::from_utf8_lossy(&body_buffer);
                    return Ok(full_message);
                }
            }

            Err(
                FixError::Parse(
                    format!("Could not parse BodyLength from: {}", body_length_field)
                ).into()
            )
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
        pending_orders: &Arc<Mutex<HashMap<String, OrderExecution>>>
    ) -> Result<()> {
        let fields = Self::parse_fix_message_fast(message);

        match fields.get("35").map(|s| s.as_str()) {
            Some("A") => {
                info!("Received Logon response");
            }
            Some("8") => {
                if let Some(exec_result) = Self::parse_execution_report(&fields)? {
                    let client_order_id = exec_result.client_order_id.clone();

                    let mut orders = pending_orders.lock();
                    if let Some(order_exec) = orders.remove(&client_order_id) {
                        let mut result = exec_result;
                        result.execution_time = order_exec.start_time.elapsed();

                        {
                            let mut guard = order_exec.result.lock().unwrap();
                            *guard = Some(result);
                        }
                        order_exec.condvar.notify_one();
                    }
                }
            }
            Some("1") => {
                if let Some(test_req_id) = fields.get("112") {
                    debug!("Received Test Request: {}", test_req_id);
                }
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

        if let Some(text) = fields.get("58") {
            if !text.is_empty() && order_status != "2" && order_status != "NEW" {
                warn!("Order execution message: {}", text);
            }
        }

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
            })
        )
    }

    fn start_heartbeat(&mut self) -> Result<()> {
        let interval_secs = self.config.heartbeat_interval as u64;
        let shutdown = Arc::clone(&self.shutdown);
        let stream_ref = Arc::clone(&self.stream);
        let sender_comp_id = self.sender_comp_id.clone();
        let target_comp_id = self.config.target_comp_id.clone();

        let handle = thread::spawn(move || {
            let mut seq = 1u32;
            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(interval_secs));

                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

                let sender_comp_field = format!("49={}", sender_comp_id);
                let target_comp_field = format!("56={}", target_comp_id);
                let seq_field = format!("34={}", seq);
                let sending_time_field = format!("52={}", sending_time);

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
                let full_message = format!("{}10={:03}\x01", message, checksum);

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
                    debug!("Sent heartbeat");
                    seq += 1;
                }
            }
        });

        self.heartbeat_handle = Some(handle);
        Ok(())
    }

    pub fn place_market_order(&mut self, order: MarketOrder) -> Result<ExecutionResult> {
        let client_order_id = format!(
            "{}_{}",
            self.sender_comp_id,
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );

        let result = Arc::new(StdMutex::new(None));
        let condvar = Arc::new(Condvar::new());

        let order_exec = OrderExecution {
            result: result.clone(),
            condvar: condvar.clone(),
            start_time: Instant::now(),
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

        let seq_num_field = format!("34={}", seq_num);
        let sender_comp_field = format!("49={}", self.sender_comp_id);
        let sending_time_field = format!("52={}", sending_time);
        let target_comp_field = format!("56={}", self.config.target_comp_id);
        let client_order_id_field = format!("11={}", client_order_id);
        let symbol_field = format!("55={}", order.symbol.to_uppercase());
        let side_field = format!("54={}", order.side.to_fix_side());

        let quantity_field = if let Some(quote_qty) = order.quote_quantity {
            // ask
            let formatted_qty = self.format_quantity(quote_qty, order.qty_precision);
            format!("152={}", formatted_qty)
        } else {
            // bid
            let formatted_qty = self.format_quantity(order.quantity, order.qty_precision);
            format!("38={}", formatted_qty)
        };

        let mut fields = Vec::with_capacity(10);
        fields.push("35=D");
        fields.push(&seq_num_field);
        fields.push(&sender_comp_field);
        fields.push(&sending_time_field);
        fields.push(&target_comp_field);
        fields.push(&client_order_id_field);
        fields.push(&quantity_field);
        fields.push("40=1");
        fields.push(&side_field);
        fields.push(&symbol_field);

        let message = self.build_fix_message_fast(&fields);
        self.send_message_fast(message)?;

        let guard = result.lock().unwrap();
        let result = condvar.wait_timeout(guard, Duration::from_secs(10)).unwrap();

        debug!("exec result {:?}", result.0);

        match result.0.clone() {
            Some(exec_result) => {
                if exec_result.order_status == "NEW" || exec_result.order_status == "2" {
                    Ok(exec_result)
                } else {
                    Err(
                        FixError::Execution(
                            format!("Order not filled: {}", exec_result.order_status)
                        ).into()
                    )
                }
            }
            None => Err(FixError::Timeout.into()),
        }
    }

    pub fn disconnect(&mut self) -> Result<()> {
        self.shutdown.store(true, Ordering::Release);
        self.is_connected.store(false, Ordering::Release);

        let seq_num = self.seq_num.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

        let sender_comp_field = format!("49={}", self.sender_comp_id);
        let target_comp_field = format!("56={}", self.config.target_comp_id);
        let seq_num_field = format!("34={}", seq_num);
        let sending_time_field = format!("52={}", sending_time);

        let fields = [
            "35=5",
            &sender_comp_field,
            &target_comp_field,
            &seq_num_field,
            &sending_time_field,
        ];

        let logout_message = self.build_fix_message_fast(&fields);
        let _ = self.send_message_fast(logout_message);

        if let Some(handle) = self.heartbeat_handle.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.connection_handle.take() {
            let _ = handle.join();
        }

        info!("FIX client disconnected");
        Ok(())
    }
}
