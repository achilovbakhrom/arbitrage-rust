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
    pub sender_comp_id: String,
    pub target_comp_id: String,
    pub heartbeat_interval: u32,
    pub endpoint: String,
}

impl Default for FixConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            private_key_base64: String::new(),
            sender_comp_id: String::new(),
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
        // Decode the private key similar to Golang code
        let key_bytes = general_purpose::STANDARD
            .decode(&config.private_key_base64)
            .map_err(|e| FixError::Parse(format!("Failed to decode private key: {}", e)))?;

        // Parse Ed25519 private key from PKCS8 format
        let signing_key = if key_bytes.len() == 32 {
            SigningKey::from_bytes(&key_bytes.try_into().unwrap())
        } else if key_bytes.len() > 32 {
            // Extract the last 32 bytes (similar to Golang logic)
            let key_start = key_bytes.len().saturating_sub(32);
            let key_slice: [u8; 32] = key_bytes[key_start..]
                .try_into()
                .map_err(|_| FixError::Parse("Invalid private key format".to_string()))?;
            SigningKey::from_bytes(&key_slice)
        } else {
            return Err(FixError::Parse("Private key too short".to_string()).into());
        };

        // SenderCompID — первые 8 символов API_KEY (exactly like Golang)
        let sender_comp_id = if config.api_key.len() > 8 {
            config.api_key[..8].to_string()
        } else {
            config.api_key.clone()
        };

        // Create new config with correct SenderCompID
        let mut fixed_config = config;
        fixed_config.sender_comp_id = sender_comp_id;

        Ok(Self {
            config: fixed_config,
            seq_num: AtomicU32::new(1),
            pending_orders: Arc::new(Mutex::new(HashMap::new())),
            signing_key,
            is_connected: AtomicBool::new(false),
            shutdown: Arc::new(AtomicBool::new(false)),
            connection_handle: None,
            heartbeat_handle: None,
            symbol_precisions: Arc::new(Mutex::new(HashMap::new())),
            stream: Arc::new(Mutex::new(None)),
        })
    }

    pub fn update_symbol_precision(&self, symbol: &str, qty_precision: u8, price_precision: u8) {
        let mut cache = self.symbol_precisions.lock();
        cache.insert(symbol.to_string(), SymbolPrecision {
            qty_precision,
            price_precision,
        });
    }

    fn format_quantity(&self, quantity: f64, precision: Option<u8>) -> String {
        match precision {
            Some(p) => format!("{:.prec$}", quantity, prec = p as usize),
            None => format!("{:.8}", quantity),
        }
    }

    pub fn validate_credentials(&self) -> Result<()> {
        info!("Validating FIX credentials...");

        // Check API key format
        if self.config.api_key.is_empty() {
            return Err(FixError::Auth("API key is empty".to_string()).into());
        }

        if self.config.api_key.len() < 10 {
            return Err(FixError::Auth("API key appears to be too short".to_string()).into());
        }

        // Check private key format
        if self.config.private_key_base64.is_empty() {
            return Err(FixError::Auth("Private key is empty".to_string()).into());
        }

        // Test private key decoding
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

        // Test TLS handshake
        info!("Testing TLS handshake...");
        let connector = TlsConnector::new()?;
        let domain = self.config.endpoint.split(':').next().unwrap();
        let _tls_stream = connector.connect(domain, tcp_stream).context("TLS handshake failed")?;

        info!("✓ TLS handshake successful");
        Ok(())
    }

    pub fn connect(&mut self) -> Result<()> {
        info!("Starting FIX connection process...");

        // Step 1: Validate credentials
        self.validate_credentials().context("Credential validation failed")?;

        // Step 2: Test basic connectivity
        self.test_tcp_connection().context("Network connectivity test failed")?;

        // Step 3: Establish the actual connection
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

        // Step 4: Send logon and wait for response
        self.send_logon_and_wait_response().context("FIX logon failed")?;

        // Step 5: Start background threads after successful logon
        self.start_message_reader().context("Failed to start message reader")?;

        self.start_heartbeat().context("Failed to start heartbeat")?;

        self.is_connected.store(true, Ordering::Release);
        info!("✓ FIX client connected successfully");
        Ok(())
    }

    fn send_logon_and_wait_response(&mut self) -> Result<()> {
        // Send logon message
        let seq_num = self.seq_num.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

        // Create signature payload exactly like Golang: "A" + senderCompID + targetCompID + seqNum + sendingTime
        let payload = format!(
            "A{}{}{}{}",
            self.config.sender_comp_id,
            self.config.target_comp_id,
            seq_num,
            sending_time
        );

        let signature: Signature = self.signing_key.sign(payload.as_bytes());
        let signature_base64 = general_purpose::STANDARD.encode(signature.to_bytes());

        info!("Logon details:");
        info!("  API Key: {}...", &self.config.api_key[..(8).min(self.config.api_key.len())]);
        info!("  SenderCompID: {}", self.config.sender_comp_id);
        info!("  TargetCompID: {}", self.config.target_comp_id);
        info!("  Sequence: {}", seq_num);
        info!("  SendingTime: {}", sending_time);
        debug!("  Signature payload: {}", payload);
        debug!("  Signature (base64): {}", signature_base64);

        println!("Logon details:");
        println!("  API Key: {}...", &self.config.api_key[..(8).min(self.config.api_key.len())]);
        println!("  SenderCompID: {}", self.config.sender_comp_id);
        println!("  TargetCompID: {}", self.config.target_comp_id);
        println!("  Sequence: {}", seq_num);
        println!("  SendingTime: {}", sending_time);
        println!("  Signature payload: {}", payload);
        println!("  Signature (base64): {}", signature_base64);

        // Build logon message with exact field order from Golang
        let mut fields = Vec::new();
        fields.push(format!("35=A")); // MsgType=Logon
        fields.push(format!("49={}", self.config.sender_comp_id)); // SenderCompID
        fields.push(format!("56={}", self.config.target_comp_id)); // TargetCompID
        fields.push(format!("34={}", seq_num)); // MsgSeqNum
        fields.push(format!("52={}", sending_time)); // SendingTime
        fields.push(format!("98=0")); // EncryptMethod=None
        fields.push(format!("108={}", self.config.heartbeat_interval)); // HeartBtInt
        fields.push(format!("141=Y")); // ResetSeqNumFlag=Y
        fields.push(format!("553={}", self.config.api_key)); // Username (API Key)
        fields.push(format!("95={}", signature_base64.len())); // SecureDataLen
        fields.push(format!("96={}", signature_base64)); // SecureData (signature)
        fields.push(format!("25035=1")); // AppVerID

        let message = self.build_fix_message(&fields);
        info!("Sending logon message ({} bytes)", message.len());
        println!("Sending logon message ({} bytes)", message.len());
        debug!("Logon message: {}", message.replace('\x01', "|"));
        println!("Logon message: {}", message.replace('\x01', "|"));

        self.send_message(message)?;
        info!("Logon message sent, waiting for response...");
        println!("Logon message sent, waiting for response...");

        // Wait for response with multiple attempts
        let start_time = Instant::now();
        let timeout = Duration::from_secs(15); // Longer timeout
        let mut attempts = 0;

        while start_time.elapsed() < timeout && attempts < 3 {
            attempts += 1;
            info!("Reading response attempt {}...", attempts);
            println!("Reading response attempt {}...", attempts);

            match self.read_fix_message_sync() {
                Ok(response) => {
                    info!("Received response: {}", response.replace('\x01', "|"));
                    println!("Received response: {}", response.replace('\x01', "|"));

                    let fields = Self::parse_fix_message(&response);
                    let msg_type = fields.get("35").unwrap_or(&"Unknown".to_string()).clone();

                    match msg_type.as_str() {
                        "A" => {
                            // Logon response
                            if let Some(text) = fields.get("58") {
                                if !text.is_empty() {
                                    info!("Logon response text: {}", text);
                                }
                            }

                            // Check for session reject reason
                            if let Some(reject_reason) = fields.get("373") {
                                return Err(
                                    FixError::Auth(
                                        format!(
                                            "Logon rejected with code {}: {}",
                                            reject_reason,
                                            fields
                                                .get("58")
                                                .unwrap_or(&"No description".to_string())
                                        )
                                    ).into()
                                );
                            }

                            info!("✓ FIX logon successful");
                            return Ok(());
                        }
                        "3" => {
                            let msg1 = "Unknown reason".to_string();
                            let msg2 = "Unknown".to_string();
                            // Reject message
                            let reason = fields.get("58").unwrap_or(&msg1);
                            let ref_tag = fields.get("371").unwrap_or(&msg2);
                            return Err(
                                FixError::Auth(
                                    format!("Logon rejected (RefTag: {}): {}", ref_tag, reason)
                                ).into()
                            );
                        }
                        "5" => {
                            // Logout message
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
                    error!("Error reading logon response (attempt {}): {}", attempts, e);
                    println!("Error reading logon response (attempt {}): {}", attempts, e);

                    // Check if this is a connection closed error
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

                    if attempts >= 3 {
                        return Err(e);
                    }

                    // Wait a bit before retry
                    thread::sleep(Duration::from_millis(500));
                }
            }
        }

        Err(FixError::Auth("Timeout waiting for logon response".to_string()).into())
    }

    fn send_logon(&mut self) -> Result<()> {
        let seq_num = self.seq_num.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

        // Create signature payload like in Golang
        let payload = format!(
            "A{}{}{}{}",
            self.config.sender_comp_id,
            self.config.target_comp_id,
            seq_num,
            sending_time
        );

        let signature: Signature = self.signing_key.sign(payload.as_bytes());
        let signature_base64 = general_purpose::STANDARD.encode(signature.to_bytes());

        // Build logon message following FIX format from Golang
        let mut fields = Vec::new();
        fields.push(format!("35=A")); // MsgType
        fields.push(format!("49={}", self.config.sender_comp_id)); // SenderCompID
        fields.push(format!("56={}", self.config.target_comp_id)); // TargetCompID
        fields.push(format!("34={}", seq_num)); // MsgSeqNum
        fields.push(format!("52={}", sending_time)); // SendingTime
        fields.push(format!("98=0")); // EncryptMethod
        fields.push(format!("108={}", self.config.heartbeat_interval)); // HeartBtInt
        fields.push(format!("141=Y")); // ResetSeqNumFlag
        fields.push(format!("553={}", self.config.api_key)); // Username
        fields.push(format!("95={}", signature_base64.len())); // SecureDataLen
        fields.push(format!("96={}", signature_base64)); // SecureData
        fields.push(format!("25035=1")); // AppVerID

        let message = self.build_fix_message(&fields);
        self.send_message(message)?;

        // Read logon response
        let response = self.read_fix_message_sync()?;
        debug!("Logon response: {}", response);

        if !response.contains("35=A") {
            return Err(FixError::Auth("Invalid logon response".to_string()).into());
        }

        info!("FIX logon successful");
        Ok(())
    }

    fn build_fix_message(&self, fields: &[String]) -> String {
        // Proper FIX format: fields are separated by SOH (0x01)
        // The body consists of all fields joined with SOH, ending with SOH
        let body = fields.join("\x01") + "\x01";

        // FIX header: BeginString + BodyLength
        // Note: BodyLength includes everything from first field to checksum (exclusive)
        let header = format!("8=FIX.4.4\x019={}\x01", body.len());

        // Complete message without checksum
        let message_without_checksum = header + &body;

        // Calculate checksum over entire message (header + body)
        let checksum = self.calculate_checksum(&message_without_checksum);

        // Final message with checksum
        let final_message = format!("{}10={:03}\x01", message_without_checksum, checksum);

        // debug!("FIX message structure:");
        // debug!("  Header: {}", &header.replace('\x01', "|"));
        // debug!("  Body: {}", body.replace('\x01', "|"));
        // debug!("  Checksum: 10={:03}", checksum);
        // debug!("  Complete: {}", final_message.replace('\x01', "|"));

        final_message
    }

    fn calculate_checksum(&self, message: &str) -> u8 {
        message.bytes().fold(0u8, |acc, b| acc.wrapping_add(b))
    }

    fn send_message(&self, message: String) -> Result<()> {
        let mut stream_guard = self.stream.lock();
        if let Some(ref mut stream) = *stream_guard {
            stream.write_all(message.as_bytes())?;
            stream.flush()?;
            debug!("Sent FIX message: {}", message.replace('\x01', "|"));
        }
        Ok(())
    }

    fn read_fix_message_sync(&self) -> Result<String> {
        let mut stream_guard = self.stream.lock();
        if let Some(ref mut stream) = *stream_guard {
            let mut reader = BufReader::new(stream);

            // Read BeginString field (8=FIX.4.4\x01)
            let mut begin_string = String::new();
            match reader.read_line(&mut begin_string) {
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

            println!("begin_string {}", begin_string);

            // Handle the fact that read_line might not encounter \n, so read until \x01
            if !begin_string.ends_with('\x01') {
                let mut buffer = Vec::new();
                match reader.read_until(0x01, &mut buffer) {
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
                    Ok(_) => {
                        begin_string = String::from_utf8_lossy(&buffer).to_string();
                    }
                    Err(e) => {
                        return Err(FixError::Connection(e).into());
                    }
                }
            }

            // Read BodyLength field (9=XXX\x01)
            let mut body_length_buffer = Vec::new();
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

            // Parse body length similar to Golang
            if let Some(length_part) = body_length_field.trim_end_matches('\x01').split('=').nth(1) {
                if let Ok(body_length) = length_part.parse::<usize>() {
                    // Read the exact body length + 7 bytes for checksum field (10=XXX\x01)
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
                        begin_string + &body_length_field + &String::from_utf8_lossy(&body_buffer);
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

                    // Check if this is a connection error
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
            let mut reader = BufReader::new(stream);

            // Read BeginString field (8=FIX.4.4\x01)
            let mut begin_buffer = Vec::new();
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

            // Read BodyLength field (9=XXX\x01)
            let mut body_length_buffer = Vec::new();
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

            // Parse body length
            if let Some(length_part) = body_length_field.trim_end_matches('\x01').split('=').nth(1) {
                if let Ok(body_length) = length_part.parse::<usize>() {
                    // Read the exact body length + 7 bytes for checksum field (10=XXX\x01)
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
        let fields = Self::parse_fix_message(message);
        debug!("Received FIX message: {}", message.replace('\x01', "|"));

        match fields.get("35").map(|s| s.as_str()) {
            Some("A") => {
                info!("Received Logon response");
            }
            Some("8") => {
                // Execution Report
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
                // Test Request
                if let Some(test_req_id) = fields.get("112") {
                    debug!("Received Test Request: {}", test_req_id);
                    // Should respond with Heartbeat containing TestReqID
                }
            }
            Some("0") => {
                // Heartbeat
                debug!("Received Heartbeat");
            }
            _ => {
                debug!("Received unknown message type: {:?}", fields.get("35"));
            }
        }

        Ok(())
    }

    fn parse_fix_message(message: &str) -> HashMap<String, String> {
        message
            .trim_end()
            .split('\x01')
            .filter_map(|field| {
                let parts: Vec<&str> = field.splitn(2, '=').collect();
                if parts.len() == 2 {
                    Some((parts[0].to_string(), parts[1].to_string()))
                } else {
                    None
                }
            })
            .collect()
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

        // Parse numeric fields
        let parse_float = |tag: &str| -> f64 {
            fields
                .get(tag)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0)
        };

        let filled_qty = parse_float("14"); // CumQty
        let avg_price = parse_float("6"); // AvgPx
        let cum_quote_qty = parse_float("25017"); // CumQuoteQty (Binance custom field)
        let leaves_qty = parse_float("151"); // LeavesQty
        let last_px = parse_float("31"); // LastPx
        let last_qty = parse_float("32"); // LastQty

        // Check for error in execution
        if let Some(text) = fields.get("58") {
            if !text.is_empty() && order_status != "2" && order_status != "FILLED" {
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
        let sender_comp_id = self.config.sender_comp_id.clone();
        let target_comp_id = self.config.target_comp_id.clone();

        let handle = thread::spawn(move || {
            let mut seq = 1u32;
            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(interval_secs));

                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

                let fields = vec![
                    "35=0".to_string(), // MsgType=Heartbeat
                    format!("49={}", sender_comp_id),
                    format!("56={}", target_comp_id),
                    format!("34={}", seq),
                    format!("52={}", sending_time)
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
            self.config.sender_comp_id,
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

        // Get precision for the symbol if not provided in order
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

        // Build New Order Single message like in Golang
        let mut fields = Vec::new();
        fields.push("35=D".to_string()); // MsgType
        fields.push(format!("49={}", self.config.sender_comp_id));
        fields.push(format!("56={}", self.config.target_comp_id));
        fields.push(format!("34={}", seq_num));
        fields.push(format!("52={}", sending_time));
        fields.push(format!("11={}", client_order_id));
        fields.push(format!("55={}", order.symbol.to_uppercase()));
        fields.push("40=1".to_string()); // OrdType=Market
        fields.push(format!("54={}", order.side.to_fix_side()));

        // Add quantity based on order type with proper precision
        if let Some(quote_qty) = order.quote_quantity {
            let formatted_qty = self.format_quantity(quote_qty, order.qty_precision);
            fields.push(format!("152={}", formatted_qty)); // CashOrderQty for quote quantity
        } else {
            let formatted_qty = self.format_quantity(order.quantity, order.qty_precision);
            fields.push(format!("38={}", formatted_qty)); // OrderQty
        }

        // Add TimeInForce if needed
        fields.push("59=4".to_string()); // TimeInForce=IOC (Immediate or Cancel)

        let message = self.build_fix_message(&fields);
        self.send_message(message)?;

        // Wait for execution report
        let guard = result.lock().unwrap();
        let result = condvar.wait_timeout(guard, Duration::from_secs(10)).unwrap();

        match result.0.clone() {
            Some(exec_result) => {
                if exec_result.order_status == "FILLED" || exec_result.order_status == "2" {
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

    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    fn validate_connection(&self) -> Result<()> {
        let stream_guard = self.stream.lock();
        if stream_guard.is_none() {
            return Err(
                FixError::Connection(
                    std::io::Error::new(std::io::ErrorKind::NotConnected, "No active connection")
                ).into()
            );
        }
        Ok(())
    }

    /// Comprehensive connection test that can be run before attempting real connection
    pub fn run_connection_diagnostics(config: &FixConfig) -> Result<()> {
        info!("=== FIX Connection Diagnostics ===");

        // Test 1: Basic configuration
        info!("1. Checking configuration...");
        if config.api_key.is_empty() {
            return Err(FixError::Auth("API key is missing".to_string()).into());
        }
        if config.private_key_base64.is_empty() {
            return Err(FixError::Auth("Private key is missing".to_string()).into());
        }
        if config.endpoint.is_empty() {
            return Err(FixError::Auth("Endpoint is missing".to_string()).into());
        }
        info!("✓ Configuration check passed");

        // Test 2: Private key validation
        info!("2. Testing private key...");
        let key_bytes = general_purpose::STANDARD
            .decode(&config.private_key_base64)
            .map_err(|e| FixError::Auth(format!("Invalid base64 private key: {}", e)))?;

        let signing_key = if key_bytes.len() == 32 {
            SigningKey::from_bytes(&key_bytes.try_into().unwrap())
        } else if key_bytes.len() > 32 {
            let key_start = key_bytes.len().saturating_sub(32);
            let key_slice: [u8; 32] = key_bytes[key_start..]
                .try_into()
                .map_err(|_| FixError::Auth("Invalid private key format".to_string()))?;
            SigningKey::from_bytes(&key_slice)
        } else {
            return Err(FixError::Auth("Private key too short".to_string()).into());
        };

        // Test signing
        let test_payload = b"test_signature";
        let _signature = signing_key.sign(test_payload);
        info!("✓ Private key validation passed");

        // Test 3: Network connectivity
        info!("3. Testing network connectivity...");
        let tcp_stream = TcpStream::connect(&config.endpoint).context(
            "Cannot reach FIX endpoint - check firewall/network"
        )?;
        info!("✓ TCP connection successful");

        // Test 4: TLS handshake
        info!("4. Testing TLS handshake...");
        let connector = TlsConnector::new()?;
        let domain = config.endpoint.split(':').next().unwrap();
        let _tls_stream = connector
            .connect(domain, tcp_stream)
            .context("TLS handshake failed - check TLS version support")?;
        info!("✓ TLS handshake successful");

        info!("=== All diagnostics passed! ===");
        info!("If connection still fails, check:");
        info!("  • API key has FIX trading permissions enabled");
        info!("  • Your IP address is whitelisted for the API key");
        info!("  • API key is for the correct environment (mainnet/testnet)");

        Ok(())
    }

    pub fn test_connection(&self) -> Result<()> {
        if !self.is_connected() {
            return Err(
                FixError::Connection(
                    std::io::Error::new(std::io::ErrorKind::NotConnected, "Client not connected")
                ).into()
            );
        }

        self.validate_connection()?;
        info!("FIX connection test passed");
        Ok(())
    }

    pub fn disconnect(&mut self) -> Result<()> {
        self.shutdown.store(true, Ordering::Release);
        self.is_connected.store(false, Ordering::Release);

        // Send logout message
        let seq_num = self.seq_num.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

        let fields = vec![
            "35=5".to_string(), // MsgType=Logout
            format!("49={}", self.config.sender_comp_id),
            format!("56={}", self.config.target_comp_id),
            format!("34={}", seq_num),
            format!("52={}", sending_time)
        ];

        let logout_message = self.build_fix_message(&fields);
        let _ = self.send_message(logout_message);

        // Wait for threads to finish
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
