use base64::{ engine::general_purpose, Engine as _ };
use chrono::Utc;
use crossbeam_channel::{ bounded, select, Receiver, Sender };
use ed25519_dalek::{ Signature, Signer, SigningKey };
use native_tls::{ TlsConnector, TlsStream };
use parking_lot::{ Mutex, RwLock };
use serde::{ Deserialize, Serialize };
use std::collections::HashMap;
use std::io::{ BufRead, BufReader, Write };
use std::net::TcpStream;
use std::sync::atomic::{ AtomicBool, AtomicU32, Ordering };
use std::sync::Arc;
use std::thread;
use std::time::{ Duration, Instant };

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

type Result<T> = std::result::Result<T, FixError>;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    fn from_str(s: &str) -> Self {
        match s {
            "ask" => OrderSide::Buy,
            "bid" => OrderSide::Sell,
            _ => OrderSide::Buy, // default
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketOrder {
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub quote_quantity: Option<f64>,
}

struct OrderExecution {
    sender: Sender<ExecutionResult>,
    start_time: Instant,
}

pub struct FixClient {
    config: FixConfig,
    stream: Option<TlsStream<TcpStream>>,
    seq_num: AtomicU32,
    pending_orders: Arc<RwLock<HashMap<String, OrderExecution>>>,
    signing_key: SigningKey,
    is_connected: AtomicBool,
    _message_thread: Option<thread::JoinHandle<()>>,
    _heartbeat_thread: Option<thread::JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl FixClient {
    pub fn new(config: FixConfig) -> Result<Self> {
        // Decode the base64 private key
        let key_bytes = general_purpose::STANDARD
            .decode(&config.private_key_base64)
            .map_err(|e| FixError::Parse(format!("Failed to decode private key: {}", e)))?;

        // Parse Ed25519 private key
        let signing_key = if key_bytes.len() == 32 {
            SigningKey::from_bytes(&key_bytes.try_into().unwrap())
        } else {
            // Extract from PKCS#8 format (simplified)
            let key_start = key_bytes.len().saturating_sub(32);
            let key_slice: [u8; 32] = key_bytes[key_start..]
                .try_into()
                .map_err(|_| FixError::Parse("Invalid private key format".to_string()))?;
            SigningKey::from_bytes(&key_slice)
        };

        Ok(Self {
            config,
            stream: None,
            seq_num: AtomicU32::new(1),
            pending_orders: Arc::new(RwLock::new(HashMap::new())),
            signing_key,
            is_connected: AtomicBool::new(false),
            _message_thread: None,
            _heartbeat_thread: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn connect(&mut self) -> Result<()> {
        eprintln!("Connecting to FIX endpoint: {}", self.config.endpoint);

        // Create TLS connection
        let connector = TlsConnector::new()?;
        let tcp_stream = TcpStream::connect(&self.config.endpoint)?;

        // Set socket options for low latency
        tcp_stream.set_nodelay(true)?;
        tcp_stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        tcp_stream.set_write_timeout(Some(Duration::from_secs(1)))?;

        let domain = self.config.endpoint.split(':').next().unwrap();
        let tls_stream = connector.connect(domain, tcp_stream).unwrap();

        self.stream = Some(tls_stream);

        // Send logon message
        self.send_logon()?;

        // Start message processing threads
        self.start_message_processing()?;
        self.start_heartbeat();

        self.is_connected.store(true, Ordering::Release);
        eprintln!("FIX client connected successfully");

        Ok(())
    }

    fn send_logon(&mut self) -> Result<()> {
        let seq_num = self.seq_num.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();
        let signature = self.sign_logon_payload(&seq_num.to_string(), &sending_time);

        let logon_msg = format!(
            "35=A\x01\
             49={}\x01\
             56={}\x01\
             34={}\x01\
             52={}\x01\
             98=0\x01\
             108={}\x01\
             141=Y\x01\
             553={}\x01\
             95={}\x01\
             96={}\x01\
             25035=1\x01",
            self.config.sender_comp_id,
            self.config.target_comp_id,
            seq_num,
            sending_time,
            self.config.heartbeat_interval,
            self.config.api_key,
            signature.len(),
            signature
        );

        let full_message = self.build_fix_message(&logon_msg);
        self.send_raw_message(&full_message)?;

        eprintln!("Logon message sent");
        Ok(())
    }

    fn send_raw_message(&mut self, message: &str) -> Result<()> {
        if let Some(ref mut stream) = self.stream {
            stream.write_all(message.as_bytes())?;
            stream.flush()?;
        }
        Ok(())
    }

    fn build_fix_message(&self, body: &str) -> String {
        let header = format!("8=FIX.4.4\x019={}\x01", body.len());
        let message = format!("{}{}", header, body);
        let checksum = self.calculate_checksum(&message);
        format!("{}10={:03}\x01", message, checksum)
    }

    fn calculate_checksum(&self, message: &str) -> u8 {
        message.bytes().fold(0u8, |acc, b| acc.wrapping_add(b)) % 255
    }

    fn sign_logon_payload(&self, seq_num: &str, sending_time: &str) -> String {
        let payload = format!(
            "A{}{}{}{}",
            self.config.sender_comp_id,
            self.config.target_comp_id,
            seq_num,
            sending_time
        );

        let signature: Signature = self.signing_key.sign(payload.as_bytes());
        general_purpose::STANDARD.encode(signature.to_bytes())
    }

    fn start_message_processing(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            let pending_orders = Arc::clone(&self.pending_orders);
            let shutdown = Arc::clone(&self.shutdown);

            // Clone the stream for reading
            let read_stream = stream.get_mut().try_clone().unwrap();
            self.stream = Some(stream);

            let handle = thread::spawn(move || {
                let mut reader = BufReader::new(read_stream);
                let mut buffer = String::new();

                while !shutdown.load(Ordering::Relaxed) {
                    buffer.clear();

                    match reader.read_line(&mut buffer) {
                        Ok(0) => {
                            eprintln!("Connection closed by server");
                            break;
                        }
                        Ok(_) => {
                            if
                                let Err(e) = Self::process_incoming_message(
                                    &buffer,
                                    &pending_orders
                                )
                            {
                                eprintln!("Error processing message: {}", e);
                            }
                        }
                        Err(e) => {
                            if e.kind() != std::io::ErrorKind::TimedOut {
                                eprintln!("Error reading message: {}", e);
                                break;
                            }
                        }
                    }
                }
            });

            self._message_thread = Some(handle);
        }

        Ok(())
    }

    fn process_incoming_message(
        message: &str,
        pending_orders: &Arc<RwLock<HashMap<String, OrderExecution>>>
    ) -> Result<()> {
        let fields = Self::parse_fix_message(message);

        match fields.get("35").map(|s| s.as_str()) {
            Some("A") => {
                eprintln!("Received Logon response");
            }
            Some("8") => {
                // Execution Report
                if let Some(exec_result) = Self::parse_execution_report(&fields)? {
                    let client_order_id = exec_result.client_order_id.clone();

                    let mut orders = pending_orders.write();
                    if let Some(order_exec) = orders.remove(&client_order_id) {
                        let mut result = exec_result;
                        result.execution_time = order_exec.start_time.elapsed();

                        if let Err(_) = order_exec.sender.send(result) {
                            eprintln!("Failed to send execution result for order {}", client_order_id);
                        }
                    }
                }
            }
            Some("1") => {
                // Test Request - would need to send heartbeat response
                if let Some(test_req_id) = fields.get("112") {
                    eprintln!("Received Test Request: {}", test_req_id);
                }
            }
            Some("0") => {
                // Heartbeat - no action needed
            }
            _ => {
                // Unknown message type - ignore
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

        let filled_qty = fields
            .get("14")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        let avg_price = fields
            .get("6")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        let cum_quote_qty = fields
            .get("25017")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        let leaves_qty = fields
            .get("151")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        let last_px = fields
            .get("31")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        let last_qty = fields
            .get("32")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        Ok(
            Some(ExecutionResult {
                client_order_id,
                order_id,
                symbol,
                side,
                filled_qty,
                avg_price,
                cum_quote_qty,
                leaves_qty,
                order_status,
                exec_type,
                last_px,
                last_qty,
                execution_time: Duration::ZERO, // Will be set by caller
            })
        )
    }

    fn start_heartbeat(&mut self) {
        let interval_secs = self.config.heartbeat_interval as u64;
        let seq_num = self.seq_num.get_mut().clone();
        let sender_comp_id = self.config.sender_comp_id.clone();
        let target_comp_id = self.config.target_comp_id.clone();
        let shutdown = Arc::clone(&self.shutdown);

        let handle = thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(interval_secs));

                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

                let _heartbeat_msg = format!(
                    "35=0\x01\
                     49={}\x01\
                     56={}\x01\
                     34={}\x01\
                     52={}\x01",
                    sender_comp_id,
                    target_comp_id,
                    seq_num,
                    sending_time
                );

                // In a real implementation, you'd need a way to send this
                // through the write stream. For now, just preparing the message.
            }
        });

        self._heartbeat_thread = Some(handle);
    }

    pub fn place_market_order(&mut self, order: MarketOrder) -> Result<ExecutionResult> {
        let start_time = Instant::now();
        let client_order_id = format!(
            "{}_{}",
            self.config.sender_comp_id,
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );

        let (tx, rx) = bounded(1);
        {
            let mut pending = self.pending_orders.write();
            pending.insert(client_order_id.clone(), OrderExecution {
                sender: tx,
                start_time,
            });
        }

        let seq_num = self.seq_num.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

        let mut order_msg = format!(
            "35=D\x01\
             49={}\x01\
             56={}\x01\
             34={}\x01\
             52={}\x01\
             11={}\x01\
             55={}\x01\
             40=1\x01\
             54={}\x01",
            self.config.sender_comp_id,
            self.config.target_comp_id,
            seq_num,
            sending_time,
            client_order_id,
            order.symbol.to_uppercase(),
            order.side.to_fix_side()
        );

        // Add quantity fields based on order side
        match order.side {
            OrderSide::Buy => {
                if let Some(quote_qty) = order.quote_quantity {
                    order_msg.push_str(&format!("152={:.8}\x01", quote_qty));
                } else {
                    order_msg.push_str(&format!("38={:.8}\x01", order.quantity));
                }
            }
            OrderSide::Sell => {
                order_msg.push_str(&format!("38={:.8}\x01", order.quantity));
            }
        }

        let full_message = self.build_fix_message(&order_msg);
        self.send_raw_message(&full_message)?;

        eprintln!(
            "Placed market order: {} {} {}",
            if matches!(order.side, OrderSide::Buy) {
                "BUY"
            } else {
                "SELL"
            },
            order.symbol,
            order.quantity
        );

        // Wait for execution result with timeout
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(result) => Ok(result),
            Err(_) => {
                // Clean up pending order
                self.pending_orders.write().remove(&client_order_id);
                Err(FixError::Timeout)
            }
        }
    }

    pub fn disconnect(&mut self) -> Result<()> {
        self.shutdown.store(true, Ordering::Release);
        self.is_connected.store(false, Ordering::Release);

        if let Some(mut stream) = self.stream.take() {
            let _ = stream.shutdown();
        }

        eprintln!("FIX client disconnected");
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }
}
