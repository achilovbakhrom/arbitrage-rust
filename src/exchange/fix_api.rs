// src/exchange/fix_api.rs - Complete implementation with precision support
use base64::{ engine::general_purpose, Engine as _ };
use chrono::Utc;
use crossbeam_channel::{ bounded, unbounded, Receiver, Sender };
use ed25519_dalek::{ Signature, Signer, SigningKey };
use native_tls::{ TlsConnector, TlsStream };
use parking_lot::RwLock;
use serde::{ Deserialize, Serialize };
use std::collections::HashMap;
use std::io::{ Write, Read };
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

struct OrderExecution {
    sender: Sender<ExecutionResult>,
    start_time: Instant,
}

#[derive(Debug)]
enum InternalMessage {
    SendMessage(String),
    Heartbeat,
    Shutdown,
}

pub struct FixClient {
    config: FixConfig,
    seq_num: AtomicU32,
    pending_orders: Arc<RwLock<HashMap<String, OrderExecution>>>,
    signing_key: SigningKey,
    is_connected: AtomicBool,

    message_sender: Option<Sender<InternalMessage>>,
    shutdown: Arc<AtomicBool>,

    connection_handle: Option<thread::JoinHandle<()>>,
    heartbeat_handle: Option<thread::JoinHandle<()>>,

    symbol_precisions: Arc<RwLock<HashMap<String, SymbolPrecision>>>,
}

impl FixClient {
    pub fn new(config: FixConfig) -> Result<Self> {
        let key_bytes = general_purpose::STANDARD
            .decode(&config.private_key_base64)
            .map_err(|e| FixError::Parse(format!("Failed to decode private key: {}", e)))?;

        let signing_key = if key_bytes.len() == 32 {
            SigningKey::from_bytes(&key_bytes.try_into().unwrap())
        } else if key_bytes.len() > 32 {
            let key_start = key_bytes.len().saturating_sub(32);
            let key_slice: [u8; 32] = key_bytes[key_start..]
                .try_into()
                .map_err(|_| FixError::Parse("Invalid private key format".to_string()))?;
            SigningKey::from_bytes(&key_slice)
        } else {
            return Err(FixError::Parse("Private key too short".to_string()));
        };

        Ok(Self {
            config,
            seq_num: AtomicU32::new(1),
            pending_orders: Arc::new(RwLock::new(HashMap::new())),
            signing_key,
            is_connected: AtomicBool::new(false),
            message_sender: None,
            shutdown: Arc::new(AtomicBool::new(false)),
            connection_handle: None,
            heartbeat_handle: None,
            symbol_precisions: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn update_symbol_precision(&self, symbol: &str, qty_precision: u8, price_precision: u8) {
        let mut cache = self.symbol_precisions.write();
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

    pub fn connect(&mut self) -> Result<()> {
        eprintln!("Connecting to FIX endpoint: {}", self.config.endpoint);

        let (message_tx, message_rx) = unbounded();
        self.message_sender = Some(message_tx);

        let connector = TlsConnector::new()?;
        let tcp_stream = TcpStream::connect(&self.config.endpoint)?;

        tcp_stream.set_nodelay(true)?;
        tcp_stream.set_nonblocking(true)?; // Make it non-blocking
        tcp_stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        tcp_stream.set_write_timeout(Some(Duration::from_secs(1)))?;

        let domain = self.config.endpoint.split(':').next().unwrap();
        let mut tls_stream = connector.connect(domain, tcp_stream).unwrap();

        let logon_message = self.build_logon_message()?;
        tls_stream.write_all(logon_message.as_bytes())?;
        tls_stream.flush()?;

        let pending_orders = Arc::clone(&self.pending_orders);
        let shutdown = Arc::clone(&self.shutdown);

        let handle = thread::spawn(move || {
            Self::connection_handler(tls_stream, message_rx, pending_orders, shutdown);
        });

        self.connection_handle = Some(handle);

        self.start_heartbeat()?;

        self.is_connected.store(true, Ordering::Release);
        eprintln!("FIX client connected successfully");

        Ok(())
    }

    fn build_logon_message(&self) -> Result<String> {
        let seq_num = self.seq_num.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

        let signature = self.sign_logon_payload(&seq_num.to_string(), &sending_time);

        let mut body = String::new();

        body.push_str(&format!("34={}\x01", seq_num));
        body.push_str("35=A\x01");
        body.push_str(&format!("49={}\x01", self.config.sender_comp_id));
        body.push_str(&format!("52={}\x01", sending_time));
        body.push_str(&format!("56={}\x01", self.config.target_comp_id));
        body.push_str(&format!("95={}\x01", signature.len()));
        body.push_str(&format!("96={}\x01", signature));
        body.push_str("98=0\x01");
        body.push_str(&format!("108={}\x01", self.config.heartbeat_interval));
        body.push_str("141=Y\x01");
        body.push_str(&format!("553={}\x01", self.config.api_key));
        body.push_str("25035=1\x01");

        Ok(self.build_fix_message(&body))
    }

    fn sign_logon_payload(&self, seq_num: &str, sending_time: &str) -> String {
        let payload = format!(
            "A\x01{}\x01{}\x01{}\x01{}",
            self.config.sender_comp_id,
            self.config.target_comp_id,
            seq_num,
            sending_time
        );

        let signature: Signature = self.signing_key.sign(payload.as_bytes());
        general_purpose::STANDARD.encode(signature.to_bytes())
    }

    fn build_fix_message(&self, body: &str) -> String {
        let header = format!("8=FIX.4.4\x019={}\x01", body.len());
        let message = format!("{}{}", header, body);
        let checksum = self.calculate_checksum(&message);
        format!("{}10={:03}\x01", message, checksum)
    }

    fn calculate_checksum(&self, message: &str) -> u8 {
        message.bytes().fold(0u8, |acc, b| acc.wrapping_add(b))
    }

    fn connection_handler(
        mut stream: TlsStream<TcpStream>,
        message_rx: Receiver<InternalMessage>,
        pending_orders: Arc<RwLock<HashMap<String, OrderExecution>>>,
        shutdown: Arc<AtomicBool>
    ) {
        let mut buffer = Vec::new();
        let mut temp_buf = [0u8; 4096];

        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            while let Ok(msg) = message_rx.try_recv() {
                match msg {
                    InternalMessage::SendMessage(fix_msg) => {
                        if let Err(e) = stream.write_all(fix_msg.as_bytes()) {
                            eprintln!("Failed to send message: {}", e);
                            return;
                        }
                        if let Err(e) = stream.flush() {
                            eprintln!("Failed to flush message: {}", e);
                            return;
                        }
                    }
                    InternalMessage::Heartbeat => {
                        let heartbeat = Self::build_heartbeat_message();
                        if let Err(e) = stream.write_all(heartbeat.as_bytes()) {
                            eprintln!("Failed to send heartbeat: {}", e);
                            return;
                        }
                        if let Err(e) = stream.flush() {
                            eprintln!("Failed to flush heartbeat: {}", e);
                            return;
                        }
                    }
                    InternalMessage::Shutdown => {
                        return;
                    }
                }
            }

            match stream.read(&mut temp_buf) {
                Ok(0) => {
                    eprintln!("Connection closed by server");
                    break;
                }
                Ok(n) => {
                    buffer.extend_from_slice(&temp_buf[0..n]);

                    while let Some(pos) = buffer.iter().position(|&b| b == 0x01) {
                        let msg_bytes: Vec<u8> = buffer.drain(0..=pos).collect();
                        if let Ok(msg_str) = String::from_utf8(msg_bytes) {
                            if
                                let Err(e) = Self::process_incoming_message(
                                    &msg_str,
                                    &pending_orders
                                )
                            {
                                eprintln!("Error processing message: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    if
                        e.kind() == std::io::ErrorKind::WouldBlock ||
                        e.kind() == std::io::ErrorKind::TimedOut
                    {
                        // No data available, continue
                    } else {
                        eprintln!("Error reading from stream: {}", e);
                        break;
                    }
                }
            }

            thread::sleep(Duration::from_millis(1));
        }

        eprintln!("Connection handler thread exiting");
    }

    fn build_heartbeat_message() -> String {
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

        let body =
            format!("34=1\x01\
         35=0\x01\
         49=RUST_ARBITRAGE\x01\
         52={}\x01\
         56=SPOT\x01", sending_time);

        let header = format!("8=FIX.4.4\x019={}\x01", body.len());
        let message = format!("{}{}", header, body);
        let checksum = message.bytes().fold(0u8, |acc, b| acc.wrapping_add(b));
        format!("{}10={:03}\x01", message, checksum)
    }

    fn start_heartbeat(&mut self) -> Result<()> {
        let interval_secs = self.config.heartbeat_interval as u64;
        let shutdown = Arc::clone(&self.shutdown);
        let message_sender = self.message_sender.as_ref().unwrap().clone();

        let handle = thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(interval_secs));

                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                if let Err(e) = message_sender.send(InternalMessage::Heartbeat) {
                    eprintln!("Failed to send heartbeat signal: {}", e);
                    break;
                }
            }
        });

        self.heartbeat_handle = Some(handle);
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
                if let Some(test_req_id) = fields.get("112") {
                    eprintln!("Received Test Request: {}", test_req_id);
                }
            }
            Some("0") => {
                // Heartbeat - no action needed
            }
            _ => {
                // Unknown message type
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
                execution_time: Duration::ZERO,
            })
        )
    }

    pub fn place_market_order(&mut self, mut order: MarketOrder) -> Result<ExecutionResult> {
        let start_time = Instant::now();
        let client_order_id = format!(
            "{}_{}",
            self.config.sender_comp_id,
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );

        if order.qty_precision.is_none() {
            let cache = self.symbol_precisions.read();
            if let Some(precision_info) = cache.get(&order.symbol) {
                order.qty_precision = Some(precision_info.qty_precision);
                order.price_precision = Some(precision_info.price_precision);
            }
        }

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

        let mut body = String::new();
        body.push_str(&format!("11={}\x01", client_order_id));
        body.push_str(&format!("34={}\x01", seq_num));
        body.push_str("35=D\x01");

        if let Some(quote_qty) = order.quote_quantity {
            let formatted_qty = self.format_quantity(quote_qty, order.qty_precision);
            body.push_str(&format!("38={}\x01", formatted_qty));
        } else {
            let formatted_qty = self.format_quantity(order.quantity, order.qty_precision);
            body.push_str(&format!("38={}\x01", formatted_qty));
        }

        body.push_str("40=1\x01"); // OrdType (Market)
        body.push_str(&format!("49={}\x01", self.config.sender_comp_id));
        body.push_str(&format!("52={}\x01", sending_time));
        body.push_str(&format!("54={}\x01", order.side.to_fix_side()));
        body.push_str(&format!("55={}\x01", order.symbol.to_uppercase()));
        body.push_str(&format!("56={}\x01", self.config.target_comp_id));
        body.push_str("59=4\x01");

        if let Some(quote_qty) = order.quote_quantity {
            let formatted_cash_qty = self.format_quantity(quote_qty, order.qty_precision);
            body.push_str(&format!("152={}\x01", formatted_cash_qty));
        }

        let full_message = self.build_fix_message(&body);

        if let Some(sender) = &self.message_sender {
            sender
                .send(InternalMessage::SendMessage(full_message))
                .map_err(|_| {
                    FixError::Connection(
                        std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "Message channel closed"
                        )
                    )
                })?;
        }

        eprintln!(
            "Placed market order: {} {} {} (precision: {:?})",
            if matches!(order.side, OrderSide::Buy) {
                "BUY"
            } else {
                "SELL"
            },
            order.symbol,
            if order.quote_quantity.is_some() {
                self.format_quantity(order.quote_quantity.unwrap(), order.qty_precision)
            } else {
                self.format_quantity(order.quantity, order.qty_precision)
            },
            order.qty_precision
        );

        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(result) => Ok(result),
            Err(_) => {
                self.pending_orders.write().remove(&client_order_id);
                Err(FixError::Timeout)
            }
        }
    }

    pub fn disconnect(&mut self) -> Result<()> {
        self.shutdown.store(true, Ordering::Release);
        self.is_connected.store(false, Ordering::Release);

        let seq_num = self.seq_num.fetch_add(1, Ordering::Relaxed);
        let sending_time = Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string();

        let body = format!(
            "34={}\x01\
             35=5\x01\
             49={}\x01\
             52={}\x01\
             56={}\x01",
            seq_num,
            self.config.sender_comp_id,
            sending_time,
            self.config.target_comp_id
        );

        let logout_message = self.build_fix_message(&body);

        if let Some(sender) = &self.message_sender {
            let _ = sender.send(InternalMessage::SendMessage(logout_message));
            let _ = sender.send(InternalMessage::Shutdown);
        }

        if let Some(handle) = self.heartbeat_handle.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.connection_handle.take() {
            let _ = handle.join();
        }

        eprintln!("FIX client disconnected");
        Ok(())
    }
}
