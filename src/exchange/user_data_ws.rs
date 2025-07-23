use tungstenite::{
    http::Request,
    protocol::Message,
    handshake::client::generate_key,
    connect,
    WebSocket,
    stream::MaybeTlsStream,
};
use anyhow::{Result, Context, anyhow};
use tracing::{debug, info, warn};
use std::{
    net::TcpStream,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
    collections::HashMap,
};
use tokio::sync::mpsc;
use serde::Deserialize;
use crate::exchange::fix_api::ExecutionResult;

const BINANCE_USER_DATA_URL: &str = "wss://stream.binance.com:9443/ws";
const BINANCE_API_URL: &str = "https://api.binance.com";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListenKeyResponse {
    listen_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionReport {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "c")]
    client_order_id: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "o")]
    order_type: String,
    #[serde(rename = "f")]
    time_in_force: String,
    #[serde(rename = "q")]
    order_quantity: String,
    #[serde(rename = "p")]
    order_price: String,
    #[serde(rename = "P")]
    stop_price: String,
    #[serde(rename = "F")]
    iceberg_quantity: String,
    #[serde(rename = "g")]
    order_list_id: i64,
    #[serde(rename = "C")]
    original_client_order_id: String,
    #[serde(rename = "x")]
    current_execution_type: String,
    #[serde(rename = "X")]
    current_order_status: String,
    #[serde(rename = "r")]
    order_reject_reason: String,
    #[serde(rename = "i")]
    order_id: u64,
    #[serde(rename = "l")]
    last_executed_quantity: String,
    #[serde(rename = "z")]
    cumulative_filled_quantity: String,
    #[serde(rename = "L")]
    last_executed_price: String,
    #[serde(rename = "n")]
    commission_amount: String,
    #[serde(rename = "N")]
    commission_asset: Option<String>,
    #[serde(rename = "T")]
    transaction_time: u64,
    #[serde(rename = "t")]
    trade_id: i64,
    #[serde(rename = "I")]
    order_id_ignore: u64,
    #[serde(rename = "w")]
    is_order_on_book: bool,
    #[serde(rename = "m")]
    is_maker_side: bool,
    #[serde(rename = "M")]
    ignore_2: bool,
    #[serde(rename = "O")]
    order_creation_time: u64,
    #[serde(rename = "Z")]
    cumulative_quote_qty: String,
    #[serde(rename = "Y")]
    last_quote_qty: String,
    #[serde(rename = "Q")]
    quote_order_qty: String,
}

#[derive(Debug, Deserialize)]
struct RawUserDataEvent {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(flatten)]
    data: serde_json::Value,
}

pub struct UserDataWebSocket {
    api_key: String,
    api_secret: String,
    listen_key: Option<String>,
    last_ping: Instant,
    pending_orders: Arc<Mutex<HashMap<String, mpsc::Sender<ExecutionResult>>>>,
}

impl UserDataWebSocket {
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self {
            api_key,
            api_secret,
            listen_key: None,
            last_ping: Instant::now(),
            pending_orders: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn create_listen_key(&mut self) -> Result<String> {
        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/userDataStream", BINANCE_API_URL);
        
        let response = client
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("Failed to create listen key")?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to create listen key: {}", error_text));
        }

        let key_response: ListenKeyResponse = response
            .json()
            .await
            .context("Failed to parse listen key response")?;

        self.listen_key = Some(key_response.listen_key.clone());
        self.last_ping = Instant::now();
        
        info!("Created listen key: {}", key_response.listen_key);
        Ok(key_response.listen_key)
    }

    pub async fn keepalive_listen_key(&mut self) -> Result<()> {
        let listen_key = self.listen_key.as_ref()
            .ok_or_else(|| anyhow!("No listen key to keep alive"))?;

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/userDataStream", BINANCE_API_URL);
        
        let response = client
            .put(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .query(&[("listenKey", listen_key)])
            .send()
            .await
            .context("Failed to keepalive listen key")?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to keepalive listen key: {}", error_text));
        }

        self.last_ping = Instant::now();
        debug!("Listen key keepalive successful");
        Ok(())
    }

    pub fn connect(&self) -> Result<WebSocket<MaybeTlsStream<TcpStream>>> {
        let listen_key = self.listen_key.as_ref()
            .ok_or_else(|| anyhow!("No listen key available"))?;

        let ws_url = format!("{}/{}", BINANCE_USER_DATA_URL, listen_key);
        
        let request = Request::builder()
            .uri(&ws_url)
            .header("Sec-WebSocket-Key", generate_key())
            .header("Sec-WebSocket-Version", "13")
            .header("Host", "stream.binance.com")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .body(())
            .context("Failed to build request")?;

        let (ws_stream, _) = connect(request)
            .context("Failed to connect to Binance User Data WebSocket")?;

        info!("Connected to user data stream");
        Ok(ws_stream)
    }

    pub fn register_order(&self, client_order_id: String, sender: mpsc::Sender<ExecutionResult>) {
        let mut pending = self.pending_orders.lock().unwrap();
        pending.insert(client_order_id, sender);
    }

    pub fn handle_message(&self, msg: Message) -> Result<()> {
        match msg {
            Message::Text(text) => {
                // First parse to check event type
                match serde_json::from_str::<RawUserDataEvent>(&text) {
                    Ok(raw_event) => {
                        match raw_event.event_type.as_str() {
                            "executionReport" => {
                                // Parse the full execution report
                                match serde_json::from_str::<ExecutionReport>(&text) {
                                    Ok(report) => {
                                        self.process_execution_report(report)?;
                                    }
                                    Err(e) => {
                                        warn!("Failed to parse execution report: {}", e);
                                    }
                                }
                            }
                            "outboundAccountPosition" => {
                                debug!("Received account position update");
                            }
                            "balanceUpdate" => {
                                debug!("Received balance update");
                            }
                            other => {
                                debug!("Received other event type: {}", other);
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Failed to parse user data event: {}. Raw text: {}", e, text);
                    }
                }
            }
            Message::Binary(data) => {
                debug!("Received binary message: {} bytes", data.len());
            }
            Message::Ping(data) => {
                debug!("Received ping: {:?}", data);
            }
            Message::Pong(data) => {
                debug!("Received pong: {:?}", data);
            }
            Message::Close(frame) => {
                warn!("WebSocket close frame: {:?}", frame);
            }
            _ => {}
        }
        Ok(())
    }

    fn process_execution_report(&self, report: ExecutionReport) -> Result<()> {
        info!(
            "Execution Report: {} {} {} - Status: {}, Filled: {}/{}",
            report.symbol,
            report.side,
            report.client_order_id,
            report.current_order_status,
            report.cumulative_filled_quantity,
            report.order_quantity
        );

        let mut pending = self.pending_orders.lock().unwrap();
        
        if let Some(sender) = pending.get(&report.client_order_id) {
            let filled_qty = report.cumulative_filled_quantity.parse::<f64>().unwrap_or(0.0);
            let cum_quote_qty = report.cumulative_quote_qty.parse::<f64>().unwrap_or(0.0);
            let avg_price = if filled_qty > 0.0 {
                cum_quote_qty / filled_qty
            } else {
                0.0
            };

            let execution_result = ExecutionResult {
                client_order_id: report.client_order_id.clone(),
                order_id: report.order_id.to_string(),
                symbol: report.symbol,
                side: report.side,
                filled_qty,
                avg_price,
                cum_quote_qty,
                leaves_qty: report.order_quantity.parse::<f64>().unwrap_or(0.0) - filled_qty,
                order_status: report.current_order_status.clone(),
                exec_type: report.current_execution_type,
                last_px: report.last_executed_price.parse::<f64>().unwrap_or(0.0),
                last_qty: report.last_executed_quantity.parse::<f64>().unwrap_or(0.0),
                execution_time: Duration::from_millis(0), // Will be calculated by receiver
                reject_reason: if report.order_reject_reason != "NONE" {
                    Some(report.order_reject_reason)
                } else {
                    None
                },
            };

            // Only send result when order reaches final status, just log intermediate statuses
            if matches!(report.current_order_status.as_str(), "FILLED" | "REJECTED" | "CANCELED" | "EXPIRED") {
                info!("📋 Order {} final status: {}", report.client_order_id, report.current_order_status);
                let _ = sender.try_send(execution_result);
                pending.remove(&report.client_order_id);
            } else {
                debug!("📋 Order {} intermediate status: {}", report.client_order_id, report.current_order_status);
            }
        }

        Ok(())
    }

    pub fn needs_keepalive(&self) -> bool {
        self.last_ping.elapsed() > Duration::from_secs(1800) // 30 minutes
    }
}