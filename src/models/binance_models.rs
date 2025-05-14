use serde::{ Deserialize, Serialize };
use rust_decimal::Decimal;

/// Snapshot of an orderbook from Binance REST API
#[derive(Debug, Deserialize)]
pub struct BinanceOrderbookSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,

    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

/// Depth update message from Binance websocket
#[derive(Debug, Deserialize)]
pub struct BinanceDepthUpdate {
    pub e: String, // Event type
    pub E: u64, // Event time
    pub s: String, // Symbol
    pub U: u64, // First update ID
    pub u: u64, // Last update ID
    pub b: Vec<[String; 2]>, // Bids to update
    pub a: Vec<[String; 2]>, // Asks to update
}

/// Websocket message for subscribing to depth updates
#[derive(Debug, Serialize)]
pub struct DepthSubscription {
    pub method: String,
    pub params: Vec<String>,
    pub id: u64,
}

/// Response from Binance websocket subscription
#[derive(Debug, Deserialize)]
pub struct SubscriptionResponse {
    pub result: Option<String>,
    pub id: u64,
}
