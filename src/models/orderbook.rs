// src/models/orderbook.rs
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{ Duration, Instant };

/// A price level in the orderbook
#[derive(Debug, Clone)]
pub struct Level {
    pub price: f64,
    pub quantity: f64,
}

/// Side of the orderbook (bids or asks)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderBookSide {
    Bids,
    Asks,
}

/// An order book for a single trading symbol
#[derive(Debug, Clone)]
pub struct OrderBook {
    /// Event type
    pub event_type: String,

    /// Event time
    pub event_time: i64,

    /// Symbol this orderbook is for
    pub symbol: String,

    /// First update ID in event
    pub first_update_id: u64,

    /// Last update ID in event
    pub last_update_id: u64,

    /// Bid side (buy orders)
    pub bids: Vec<Level>,

    /// Ask side (sell orders)
    pub asks: Vec<Level>,
}

/// In-memory orderbook state
#[derive(Debug, Clone)]
pub struct OrderBookState {
    /// Symbol this orderbook is for
    pub symbol: Arc<str>,

    /// Bid side (buy orders), ordered from highest to lowest price
    pub bids: BTreeMap<f64, f64>,

    /// Ask side (sell orders), ordered from lowest to highest price
    pub asks: BTreeMap<f64, f64>,

    /// Last update time
    pub last_update_time: Instant,

    /// Last update ID from the exchange
    pub last_update_id: u64,

    /// First update ID in the event
    pub first_update_id: u64,

    /// Whether this is a snapshot or an incremental update
    pub is_snapshot: bool,
}

impl OrderBookState {
    /// Create a new empty orderbook for a symbol
    pub fn new(symbol: Arc<str>) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_time: Instant::now(),
            last_update_id: 0,
            first_update_id: 0,
            is_snapshot: false,
        }
    }

    /// Update a price level in the orderbook
    pub fn update_price_level(&mut self, side: OrderBookSide, price: f64, quantity: f64) {
        let book = match side {
            OrderBookSide::Bids => &mut self.bids,
            OrderBookSide::Asks => &mut self.asks,
        };

        if quantity == 0.0 {
            // Remove the price level if quantity is zero
            book.remove(&price);
        } else {
            // Update or insert the price level
            book.insert(price, quantity);
        }

        self.last_update_time = Instant::now();
    }

    /// Apply a depth update to the orderbook
    pub fn apply_depth_update(&mut self, update: &OrderBook) {
        // Update bids
        for level in &update.bids {
            self.update_price_level(OrderBookSide::Bids, level.price, level.quantity);
        }

        // Update asks
        for level in &update.asks {
            self.update_price_level(OrderBookSide::Asks, level.price, level.quantity);
        }

        // Update metadata
        self.last_update_id = update.last_update_id;
        self.first_update_id = update.first_update_id;
        self.last_update_time = Instant::now();
    }

    /// Get the best bid price
    pub fn best_bid(&self) -> Option<f64> {
        // For bids, the highest price is the best
        self.bids.keys().next_back().copied()
    }

    /// Get the best ask price
    pub fn best_ask(&self) -> Option<f64> {
        // For asks, the lowest price is the best
        self.asks.keys().next().copied()
    }

    /// Check if the orderbook is valid (has both bids and asks)
    pub fn is_valid(&self) -> bool {
        !self.bids.is_empty() && !self.asks.is_empty()
    }

    /// Check if the orderbook is stale (no updates for a while)
    pub fn is_stale(&self, max_age: Duration) -> bool {
        self.last_update_time.elapsed() > max_age
    }
}

/// Manager for multiple orderbooks
#[derive(Debug)]
pub struct OrderBookManager {
    /// Orderbooks indexed by symbol
    orderbooks: RwLock<BTreeMap<Arc<str>, Arc<RwLock<OrderBookState>>>>,

    /// Maximum age for orderbooks before considered stale
    max_age: Duration,
}

impl OrderBookManager {
    /// Create a new orderbook manager
    pub fn new(max_age: Duration) -> Self {
        Self {
            orderbooks: RwLock::new(BTreeMap::new()),
            max_age,
        }
    }

    /// Get or create an orderbook for a symbol
    pub async fn get_or_create_orderbook(&self, symbol: Arc<str>) -> Arc<RwLock<OrderBookState>> {
        let mut orderbooks = self.orderbooks.write().await;

        if let Some(orderbook) = orderbooks.get(&symbol) {
            return orderbook.clone();
        }

        // Create a new orderbook if it doesn't exist
        let orderbook = Arc::new(RwLock::new(OrderBookState::new(symbol.clone())));
        orderbooks.insert(symbol.clone(), orderbook.clone());

        orderbook
    }

    /// Apply a depth update to an orderbook
    pub async fn apply_depth_update(&self, symbol: Arc<str>, update: OrderBook) {
        let orderbook = self.get_or_create_orderbook(symbol).await;
        let mut ob = orderbook.write().await;
        ob.apply_depth_update(&update);
    }

    /// Get an orderbook for a symbol if it exists
    pub async fn get_orderbook(&self, symbol: &str) -> Option<Arc<RwLock<OrderBookState>>> {
        let orderbooks = self.orderbooks.read().await;
        orderbooks.get(symbol).cloned()
    }

    /// Check if an orderbook is available and valid for a symbol
    pub async fn is_orderbook_valid(&self, symbol: &str) -> bool {
        if let Some(orderbook) = self.get_orderbook(symbol).await {
            let ob = orderbook.read().await;
            ob.is_valid() && !ob.is_stale(self.max_age)
        } else {
            false
        }
    }

    /// Get the best bid price for a symbol
    pub async fn get_best_bid(&self, symbol: &str) -> Option<f64> {
        if let Some(orderbook) = self.get_orderbook(symbol).await {
            let ob = orderbook.read().await;
            ob.best_bid()
        } else {
            None
        }
    }

    /// Get the best ask price for a symbol
    pub async fn get_best_ask(&self, symbol: &str) -> Option<f64> {
        if let Some(orderbook) = self.get_orderbook(symbol).await {
            let ob = orderbook.read().await;
            ob.best_ask()
        } else {
            None
        }
    }
}
