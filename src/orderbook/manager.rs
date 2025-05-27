// use std::collections::HashMap;
// use std::sync::Arc;
// use tokio::sync::Mutex;
// use parking_lot::RwLock;
// use tracing::{ debug, error, info, warn };
// use anyhow::{ Result, Context };
// use std::time::{ Duration, Instant };
// use dashmap::DashMap;

// use crate::models::level::Level;
// use crate::exchange::client::ExchangeClient;
// use super::orderbook::OrderBook;

// // Use a trait object callback for maximum flexibility and zero overhead
// pub type OrderbookUpdateCallback = Arc<dyn Fn(&Arc<str>, &Arc<OrderBook>) + Send + Sync>;

// pub struct OrderBookManager {
//     /// Maps symbol to orderbook
//     pub books: Arc<DashMap<Arc<str>, Arc<OrderBook>>>,
//     /// Default depth to use for new orderbooks
//     default_depth: usize,
//     /// Exchange client for API requests
//     exchange_client: Arc<dyn ExchangeClient + Send + Sync>,
//     /// Mutex for the recovery process
//     recovery_mutex: Arc<Mutex<()>>,
//     /// Tracks last recovery attempt per symbol to avoid too frequent retries
//     last_recovery: Arc<DashMap<Arc<str>, Instant>>,

//     // New field: update callbacks
//     update_callbacks: Arc<RwLock<Vec<OrderbookUpdateCallback>>>,
// }

// impl OrderBookManager {
//     /// Create a new orderbook manager with default depth
//     #[inline]
//     pub fn new(
//         default_depth: usize,
//         exchange_client: Arc<dyn ExchangeClient + Send + Sync>
//     ) -> Self {
//         Self {
//             books: Arc::new(DashMap::with_capacity(1000)),
//             default_depth,
//             exchange_client: exchange_client.clone(),
//             recovery_mutex: Arc::new(tokio::sync::Mutex::new(())),
//             last_recovery: Arc::new(DashMap::with_capacity(1000)),
//             update_callbacks: Arc::new(RwLock::new(Vec::with_capacity(10))),
//         }
//     }

//     /// Register a callback to be notified when an orderbook is updated
//     #[inline]
//     pub fn register_update_callback(&self, callback: OrderbookUpdateCallback) {
//         let mut callbacks = self.update_callbacks.write();
//         callbacks.push(callback);
//     }

//     /// Notify all registered callbacks about an orderbook update
//     /// Optimized to avoid unnecessary locking and cloning
//     #[inline]
//     fn notify_update(&self, symbol: &Arc<str>, book: &Arc<OrderBook>) {
//         // Fast path: first check if we have any callbacks at all
//         {
//             let callbacks = self.update_callbacks.read();
//             if callbacks.is_empty() {
//                 return;
//             }

//             // Execute callbacks without holding the lock
//             for callback in callbacks.iter() {
//                 callback(symbol, book);
//             }
//         }
//     }

//     /// Get or create an orderbook for a symbol - fast path optimized
//     pub fn get_or_create_book(&self, symbol: &Arc<str>) -> Arc<OrderBook> {
//         if let Some(book) = self.books.get(symbol) {
//             return book.clone();
//         }

//         // Not found, insert if absent (atomic operation with DashMap)
//         let book = Arc::new(OrderBook::new(self.default_depth));

//         // Note: entry API ensures atomic get_or_insert
//         let result = self.books.entry(symbol.clone()).or_insert(book).clone();
//         debug!(?symbol, "Created new orderbook");

//         result
//     }

//     pub fn apply_snapshot(
//         &self,
//         symbol: Arc<str>,
//         bids: &[(f64, f64)],
//         asks: &[(f64, f64)],
//         last_update_id: u64
//     ) {
//         // Get or create the orderbook
//         let book = self.get_or_create_book(&symbol);

//         // Convert to Level structs
//         let bids_levels: Vec<Level> = bids
//             .iter()
//             .map(|(price, qty)| Level { price: *price, quantity: *qty })
//             .collect();

//         let asks_levels: Vec<Level> = asks
//             .iter()
//             .map(|(price, qty)| Level { price: *price, quantity: *qty })
//             .collect();

//         // Apply the snapshot
//         book.apply_snapshot(bids_levels, asks_levels, last_update_id);

//         // Reset recovery tracker - using DashMap remove
//         self.last_recovery.remove(&symbol);

//         // Notify callbacks about the update
//         self.notify_update(&symbol, &book);

//         debug!(
//             symbol = %symbol,
//             update_id = last_update_id,
//             bid_count = bids.len(),
//             ask_count = asks.len(),
//             "Applied orderbook snapshot"
//         );
//     }

//     pub async fn trigger_recovery(&self, symbol: &Arc<str>) {
//         if symbol.as_ref().trim().is_empty() {
//             error!("Cannot trigger recovery for empty symbol");
//             return;
//         }

//         // Check if we've attempted recovery recently (don't spam with requests)
//         // Using DashMap get instead of RwLock read
//         if let Some(last_time) = self.last_recovery.get(symbol) {
//             if last_time.elapsed() < Duration::from_secs(3) {
//                 debug!(
//                     symbol = %symbol,
//                     "Skipping recovery, too soon since last attempt: {:?}",
//                     last_time.elapsed()
//                 );
//                 return;
//             }
//         }

//         // Mark that we're attempting recovery - using DashMap insert
//         self.last_recovery.insert(symbol.clone(), Instant::now());

//         // Clone what we need for the task
//         let symbol_clone = symbol.clone();
//         let manager = self.clone();

//         // Launch async task for recovery
//         tokio::spawn(async move {
//             // Add some jitter to avoid all symbols recovering at the same time
//             let jitter = rand::random::<u64>() % 200;
//             tokio::time::sleep(Duration::from_millis(jitter)).await;
//             // Use a short timeout for the lock to avoid blocking other symbols
//             let _lock = manager.recovery_mutex.lock().await;

//             info!(
//                 symbol = %symbol_clone,
//                 "Starting orderbook recovery process"
//             );

//             // Fetch snapshot from REST API
//             match manager.fetch_snapshot(symbol_clone.as_ref()).await {
//                 Ok((snapshot_bids, snapshot_asks, snapshot_last_id)) => {
//                     if snapshot_bids.is_empty() || snapshot_asks.is_empty() {
//                         warn!(
//                             symbol = %symbol_clone,
//                             "Empty orderbook snapshot received"
//                         );
//                         return;
//                     }

//                     // Apply the snapshot
//                     manager.apply_snapshot(
//                         symbol_clone.clone(),
//                         &snapshot_bids,
//                         &snapshot_asks,
//                         snapshot_last_id
//                     );

//                     info!(
//                         symbol = %symbol_clone,
//                         "Orderbook recovery completed successfully"
//                     );
//                 }
//                 Err(e) => {
//                     error!(
//                         symbol = %symbol_clone,
//                         error = %e,
//                         "Failed to fetch orderbook snapshot"
//                     );
//                 }
//             }
//         });
//     }

//     pub fn apply_depth_update(
//         &self,
//         symbol: &Arc<str>,
//         bids: &[(f64, f64)],
//         asks: &[(f64, f64)],
//         first_update_id: u64,
//         last_update_id: u64
//     ) -> bool {
//         // Get existing book without creating if missing - using DashMap get
//         let book = {
//             match self.books.get(symbol) {
//                 Some(book_ref) => book_ref.clone(),
//                 None => {
//                     // If book doesn't exist, we trigger recovery process
//                     debug!(
//                         symbol = %symbol,
//                         "Book doesn't exist, triggering recovery"
//                     );

//                     // self.trigger_recovery(symbol);

//                     let symbol_clone = symbol.clone();
//                     let self_clone = self.clone();
//                     tokio::spawn(async move {
//                         self_clone.trigger_recovery(&symbol_clone).await;
//                     });
//                     return false;
//                 }
//             }
//         };

//         // Try to apply the update
//         match book.apply_depth_update(bids, asks, first_update_id, last_update_id) {
//             Ok(true) => {
//                 // Update applied successfully
//                 self.notify_update(symbol, &book);
//                 true
//             }
//             Ok(false) => {
//                 // Book out of sync, need recovery
//                 debug!(
//                     symbol = %symbol,
//                     first_id = first_update_id,
//                     last_id = last_update_id,
//                     book_last_id = book.get_last_update_id(),
//                     "Book out of sync, triggering recovery"
//                 );
//                 // self.trigger_recovery(symbol);
//                 let symbol_clone = symbol.clone();
//                 let self_clone = self.clone();
//                 tokio::spawn(async move {
//                     self_clone.trigger_recovery(&symbol_clone).await;
//                 });
//                 false
//             }
//             Err(_) => {
//                 // Error applying update
//                 warn!(
//                     symbol = %symbol,
//                     "Error applying depth update, triggering recovery"
//                 );
//                 // self.trigger_recovery(symbol);
//                 let symbol_clone = symbol.clone();
//                 let self_clone = self.clone();
//                 tokio::spawn(async move {
//                     self_clone.trigger_recovery(&symbol_clone).await;
//                 });
//                 false
//             }
//         }
//     }

//     /// Get the best bid and ask for a symbol - fast path optimized
//     #[inline]
//     pub fn get_top_of_book(&self, symbol: &Arc<str>) -> Option<(Option<Level>, Option<Level>)> {
//         // Fast path with DashMap
//         match self.books.get(symbol) {
//             Some(book_ref) if book_ref.is_synced() => { Some(book_ref.snapshot()) }
//             Some(_) => {
//                 // Book exists but not in sync, trigger recovery without waiting
//                 // self.trigger_recovery(symbol);

//                 let symbol_clone = symbol.clone();
//                 let self_clone = self.clone();
//                 tokio::spawn(async move {
//                     self_clone.trigger_recovery(&symbol_clone).await;
//                 });
//                 None
//             }
//             None => None,
//         }
//     }

//     /// Fetch full orderbook snapshot from the exchange
//     pub async fn fetch_snapshot(
//         &self,
//         symbol: &str
//     ) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>, u64)> {
//         if symbol.trim().is_empty() {
//             return Err(anyhow::anyhow!("Cannot fetch snapshot for empty symbol"));
//         }

//         // Use the exchange client to fetch depth data
//         let depth_response = self.exchange_client
//             .fetch_depth(symbol, self.default_depth).await
//             .context(format!("Failed to fetch orderbook snapshot for {}", symbol))?;

//         // Convert Level structs to (f64, f64) tuples for consistency
//         let bids = depth_response.bids
//             .iter()
//             .map(|level| (level.price, level.quantity))
//             .collect::<Vec<(f64, f64)>>();

//         let asks = depth_response.asks
//             .iter()
//             .map(|level| (level.price, level.quantity))
//             .collect::<Vec<(f64, f64)>>();

//         Ok((bids, asks, depth_response.last_update_id))
//     }

//     /// Check if an orderbook is initialized
//     pub async fn is_initialized(&self, symbol: &Arc<str>) -> bool {
//         if let Some(book_ref) = self.books.get(symbol) { book_ref.is_synced() } else { false }
//     }

//     /// Get all the symbols that have orderbooks
//     pub async fn get_symbols(&self) -> Vec<Arc<str>> {
//         self.books
//             .iter()
//             .map(|entry| entry.key().clone())
//             .collect()
//     }

//     /// Get statistics about all orderbooks
//     pub async fn get_stats(&self) -> HashMap<Arc<str>, (bool, u64)> {
//         let mut stats = HashMap::with_capacity(self.books.len());

//         for entry in self.books.iter() {
//             stats.insert(entry.key().clone(), (entry.is_synced(), entry.get_last_update_id()));
//         }

//         stats
//     }

//     /// Get the mid-price for a symbol
//     pub fn get_mid_price(&self, symbol: &Arc<str>) -> Option<f64> {
//         if let Some(book_ref) = self.books.get(symbol) {
//             if book_ref.is_synced() { book_ref.mid_price() } else { None }
//         } else {
//             None
//         }
//     }

//     /// Get multiple price levels for a symbol
//     pub fn get_price_levels(
//         &self,
//         symbol: &Arc<str>,
//         depth: Option<usize>
//     ) -> Option<(Vec<Level>, Vec<Level>)> {
//         if let Some(book_ref) = self.books.get(symbol) {
//             if book_ref.is_synced() {
//                 Some((book_ref.get_bids(depth), book_ref.get_asks(depth)))
//             } else {
//                 None
//             }
//         } else {
//             None
//         }
//     }

//     /// Ensure all specified symbols have initialized orderbooks
//     pub async fn ensure_all_initialized(&self, symbols: &[Arc<str>]) -> Result<()> {
//         let mut failed_symbols = Vec::new();

//         for symbol in symbols {
//             let book = self.get_or_create_book(symbol);

//             if !book.is_synced() {
//                 match self.fetch_snapshot(symbol.as_ref()).await {
//                     Ok((bids, asks, last_update_id)) => {
//                         self.apply_snapshot(symbol.clone(), &bids, &asks, last_update_id);
//                     }
//                     Err(e) => {
//                         error!(
//                             symbol = %symbol,
//                             error = %e,
//                             "Failed to initialize orderbook"
//                         );
//                         failed_symbols.push(symbol.clone());
//                     }
//                 }
//             }
//         }

//         if failed_symbols.is_empty() {
//             Ok(())
//         } else {
//             let symbols_str = failed_symbols
//                 .iter()
//                 .map(|s| s.as_ref())
//                 .collect::<Vec<&str>>()
//                 .join(", ");

//             Err(anyhow::anyhow!("Failed to initialize orderbooks for: {}", symbols_str))
//         }
//     }

//     /// Clear all orderbooks
//     pub async fn clear_all(&self) {
//         self.books.clear();
//         debug!("Cleared all orderbooks");
//     }
// }

// // Proper Clone implementation for OrderBookManager that preserves all data
// impl Clone for OrderBookManager {
//     fn clone(&self) -> Self {
//         Self {
//             books: self.books.clone(), // Clone Arc, not creating new empty map
//             default_depth: self.default_depth,
//             exchange_client: self.exchange_client.clone(),
//             recovery_mutex: self.recovery_mutex.clone(), // Clone Arc<Mutex>
//             last_recovery: self.last_recovery.clone(), // Clone Arc, not creating new empty map
//             update_callbacks: self.update_callbacks.clone(), // Clone Arc, not creating new empty vector
//         }
//     }
// }

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use parking_lot::RwLock;
use tracing::{ debug, error, info, warn };
use anyhow::{ Result, Context };
use std::time::{ Duration, Instant };
use dashmap::DashMap;
use std::thread;

use crate::models::level::Level;
use crate::exchange::client::ExchangeClient;
use super::orderbook::OrderBook;

// Use a trait object callback for maximum flexibility and zero overhead
pub type OrderbookUpdateCallback = Arc<dyn Fn(&Arc<str>, &Arc<OrderBook>) + Send + Sync>;

pub struct OrderBookManager {
    /// Maps symbol to orderbook
    pub books: Arc<DashMap<Arc<str>, Arc<OrderBook>>>,
    /// Default depth to use for new orderbooks
    default_depth: usize,
    /// Exchange client for API requests
    exchange_client: Arc<dyn ExchangeClient + Send + Sync>,
    /// Mutex for the recovery process
    recovery_mutex: Arc<Mutex<()>>,
    /// Tracks last recovery attempt per symbol to avoid too frequent retries
    last_recovery: Arc<DashMap<Arc<str>, Instant>>,
    /// Update callbacks
    update_callbacks: Arc<RwLock<Vec<OrderbookUpdateCallback>>>,
    /// Runtime handle for async operations
    runtime_handle: tokio::runtime::Handle,
}

impl OrderBookManager {
    /// Create a new orderbook manager with default depth
    #[inline]
    pub fn new(
        default_depth: usize,
        exchange_client: Arc<dyn ExchangeClient + Send + Sync>
    ) -> Self {
        // Get the current runtime handle, or create a new runtime if none exists
        let runtime_handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                // If no runtime exists, create a new one in a separate thread
                let (tx, rx) = std::sync::mpsc::channel();
                thread::spawn(move || {
                    let rt = tokio::runtime::Builder
                        ::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .build()
                        .expect("Failed to create runtime for OrderBookManager");

                    tx.send(rt.handle().clone()).expect("Failed to send runtime handle");

                    // Keep the runtime alive
                    rt.block_on(async {
                        loop {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    });
                });
                rx.recv().expect("Failed to receive runtime handle")
            }
        };

        Self {
            books: Arc::new(DashMap::with_capacity(1000)),
            default_depth,
            exchange_client: exchange_client.clone(),
            recovery_mutex: Arc::new(Mutex::new(())),
            last_recovery: Arc::new(DashMap::with_capacity(1000)),
            update_callbacks: Arc::new(RwLock::new(Vec::with_capacity(10))),
            runtime_handle,
        }
    }

    /// Alternative constructor that accepts a runtime handle
    #[inline]
    pub fn with_runtime(
        default_depth: usize,
        exchange_client: Arc<dyn ExchangeClient + Send + Sync>,
        runtime_handle: tokio::runtime::Handle
    ) -> Self {
        Self {
            books: Arc::new(DashMap::with_capacity(1000)),
            default_depth,
            exchange_client: exchange_client.clone(),
            recovery_mutex: Arc::new(Mutex::new(())),
            last_recovery: Arc::new(DashMap::with_capacity(1000)),
            update_callbacks: Arc::new(RwLock::new(Vec::with_capacity(10))),
            runtime_handle,
        }
    }

    /// Register a callback to be notified when an orderbook is updated
    #[inline]
    pub fn register_update_callback(&self, callback: OrderbookUpdateCallback) {
        let mut callbacks = self.update_callbacks.write();
        callbacks.push(callback);
    }

    /// Notify all registered callbacks about an orderbook update
    /// Optimized to avoid unnecessary locking and cloning
    #[inline]
    fn notify_update(&self, symbol: &Arc<str>, book: &Arc<OrderBook>) {
        // Fast path: first check if we have any callbacks at all
        {
            let callbacks = self.update_callbacks.read();
            if callbacks.is_empty() {
                return;
            }

            // Execute callbacks without holding the lock
            for callback in callbacks.iter() {
                callback(symbol, book);
            }
        }
    }

    /// Get or create an orderbook for a symbol - fast path optimized
    pub fn get_or_create_book(&self, symbol: &Arc<str>) -> Arc<OrderBook> {
        if let Some(book) = self.books.get(symbol) {
            return book.clone();
        }

        // Not found, insert if absent (atomic operation with DashMap)
        let book = Arc::new(OrderBook::new(self.default_depth));

        // Note: entry API ensures atomic get_or_insert
        let result = self.books.entry(symbol.clone()).or_insert(book).clone();
        debug!(?symbol, "Created new orderbook");

        result
    }

    pub fn apply_snapshot(
        &self,
        symbol: Arc<str>,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        last_update_id: u64
    ) {
        // Get or create the orderbook
        let book = self.get_or_create_book(&symbol);

        // Convert to Level structs
        let bids_levels: Vec<Level> = bids
            .iter()
            .map(|(price, qty)| Level { price: *price, quantity: *qty })
            .collect();

        let asks_levels: Vec<Level> = asks
            .iter()
            .map(|(price, qty)| Level { price: *price, quantity: *qty })
            .collect();

        // Apply the snapshot
        book.apply_snapshot(bids_levels, asks_levels, last_update_id);

        // Reset recovery tracker - using DashMap remove
        self.last_recovery.remove(&symbol);

        // Notify callbacks about the update
        self.notify_update(&symbol, &book);

        debug!(
            symbol = %symbol,
            update_id = last_update_id,
            bid_count = bids.len(),
            ask_count = asks.len(),
            "Applied orderbook snapshot"
        );
    }

    pub fn trigger_recovery(&self, symbol: &Arc<str>) {
        if symbol.as_ref().trim().is_empty() {
            error!("Cannot trigger recovery for empty symbol");
            return;
        }

        // Check if we've attempted recovery recently (don't spam with requests)
        if let Some(last_time) = self.last_recovery.get(symbol) {
            if last_time.elapsed() < Duration::from_secs(3) {
                debug!(
                    symbol = %symbol,
                    "Skipping recovery, too soon since last attempt: {:?}",
                    last_time.elapsed()
                );
                return;
            }
        }

        // Mark that we're attempting recovery
        self.last_recovery.insert(symbol.clone(), Instant::now());

        // Clone what we need for the task
        let symbol_clone = symbol.clone();
        let manager = self.clone();

        // Spawn recovery task on the runtime
        self.runtime_handle.spawn(async move {
            manager.recovery_task(symbol_clone).await;
        });
    }

    /// Internal async recovery task
    async fn recovery_task(&self, symbol: Arc<str>) {
        // Add some jitter to avoid all symbols recovering at the same time
        let jitter = rand::random::<u64>() % 200;
        tokio::time::sleep(Duration::from_millis(jitter)).await;

        // Try to acquire the lock, but don't hold it across await points
        let can_proceed = {
            match self.recovery_mutex.try_lock() {
                Ok(_lock) => {
                    // Lock acquired, drop it immediately by letting it go out of scope
                    true
                }
                Err(_) => {
                    debug!(symbol = %symbol, "Recovery already in progress, skipping");
                    false
                }
            }
        };

        if !can_proceed {
            return;
        }

        info!(
            symbol = %symbol,
            "Starting orderbook recovery process"
        );

        // Fetch snapshot from REST API
        match self.fetch_snapshot(symbol.as_ref()).await {
            Ok((snapshot_bids, snapshot_asks, snapshot_last_id)) => {
                if snapshot_bids.is_empty() || snapshot_asks.is_empty() {
                    warn!(
                        symbol = %symbol,
                        "Empty orderbook snapshot received"
                    );
                    return;
                }

                // Apply the snapshot
                self.apply_snapshot(
                    symbol.clone(),
                    &snapshot_bids,
                    &snapshot_asks,
                    snapshot_last_id
                );

                info!(
                    symbol = %symbol,
                    "Orderbook recovery completed successfully"
                );
            }
            Err(e) => {
                error!(
                    symbol = %symbol,
                    error = %e,
                    "Failed to fetch orderbook snapshot"
                );
            }
        }
    }

    pub fn apply_depth_update(
        &self,
        symbol: &Arc<str>,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        first_update_id: u64,
        last_update_id: u64
    ) -> bool {
        // Get existing book without creating if missing
        let book = {
            match self.books.get(symbol) {
                Some(book_ref) => book_ref,
                None => {
                    // If book doesn't exist, we trigger recovery process
                    debug!(
                        symbol = %symbol,
                        "Book doesn't exist, triggering recovery"
                    );

                    self.trigger_recovery(symbol);
                    return false;
                }
            }
        };

        // Try to apply the update
        match book.apply_depth_update(bids, asks, first_update_id, last_update_id) {
            Ok(true) => {
                // Update applied successfully
                self.notify_update(symbol, &book);

                true
            }
            Ok(false) => {
                // Book out of sync, need recovery
                // debug!(
                //     symbol = %symbol,
                //     first_id = first_update_id,
                //     last_id = last_update_id,
                //     book_last_id = book.get_last_update_id(),
                //     "Book out of sync, triggering recovery"
                // );
                self.trigger_recovery(symbol);
                false
            }
            Err(_) => {
                // Error applying update
                warn!(
                    symbol = %symbol,
                    "Error applying depth update, triggering recovery"
                );
                self.trigger_recovery(symbol);
                false
            }
        }
    }

    /// Get the best bid and ask for a symbol - fast path optimized
    #[inline]
    pub fn get_top_of_book(&self, symbol: &Arc<str>) -> Option<(Option<Level>, Option<Level>)> {
        // Fast path with DashMap
        match self.books.get(symbol) {
            Some(book_ref) if book_ref.is_synced() => { Some(book_ref.snapshot()) }
            Some(_) => {
                // Book exists but not in sync, trigger recovery without waiting
                self.trigger_recovery(symbol);
                None
            }
            None => None,
        }
    }

    /// Fetch full orderbook snapshot from the exchange
    pub async fn fetch_snapshot(
        &self,
        symbol: &str
    ) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>, u64)> {
        if symbol.trim().is_empty() {
            return Err(anyhow::anyhow!("Cannot fetch snapshot for empty symbol"));
        }

        // Use the exchange client to fetch depth data
        let depth_response = self.exchange_client
            .fetch_depth(symbol, self.default_depth).await
            .context(format!("Failed to fetch orderbook snapshot for {}", symbol))?;

        // Convert Level structs to (f64, f64) tuples for consistency
        let bids = depth_response.bids
            .iter()
            .map(|level| (level.price, level.quantity))
            .collect::<Vec<(f64, f64)>>();

        let asks = depth_response.asks
            .iter()
            .map(|level| (level.price, level.quantity))
            .collect::<Vec<(f64, f64)>>();

        Ok((bids, asks, depth_response.last_update_id))
    }

    /// Check if an orderbook is initialized
    pub fn is_initialized(&self, symbol: &Arc<str>) -> bool {
        if let Some(book_ref) = self.books.get(symbol) { book_ref.is_synced() } else { false }
    }

    /// Get all the symbols that have orderbooks
    pub fn get_symbols(&self) -> Vec<Arc<str>> {
        self.books
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get statistics about all orderbooks
    pub fn get_stats(&self) -> HashMap<Arc<str>, (bool, u64)> {
        let mut stats = HashMap::with_capacity(self.books.len());

        for entry in self.books.iter() {
            stats.insert(entry.key().clone(), (entry.is_synced(), entry.get_last_update_id()));
        }

        stats
    }

    /// Get the mid-price for a symbol
    pub fn get_mid_price(&self, symbol: &Arc<str>) -> Option<f64> {
        if let Some(book_ref) = self.books.get(symbol) {
            if book_ref.is_synced() { book_ref.mid_price() } else { None }
        } else {
            None
        }
    }

    /// Get multiple price levels for a symbol
    pub fn get_price_levels(
        &self,
        symbol: &Arc<str>,
        depth: Option<usize>
    ) -> Option<(Vec<Level>, Vec<Level>)> {
        if let Some(book_ref) = self.books.get(symbol) {
            if book_ref.is_synced() {
                Some((book_ref.get_bids(depth), book_ref.get_asks(depth)))
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Ensure all specified symbols have initialized orderbooks
    pub async fn ensure_all_initialized(&self, symbols: &[Arc<str>]) -> Result<()> {
        let mut failed_symbols = Vec::new();

        for symbol in symbols {
            let book = self.get_or_create_book(symbol);

            if !book.is_synced() {
                match self.fetch_snapshot(symbol.as_ref()).await {
                    Ok((bids, asks, last_update_id)) => {
                        self.apply_snapshot(symbol.clone(), &bids, &asks, last_update_id);
                    }
                    Err(e) => {
                        error!(
                            symbol = %symbol,
                            error = %e,
                            "Failed to initialize orderbook"
                        );
                        failed_symbols.push(symbol.clone());
                    }
                }
            }
        }

        if failed_symbols.is_empty() {
            Ok(())
        } else {
            let symbols_str = failed_symbols
                .iter()
                .map(|s| s.as_ref())
                .collect::<Vec<&str>>()
                .join(", ");

            Err(anyhow::anyhow!("Failed to initialize orderbooks for: {}", symbols_str))
        }
    }

    /// Clear all orderbooks
    pub fn clear_all(&self) {
        self.books.clear();
        debug!("Cleared all orderbooks");
    }

    /// Synchronous version of ensure_all_initialized for use in sync contexts
    pub fn ensure_all_initialized_sync(&self, symbols: &[Arc<str>]) -> Result<()> {
        let manager = self.clone();
        let symbols = symbols.to_vec();
        self.runtime_handle.block_on(async move { manager.ensure_all_initialized(&symbols).await })
    }
}

// Proper Clone implementation for OrderBookManager that preserves all data
impl Clone for OrderBookManager {
    fn clone(&self) -> Self {
        Self {
            books: self.books.clone(), // Clone Arc, not creating new empty map
            default_depth: self.default_depth,
            exchange_client: self.exchange_client.clone(),
            recovery_mutex: self.recovery_mutex.clone(), // Clone Arc<Mutex>
            last_recovery: self.last_recovery.clone(), // Clone Arc, not creating new empty map
            update_callbacks: self.update_callbacks.clone(), // Clone Arc, not creating new empty vector
            runtime_handle: self.runtime_handle.clone(), // Clone the runtime handle
        }
    }
}
