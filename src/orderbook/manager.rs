// // use std::collections::HashMap;
// // use std::sync::Arc;
// // use std::sync::Mutex;
// // use parking_lot::RwLock;
// // use tracing::{ debug, error, info, warn };
// // use anyhow::{ Result, Context };
// // use std::time::{ Duration, Instant };
// // use dashmap::DashMap;
// // use std::thread;

// // use crate::models::level::Level;
// // use crate::exchange::client::ExchangeClient;
// // use super::orderbook::OrderBook;

// // // Use a trait object callback for maximum flexibility and zero overhead
// // pub type OrderbookUpdateCallback = Arc<dyn Fn(&Arc<str>, &Arc<OrderBook>) + Send + Sync>;

// // pub struct OrderBookManager {
// //     /// Maps symbol to orderbook
// //     pub books: Arc<DashMap<Arc<str>, Arc<OrderBook>>>,
// //     /// Default depth to use for new orderbooks
// //     default_depth: usize,
// //     /// Exchange client for API requests
// //     exchange_client: Arc<dyn ExchangeClient + Send + Sync>,
// //     /// Mutex for the recovery process
// //     recovery_mutex: Arc<Mutex<()>>,
// //     /// Tracks last recovery attempt per symbol to avoid too frequent retries
// //     last_recovery: Arc<DashMap<Arc<str>, Instant>>,
// //     /// Update callbacks
// //     update_callbacks: Arc<RwLock<Vec<OrderbookUpdateCallback>>>,
// //     /// Runtime handle for async operations
// //     runtime_handle: tokio::runtime::Handle,
// // }

// // impl OrderBookManager {
// //     /// Create a new orderbook manager with default depth
// //     #[inline]
// //     pub fn new(
// //         default_depth: usize,
// //         exchange_client: Arc<dyn ExchangeClient + Send + Sync>
// //     ) -> Self {
// //         // Get the current runtime handle, or create a new runtime if none exists
// //         let runtime_handle = match tokio::runtime::Handle::try_current() {
// //             Ok(handle) => handle,
// //             Err(_) => {
// //                 // If no runtime exists, create a new one in a separate thread
// //                 let (tx, rx) = std::sync::mpsc::channel();
// //                 thread::spawn(move || {
// //                     let rt = tokio::runtime::Builder
// //                         ::new_multi_thread()
// //                         .worker_threads(2)
// //                         .enable_all()
// //                         .build()
// //                         .expect("Failed to create runtime for OrderBookManager");

// //                     tx.send(rt.handle().clone()).expect("Failed to send runtime handle");

// //                     // Keep the runtime alive
// //                     rt.block_on(async {
// //                         loop {
// //                             tokio::time::sleep(Duration::from_secs(1)).await;
// //                         }
// //                     });
// //                 });
// //                 rx.recv().expect("Failed to receive runtime handle")
// //             }
// //         };

// //         Self {
// //             books: Arc::new(DashMap::with_capacity(1000)),
// //             default_depth,
// //             exchange_client: exchange_client.clone(),
// //             recovery_mutex: Arc::new(Mutex::new(())),
// //             last_recovery: Arc::new(DashMap::with_capacity(1000)),
// //             update_callbacks: Arc::new(RwLock::new(Vec::with_capacity(10))),
// //             runtime_handle,
// //         }
// //     }

// //     /// Alternative constructor that accepts a runtime handle
// //     #[inline]
// //     pub fn with_runtime(
// //         default_depth: usize,
// //         exchange_client: Arc<dyn ExchangeClient + Send + Sync>,
// //         runtime_handle: tokio::runtime::Handle
// //     ) -> Self {
// //         Self {
// //             books: Arc::new(DashMap::with_capacity(1000)),
// //             default_depth,
// //             exchange_client: exchange_client.clone(),
// //             recovery_mutex: Arc::new(Mutex::new(())),
// //             last_recovery: Arc::new(DashMap::with_capacity(1000)),
// //             update_callbacks: Arc::new(RwLock::new(Vec::with_capacity(10))),
// //             runtime_handle,
// //         }
// //     }

// //     /// Register a callback to be notified when an orderbook is updated
// //     #[inline]
// //     pub fn register_update_callback(&self, callback: OrderbookUpdateCallback) {
// //         let mut callbacks = self.update_callbacks.write();
// //         callbacks.push(callback);
// //     }

// //     /// Notify all registered callbacks about an orderbook update
// //     /// Optimized to avoid unnecessary locking and cloning
// //     #[inline]
// //     fn notify_update(&self, symbol: &Arc<str>, book: &Arc<OrderBook>) {
// //         // Fast path: first check if we have any callbacks at all
// //         {
// //             let callbacks = self.update_callbacks.read();
// //             if callbacks.is_empty() {
// //                 return;
// //             }

// //             // Execute callbacks without holding the lock
// //             for callback in callbacks.iter() {
// //                 callback(symbol, book);
// //             }
// //         }
// //     }

// //     /// Get or create an orderbook for a symbol - fast path optimized
// //     pub fn get_or_create_book(&self, symbol: &Arc<str>) -> Arc<OrderBook> {
// //         if let Some(book) = self.books.get(symbol) {
// //             return book.clone();
// //         }

// //         // Not found, insert if absent (atomic operation with DashMap)
// //         let book = Arc::new(OrderBook::new(self.default_depth));

// //         // Note: entry API ensures atomic get_or_insert
// //         let result = self.books.entry(symbol.clone()).or_insert(book).clone();
// //         debug!(?symbol, "Created new orderbook");

// //         result
// //     }

// //     pub fn apply_snapshot(
// //         &self,
// //         symbol: Arc<str>,
// //         bids: &[(f64, f64)],
// //         asks: &[(f64, f64)],
// //         last_update_id: u64
// //     ) {
// //         // Get or create the orderbook
// //         let book = self.get_or_create_book(&symbol);

// //         // Convert to Level structs
// //         let bids_levels: Vec<Level> = bids
// //             .iter()
// //             .map(|(price, qty)| Level { price: *price, quantity: *qty })
// //             .collect();

// //         let asks_levels: Vec<Level> = asks
// //             .iter()
// //             .map(|(price, qty)| Level { price: *price, quantity: *qty })
// //             .collect();

// //         // Apply the snapshot
// //         book.apply_snapshot(bids_levels, asks_levels, last_update_id);

// //         // Reset recovery tracker - using DashMap remove
// //         self.last_recovery.remove(&symbol);

// //         // Notify callbacks about the update
// //         self.notify_update(&symbol, &book);

// //         debug!(
// //             symbol = %symbol,
// //             update_id = last_update_id,
// //             bid_count = bids.len(),
// //             ask_count = asks.len(),
// //             "Applied orderbook snapshot"
// //         );
// //     }

// //     pub fn trigger_recovery(&self, symbol: &Arc<str>) {
// //         if symbol.as_ref().trim().is_empty() {
// //             error!("Cannot trigger recovery for empty symbol");
// //             return;
// //         }

// //         // Check if we've attempted recovery recently (don't spam with requests)
// //         if let Some(last_time) = self.last_recovery.get(symbol) {
// //             if last_time.elapsed() < Duration::from_secs(3) {
// //                 debug!(
// //                     symbol = %symbol,
// //                     "Skipping recovery, too soon since last attempt: {:?}",
// //                     last_time.elapsed()
// //                 );
// //                 return;
// //             }
// //         }

// //         // Mark that we're attempting recovery
// //         self.last_recovery.insert(symbol.clone(), Instant::now());

// //         // Clone what we need for the task
// //         let symbol_clone = symbol.clone();
// //         let manager = self.clone();

// //         // Spawn recovery task on the runtime
// //         self.runtime_handle.spawn(async move {
// //             manager.recovery_task(symbol_clone).await;
// //         });
// //     }

// //     /// Internal async recovery task
// //     async fn recovery_task(&self, symbol: Arc<str>) {
// //         // Add some jitter to avoid all symbols recovering at the same time
// //         let jitter = rand::random::<u64>() % 200;
// //         tokio::time::sleep(Duration::from_millis(jitter)).await;

// //         // Try to acquire the lock, but don't hold it across await points
// //         let can_proceed = {
// //             match self.recovery_mutex.try_lock() {
// //                 Ok(_lock) => {
// //                     // Lock acquired, drop it immediately by letting it go out of scope
// //                     true
// //                 }
// //                 Err(_) => {
// //                     debug!(symbol = %symbol, "Recovery already in progress, skipping");
// //                     false
// //                 }
// //             }
// //         };

// //         if !can_proceed {
// //             return;
// //         }

// //         info!(
// //             symbol = %symbol,
// //             "Starting orderbook recovery process"
// //         );

// //         // Fetch snapshot from REST API
// //         match self.fetch_snapshot(symbol.as_ref()).await {
// //             Ok((snapshot_bids, snapshot_asks, snapshot_last_id)) => {
// //                 if snapshot_bids.is_empty() || snapshot_asks.is_empty() {
// //                     warn!(
// //                         symbol = %symbol,
// //                         "Empty orderbook snapshot received"
// //                     );
// //                     return;
// //                 }

// //                 // Apply the snapshot
// //                 self.apply_snapshot(
// //                     symbol.clone(),
// //                     &snapshot_bids,
// //                     &snapshot_asks,
// //                     snapshot_last_id
// //                 );

// //                 info!(
// //                     symbol = %symbol,
// //                     "Orderbook recovery completed successfully"
// //                 );
// //             }
// //             Err(e) => {
// //                 error!(
// //                     symbol = %symbol,
// //                     error = %e,
// //                     "Failed to fetch orderbook snapshot"
// //                 );
// //             }
// //         }
// //     }

// //     pub fn apply_depth_update(
// //         &self,
// //         symbol: &Arc<str>,
// //         bids: &[(f64, f64)],
// //         asks: &[(f64, f64)],
// //         first_update_id: u64,
// //         last_update_id: u64
// //     ) -> bool {
// //         // Get existing book without creating if missing
// //         let book = {
// //             match self.books.get(symbol) {
// //                 Some(book_ref) => book_ref,
// //                 None => {
// //                     // If book doesn't exist, we trigger recovery process
// //                     debug!(
// //                         symbol = %symbol,
// //                         "Book doesn't exist, triggering recovery"
// //                     );

// //                     self.trigger_recovery(symbol);
// //                     return false;
// //                 }
// //             }
// //         };

// //         // Try to apply the update
// //         match book.apply_depth_update(bids, asks, first_update_id, last_update_id) {
// //             Ok(true) => {
// //                 // Update applied successfully
// //                 self.notify_update(symbol, &book);

// //                 true
// //             }
// //             Ok(false) => {
// //                 // Book out of sync, need recovery

// //                 self.trigger_recovery(symbol);
// //                 false
// //             }
// //             Err(_) => {
// //                 // Error applying update
// //                 warn!(
// //                     symbol = %symbol,
// //                     "Error applying depth update, triggering recovery"
// //                 );
// //                 self.trigger_recovery(symbol);
// //                 false
// //             }
// //         }
// //     }

// //     /// Get the best bid and ask for a symbol - fast path optimized
// //     #[inline]
// //     pub fn get_top_of_book(&self, symbol: &Arc<str>) -> Option<(Option<Level>, Option<Level>)> {
// //         // Fast path with DashMap
// //         match self.books.get(symbol) {
// //             Some(book_ref) if book_ref.is_synced() => { Some(book_ref.snapshot()) }
// //             Some(_) => {
// //                 // Book exists but not in sync, trigger recovery without waiting
// //                 self.trigger_recovery(symbol);
// //                 None
// //             }
// //             None => None,
// //         }
// //     }

// //     /// Fetch full orderbook snapshot from the exchange
// //     pub async fn fetch_snapshot(
// //         &self,
// //         symbol: &str
// //     ) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>, u64)> {
// //         if symbol.trim().is_empty() {
// //             return Err(anyhow::anyhow!("Cannot fetch snapshot for empty symbol"));
// //         }

// //         // Use the exchange client to fetch depth data
// //         let depth_response = self.exchange_client
// //             .fetch_depth(symbol, self.default_depth).await
// //             .context(format!("Failed to fetch orderbook snapshot for {}", symbol))?;

// //         // Convert Level structs to (f64, f64) tuples for consistency
// //         let bids = depth_response.bids
// //             .iter()
// //             .map(|level| (level.price, level.quantity))
// //             .collect::<Vec<(f64, f64)>>();

// //         let asks = depth_response.asks
// //             .iter()
// //             .map(|level| (level.price, level.quantity))
// //             .collect::<Vec<(f64, f64)>>();

// //         Ok((bids, asks, depth_response.last_update_id))
// //     }

// //     /// Check if an orderbook is initialized
// //     pub fn is_initialized(&self, symbol: &Arc<str>) -> bool {
// //         if let Some(book_ref) = self.books.get(symbol) { book_ref.is_synced() } else { false }
// //     }

// //     /// Get all the symbols that have orderbooks
// //     pub fn get_symbols(&self) -> Vec<Arc<str>> {
// //         self.books
// //             .iter()
// //             .map(|entry| entry.key().clone())
// //             .collect()
// //     }

// //     /// Get statistics about all orderbooks
// //     pub fn get_stats(&self) -> HashMap<Arc<str>, (bool, u64)> {
// //         let mut stats = HashMap::with_capacity(self.books.len());

// //         for entry in self.books.iter() {
// //             stats.insert(entry.key().clone(), (entry.is_synced(), entry.get_last_update_id()));
// //         }

// //         stats
// //     }

// //     /// Get the mid-price for a symbol
// //     pub fn get_mid_price(&self, symbol: &Arc<str>) -> Option<f64> {
// //         if let Some(book_ref) = self.books.get(symbol) {
// //             if book_ref.is_synced() { book_ref.mid_price() } else { None }
// //         } else {
// //             None
// //         }
// //     }

// //     /// Get multiple price levels for a symbol
// //     pub fn get_price_levels(
// //         &self,
// //         symbol: &Arc<str>,
// //         depth: Option<usize>
// //     ) -> Option<(Vec<Level>, Vec<Level>)> {
// //         if let Some(book_ref) = self.books.get(symbol) {
// //             if book_ref.is_synced() {
// //                 Some((book_ref.get_bids(depth), book_ref.get_asks(depth)))
// //             } else {
// //                 None
// //             }
// //         } else {
// //             None
// //         }
// //     }

// //     /// Ensure all specified symbols have initialized orderbooks
// //     pub async fn ensure_all_initialized(&self, symbols: &[Arc<str>]) -> Result<()> {
// //         let mut failed_symbols = Vec::new();

// //         for symbol in symbols {
// //             let book = self.get_or_create_book(symbol);

// //             if !book.is_synced() {
// //                 match self.fetch_snapshot(symbol.as_ref()).await {
// //                     Ok((bids, asks, last_update_id)) => {
// //                         self.apply_snapshot(symbol.clone(), &bids, &asks, last_update_id);
// //                     }
// //                     Err(e) => {
// //                         error!(
// //                             symbol = %symbol,
// //                             error = %e,
// //                             "Failed to initialize orderbook"
// //                         );
// //                         failed_symbols.push(symbol.clone());
// //                     }
// //                 }
// //             }
// //         }

// //         if failed_symbols.is_empty() {
// //             Ok(())
// //         } else {
// //             let symbols_str = failed_symbols
// //                 .iter()
// //                 .map(|s| s.as_ref())
// //                 .collect::<Vec<&str>>()
// //                 .join(", ");

// //             Err(anyhow::anyhow!("Failed to initialize orderbooks for: {}", symbols_str))
// //         }
// //     }

// //     /// Clear all orderbooks
// //     pub fn clear_all(&self) {
// //         self.books.clear();
// //         debug!("Cleared all orderbooks");
// //     }

// //     /// Synchronous version of ensure_all_initialized for use in sync contexts
// //     pub fn ensure_all_initialized_sync(&self, symbols: &[Arc<str>]) -> Result<()> {
// //         let manager = self.clone();
// //         let symbols = symbols.to_vec();
// //         self.runtime_handle.block_on(async move { manager.ensure_all_initialized(&symbols).await })
// //     }
// // }

// // // Proper Clone implementation for OrderBookManager that preserves all data
// // impl Clone for OrderBookManager {
// //     fn clone(&self) -> Self {
// //         Self {
// //             books: self.books.clone(), // Clone Arc, not creating new empty map
// //             default_depth: self.default_depth,
// //             exchange_client: self.exchange_client.clone(),
// //             recovery_mutex: self.recovery_mutex.clone(), // Clone Arc<Mutex>
// //             last_recovery: self.last_recovery.clone(), // Clone Arc, not creating new empty map
// //             update_callbacks: self.update_callbacks.clone(), // Clone Arc, not creating new empty vector
// //             runtime_handle: self.runtime_handle.clone(), // Clone the runtime handle
// //         }
// //     }
// // }

// // src/orderbook/manager.rs - Optimized version replacing existing
// use std::sync::Arc;
// use std::sync::Mutex;
// use parking_lot::RwLock;
// use tracing::{ debug, error, info, warn };
// use anyhow::{ Result, Context };
// use std::time::{ Duration, Instant };
// use dashmap::DashMap;
// use std::thread;

// use crate::models::level::Level;
// use crate::exchange::client::ExchangeClient;
// use crate::orderbook::orderbook::OrderBook;
// use crate::utils::string_interner::SymbolInterner;

// // Optimized callback type - uses string slice to avoid Arc cloning
// pub type OrderbookUpdateCallback = Arc<dyn Fn(&str, &Arc<OrderBook>) + Send + Sync>;

// pub struct OrderBookManager {
//     /// Core orderbook storage - same structure but optimized usage
//     pub books: Arc<DashMap<Arc<str>, Arc<OrderBook>>>,

//     /// String interner for symbol deduplication
//     symbol_interner: Arc<SymbolInterner>,

//     /// Default depth to use for new orderbooks
//     default_depth: usize,

//     /// Exchange client for API requests
//     exchange_client: Arc<dyn ExchangeClient + Send + Sync>,

//     /// Recovery coordination - optimized with hash-based tracking
//     recovery_mutex: Arc<Mutex<()>>,
//     last_recovery: Arc<DashMap<u64, Instant>>, // Use hash instead of Arc<str>

//     /// Update callbacks
//     update_callbacks: Arc<RwLock<Vec<OrderbookUpdateCallback>>>,

//     /// Runtime handle for async operations
//     runtime_handle: tokio::runtime::Handle,

//     /// Symbol hash cache for fast lookups
//     symbol_hashes: Arc<DashMap<Arc<str>, u64>>,
// }

// impl OrderBookManager {
//     /// Create a new orderbook manager with default depth
//     #[inline]
//     pub fn new(
//         default_depth: usize,
//         exchange_client: Arc<dyn ExchangeClient + Send + Sync>
//     ) -> Self {
//         // Get the current runtime handle, or create a new runtime if none exists
//         let runtime_handle = match tokio::runtime::Handle::try_current() {
//             Ok(handle) => handle,
//             Err(_) => {
//                 // If no runtime exists, create a new one in a separate thread
//                 let (tx, rx) = std::sync::mpsc::channel();
//                 thread::spawn(move || {
//                     let rt = tokio::runtime::Builder
//                         ::new_multi_thread()
//                         .worker_threads(2)
//                         .enable_all()
//                         .build()
//                         .expect("Failed to create runtime for OrderBookManager");

//                     tx.send(rt.handle().clone()).expect("Failed to send runtime handle");

//                     // Keep the runtime alive
//                     rt.block_on(async {
//                         loop {
//                             tokio::time::sleep(Duration::from_secs(1)).await;
//                         }
//                     });
//                 });
//                 rx.recv().expect("Failed to receive runtime handle")
//             }
//         };

//         let symbol_interner = Arc::new(SymbolInterner::new());

//         Self {
//             books: Arc::new(DashMap::with_capacity(1000)),
//             symbol_interner,
//             default_depth,
//             exchange_client,
//             recovery_mutex: Arc::new(Mutex::new(())),
//             last_recovery: Arc::new(DashMap::with_capacity(1000)),
//             update_callbacks: Arc::new(RwLock::new(Vec::with_capacity(10))),
//             runtime_handle,
//             symbol_hashes: Arc::new(DashMap::with_capacity(1000)),
//         }
//     }

//     /// Preload symbols into the interner for better performance
//     pub fn preload_symbols(&self, symbols: &[String]) {
//         info!("Preloading {} symbols into interner", symbols.len());
//         self.symbol_interner.preload(symbols);

//         // Precompute hashes
//         for symbol in symbols {
//             let interned = self.symbol_interner.intern(symbol);
//             let hash = self.compute_hash(&interned);
//             self.symbol_hashes.insert(interned, hash);
//         }

//         let (hash_cache, string_cache) = self.symbol_interner.stats();
//         info!("Interner stats: {} hash entries, {} string entries", hash_cache, string_cache);
//     }

//     /// Fast hash computation for symbols
//     #[inline]
//     fn compute_hash(&self, symbol: &Arc<str>) -> u64 {
//         use std::hash::{ Hash, Hasher };
//         let mut hasher = std::collections::hash_map::DefaultHasher::new();
//         symbol.hash(&mut hasher);
//         hasher.finish()
//     }

//     /// Get symbol hash with caching
//     #[inline]
//     fn get_symbol_hash(&self, symbol: &Arc<str>) -> u64 {
//         if let Some(hash) = self.symbol_hashes.get(symbol) {
//             *hash
//         } else {
//             let hash = self.compute_hash(symbol);
//             self.symbol_hashes.insert(symbol.clone(), hash);
//             hash
//         }
//     }

//     /// Register a callback to be notified when an orderbook is updated
//     #[inline]
//     pub fn register_update_callback(&self, callback: OrderbookUpdateCallback) {
//         let mut callbacks = self.update_callbacks.write();
//         callbacks.push(callback);
//     }

//     /// Optimized callback notification - passes string slice to avoid Arc cloning
//     #[inline]
//     fn notify_update(&self, symbol: &str, book: &Arc<OrderBook>) {
//         let callbacks = self.update_callbacks.read();
//         if callbacks.is_empty() {
//             return;
//         }

//         // Execute callbacks with string slice to avoid Arc overhead
//         for callback in callbacks.iter() {
//             callback(symbol, book);
//         }
//     }

//     /// Get or create an orderbook for a symbol - optimized with string interning
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

//         // Reset recovery tracker using hash
//         let symbol_hash = self.get_symbol_hash(&symbol);
//         self.last_recovery.remove(&symbol_hash);

//         // Notify callbacks with string slice
//         self.notify_update(symbol.as_ref(), &book);

//         debug!(
//             symbol = %symbol,
//             update_id = last_update_id,
//             bid_count = bids.len(),
//             ask_count = asks.len(),
//             "Applied orderbook snapshot"
//         );
//     }

//     pub fn trigger_recovery(&self, symbol: &Arc<str>) {
//         if symbol.as_ref().trim().is_empty() {
//             error!("Cannot trigger recovery for empty symbol");
//             return;
//         }

//         let symbol_hash = self.get_symbol_hash(symbol);

//         // Check if we've attempted recovery recently using hash
//         if let Some(last_time) = self.last_recovery.get(&symbol_hash) {
//             if last_time.elapsed() < Duration::from_secs(3) {
//                 debug!(
//                     symbol = %symbol,
//                     "Skipping recovery, too recent: {:?}",
//                     last_time.elapsed()
//                 );
//                 return;
//             }
//         }

//         // Mark that we're attempting recovery using hash
//         self.last_recovery.insert(symbol_hash, Instant::now());

//         // Clone what we need for the task
//         let symbol_clone = symbol.clone();
//         let manager = self.clone();

//         // Spawn recovery task on the runtime
//         self.runtime_handle.spawn(async move {
//             manager.recovery_task(symbol_clone).await;
//         });
//     }

//     /// Internal async recovery task
//     async fn recovery_task(&self, symbol: Arc<str>) {
//         // Add some jitter to avoid all symbols recovering at the same time
//         let jitter = rand::random::<u64>() % 200;
//         tokio::time::sleep(Duration::from_millis(jitter)).await;

//         // Try to acquire the lock, but don't hold it across await points
//         let can_proceed = {
//             match self.recovery_mutex.try_lock() {
//                 Ok(_lock) => {
//                     // Lock acquired, drop it immediately by letting it go out of scope
//                     true
//                 }
//                 Err(_) => {
//                     debug!(symbol = %symbol, "Recovery already in progress, skipping");
//                     false
//                 }
//             }
//         };

//         if !can_proceed {
//             return;
//         }

//         info!(
//             symbol = %symbol,
//             "Starting orderbook recovery process"
//         );

//         // Fetch snapshot from REST API
//         match self.fetch_snapshot(symbol.as_ref()).await {
//             Ok((snapshot_bids, snapshot_asks, snapshot_last_id)) => {
//                 if snapshot_bids.is_empty() || snapshot_asks.is_empty() {
//                     warn!(
//                         symbol = %symbol,
//                         "Empty orderbook snapshot received"
//                     );
//                     return;
//                 }

//                 // Apply the snapshot
//                 self.apply_snapshot(
//                     symbol.clone(),
//                     &snapshot_bids,
//                     &snapshot_asks,
//                     snapshot_last_id
//                 );

//                 info!(
//                     symbol = %symbol,
//                     "Orderbook recovery completed successfully"
//                 );
//             }
//             Err(e) => {
//                 error!(
//                     symbol = %symbol,
//                     error = %e,
//                     "Failed to fetch orderbook snapshot"
//                 );
//             }
//         }
//     }

//     pub fn apply_depth_update_internal(
//         &self,
//         symbol: &Arc<str>,
//         bids: &[(f64, f64)],
//         asks: &[(f64, f64)],
//         first_update_id: u64,
//         last_update_id: u64
//     ) -> bool {
//         // Get existing book without creating if missing
//         let book = {
//             match self.books.get(symbol) {
//                 Some(book_ref) => book_ref,
//                 None => {
//                     // If book doesn't exist, we trigger recovery process
//                     debug!(
//                         symbol = %symbol,
//                         "Book doesn't exist, triggering recovery"
//                     );

//                     self.trigger_recovery(symbol);
//                     return false;
//                 }
//             }
//         };

//         // Try to apply the update
//         match book.apply_depth_update(bids, asks, first_update_id, last_update_id) {
//             Ok(true) => {
//                 // Update applied successfully - use string slice in callback
//                 self.notify_update(symbol.as_ref(), &book);
//                 true
//             }
//             Ok(false) => {
//                 // Book out of sync, need recovery
//                 self.trigger_recovery(symbol);
//                 false
//             }
//             Err(_) => {
//                 // Error applying update
//                 warn!(
//                     symbol = %symbol,
//                     "Error applying depth update, triggering recovery"
//                 );
//                 self.trigger_recovery(symbol);
//                 false
//             }
//         }
//     }

//     /// Apply depth update using string slice
//     pub fn apply_depth_update(
//         &self,
//         symbol_str: &str,
//         bids: &[(f64, f64)],
//         asks: &[(f64, f64)],
//         first_update_id: u64,
//         last_update_id: u64
//     ) -> bool {
//         let symbol = self.symbol_interner.intern(symbol_str);
//         self.apply_depth_update_internal(&symbol, bids, asks, first_update_id, last_update_id)
//     }

//     /// Get the best bid and ask for a symbol - fast path optimized
//     #[inline]
//     pub fn get_top_of_book_internal(
//         &self,
//         symbol: &Arc<str>
//     ) -> Option<(Option<Level>, Option<Level>)> {
//         // Fast path with DashMap
//         match self.books.get(symbol) {
//             Some(book_ref) if book_ref.is_synced() => { Some(book_ref.snapshot()) }
//             Some(_) => {
//                 // Book exists but not in sync, trigger recovery without waiting
//                 self.trigger_recovery(symbol);
//                 None
//             }
//             None => None,
//         }
//     }

//     /// Get top of book using string slice
//     #[inline]
//     pub fn get_top_of_book(&self, symbol_str: &str) -> Option<(Option<Level>, Option<Level>)> {
//         let symbol = self.symbol_interner.intern(symbol_str);
//         self.get_top_of_book_internal(&symbol)
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

//     /// Get optimization statistics
//     pub fn get_optimization_stats(&self) -> OptimizationStats {
//         let (_, interner_strings) = self.symbol_interner.stats();

//         OptimizationStats {
//             total_books: self.books.len(),
//             interner_string_entries: interner_strings,
//             symbol_hash_cache_size: self.symbol_hashes.len(),
//         }
//     }
// }

// #[derive(Debug)]
// pub struct OptimizationStats {
//     pub total_books: usize,
//     pub interner_string_entries: usize,
//     pub symbol_hash_cache_size: usize,
// }

// // Proper Clone implementation for OrderBookManager that preserves all data
// impl Clone for OrderBookManager {
//     fn clone(&self) -> Self {
//         Self {
//             books: self.books.clone(),
//             symbol_interner: self.symbol_interner.clone(),
//             default_depth: self.default_depth,
//             exchange_client: self.exchange_client.clone(),
//             recovery_mutex: self.recovery_mutex.clone(),
//             last_recovery: self.last_recovery.clone(),
//             update_callbacks: self.update_callbacks.clone(),
//             runtime_handle: self.runtime_handle.clone(),
//             symbol_hashes: self.symbol_hashes.clone(),
//         }
//     }
// }

// src/orderbook/manager.rs - Ultra-optimized version targeting 1-3μs
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
use crate::orderbook::orderbook::OrderBook;
use crate::utils::string_interner::SymbolInterner;

// Ultra-fast symbol ID mapping
const MAX_SYMBOLS: usize = 2048;
type SymbolId = u16;

// Optimized callback type - uses SymbolId instead of string
pub type OrderbookUpdateCallback = Arc<dyn Fn(SymbolId, &Arc<OrderBook>) + Send + Sync>;

pub struct OrderBookManager {
    /// Core orderbook storage - now array-based for O(1) access
    pub books: Box<[Option<Arc<OrderBook>>; MAX_SYMBOLS]>, // Array instead of DashMap

    /// Fast symbol mapping - compute once, use forever
    pub symbol_to_id: DashMap<Arc<str>, SymbolId>,
    id_to_symbol: Box<[Option<Arc<str>>; MAX_SYMBOLS]>,
    next_symbol_id: std::sync::atomic::AtomicU16,

    /// String interner for symbol deduplication (used only during setup)
    symbol_interner: Arc<SymbolInterner>,

    /// Default depth to use for new orderbooks
    default_depth: usize,

    /// Exchange client for API requests
    exchange_client: Arc<dyn ExchangeClient + Send + Sync>,

    /// Recovery coordination - optimized with array-based tracking
    recovery_mutex: Arc<Mutex<()>>,
    last_recovery: Box<[std::sync::atomic::AtomicU64; MAX_SYMBOLS]>, // Array for O(1) access

    /// Update callbacks
    update_callbacks: Arc<RwLock<Vec<OrderbookUpdateCallback>>>,

    /// Runtime handle for async operations
    runtime_handle: tokio::runtime::Handle,
}

impl OrderBookManager {
    /// Create a new orderbook manager with default depth
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

        let symbol_interner = Arc::new(SymbolInterner::new());

        // Initialize arrays with None/default values
        let books = Box::new([const { None }; MAX_SYMBOLS]);
        let id_to_symbol = Box::new([const { None }; MAX_SYMBOLS]);

        // Initialize atomic array for recovery tracking
        let last_recovery = Box::new([const { std::sync::atomic::AtomicU64::new(0) }; MAX_SYMBOLS]);

        Self {
            books,
            symbol_to_id: DashMap::with_capacity(MAX_SYMBOLS),
            id_to_symbol,
            next_symbol_id: std::sync::atomic::AtomicU16::new(0),
            symbol_interner,
            default_depth,
            exchange_client,
            recovery_mutex: Arc::new(Mutex::new(())),
            last_recovery,
            update_callbacks: Arc::new(RwLock::new(Vec::with_capacity(10))),
            runtime_handle,
        }
    }

    /// Preload symbols into the interner and assign IDs - CRITICAL for performance
    pub fn preload_symbols(&self, symbols: &[String]) {
        info!("Preloading {} symbols with ID assignment", symbols.len());

        for symbol in symbols {
            let interned = self.symbol_interner.intern(symbol);
            self.get_or_assign_symbol_id(&interned); // Pre-assign IDs
        }

        let (hash_cache, string_cache) = self.symbol_interner.stats();
        info!(
            "Symbol preloading complete: {} hash entries, {} string entries, {} IDs assigned",
            hash_cache,
            string_cache,
            self.next_symbol_id.load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    /// Get or assign a symbol ID - called only during setup/preload
    fn get_or_assign_symbol_id(&self, symbol: &Arc<str>) -> SymbolId {
        if let Some(id) = self.symbol_to_id.get(symbol) {
            return *id;
        }

        // Assign new ID
        let id = self.next_symbol_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if id >= (MAX_SYMBOLS as u16) {
            panic!("Too many symbols! Increase MAX_SYMBOLS");
        }

        self.symbol_to_id.insert(symbol.clone(), id);
        // SAFETY: We just checked bounds above
        unsafe {
            let id_to_symbol_ptr = self.id_to_symbol.as_ptr() as *mut Option<Arc<str>>;
            *id_to_symbol_ptr.add(id as usize) = Some(symbol.clone());
        }

        id
    }

    /// ULTRA-FAST: Get symbol ID from string - hot path optimized
    #[inline(always)]
    fn get_symbol_id(&self, symbol_str: &str) -> Option<SymbolId> {
        // Try direct lookup first (should hit after preloading)
        let interned = self.symbol_interner.intern(symbol_str);
        self.symbol_to_id.get(&interned).map(|id| *id)
    }

    /// Register a callback to be notified when an orderbook is updated
    #[inline]
    pub fn register_update_callback(&self, callback: OrderbookUpdateCallback) {
        let mut callbacks = self.update_callbacks.write();
        callbacks.push(callback);
    }

    /// Optimized callback notification - uses SymbolId instead of string
    #[inline(always)]
    fn notify_update(&self, symbol_id: SymbolId, book: &Arc<OrderBook>) {
        let callbacks = self.update_callbacks.read();
        if callbacks.is_empty() {
            return;
        }

        // Execute callbacks with SymbolId for maximum speed
        for callback in callbacks.iter() {
            callback(symbol_id, book);
        }
    }

    /// Get or create an orderbook for a symbol ID - ultra-fast array access
    #[inline(always)]
    pub fn get_or_create_book_by_id(&self, symbol_id: SymbolId) -> Arc<OrderBook> {
        // SAFETY: We ensure symbol_id is valid during preloading
        unsafe {
            let books_ptr = self.books.as_ptr() as *mut Option<Arc<OrderBook>>;
            let book_option = &mut *books_ptr.add(symbol_id as usize);

            if let Some(book) = book_option {
                return book.clone();
            }

            // Create new book
            let book = Arc::new(OrderBook::new(self.default_depth));
            *book_option = Some(book.clone());
            book
        }
    }

    /// ULTRA-FAST: Apply depth update using symbol ID - main hot path
    #[inline(always)]
    pub fn apply_depth_update_fast(
        &self,
        symbol_id: SymbolId,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        first_update_id: u64,
        last_update_id: u64
    ) -> bool {
        // Get existing book - array access is O(1)
        let book = if symbol_id < (MAX_SYMBOLS as u16) {
            // SAFETY: We checked bounds
            unsafe {
                let books_ptr = self.books.as_ptr();
                if let Some(book) = &*books_ptr.add(symbol_id as usize) {
                    book.clone()
                } else {
                    // Book doesn't exist, trigger recovery
                    self.trigger_recovery_by_id(symbol_id);
                    return false;
                }
            }
        } else {
            return false;
        };

        // Try to apply the update - NO ALLOCATIONS
        match book.apply_depth_update_raw(bids, asks, first_update_id, last_update_id) {
            Ok(true) => {
                // Update applied successfully
                self.notify_update(symbol_id, &book);
                true
            }
            Ok(false) => {
                // Book out of sync, need recovery
                self.trigger_recovery_by_id(symbol_id);
                false
            }
            Err(_) => {
                // Error applying update
                self.trigger_recovery_by_id(symbol_id);
                false
            }
        }
    }

    /// Public method that converts string to ID then calls fast path
    pub fn apply_depth_update(
        &self,
        symbol_str: &str,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        first_update_id: u64,
        last_update_id: u64
    ) -> bool {
        if let Some(symbol_id) = self.get_symbol_id(symbol_str) {
            self.apply_depth_update_fast(symbol_id, bids, asks, first_update_id, last_update_id)
        } else {
            // Symbol not preloaded - this shouldn't happen in optimized flow
            warn!("Symbol {} not preloaded, falling back to slow path", symbol_str);
            false
        }
    }

    pub fn trigger_recovery_by_id(&self, symbol_id: SymbolId) {
        if symbol_id >= (MAX_SYMBOLS as u16) {
            return;
        }

        // Check if we've attempted recovery recently using array access
        let now = std::time::SystemTime
            ::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // SAFETY: We checked bounds above
        let last_time = unsafe {
            self.last_recovery
                .get_unchecked(symbol_id as usize)
                .load(std::sync::atomic::Ordering::Relaxed)
        };

        if now < last_time + 3 {
            return; // Too recent
        }

        // Mark that we're attempting recovery
        unsafe {
            self.last_recovery
                .get_unchecked(symbol_id as usize)
                .store(now, std::sync::atomic::Ordering::Relaxed);
        }

        // Get symbol string for recovery
        let symbol = unsafe {
            if let Some(sym) = &*self.id_to_symbol.as_ptr().add(symbol_id as usize) {
                sym.clone()
            } else {
                return;
            }
        };

        // Spawn recovery task
        let manager = self.clone();
        self.runtime_handle.spawn(async move {
            manager.recovery_task(symbol).await;
        });
    }

    pub fn trigger_recovery(&self, symbol: &Arc<str>) {
        if let Some(symbol_id) = self.symbol_to_id.get(symbol).map(|id| *id) {
            self.trigger_recovery_by_id(symbol_id);
        }
    }

    /// Apply snapshot with symbol string conversion
    pub fn apply_snapshot(
        &self,
        symbol: Arc<str>,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        last_update_id: u64
    ) {
        if let Some(symbol_id) = self.symbol_to_id.get(&symbol).map(|id| *id) {
            self.apply_snapshot_by_id(symbol_id, bids, asks, last_update_id);
        }
    }

    /// Apply snapshot using symbol ID - faster version
    pub fn apply_snapshot_by_id(
        &self,
        symbol_id: SymbolId,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        last_update_id: u64
    ) {
        let book = self.get_or_create_book_by_id(symbol_id);

        // Convert to Level structs - only when needed
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

        // Reset recovery tracker using array access
        unsafe {
            self.last_recovery
                .get_unchecked(symbol_id as usize)
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }

        // Notify callbacks with symbol ID
        self.notify_update(symbol_id, &book);

        if let Some(symbol) = (unsafe { &*self.id_to_symbol.as_ptr().add(symbol_id as usize) }) {
            debug!(
                symbol = %symbol,
                update_id = last_update_id,
                bid_count = bids.len(),
                ask_count = asks.len(),
                "Applied orderbook snapshot"
            );
        }
    }

    /// Internal async recovery task - unchanged
    async fn recovery_task(&self, symbol: Arc<str>) {
        let jitter = rand::random::<u64>() % 200;
        tokio::time::sleep(Duration::from_millis(jitter)).await;

        let can_proceed = {
            match self.recovery_mutex.try_lock() {
                Ok(_lock) => true,
                Err(_) => {
                    debug!(symbol = %symbol, "Recovery already in progress, skipping");
                    false
                }
            }
        };

        if !can_proceed {
            return;
        }

        info!(symbol = %symbol, "Starting orderbook recovery process");

        match self.fetch_snapshot(symbol.as_ref()).await {
            Ok((snapshot_bids, snapshot_asks, snapshot_last_id)) => {
                if snapshot_bids.is_empty() || snapshot_asks.is_empty() {
                    warn!(symbol = %symbol, "Empty orderbook snapshot received");
                    return;
                }

                self.apply_snapshot(
                    symbol.clone(),
                    &snapshot_bids,
                    &snapshot_asks,
                    snapshot_last_id
                );
                info!(symbol = %symbol, "Orderbook recovery completed successfully");
            }
            Err(e) => {
                error!(symbol = %symbol, error = %e, "Failed to fetch orderbook snapshot");
            }
        }
    }

    /// Get the best bid and ask for a symbol ID - ultra-fast
    #[inline(always)]
    pub fn get_top_of_book_by_id(
        &self,
        symbol_id: SymbolId
    ) -> Option<(Option<Level>, Option<Level>)> {
        if symbol_id >= (MAX_SYMBOLS as u16) {
            return None;
        }

        // SAFETY: We checked bounds above
        unsafe {
            let books_ptr = self.books.as_ptr();
            if let Some(book) = &*books_ptr.add(symbol_id as usize) {
                if book.is_synced() {
                    Some(book.snapshot())
                } else {
                    self.trigger_recovery_by_id(symbol_id);
                    None
                }
            } else {
                None
            }
        }
    }

    /// Get top of book using string slice - converts to ID then calls fast version
    #[inline]
    pub fn get_top_of_book(&self, symbol_str: &str) -> Option<(Option<Level>, Option<Level>)> {
        if let Some(symbol_id) = self.get_symbol_id(symbol_str) {
            self.get_top_of_book_by_id(symbol_id)
        } else {
            None
        }
    }

    pub fn get_top_of_book_internal(
        &self,
        symbol: &Arc<str>
    ) -> Option<(Option<Level>, Option<Level>)> {
        if let Some(symbol_id) = self.symbol_to_id.get(symbol).map(|id| *id) {
            self.get_top_of_book_by_id(symbol_id)
        } else {
            None
        }
    }

    /// Fetch full orderbook snapshot from the exchange - unchanged
    pub async fn fetch_snapshot(
        &self,
        symbol: &str
    ) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>, u64)> {
        if symbol.trim().is_empty() {
            return Err(anyhow::anyhow!("Cannot fetch snapshot for empty symbol"));
        }

        let depth_response = self.exchange_client
            .fetch_depth(symbol, self.default_depth).await
            .context(format!("Failed to fetch orderbook snapshot for {}", symbol))?;

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

    /// Get optimization statistics
    pub fn get_optimization_stats(&self) -> OptimizationStats {
        let (_, interner_strings) = self.symbol_interner.stats();
        let assigned_symbols = self.next_symbol_id.load(std::sync::atomic::Ordering::Relaxed);

        OptimizationStats {
            total_books: assigned_symbols as usize,
            interner_string_entries: interner_strings,
            symbol_hash_cache_size: self.symbol_to_id.len(),
        }
    }
}

#[derive(Debug)]
pub struct OptimizationStats {
    pub total_books: usize,
    pub interner_string_entries: usize,
    pub symbol_hash_cache_size: usize,
}

// Optimized Clone implementation
impl Clone for OrderBookManager {
    fn clone(&self) -> Self {
        // Clone the atomic arrays properly
        let mut new_last_recovery = Box::new(
            [const { std::sync::atomic::AtomicU64::new(0) }; MAX_SYMBOLS]
        );
        for i in 0..MAX_SYMBOLS {
            let val = self.last_recovery[i].load(std::sync::atomic::Ordering::Relaxed);
            new_last_recovery[i].store(val, std::sync::atomic::Ordering::Relaxed);
        }

        Self {
            books: self.books.clone(),
            symbol_to_id: self.symbol_to_id.clone(),
            id_to_symbol: self.id_to_symbol.clone(),
            next_symbol_id: std::sync::atomic::AtomicU16::new(
                self.next_symbol_id.load(std::sync::atomic::Ordering::Relaxed)
            ),
            symbol_interner: self.symbol_interner.clone(),
            default_depth: self.default_depth,
            exchange_client: self.exchange_client.clone(),
            recovery_mutex: self.recovery_mutex.clone(),
            last_recovery: new_last_recovery,
            update_callbacks: self.update_callbacks.clone(),
            runtime_handle: self.runtime_handle.clone(),
        }
    }
}
