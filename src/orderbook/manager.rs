// src/orderbook/manager.rs

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{ RwLock, Mutex };
use tracing::{ debug, error, info, warn };
use anyhow::{ Result, Context };
use std::time::{ Duration, Instant };

use crate::models::level::{ Level, LevelUpdate };
use crate::enums::side::Side;
use crate::exchange::client::ExchangeClient;
use crate::exchange::client::DepthResponse;
use super::orderbook::OrderBook;
use super::orderbook::OrderBookState;

/// Manages multiple orderbooks for different symbols
pub struct OrderBookManager {
    /// Maps symbol to orderbook
    books: RwLock<HashMap<Arc<str>, Arc<OrderBook>>>,
    /// Default depth to use for new orderbooks
    default_depth: usize,
    /// Exchange client for API requests
    exchange_client: Arc<dyn ExchangeClient + Send + Sync>,
    /// Mutex for the recovery process
    recovery_mutex: Mutex<()>,
    /// Tracks last recovery attempt per symbol to avoid too frequent retries
    last_recovery: RwLock<HashMap<Arc<str>, Instant>>,
}

impl OrderBookManager {
    /// Create a new orderbook manager with default depth
    #[inline]
    pub fn new(
        default_depth: usize,
        exchange_client: Arc<dyn ExchangeClient + Send + Sync>
    ) -> Self {
        Self {
            books: RwLock::new(HashMap::with_capacity(100)),
            default_depth,
            exchange_client,
            recovery_mutex: Mutex::new(()),
            last_recovery: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create an orderbook for a symbol - fast path optimized
    pub async fn get_or_create_book(&self, symbol: &Arc<str>) -> Arc<OrderBook> {
        // First try read lock for faster path
        {
            let books = self.books.read().await;
            if let Some(book) = books.get(symbol) {
                return book.clone();
            }
        }

        // Not found, upgrade to write lock
        let mut books = self.books.write().await;

        // Double-check in case another thread created it while we were waiting
        match books.get(symbol) {
            Some(book) => book.clone(),
            None => {
                let book = Arc::new(OrderBook::new(self.default_depth));
                books.insert(symbol.clone(), book.clone());
                debug!(?symbol, "Created new orderbook");
                book
            }
        }
    }

    /// Apply a snapshot to initialize or reset an orderbook
    pub async fn apply_snapshot(
        &self,
        symbol: Arc<str>,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        last_update_id: u64
    ) {
        let book = self.get_or_create_book(&symbol).await;

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
        book.apply_snapshot(bids_levels, asks_levels, last_update_id).await;

        // Reset recovery tracker
        let mut last_recovery = self.last_recovery.write().await;
        last_recovery.remove(&symbol);

        // Log the snapshot application
        debug!(
            symbol = %symbol,
            update_id = last_update_id,
            bid_count = bids.len(),
            ask_count = asks.len(),
            "Applied orderbook snapshot"
        );
    }

    /// Apply incremental depth update - optimized fast path
    pub async fn apply_depth_update(
        &self,
        symbol: &Arc<str>,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        first_update_id: u64,
        last_update_id: u64
    ) -> bool {
        // Get existing book without creating if missing
        let books = self.books.read().await;
        let book = match books.get(symbol) {
            Some(book) => book,
            None => {
                // If book doesn't exist, we trigger recovery process
                drop(books); // Release lock first
                self.trigger_recovery(symbol).await;
                return false;
            }
        };

        // Try to apply the update (fast path)
        match book.apply_depth_update(bids, asks, first_update_id, last_update_id).await {
            Ok(true) => true, // Update applied successfully
            Ok(false) => {
                // Book needs recovery, trigger async recovery
                self.trigger_recovery(symbol).await;
                false
            }
            Err(_) => {
                // Fatal error, trigger recovery
                self.trigger_recovery(symbol).await;
                false
            }
        }
    }

    /// Apply a single update - simplified interface for individual price level updates
    #[inline]
    pub async fn apply_single_update(
        &self,
        symbol: &Arc<str>,
        side: Side,
        price: f64,
        quantity: f64
    ) {
        // Create a single-item update
        let updates = vec![(price, quantity)];

        // Use the same mechanism as regular depth updates
        match side {
            Side::Bid => {
                self.apply_depth_update(symbol, &updates, &[], u64::MAX - 1, u64::MAX).await;
            }
            Side::Ask => {
                self.apply_depth_update(symbol, &[], &updates, u64::MAX - 1, u64::MAX).await;
            }
        }
    }

    /// Trigger recovery process but don't wait for it to complete
    /// Uses rate limiting to avoid excessive recovery attempts
    pub async fn trigger_recovery(&self, symbol: &Arc<str>) {
        if symbol.as_ref().trim().is_empty() {
            error!("Cannot trigger recovery for empty symbol");
            return;
        }

        // Check if we've attempted recovery recently (don't spam with requests)
        {
            let last_recovery = self.last_recovery.read().await;
            if let Some(last_time) = last_recovery.get(symbol) {
                if last_time.elapsed() < Duration::from_secs(5) {
                    // Skip recovery if attempted in last 5 seconds
                    return;
                }
            }
        }

        // Mark that we're attempting recovery
        {
            let mut last_recovery = self.last_recovery.write().await;
            last_recovery.insert(symbol.clone(), Instant::now());
        }

        // Clone what we need for the task
        let symbol_clone = symbol.clone();
        let manager = self.clone();

        // Launch async task for recovery
        tokio::spawn(async move {
            // Use a short timeout for the lock to avoid blocking other symbols
            let lock_result = tokio::time::timeout(
                Duration::from_millis(100),
                manager.recovery_mutex.lock()
            ).await;

            let _lock = match lock_result {
                Ok(lock) => lock,
                Err(_) => {
                    // Couldn't get lock in time, another recovery is in progress
                    debug!(?symbol_clone, "Skipping orderbook recovery due to lock timeout");
                    return;
                }
            };

            // Double check if we still need recovery
            let book = manager.get_or_create_book(&symbol_clone).await;
            if book.is_synced() {
                return; // No longer needs recovery
            }

            debug!(
                symbol = %symbol_clone,
                "Starting orderbook recovery process"
            );

            // Fetch snapshot (with retry logic)
            for attempt in 1..=3 {
                match manager.fetch_snapshot(symbol_clone.as_ref()).await {
                    Ok((snapshot_bids, snapshot_asks, snapshot_last_id)) => {
                        // Apply the snapshot
                        manager.apply_snapshot(
                            symbol_clone.clone(),
                            &snapshot_bids,
                            &snapshot_asks,
                            snapshot_last_id
                        ).await;

                        debug!(
                            symbol = %symbol_clone,
                            "Orderbook recovery completed successfully"
                        );

                        return; // Success
                    }
                    Err(e) => {
                        if attempt < 3 {
                            warn!(
                                symbol = %symbol_clone,
                                error = %e,
                                attempt = attempt,
                                "Orderbook recovery snapshot fetch failed, retrying"
                            );
                            tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
                        } else {
                            error!(
                                symbol = %symbol_clone,
                                error = %e,
                                "Orderbook recovery failed after all retry attempts"
                            );
                        }
                    }
                }
            }
        });
    }

    /// Get the best bid and ask for a symbol - fast path optimized
    #[inline]
    pub async fn get_top_of_book(
        &self,
        symbol: &Arc<str>
    ) -> Option<(Option<Level>, Option<Level>)> {
        // Fast path - read only
        let books = self.books.read().await;
        match books.get(symbol) {
            Some(book) if book.is_synced() => { Some(book.snapshot().await) }
            Some(_) => {
                // Book exists but not in sync, trigger recovery without waiting
                drop(books);
                self.trigger_recovery(symbol).await;
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
    pub async fn is_initialized(&self, symbol: &Arc<str>) -> bool {
        let books = self.books.read().await;
        if let Some(book) = books.get(symbol) {
            book.is_synced()
        } else {
            false
        }
    }

    /// Get all the symbols that have orderbooks
    pub async fn get_symbols(&self) -> Vec<Arc<str>> {
        let books = self.books.read().await;
        books.keys().cloned().collect()
    }

    /// Get statistics about all orderbooks
    pub async fn get_stats(&self) -> HashMap<Arc<str>, (bool, u64)> {
        let books = self.books.read().await;
        let mut stats = HashMap::with_capacity(books.len());

        for (symbol, book) in books.iter() {
            stats.insert(symbol.clone(), (book.is_synced(), book.get_last_update_id()));
        }

        stats
    }

    /// Get the mid-price for a symbol
    pub async fn get_mid_price(&self, symbol: &Arc<str>) -> Option<f64> {
        let books = self.books.read().await;
        if let Some(book) = books.get(symbol) {
            if book.is_synced() { book.mid_price().await } else { None }
        } else {
            None
        }
    }

    /// Get multiple price levels for a symbol
    pub async fn get_price_levels(
        &self,
        symbol: &Arc<str>,
        depth: Option<usize>
    ) -> Option<(Vec<Level>, Vec<Level>)> {
        let books = self.books.read().await;
        if let Some(book) = books.get(symbol) {
            if book.is_synced() {
                Some((book.get_bids(depth).await, book.get_asks(depth).await))
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
            let book = self.get_or_create_book(symbol).await;

            if !book.is_synced() {
                match self.fetch_snapshot(symbol.as_ref()).await {
                    Ok((bids, asks, last_update_id)) => {
                        self.apply_snapshot(symbol.clone(), &bids, &asks, last_update_id).await;
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
    pub async fn clear_all(&self) {
        let mut books = self.books.write().await;
        books.clear();

        debug!("Cleared all orderbooks");
    }
}

// Clone implementation for OrderBookManager to use in async tasks
impl Clone for OrderBookManager {
    fn clone(&self) -> Self {
        Self {
            books: RwLock::new(HashMap::new()), // Create new empty map
            default_depth: self.default_depth,
            exchange_client: self.exchange_client.clone(), // Clone the Arc
            recovery_mutex: Mutex::new(()), // Create new mutex
            last_recovery: RwLock::new(HashMap::new()), // Create new map
        }
    }
}

impl fmt::Debug for OrderBookManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderBookManager")
            .field("books", &self.books)
            .field("default_depth", &self.default_depth)
            .field("exchange_client", &"<dyn ExchangeClient>")
            .field("recovery_mutex", &self.recovery_mutex)
            .field("last_recovery", &self.last_recovery)
            .finish()
    }
}
