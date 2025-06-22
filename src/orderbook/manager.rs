use std::sync::Arc;
use std::sync::Mutex;
use tracing::{ debug, error, info, warn };
use anyhow::{ Result, Context };
use std::time::Duration;
use parking_lot::RwLock;
use std::thread;

use crate::models::level::Level;
use crate::exchange::client::ExchangeClient;
use crate::orderbook::orderbook::OrderBook;
use crate::utils::symbol_interner::{ global, SymbolId };

const MAX_SYMBOLS: usize = 2048;

pub type OrderbookUpdateCallback = Arc<dyn Fn(SymbolId, &OrderBook) + Send + Sync>;

pub struct OrderBookManager {
    pub books: Box<[Option<Arc<OrderBook>>; MAX_SYMBOLS]>,
    default_depth: usize,
    exchange_client: Arc<dyn ExchangeClient + Send + Sync>,
    recovery_mutex: Arc<Mutex<()>>,
    last_recovery: Box<[std::sync::atomic::AtomicU64; MAX_SYMBOLS]>,
    update_callbacks: Arc<RwLock<Vec<OrderbookUpdateCallback>>>,
    runtime_handle: tokio::runtime::Handle,
}

impl OrderBookManager {
    pub fn new(
        default_depth: usize,
        exchange_client: Arc<dyn ExchangeClient + Send + Sync>
    ) -> Self {
        let runtime_handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                let (tx, rx) = std::sync::mpsc::channel();
                thread::spawn(move || {
                    let rt = tokio::runtime::Builder
                        ::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .build()
                        .expect("Failed to create runtime for OrderBookManager");

                    tx.send(rt.handle().clone()).expect("Failed to send runtime handle");

                    rt.block_on(async {
                        loop {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    });
                });
                rx.recv().expect("Failed to receive runtime handle")
            }
        };

        let books = Box::new([const { None }; MAX_SYMBOLS]);
        let last_recovery = Box::new([const { std::sync::atomic::AtomicU64::new(0) }; MAX_SYMBOLS]);

        Self {
            books,
            default_depth,
            exchange_client,
            recovery_mutex: Arc::new(Mutex::new(())),
            last_recovery,
            update_callbacks: Arc::new(RwLock::new(Vec::with_capacity(10))),
            runtime_handle,
        }
    }

    pub fn preload_symbols(&self, symbols: &[String]) {
        global::intern_batch(symbols);
    }

    pub fn get_symbol_mappings(&self) -> Vec<(String, u16)> {
        global::get_all_mappings()
    }

    #[inline]
    pub fn register_update_callback(&self, callback: OrderbookUpdateCallback) {
        let mut callbacks = self.update_callbacks.write();
        callbacks.push(callback);
    }

    #[inline(always)]
    fn notify_update(&self, symbol_id: SymbolId, book: &OrderBook) {
        let callbacks = self.update_callbacks.read();
        if callbacks.is_empty() {
            return;
        }

        for callback in callbacks.iter() {
            callback(symbol_id, book);
        }
    }

    #[inline(always)]
    pub fn get_or_create_book_by_id(&self, symbol_id: SymbolId) -> Arc<OrderBook> {
        unsafe {
            let books_ptr = self.books.as_ptr() as *mut Option<Arc<OrderBook>>;
            let book_option = &mut *books_ptr.add(symbol_id as usize);

            if let Some(book) = book_option {
                return book.clone();
            }

            let book = Arc::new(OrderBook::new(self.default_depth));
            *book_option = Some(book.clone());
            book
        }
    }

    #[inline(always)]
    pub fn apply_depth_update(
        &self,
        symbol_id: SymbolId,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        first_update_id: u64,
        last_update_id: u64
    ) -> bool {
        if symbol_id >= (MAX_SYMBOLS as u16) {
            return false;
        }

        let book = unsafe {
            let books_ptr = self.books.as_ptr();
            if let Some(book) = &*books_ptr.add(symbol_id as usize) {
                book
            } else {
                self.trigger_recovery_by_id(symbol_id);
                return false;
            }
        };

        match book.apply_depth_update_raw(bids, asks, first_update_id, last_update_id) {
            Ok(true) => {
                // self.notify_update(symbol_id, book);
                true
            }
            Ok(false) => {
                self.trigger_recovery_by_id(symbol_id);
                false
            }
            Err(_) => {
                self.trigger_recovery_by_id(symbol_id);
                false
            }
        }
    }

    pub fn trigger_recovery_by_id(&self, symbol_id: SymbolId) {
        if symbol_id >= (MAX_SYMBOLS as u16) {
            return;
        }

        let now = std::time::SystemTime
            ::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let last_time = unsafe {
            self.last_recovery
                .get_unchecked(symbol_id as usize)
                .load(std::sync::atomic::Ordering::Relaxed)
        };

        if now < last_time + 3 {
            return;
        }

        unsafe {
            self.last_recovery
                .get_unchecked(symbol_id as usize)
                .store(now, std::sync::atomic::Ordering::Relaxed);
        }

        let symbol = if let Some(sym) = global::get_symbol(symbol_id) {
            sym
        } else {
            return;
        };

        let manager = self.clone();
        self.runtime_handle.spawn(async move {
            manager.recovery_task(symbol).await;
        });
    }

    pub fn apply_snapshot(
        &self,
        symbol: Arc<str>,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        last_update_id: u64
    ) {
        let symbol_id = global::intern_arc(&symbol);
        self.apply_snapshot_by_id(symbol_id, bids, asks, last_update_id);
    }

    pub fn apply_snapshot_by_id(
        &self,
        symbol_id: SymbolId,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        last_update_id: u64
    ) {
        let book = self.get_or_create_book_by_id(symbol_id);

        let bids_levels: Vec<Level> = bids
            .iter()
            .map(|(price, qty)| Level { price: *price, quantity: *qty })
            .collect();

        let asks_levels: Vec<Level> = asks
            .iter()
            .map(|(price, qty)| Level { price: *price, quantity: *qty })
            .collect();

        book.apply_snapshot(bids_levels, asks_levels, last_update_id);

        unsafe {
            self.last_recovery
                .get_unchecked(symbol_id as usize)
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }

        self.notify_update(symbol_id, &book);

        if let Some(symbol) = global::get_symbol(symbol_id) {
            debug!(
                symbol = %symbol,
                update_id = last_update_id,
                bid_count = bids.len(),
                ask_count = asks.len(),
                "Applied orderbook snapshot"
            );
        }
    }

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

    #[inline(always)]
    pub fn get_top_of_book_by_id(
        &self,
        symbol_id: SymbolId
    ) -> Option<(Option<Level>, Option<Level>)> {
        if symbol_id >= (MAX_SYMBOLS as u16) {
            return None;
        }

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

    pub fn get_top_of_book_internal(
        &self,
        symbol: &Arc<str>
    ) -> Option<(Option<Level>, Option<Level>)> {
        if let Some(symbol_id) = global::get_id_arc(symbol) {
            self.get_top_of_book_by_id(symbol_id)
        } else {
            None
        }
    }

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

    pub fn get_optimization_stats(&self) -> OptimizationStats {
        let interner_len = global::len();

        OptimizationStats {
            total_books: interner_len,
            symbol_hash_cache_size: interner_len,
        }
    }
}

#[derive(Debug)]
pub struct OptimizationStats {
    pub total_books: usize,
    pub symbol_hash_cache_size: usize,
}

impl Clone for OrderBookManager {
    fn clone(&self) -> Self {
        let new_last_recovery = Box::new(
            [const { std::sync::atomic::AtomicU64::new(0) }; MAX_SYMBOLS]
        );
        for i in 0..MAX_SYMBOLS {
            let val = self.last_recovery[i].load(std::sync::atomic::Ordering::Relaxed);
            new_last_recovery[i].store(val, std::sync::atomic::Ordering::Relaxed);
        }

        Self {
            books: self.books.clone(),
            default_depth: self.default_depth,
            exchange_client: self.exchange_client.clone(),
            recovery_mutex: self.recovery_mutex.clone(),
            last_recovery: new_last_recovery,
            update_callbacks: self.update_callbacks.clone(),
            runtime_handle: self.runtime_handle.clone(),
        }
    }
}
