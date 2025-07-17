use std::sync::atomic::{ AtomicU64, Ordering };
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Semaphore;
use anyhow::{ Result, Context };
use std::time::Duration;
use parking_lot::RwLock;
use std::thread;
use crate::models::level::Level;
use crate::exchange::client::ExchangeClient;
use crate::orderbook_manager::orderbook::OrderBook;
use crate::utils::symbol_interner::{ global, SymbolId };

const MAX_SYMBOLS: usize = 2048;
static LAST_REQUEST_TIME: AtomicU64 = AtomicU64::new(0);
const MIN_REQUEST_INTERVAL_MS: u64 = 100;
const MAX_CONCURRENT_SNAPSHOTS: usize = 2;

lazy_static::lazy_static! {
    static ref SNAPSHOT_SEMAPHORE: Arc<Semaphore> = Arc::new(
        Semaphore::new(MAX_CONCURRENT_SNAPSHOTS)
    );
}

pub type OrderbookUpdateCallback = Arc<dyn Fn(SymbolId, &OrderBook) + Send + Sync>;

pub struct OrderBookManager {
    pub books: Box<[Option<Arc<OrderBook>>; MAX_SYMBOLS]>,
    default_depth: usize,
    exchange_client: Arc<dyn ExchangeClient + Send + Sync>,
    recovery_mutex: Arc<Mutex<()>>,
    last_recovery: Box<[std::sync::atomic::AtomicU64; MAX_SYMBOLS]>,
    update_callback: Arc<RwLock<Option<OrderbookUpdateCallback>>>,
    runtime_handle: tokio::runtime::Handle,
}

unsafe impl Sync for OrderBookManager {}

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
            update_callback: Arc::new(RwLock::new(None)),
            runtime_handle,
        }
    }

    #[inline]
    pub fn register_update_callback(&self, callback: OrderbookUpdateCallback) {
        let mut cb = self.update_callback.write();
        *cb = Some(callback);
    }

    #[inline(always)]
    pub fn trigger_arbitrage_detection(&self, symbol_id: SymbolId) {
        if let Some(callback) = self.update_callback.try_read() {
            if let Some(ref cb) = *callback {
                if let Some(book) = self.get_book_by_id(symbol_id) {
                    cb(symbol_id, &book);
                }
            }
        }
    }

    #[inline(always)]
    pub fn get_or_create_book_by_id(&self, symbol_id: SymbolId) -> Arc<OrderBook> {
        unsafe {
            let books_ptr = self.books.as_ptr() as *mut Option<Arc<OrderBook>>;
            let book_option = &mut *books_ptr.add(symbol_id as usize);

            if let Some(book) = book_option {
                return Arc::clone(book);
            }

            let book = Arc::new(OrderBook::new(self.default_depth));
            *book_option = Some(Arc::clone(&book));
            book
        }
    }

    #[inline(always)]
    pub fn get_book_by_id(&self, symbol_id: SymbolId) -> Option<Arc<OrderBook>> {
        if symbol_id >= (MAX_SYMBOLS as u16) {
            return None;
        }

        unsafe {
            let books_ptr = self.books.as_ptr();
            (*books_ptr.add(symbol_id as usize)).clone()
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
        let success = self.apply_depth_update_pure(
            symbol_id,
            bids,
            asks,
            first_update_id,
            last_update_id
        );

        if success {
            self.trigger_arbitrage_detection(symbol_id);
        }

        success
    }

    #[inline(always)]
    pub fn apply_depth_update_pure(
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

        match book.apply_depth_update_internal(bids, asks, first_update_id, last_update_id) {
            Ok(true) => true,
            Ok(false) | Err(_) => {
                self.trigger_recovery_by_id(symbol_id);
                false
            }
        }
    }

    pub fn trigger_recovery_by_id(&self, symbol_id: SymbolId) {
        if symbol_id >= (MAX_SYMBOLS as u16) {
            return;
        }

        let now_ms = std::time::SystemTime
            ::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let last_time = unsafe {
            self.last_recovery
                .get_unchecked(symbol_id as usize)
                .load(std::sync::atomic::Ordering::Relaxed)
        };

        if now_ms < last_time + 2000 {
            return;
        }

        unsafe {
            self.last_recovery
                .get_unchecked(symbol_id as usize)
                .store(now_ms, std::sync::atomic::Ordering::Relaxed);
        }

        let symbol = if let Some(sym) = global::get_symbol(symbol_id) {
            sym
        } else {
            return;
        };

        let manager = self.clone();
        self.runtime_handle.spawn(async move {
            manager.recovery_task_rate_limited(symbol, symbol_id).await;
        });
    }

    async fn recovery_task_rate_limited(&self, symbol: Arc<str>, symbol_id: SymbolId) {
        let _permit = match SNAPSHOT_SEMAPHORE.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                return;
            }
        };

        let now_ms = std::time::SystemTime
            ::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let last_request = LAST_REQUEST_TIME.load(Ordering::Relaxed);
        if now_ms < last_request + MIN_REQUEST_INTERVAL_MS {
            let wait_time = last_request + MIN_REQUEST_INTERVAL_MS - now_ms;
            tokio::time::sleep(Duration::from_millis(wait_time)).await;
        }

        LAST_REQUEST_TIME.store(
            std::time::SystemTime
                ::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            Ordering::Relaxed
        );

        let jitter = rand::random::<u64>() % 100;
        tokio::time::sleep(Duration::from_millis(jitter)).await;

        match self.fetch_snapshot(symbol.as_ref()).await {
            Ok((snapshot_bids, snapshot_asks, snapshot_last_id)) => {
                if !snapshot_bids.is_empty() && !snapshot_asks.is_empty() {
                    self.apply_snapshot_by_id(
                        symbol_id,
                        &snapshot_bids,
                        &snapshot_asks,
                        snapshot_last_id
                    );
                }
            }
            Err(e) => {
                if e.to_string().contains("429") || e.to_string().contains("418") {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
        }
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

        self.trigger_arbitrage_detection(symbol_id);
    }

    pub async fn fetch_snapshot(
        &self,
        symbol: &str
    ) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>, u64)> {
        if symbol.trim().is_empty() {
            return Err(anyhow::anyhow!("Cannot fetch snapshot for empty symbol"));
        }

        let depth_response = self.exchange_client
            .fetch_depth(symbol, 10).await
            .context(format!("Failed to fetch orderbook snapshot for {symbol}"))?;

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

    pub fn get_symbol_mappings(&self) -> Vec<(String, u16)> {
        global::get_all_mappings()
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
            exchange_client: Arc::clone(&self.exchange_client),
            recovery_mutex: Arc::clone(&self.recovery_mutex),
            last_recovery: new_last_recovery,
            update_callback: Arc::clone(&self.update_callback),
            runtime_handle: self.runtime_handle.clone(),
        }
    }
}
