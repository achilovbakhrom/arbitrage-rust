use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{ AtomicUsize, AtomicBool, AtomicU64, Ordering };
use std::thread;

use crate::models::triangular_path::TriangularPath;
use crate::orderbook::manager::OrderBookManager;
use crate::orderbook::orderbook::OrderBook;
use crate::executor::executor::ArbitrageExecutor;
use crate::executor::fix_executor::FixArbitrageExecutor;
use crate::utils::symbol_interner::global;

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct Opportunity {
    pub path_index: u16,
    pub profit_ratio: f32,
    pub execution_time_ns: u32,
    pub liquidity_score: u8,
}

#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub path: Arc<TriangularPath>,
    pub profit_ratio: f64,
    pub start_amount: f64,
    pub end_amount: f64,
    pub execution_time_ns: u64,
    pub symbols_involved: [u16; 3],
}

impl ArbitrageOpportunity {
    #[inline(always)]
    pub fn profit_percentage(&self) -> f64 {
        (self.profit_ratio - 1.0) * 100.0
    }

    pub fn display(&self) -> String {
        format!(
            "{} → {} → {} | Profit: {:.4}% | Volume: OK | Time: {}ns",
            self.path.first_symbol,
            self.path.second_symbol,
            self.path.third_symbol,
            self.profit_percentage(),
            self.execution_time_ns
        )
    }
}

#[derive(Debug)]
pub struct ExecutionRequest {
    pub opportunity: ArbitrageOpportunity,
    pub timestamp: Instant,
    pub priority: u8,
}

#[derive(Debug)]
pub struct ExecutionResult {
    pub success: bool,
    pub profit_amount: f64,
    pub execution_time: std::time::Duration,
    pub error: Option<String>,
}

pub type ExecutionCallback = Arc<dyn Fn(ArbitrageOpportunity) + Send + Sync + 'static>;

#[repr(C, align(64))]
#[derive(Clone, Debug)]
struct PathData {
    first_symbol_id: u16,
    second_symbol_id: u16,
    third_symbol_id: u16,
    directions: u8,
    fee_multiplier_cubed: f32,
    min_threshold: f32,
    volume_multiplier: f32,
    _padding: [u8; 49],
}

impl PathData {
    #[inline(always)]
    fn first_is_base_to_quote(&self) -> bool {
        (self.directions & 0b001) != 0
    }

    #[inline(always)]
    fn second_is_base_to_quote(&self) -> bool {
        (self.directions & 0b010) != 0
    }

    #[inline(always)]
    fn third_is_base_to_quote(&self) -> bool {
        (self.directions & 0b100) != 0
    }
}

const MAX_PATHS_PER_SYMBOL: usize = 16;
const MAX_SYMBOLS: usize = 2048;

#[repr(C, align(64))]
#[derive(Clone)]
struct SymbolPaths {
    count: u8,
    indices: [u16; MAX_PATHS_PER_SYMBOL],
    _padding: [u8; 15],
}

pub struct ArbitrageDetectorState {
    pub orderbook_manager: Arc<OrderBookManager>,
    pub all_paths: Box<[TriangularPath]>,
    path_data: Box<[PathData]>,
    symbol_map: Box<[SymbolPaths; MAX_SYMBOLS]>,

    execution_callbacks: parking_lot::RwLock<Vec<ExecutionCallback>>,

    start_amount: f32,
    min_volume_multiplier: f32,
    min_profit_for_execution: f32,
    use_fix_execution: bool,

    pub stats_counter: Arc<AtomicUsize>,
    pub profit_counter: Arc<AtomicUsize>,
    pub execution_counter: Arc<AtomicU64>,
    pub opportunities_found_counter: Arc<AtomicU64>,
    pub opportunities_sent_counter: Arc<AtomicU64>,
    pub opportunities_executed_counter: Arc<AtomicU64>,
    pub opportunities_rejected_counter: Arc<AtomicU64>,

    paths_checked_counter: Arc<AtomicU64>,
    symbols_processed_counter: Arc<AtomicU64>,
    last_execution_ns: Arc<AtomicU64>,

    shutdown: Arc<AtomicBool>,
}

impl ArbitrageDetectorState {
    pub fn new(
        orderbook_manager: Arc<OrderBookManager>,
        paths: Vec<Arc<TriangularPath>>,
        fee_rate: f64,
        min_profit_threshold: f64,
        start_amount: f64,
        min_volume_multiplier: f64,
        fallback_executor: Arc<ArbitrageExecutor>,
        fix_executor: Option<Arc<FixArbitrageExecutor>>
    ) -> Arc<Self> {
        let unwrapped_paths: Vec<TriangularPath> = paths
            .iter()
            .map(|arc_path| (**arc_path).clone())
            .collect();

        let all_symbols: Vec<String> = unwrapped_paths
            .iter()
            .flat_map(|path| {
                [
                    path.first_symbol.to_string(),
                    path.second_symbol.to_string(),
                    path.third_symbol.to_string(),
                ]
            })
            .collect();

        global::intern_batch(&all_symbols);

        let mut path_data = Vec::with_capacity(unwrapped_paths.len());
        let mut symbol_map = Box::new(
            [
                const {
                    SymbolPaths {
                        count: 0,
                        indices: [0; MAX_PATHS_PER_SYMBOL],
                        _padding: [0; 15],
                    }
                };
                MAX_SYMBOLS
            ]
        );

        for (i, path) in unwrapped_paths.iter().enumerate() {
            let first_symbol_id = global::get_id(&path.first_symbol).unwrap();
            let second_symbol_id = global::get_id(&path.second_symbol).unwrap();
            let third_symbol_id = global::get_id(&path.third_symbol).unwrap();

            let directions =
                (path.first_is_base_to_quote as u8) |
                ((path.second_is_base_to_quote as u8) << 1) |
                ((path.third_is_base_to_quote as u8) << 2);

            path_data.push(PathData {
                first_symbol_id,
                second_symbol_id,
                third_symbol_id,
                directions,
                fee_multiplier_cubed: ((1.0 - fee_rate) as f32).powi(3),
                min_threshold: (1.0 + min_profit_threshold) as f32,
                volume_multiplier: min_volume_multiplier as f32,
                _padding: [0; 49],
            });

            let path_idx = i as u16;

            Self::add_path_to_symbol(&mut symbol_map, first_symbol_id, path_idx);
            Self::add_path_to_symbol(&mut symbol_map, second_symbol_id, path_idx);
            Self::add_path_to_symbol(&mut symbol_map, third_symbol_id, path_idx);
        }

        let use_fix_execution = fix_executor.is_some();

        Arc::new(ArbitrageDetectorState {
            orderbook_manager: orderbook_manager.clone(),
            all_paths: unwrapped_paths.into_boxed_slice(),
            path_data: path_data.into_boxed_slice(),
            symbol_map,
            execution_callbacks: parking_lot::RwLock::new(Vec::with_capacity(4)),
            start_amount: start_amount as f32,
            min_volume_multiplier: min_volume_multiplier as f32,
            min_profit_for_execution: (1.0 + min_profit_threshold) as f32,
            use_fix_execution,
            stats_counter: Arc::new(AtomicUsize::new(0)),
            profit_counter: Arc::new(AtomicUsize::new(0)),
            execution_counter: Arc::new(AtomicU64::new(0)),
            opportunities_found_counter: Arc::new(AtomicU64::new(0)),
            opportunities_sent_counter: Arc::new(AtomicU64::new(0)),
            opportunities_executed_counter: Arc::new(AtomicU64::new(0)),
            opportunities_rejected_counter: Arc::new(AtomicU64::new(0)),
            paths_checked_counter: Arc::new(AtomicU64::new(0)),
            symbols_processed_counter: Arc::new(AtomicU64::new(0)),
            last_execution_ns: Arc::new(AtomicU64::new(0)),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    #[inline]
    pub fn register_execution_callback(&self, callback: ExecutionCallback) {
        let mut callbacks = self.execution_callbacks.write();
        callbacks.push(callback);
        tracing::info!("Registered execution callback (total: {})", callbacks.len());
    }

    #[inline(always)]
    fn add_path_to_symbol(
        symbol_map: &mut Box<[SymbolPaths; MAX_SYMBOLS]>,
        symbol_id: u16,
        path_idx: u16
    ) {
        if (symbol_id as usize) < MAX_SYMBOLS {
            let symbol_paths = &mut symbol_map[symbol_id as usize];
            if (symbol_paths.count as usize) < MAX_PATHS_PER_SYMBOL {
                symbol_paths.indices[symbol_paths.count as usize] = path_idx;
                symbol_paths.count += 1;
            }
        }
    }

    #[inline(always)]
    fn check_path_zero_alloc(&self, path_idx: u16) -> Option<Opportunity> {
        unsafe {
            let path_data = self.path_data.get_unchecked(path_idx as usize);

            let first_book = self.orderbook_manager.get_or_create_book_by_id(
                path_data.first_symbol_id
            );
            let second_book = self.orderbook_manager.get_or_create_book_by_id(
                path_data.second_symbol_id
            );
            let third_book = self.orderbook_manager.get_or_create_book_by_id(
                path_data.third_symbol_id
            );

            if !first_book.is_synced() || !second_book.is_synced() || !third_book.is_synced() {
                return None;
            }

            let (first_bid, first_ask) = first_book.get_cached_top_of_book();
            let (second_bid, second_ask) = second_book.get_cached_top_of_book();
            let (third_bid, third_ask) = third_book.get_cached_top_of_book();

            let (first_price, first_qty) = if path_data.first_is_base_to_quote() {
                first_bid?
            } else {
                first_ask?
            };

            let (second_price, second_qty) = if path_data.second_is_base_to_quote() {
                second_bid?
            } else {
                second_ask?
            };

            let (third_price, third_qty) = if path_data.third_is_base_to_quote() {
                third_bid?
            } else {
                third_ask?
            };

            let mut amount = self.start_amount;

            amount = if path_data.first_is_base_to_quote() {
                amount * (first_price as f32)
            } else {
                amount / (first_price as f32)
            };

            amount = if path_data.second_is_base_to_quote() {
                amount * (second_price as f32)
            } else {
                amount / (second_price as f32)
            };

            amount = if path_data.third_is_base_to_quote() {
                amount * (third_price as f32)
            } else {
                amount / (third_price as f32)
            };

            amount *= path_data.fee_multiplier_cubed;

            let profit_ratio = amount / self.start_amount;

            if profit_ratio <= path_data.min_threshold {
                return None;
            }

            let min_vol = self.start_amount * self.min_volume_multiplier;
            let volume_ok =
                (first_qty as f32) >= min_vol &&
                (second_qty as f32) >= min_vol &&
                (third_qty as f32) >= min_vol;

            if !volume_ok {
                return None;
            }

            let avg_qty = ((first_qty + second_qty + third_qty) as f32) / 3.0;
            let liquidity_score = ((avg_qty / min_vol).min(2.0) * 127.5) as u8;

            Some(Opportunity {
                path_index: path_idx,
                profit_ratio,
                execution_time_ns: 0,
                liquidity_score,
            })
        }
    }

    #[inline(always)]
    fn trigger_execution_callbacks(&self, opportunity: ArbitrageOpportunity) -> bool {
        let callbacks = self.execution_callbacks.read();
        if callbacks.is_empty() {
            return false;
        }

        self.opportunities_sent_counter.fetch_add(1, Ordering::Relaxed);

        for callback in callbacks.iter() {
            callback(opportunity.clone());
        }

        true
    }

    #[inline(always)]
    pub fn handle_update_by_id(&self, symbol_id: u16, _book: &OrderBook) {
        self.symbols_processed_counter.fetch_add(1, Ordering::Relaxed);

        if (symbol_id as usize) < MAX_SYMBOLS {
            let symbol_paths = &self.symbol_map[symbol_id as usize];
            let path_count = symbol_paths.count as usize;

            if path_count == 0 {
                return;
            }

            let max_paths_to_check = path_count.min(8);

            for i in 0..max_paths_to_check {
                let path_idx = unsafe { *symbol_paths.indices.get_unchecked(i) };

                self.paths_checked_counter.fetch_add(1, Ordering::Relaxed);

                if let Some(opportunity) = self.check_path_zero_alloc(path_idx) {
                    if opportunity.profit_ratio >= self.min_profit_for_execution {
                        self.opportunities_found_counter.fetch_add(1, Ordering::Relaxed);

                        let path = &self.all_paths[opportunity.path_index as usize];

                        let first_symbol_id = global::get_id(&path.first_symbol).unwrap_or(0);
                        let second_symbol_id = global::get_id(&path.second_symbol).unwrap_or(0);
                        let third_symbol_id = global::get_id(&path.third_symbol).unwrap_or(0);

                        let arb_opportunity = ArbitrageOpportunity {
                            path: Arc::new(path.clone()),
                            profit_ratio: opportunity.profit_ratio as f64,
                            start_amount: self.start_amount as f64,
                            end_amount: (self.start_amount as f64) *
                            (opportunity.profit_ratio as f64),
                            execution_time_ns: 0,
                            symbols_involved: [first_symbol_id, second_symbol_id, third_symbol_id],
                        };

                        if self.trigger_execution_callbacks(arb_opportunity) {
                            self.stats_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }

    pub fn get_paths_for_symbol(&self, symbol_id: u16) -> usize {
        if (symbol_id as usize) < MAX_SYMBOLS {
            self.symbol_map[symbol_id as usize].count as usize
        } else {
            0
        }
    }

    pub fn get_execution_stats(&self) -> (u64, u64, u64) {
        (
            self.execution_counter.load(Ordering::Relaxed),
            self.profit_counter.load(Ordering::Relaxed) as u64,
            self.last_execution_ns.load(Ordering::Relaxed),
        )
    }

    pub fn get_performance_stats(&self) -> DetectorPerformanceStats {
        DetectorPerformanceStats {
            symbols_processed: self.symbols_processed_counter.load(Ordering::Relaxed),
            paths_checked: self.paths_checked_counter.load(Ordering::Relaxed),
            opportunities_found: self.opportunities_found_counter.load(Ordering::Relaxed),
            opportunities_sent: self.opportunities_sent_counter.load(Ordering::Relaxed),
            opportunities_executed: self.opportunities_executed_counter.load(Ordering::Relaxed),
            opportunities_rejected: self.opportunities_rejected_counter.load(Ordering::Relaxed),
            executions: self.execution_counter.load(Ordering::Relaxed),
            profitable_executions: self.profit_counter.load(Ordering::Relaxed) as u64,
        }
    }

    pub fn get_fix_execution_stats(&self) -> Option<crate::executor::fix_executor::ExecutionStats> {
        None
    }

    pub fn get_optimization_stats(&self) -> DetectorOptimizationStats {
        DetectorOptimizationStats {
            total_paths: self.all_paths.len(),
            symbol_map_entries: global::len(),
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        tracing::info!("Detector shutdown initiated");
    }

    pub fn run_stats_task(&self) {
        let stats_counter = self.stats_counter.clone();
        let profit_counter = self.profit_counter.clone();
        let execution_counter = self.execution_counter.clone();
        let paths_checked_counter = self.paths_checked_counter.clone();
        let symbols_processed_counter = self.symbols_processed_counter.clone();
        let opportunities_found_counter = self.opportunities_found_counter.clone();
        let opportunities_sent_counter = self.opportunities_sent_counter.clone();
        let opportunities_executed_counter = self.opportunities_executed_counter.clone();
        let opportunities_rejected_counter = self.opportunities_rejected_counter.clone();

        thread::Builder
            ::new()
            .name("arb-stats".to_string())
            .spawn(move || {
                loop {
                    thread::sleep(std::time::Duration::from_secs(30));

                    let scans = stats_counter.swap(0, Ordering::Relaxed);
                    let profits = profit_counter.load(Ordering::Relaxed);
                    let executions = execution_counter.load(Ordering::Relaxed);
                    let paths_checked = paths_checked_counter.swap(0, Ordering::Relaxed);
                    let symbols_processed = symbols_processed_counter.swap(0, Ordering::Relaxed);
                    let found = opportunities_found_counter.swap(0, Ordering::Relaxed);
                    let sent = opportunities_sent_counter.swap(0, Ordering::Relaxed);
                    let executed = opportunities_executed_counter.swap(0, Ordering::Relaxed);
                    let rejected = opportunities_rejected_counter.swap(0, Ordering::Relaxed);

                    if scans > 0 || symbols_processed > 0 {
                        eprintln!(
                            "Detection: {} symbols, {} paths checked, {} opportunities found",
                            symbols_processed,
                            paths_checked,
                            found
                        );
                        eprintln!(
                            "Execution: {} sent, {} executed, {} rejected, {} profitable ({}%)",
                            sent,
                            executed,
                            rejected,
                            profits,
                            if executed > 0 {
                                (profits * 100) / (executed as usize)
                            } else {
                                0
                            }
                        );
                    }
                }
            })
            .expect("Failed to spawn stats thread");
    }
}

#[derive(Debug)]
pub struct DetectorOptimizationStats {
    pub total_paths: usize,
    pub symbol_map_entries: usize,
}

#[derive(Debug)]
pub struct DetectorPerformanceStats {
    pub symbols_processed: u64,
    pub paths_checked: u64,
    pub opportunities_found: u64,
    pub opportunities_sent: u64,
    pub opportunities_executed: u64,
    pub opportunities_rejected: u64,
    pub executions: u64,
    pub profitable_executions: u64,
}

impl Clone for ArbitrageDetectorState {
    fn clone(&self) -> Self {
        panic!("ArbitrageDetectorState cannot be cloned due to execution callbacks");
    }
}

pub fn create_detector(
    orderbook_manager: Arc<OrderBookManager>,
    fee_rate: f64,
    min_profit_threshold: f64,
    paths: Vec<Arc<TriangularPath>>,
    start_amount: f64,
    fallback_executor: Arc<ArbitrageExecutor>,
    fix_executor: Option<Arc<FixArbitrageExecutor>>,
    min_volume_multiplier: f64,
    is_perf: bool
) -> Arc<ArbitrageDetectorState> {
    let detector = ArbitrageDetectorState::new(
        orderbook_manager,
        paths,
        fee_rate,
        min_profit_threshold,
        start_amount,
        min_volume_multiplier,
        fallback_executor,
        fix_executor
    );

    if is_perf {
        detector.run_stats_task();
    }

    detector
}

pub fn start_detection_thread(detector: Arc<ArbitrageDetectorState>) -> thread::JoinHandle<()> {
    thread::Builder
        ::new()
        .name("arbitrage-detector".to_string())
        .spawn(move || {
            #[cfg(target_os = "linux")]
            {
                let mut cpu_set = nix::sched::CpuSet::new();
                cpu_set.set(1).ok();
                nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &cpu_set).ok();
            }

            let detector_for_callback = detector.clone();
            detector.orderbook_manager.register_update_callback(
                Arc::new(move |symbol_id: u16, book: &crate::orderbook::orderbook::OrderBook| {
                    detector_for_callback.handle_update_by_id(symbol_id, book);
                })
            );

            loop {
                std::thread::sleep(std::time::Duration::from_millis(1000));
            }
        })
        .expect("Failed to spawn arbitrage detection thread")
}
