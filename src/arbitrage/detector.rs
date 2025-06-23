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
    fallback_executor: Arc<ArbitrageExecutor>,
    fix_executor: Option<Arc<FixArbitrageExecutor>>,
    is_executing: Arc<AtomicBool>,
    execution_counter: Arc<AtomicU64>,
    start_amount: f32,
    min_volume_multiplier: f32,
    min_profit_for_execution: f32,
    use_fix_execution: bool,
    pub stats_counter: Arc<AtomicUsize>,
    pub profit_counter: Arc<AtomicUsize>,
    last_execution_ns: Arc<AtomicU64>,
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

            let idx = i as u16;

            let mut add_path = |symbol_id: u16, idx: u16| {
                if (symbol_id as usize) < MAX_SYMBOLS {
                    let symbol_paths = &mut symbol_map[symbol_id as usize];
                    if (symbol_paths.count as usize) < MAX_PATHS_PER_SYMBOL {
                        symbol_paths.indices[symbol_paths.count as usize] = idx;
                        symbol_paths.count += 1;
                    }
                }
            };

            add_path(first_symbol_id, idx);
            add_path(second_symbol_id, idx);
            add_path(third_symbol_id, idx);
        }

        let use_fix_execution = fix_executor.is_some();

        Arc::new(ArbitrageDetectorState {
            orderbook_manager: orderbook_manager.clone(),
            all_paths: unwrapped_paths.into_boxed_slice(),
            path_data: path_data.into_boxed_slice(),
            symbol_map,
            fallback_executor,
            fix_executor,
            is_executing: Arc::new(AtomicBool::new(false)),
            execution_counter: Arc::new(AtomicU64::new(0)),
            start_amount: start_amount as f32,
            min_volume_multiplier: min_volume_multiplier as f32,
            min_profit_for_execution: (1.0 + min_profit_threshold) as f32,
            use_fix_execution,
            stats_counter: Arc::new(AtomicUsize::new(0)),
            profit_counter: Arc::new(AtomicUsize::new(0)),
            last_execution_ns: Arc::new(AtomicU64::new(0)),
        })
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
    fn execute_immediately(&self, opportunity: Opportunity) -> bool {
        if
            self.is_executing
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
        {
            return false;
        }

        let path = &self.all_paths[opportunity.path_index as usize];

        let exec_opportunity = ArbitrageOpportunity {
            path: Arc::new(path.clone()),
            profit_ratio: opportunity.profit_ratio as f64,
            start_amount: self.start_amount as f64,
            end_amount: (self.start_amount as f64) * (opportunity.profit_ratio as f64),
            execution_time_ns: 0,
        };

        let execution_start = Instant::now();
        let success = if self.use_fix_execution {
            if let Some(fix_executor) = &self.fix_executor {
                let result = fix_executor.execute_triangular_arbitrage(&exec_opportunity);

                if result.success {
                    tracing::info!(
                        "FIX Execution successful: {:.4}% profit, {} trades, actual profit: ${:.2}",
                        exec_opportunity.profit_percentage(),
                        result.trades_executed,
                        result.actual_profit
                    );
                } else {
                    tracing::warn!(
                        "FIX Execution failed: {} trades completed, error: {:?}",
                        result.trades_executed,
                        result.error
                    );
                }

                result.success
            } else {
                false
            }
        } else {
            let result = self.fallback_executor.execute(&exec_opportunity);
            result.success
        };

        let execution_time = execution_start.elapsed().as_nanos() as u64;

        self.execution_counter.fetch_add(1, Ordering::Relaxed);
        self.last_execution_ns.store(execution_time, Ordering::Relaxed);

        if success {
            self.profit_counter.fetch_add(1, Ordering::Relaxed);

            eprintln!(
                "EXEC: {:.4}% profit in {}μs",
                (opportunity.profit_ratio - 1.0) * 100.0,
                execution_time / 1000
            );
        }

        self.is_executing.store(false, Ordering::Release);

        success
    }

    #[inline(always)]
    pub fn handle_update_by_id(&self, symbol_id: u16, _book: &OrderBook) {
        let now_ns = std::time::SystemTime
            ::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let last_exec = self.last_execution_ns.load(Ordering::Relaxed);
        if now_ns.saturating_sub(last_exec) < 10_000 {
            return;
        }

        if self.is_executing.load(Ordering::Relaxed) {
            return;
        }

        if (symbol_id as usize) < MAX_SYMBOLS {
            let symbol_paths = &self.symbol_map[symbol_id as usize];
            let count = symbol_paths.count as usize;

            for i in 0..count.min(8) {
                let path_idx = unsafe { *symbol_paths.indices.get_unchecked(i) };
                if let Some(opportunity) = self.check_path_zero_alloc(path_idx) {
                    if opportunity.profit_ratio >= self.min_profit_for_execution {
                        if self.execute_immediately(opportunity) {
                            self.stats_counter.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    }
                }
            }
        }
    }

    pub fn get_execution_stats(&self) -> (u64, u64, u64) {
        (
            self.execution_counter.load(Ordering::Relaxed),
            self.profit_counter.load(Ordering::Relaxed) as u64,
            self.last_execution_ns.load(Ordering::Relaxed),
        )
    }

    pub fn get_fix_execution_stats(&self) -> Option<crate::executor::fix_executor::ExecutionStats> {
        self.fix_executor.as_ref().map(|executor| executor.get_execution_stats())
    }

    pub fn get_optimization_stats(&self) -> DetectorOptimizationStats {
        DetectorOptimizationStats {
            total_paths: self.all_paths.len(),
            symbol_map_entries: global::len(),
        }
    }

    pub fn run_stats_task(&self) {
        let stats_counter = self.stats_counter.clone();
        let profit_counter = self.profit_counter.clone();
        let execution_counter = self.execution_counter.clone();
        let fix_executor = self.fix_executor.clone();

        thread::Builder
            ::new()
            .name("arb-stats".to_string())
            .spawn(move || {
                loop {
                    thread::sleep(std::time::Duration::from_secs(30));
                    let scans = stats_counter.swap(0, Ordering::Relaxed);
                    let profits = profit_counter.load(Ordering::Relaxed);
                    let executions = execution_counter.load(Ordering::Relaxed);

                    if scans > 0 {
                        eprintln!(
                            "Stats: {} opportunities found, {} executions, {} profitable",
                            scans,
                            executions,
                            profits
                        );

                        if let Some(fix_exec) = &fix_executor {
                            let fix_stats = fix_exec.get_execution_stats();
                            eprintln!("FIX Stats: {}", fix_stats);
                        }
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

impl Clone for ArbitrageDetectorState {
    fn clone(&self) -> Self {
        Self {
            orderbook_manager: self.orderbook_manager.clone(),
            all_paths: self.all_paths.clone(),
            path_data: self.path_data.clone(),
            symbol_map: self.symbol_map.clone(),
            fallback_executor: self.fallback_executor.clone(),
            fix_executor: self.fix_executor.clone(),
            is_executing: self.is_executing.clone(),
            execution_counter: self.execution_counter.clone(),
            start_amount: self.start_amount,
            min_volume_multiplier: self.min_volume_multiplier,
            min_profit_for_execution: self.min_profit_for_execution,
            use_fix_execution: self.use_fix_execution,
            stats_counter: self.stats_counter.clone(),
            profit_counter: self.profit_counter.clone(),
            last_execution_ns: self.last_execution_ns.clone(),
        }
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
