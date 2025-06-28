// use std::sync::Arc;
// use std::sync::atomic::{ AtomicBool, Ordering };
// use crate::models::triangular_path::TriangularPath;
// use crate::orderbook::manager::OrderBookManager;
// use crate::utils::symbol_interner::global;

// #[derive(Debug, Clone)]
// pub struct ArbitrageOpportunity {
//     pub path: Arc<TriangularPath>,
//     pub profit_ratio: f64,
//     pub start_amount: f64,
//     pub execution_time_ns: u64,
//     pub symbols_involved: [u16; 3],
// }

// impl ArbitrageOpportunity {
//     #[inline(always)]
//     pub fn profit_percentage(&self) -> f64 {
//         (self.profit_ratio - 1.0) * 100.0
//     }

//     pub fn display(&self) -> String {
//         format!(
//             "{} → {} → {} | {:.4}% | {}ns",
//             self.path.first_symbol,
//             self.path.second_symbol,
//             self.path.third_symbol,
//             self.profit_percentage(),
//             self.execution_time_ns
//         )
//     }
// }

// pub type ExecutionCallback = Arc<dyn Fn(ArbitrageOpportunity) + Send + Sync + 'static>;

// #[repr(C, align(64))]
// #[derive(Clone)]
// struct PathData {
//     first_symbol_id: u16,
//     second_symbol_id: u16,
//     third_symbol_id: u16,
//     directions: u8,
//     fee_multiplier_cubed: f32,
//     min_threshold: f32,
//     volume_multiplier: f32,
//     _padding: [u8; 49],
// }

// impl PathData {
//     #[inline(always)]
//     fn first_is_base_to_quote(&self) -> bool {
//         (self.directions & 0b001) != 0
//     }

//     #[inline(always)]
//     fn second_is_base_to_quote(&self) -> bool {
//         (self.directions & 0b010) != 0
//     }

//     #[inline(always)]
//     fn third_is_base_to_quote(&self) -> bool {
//         (self.directions & 0b100) != 0
//     }
// }

// const MAX_PATHS_PER_SYMBOL: usize = 16;
// const MAX_SYMBOLS: usize = 2048;

// #[repr(C, align(64))]
// #[derive(Clone)]
// struct SymbolPaths {
//     count: u8,
//     indices: [u16; MAX_PATHS_PER_SYMBOL],
//     _padding: [u8; 15],
// }

// pub struct ArbitrageDetector {
//     orderbook_manager: Arc<OrderBookManager>,
//     all_paths: Box<[TriangularPath]>,
//     path_data: Box<[PathData]>,
//     symbol_map: Box<[SymbolPaths; MAX_SYMBOLS]>,
//     execution_callbacks: parking_lot::RwLock<Vec<ExecutionCallback>>,
//     start_amount: f32,
//     min_volume_multiplier: f32,
//     min_profit_for_execution: f32,
//     shutdown: Arc<AtomicBool>,
// }

// impl ArbitrageDetector {
//     pub fn new(
//         orderbook_manager: Arc<OrderBookManager>,
//         paths: Vec<Arc<TriangularPath>>,
//         shutdown: Arc<AtomicBool>,
//         fee_rate: f64,
//         min_profit_threshold: f64,
//         start_amount: f64,
//         min_volume_multiplier: f64
//     ) -> Arc<Self> {
//         let unwrapped_paths: Vec<TriangularPath> = paths
//             .iter()
//             .map(|arc_path| (**arc_path).clone())
//             .collect();

//         let all_symbols: Vec<String> = unwrapped_paths
//             .iter()
//             .flat_map(|path| [
//                 path.first_symbol.to_string(),
//                 path.second_symbol.to_string(),
//                 path.third_symbol.to_string(),
//             ])
//             .collect();

//         global::intern_batch(&all_symbols);

//         let mut path_data = Vec::with_capacity(unwrapped_paths.len());
//         let mut symbol_map = Box::new(
//             [
//                 const {
//                     SymbolPaths {
//                         count: 0,
//                         indices: [0; MAX_PATHS_PER_SYMBOL],
//                         _padding: [0; 15],
//                     }
//                 };
//                 MAX_SYMBOLS
//             ]
//         );

//         for (i, path) in unwrapped_paths.iter().enumerate() {
//             let first_symbol_id = global::get_id(&path.first_symbol).unwrap();
//             let second_symbol_id = global::get_id(&path.second_symbol).unwrap();
//             let third_symbol_id = global::get_id(&path.third_symbol).unwrap();

//             let directions =
//                 (path.first_is_base_to_quote as u8) |
//                 ((path.second_is_base_to_quote as u8) << 1) |
//                 ((path.third_is_base_to_quote as u8) << 2);

//             path_data.push(PathData {
//                 first_symbol_id,
//                 second_symbol_id,
//                 third_symbol_id,
//                 directions,
//                 fee_multiplier_cubed: ((1.0 - fee_rate) as f32).powi(3),
//                 min_threshold: (1.0 + min_profit_threshold) as f32,
//                 volume_multiplier: min_volume_multiplier as f32,
//                 _padding: [0; 49],
//             });

//             let path_idx = i as u16;
//             Self::add_path_to_symbol(&mut symbol_map, first_symbol_id, path_idx);
//             Self::add_path_to_symbol(&mut symbol_map, second_symbol_id, path_idx);
//             Self::add_path_to_symbol(&mut symbol_map, third_symbol_id, path_idx);
//         }

//         Arc::new(ArbitrageDetector {
//             orderbook_manager,
//             all_paths: unwrapped_paths.into_boxed_slice(),
//             path_data: path_data.into_boxed_slice(),
//             symbol_map,
//             execution_callbacks: parking_lot::RwLock::new(Vec::with_capacity(4)),
//             start_amount: start_amount as f32,
//             min_volume_multiplier: min_volume_multiplier as f32,
//             min_profit_for_execution: (1.0 + min_profit_threshold) as f32,
//             shutdown: shutdown,
//         })
//     }

//     #[inline]
//     pub fn register_execution_callback(&self, callback: ExecutionCallback) {
//         self.execution_callbacks.write().push(callback);
//     }

//     #[inline]
//     fn add_path_to_symbol(
//         symbol_map: &mut Box<[SymbolPaths; MAX_SYMBOLS]>,
//         symbol_id: u16,
//         path_idx: u16
//     ) {
//         if (symbol_id as usize) < MAX_SYMBOLS {
//             let symbol_paths = &mut symbol_map[symbol_id as usize];
//             if (symbol_paths.count as usize) < MAX_PATHS_PER_SYMBOL {
//                 symbol_paths.indices[symbol_paths.count as usize] = path_idx;
//                 symbol_paths.count += 1;
//             }
//         }
//     }

//     #[inline(always)]
//     fn check_path(&self, path_idx: u16) -> Option<ArbitrageOpportunity> {
//         unsafe {
//             let path_data = self.path_data.get_unchecked(path_idx as usize);

//             let first_book = self.orderbook_manager.get_or_create_book_by_id(
//                 path_data.first_symbol_id
//             );
//             let second_book = self.orderbook_manager.get_or_create_book_by_id(
//                 path_data.second_symbol_id
//             );
//             let third_book = self.orderbook_manager.get_or_create_book_by_id(
//                 path_data.third_symbol_id
//             );

//             if !first_book.is_synced() || !second_book.is_synced() || !third_book.is_synced() {
//                 return None;
//             }

//             let (first_bid, first_ask) = first_book.get_cached_top_of_book();
//             let (second_bid, second_ask) = second_book.get_cached_top_of_book();
//             let (third_bid, third_ask) = third_book.get_cached_top_of_book();

//             let (first_price, first_qty) = if path_data.first_is_base_to_quote() {
//                 first_bid?
//             } else {
//                 first_ask?
//             };

//             let (second_price, second_qty) = if path_data.second_is_base_to_quote() {
//                 second_bid?
//             } else {
//                 second_ask?
//             };

//             let (third_price, third_qty) = if path_data.third_is_base_to_quote() {
//                 third_bid?
//             } else {
//                 third_ask?
//             };

//             let mut amount = self.start_amount;

//             amount = if path_data.first_is_base_to_quote() {
//                 amount * (first_price as f32)
//             } else {
//                 amount / (first_price as f32)
//             };

//             amount = if path_data.second_is_base_to_quote() {
//                 amount * (second_price as f32)
//             } else {
//                 amount / (second_price as f32)
//             };

//             amount = if path_data.third_is_base_to_quote() {
//                 amount * (third_price as f32)
//             } else {
//                 amount / (third_price as f32)
//             };

//             amount *= path_data.fee_multiplier_cubed;
//             let profit_ratio = amount / self.start_amount;

//             if profit_ratio <= path_data.min_threshold {
//                 return None;
//             }

//             let min_vol = self.start_amount * self.min_volume_multiplier;
//             let volume_ok =
//                 (first_qty as f32) >= min_vol &&
//                 (second_qty as f32) >= min_vol &&
//                 (third_qty as f32) >= min_vol;

//             if !volume_ok {
//                 return None;
//             }

//             let path = &self.all_paths[path_idx as usize];
//             let first_symbol_id = global::get_id(&path.first_symbol).unwrap_or(0);
//             let second_symbol_id = global::get_id(&path.second_symbol).unwrap_or(0);
//             let third_symbol_id = global::get_id(&path.third_symbol).unwrap_or(0);

//             Some(ArbitrageOpportunity {
//                 path: Arc::new(path.clone()),
//                 profit_ratio: profit_ratio as f64,
//                 start_amount: self.start_amount as f64,
//                 execution_time_ns: 0,
//                 symbols_involved: [first_symbol_id, second_symbol_id, third_symbol_id],
//             })
//         }
//     }

//     #[inline(always)]
//     pub fn process_symbol_update(&self, symbol_id: u16) {
//         if (symbol_id as usize) < MAX_SYMBOLS {
//             let symbol_paths = &self.symbol_map[symbol_id as usize];
//             let path_count = symbol_paths.count as usize;

//             if path_count == 0 {
//                 return;
//             }

//             let max_paths_to_check = path_count.min(8);

//             for i in 0..max_paths_to_check {
//                 let path_idx = unsafe { *symbol_paths.indices.get_unchecked(i) };

//                 if let Some(opportunity) = self.check_path(path_idx) {
//                     if opportunity.profit_ratio >= (self.min_profit_for_execution as f64) {
//                         self.trigger_execution_callbacks(opportunity);
//                     }
//                 }
//             }
//         }
//     }

//     #[inline]
//     fn trigger_execution_callbacks(&self, opportunity: ArbitrageOpportunity) {
//         let callbacks = self.execution_callbacks.read();
//         if !callbacks.is_empty() {
//             for callback in callbacks.iter() {
//                 callback(opportunity.clone());
//             }
//         }
//     }

//     pub fn get_fix_execution_stats(&self) -> Option<crate::executor::fix_executor::ExecutionStats> {
//         None
//     }

//     #[inline]
//     pub fn is_shutdown(&self) -> bool {
//         self.shutdown.load(Ordering::Relaxed)
//     }
// }

// pub fn create_detector(
//     orderbook_manager: Arc<OrderBookManager>,
//     paths: Vec<Arc<TriangularPath>>,
//     shutdown: Arc<AtomicBool>,
//     fee_rate: f64,
//     min_profit_threshold: f64,
//     start_amount: f64,
//     min_volume_multiplier: f64
// ) -> Arc<ArbitrageDetector> {
//     ArbitrageDetector::new(
//         orderbook_manager,
//         paths,
//         shutdown,
//         fee_rate,
//         min_profit_threshold,
//         start_amount,
//         min_volume_multiplier
//     )
// }

use std::sync::Arc;
use std::sync::atomic::{ AtomicBool, Ordering };
use crate::models::triangular_path::TriangularPath;
use crate::orderbook::manager::OrderBookManager;
use crate::utils::symbol_interner::global;

#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub path: Arc<TriangularPath>,
    pub profit_ratio: f64,
    pub start_amount: f64,
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
            "{} → {} → {} | {:.4}% | {}ns",
            self.path.first_symbol,
            self.path.second_symbol,
            self.path.third_symbol,
            self.profit_percentage(),
            self.execution_time_ns
        )
    }
}

pub type ExecutionCallback = Arc<dyn Fn(ArbitrageOpportunity) + Send + Sync + 'static>;

#[repr(C, align(64))]
#[derive(Clone)]
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

pub struct ArbitrageDetector {
    orderbook_manager: Arc<OrderBookManager>,
    all_paths: Box<[TriangularPath]>,
    path_data: Box<[PathData]>,
    symbol_map: Box<[SymbolPaths; MAX_SYMBOLS]>,
    execution_callbacks: parking_lot::RwLock<Vec<ExecutionCallback>>,
    start_amount: f32,
    min_volume_multiplier: f32,
    min_profit_for_execution: f32,
    shutdown: Arc<AtomicBool>,
}

impl ArbitrageDetector {
    pub fn new(
        orderbook_manager: Arc<OrderBookManager>,
        paths: Vec<Arc<TriangularPath>>,
        shutdown: Arc<AtomicBool>,
        fee_rate: f64,
        min_profit_threshold: f64,
        start_amount: f64,
        min_volume_multiplier: f64
    ) -> Arc<Self> {
        let unwrapped_paths: Vec<TriangularPath> = paths
            .iter()
            .map(|arc_path| (**arc_path).clone())
            .collect();

        let all_symbols: Vec<String> = unwrapped_paths
            .iter()
            .flat_map(|path| [
                path.first_symbol.to_string(),
                path.second_symbol.to_string(),
                path.third_symbol.to_string(),
            ])
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
        let mut symbols_cache = Box::new([[0u16; 3]; MAX_SYMBOLS]);

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

            if (first_symbol_id as usize) < MAX_SYMBOLS {
                symbols_cache[first_symbol_id as usize] = [
                    first_symbol_id,
                    second_symbol_id,
                    third_symbol_id,
                ];
            }
            if (second_symbol_id as usize) < MAX_SYMBOLS {
                symbols_cache[second_symbol_id as usize] = [
                    first_symbol_id,
                    second_symbol_id,
                    third_symbol_id,
                ];
            }
            if (third_symbol_id as usize) < MAX_SYMBOLS {
                symbols_cache[third_symbol_id as usize] = [
                    first_symbol_id,
                    second_symbol_id,
                    third_symbol_id,
                ];
            }
        }

        Arc::new(ArbitrageDetector {
            orderbook_manager,
            all_paths: unwrapped_paths.into_boxed_slice(),
            path_data: path_data.into_boxed_slice(),
            symbol_map,
            execution_callbacks: parking_lot::RwLock::new(Vec::with_capacity(4)),
            start_amount: start_amount as f32,
            min_volume_multiplier: min_volume_multiplier as f32,
            min_profit_for_execution: (1.0 + min_profit_threshold) as f32,
            shutdown: shutdown,
        })
    }

    #[inline]
    pub fn register_execution_callback(&self, callback: ExecutionCallback) {
        self.execution_callbacks.write().push(callback);
    }

    #[inline]
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
    fn check_path_fast(&self, path_idx: u16) -> Option<f32> {
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

            if !first_book.is_synced() {
                return None;
            }
            if !second_book.is_synced() {
                return None;
            }
            if !third_book.is_synced() {
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

            let min_vol = self.start_amount * self.min_volume_multiplier;
            if
                (first_qty as f32) < min_vol ||
                (second_qty as f32) < min_vol ||
                (third_qty as f32) < min_vol
            {
                return None;
            }

            let mut amount = self.start_amount;
            let first_price_f32 = first_price as f32;
            let second_price_f32 = second_price as f32;
            let third_price_f32 = third_price as f32;

            if path_data.first_is_base_to_quote() {
                amount *= first_price_f32;
            } else {
                amount /= first_price_f32;
            }

            if path_data.second_is_base_to_quote() {
                amount *= second_price_f32;
            } else {
                amount /= second_price_f32;
            }

            if path_data.third_is_base_to_quote() {
                amount *= third_price_f32;
            } else {
                amount /= third_price_f32;
            }

            amount *= path_data.fee_multiplier_cubed;
            let profit_ratio = amount / self.start_amount;

            if profit_ratio > path_data.min_threshold {
                Some(profit_ratio)
            } else {
                None
            }
        }
    }

    #[inline(always)]
    fn check_path(&self, path_idx: u16) -> Option<ArbitrageOpportunity> {
        let profit_ratio = self.check_path_fast(path_idx)?;

        unsafe {
            let path = self.all_paths.get_unchecked(path_idx as usize);
            let path_data = self.path_data.get_unchecked(path_idx as usize);

            Some(ArbitrageOpportunity {
                path: Arc::new(path.clone()),
                profit_ratio: profit_ratio as f64,
                start_amount: self.start_amount as f64,
                execution_time_ns: 0,
                symbols_involved: [
                    path_data.first_symbol_id,
                    path_data.second_symbol_id,
                    path_data.third_symbol_id,
                ],
            })
        }
    }

    #[inline(always)]
    pub fn process_symbol_update(&self, symbol_id: u16) {
        if (symbol_id as usize) >= MAX_SYMBOLS {
            return;
        }

        unsafe {
            let symbol_paths = self.symbol_map.get_unchecked(symbol_id as usize);
            let path_count = symbol_paths.count as usize;

            if path_count == 0 {
                return;
            }

            let max_paths_to_check = path_count.min(8);

            for i in 0..max_paths_to_check {
                let path_idx = *symbol_paths.indices.get_unchecked(i);

                if let Some(profit_ratio) = self.check_path_fast(path_idx) {
                    if profit_ratio >= self.min_profit_for_execution {
                        if let Some(opportunity) = self.check_path(path_idx) {
                            self.trigger_execution_callbacks(opportunity);
                        }
                    }
                }
            }
        }
    }

    #[inline]
    fn trigger_execution_callbacks(&self, opportunity: ArbitrageOpportunity) {
        let callbacks = self.execution_callbacks.read();
        if !callbacks.is_empty() {
            for callback in callbacks.iter() {
                callback(opportunity.clone());
            }
        }
    }

    pub fn get_fix_execution_stats(&self) -> Option<crate::executor::fix_executor::ExecutionStats> {
        None
    }

    #[inline]
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
}

pub fn create_detector(
    orderbook_manager: Arc<OrderBookManager>,
    paths: Vec<Arc<TriangularPath>>,
    shutdown: Arc<AtomicBool>,
    fee_rate: f64,
    min_profit_threshold: f64,
    start_amount: f64,
    min_volume_multiplier: f64
) -> Arc<ArbitrageDetector> {
    ArbitrageDetector::new(
        orderbook_manager,
        paths,
        shutdown,
        fee_rate,
        min_profit_threshold,
        start_amount,
        min_volume_multiplier
    )
}
