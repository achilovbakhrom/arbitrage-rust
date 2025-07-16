use std::sync::Arc;
use std::sync::atomic::{ AtomicBool, Ordering };

use crate::models::triangular_path::TriangularPath;
use crate::orderbook_manager::manager::OrderBookManager;
use crate::orderbook_manager::orderbook::OrderBook;
use crate::utils::symbol_interner::global;

#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub path: Arc<TriangularPath>,
    pub profit_ratio: f64,
    pub start_amount: f64,
    pub symbols_involved: [u16; 3],
    pub estimated_slippage: f64,
}

impl ArbitrageOpportunity {
    #[inline(always)]
    pub fn profit_percentage(&self) -> f64 {
        (self.profit_ratio - 1.0) * 100.0
    }

    #[inline(always)]
    pub fn net_profit_percentage(&self) -> f64 {
        (self.profit_ratio - self.estimated_slippage - 1.0) * 100.0
    }

    pub fn display(&self) -> String {
        format!(
            "{} → {} → {} | Gross: {:.4}% | Net: {:.4}%",
            self.path.first_symbol,
            self.path.second_symbol,
            self.path.third_symbol,
            self.profit_percentage(),
            self.net_profit_percentage()
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
    fee_rate: f32,
    min_threshold: f32,
    _padding: [u8; 2],
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

    #[inline(always)]
    fn fee_multiplier(&self) -> f32 {
        1.0 - self.fee_rate
    }
}

const MAX_PATHS_PER_SYMBOL: usize = 12;
const MAX_SYMBOLS: usize = 2048;

#[repr(C, align(64))]
#[derive(Clone)]
struct SymbolPaths {
    count: u8,
    indices: [u16; MAX_PATHS_PER_SYMBOL],
    _padding: [u8; 7],
}

pub struct ArbitrageDetector {
    orderbook_manager: Arc<OrderBookManager>,
    all_paths: Box<[TriangularPath]>,
    path_data: Box<[PathData]>,
    symbol_map: Box<[SymbolPaths; MAX_SYMBOLS]>,
    execution_callbacks: parking_lot::RwLock<Vec<ExecutionCallback>>,
    start_amount: f32,
    min_volume_multiplier: f32,
    shutdown: Arc<AtomicBool>,
    symbol_precisions: Box<[(u8, u8); MAX_SYMBOLS]>,
    slippage_buffer: f32,
    execution_delay_buffer: f32,
}

impl ArbitrageDetector {
    pub fn new(
        orderbook_manager: Arc<OrderBookManager>,
        paths: Vec<Arc<TriangularPath>>,
        shutdown: Arc<AtomicBool>,
        fee_rate: f64,
        min_profit_threshold: f64,
        start_amount: f64,
        min_volume_multiplier: f64,
        symbol_precision_map: &crate::models::symbol_map::SymbolMap
    ) -> Arc<Self> {
        let path_count = paths.len();
        let unwrapped_paths: Vec<TriangularPath> = paths
            .into_iter()
            .map(|arc_path| (*arc_path).clone())
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

        let mut path_data = Vec::with_capacity(path_count);
        let mut symbol_map = Box::new(
            [
                const {
                    SymbolPaths {
                        count: 0,
                        indices: [0; MAX_PATHS_PER_SYMBOL],
                        _padding: [0; 7],
                    }
                };
                MAX_SYMBOLS
            ]
        );

        let mut symbol_precisions = Box::new([(8u8, 8u8); MAX_SYMBOLS]);

        for symbol_info in symbol_precision_map.get_all_symbols() {
            if let Some(symbol_id) = global::get_id(&symbol_info.symbol) {
                if (symbol_id as usize) < MAX_SYMBOLS {
                    symbol_precisions[symbol_id as usize] = (
                        symbol_info.qty_precision,
                        symbol_info.price_precision,
                    );
                }
            }
        }

        let min_threshold = (1.0 + min_profit_threshold * 1.5) as f32;

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
                fee_rate: fee_rate as f32,
                min_threshold,
                _padding: [0; 2],
            });

            let path_idx = i as u16;
            Self::add_path_to_symbol(&mut symbol_map, first_symbol_id, path_idx);
            Self::add_path_to_symbol(&mut symbol_map, second_symbol_id, path_idx);
            Self::add_path_to_symbol(&mut symbol_map, third_symbol_id, path_idx);
        }

        Arc::new(ArbitrageDetector {
            orderbook_manager,
            shutdown,
            all_paths: unwrapped_paths.into_boxed_slice(),
            path_data: path_data.into_boxed_slice(),
            symbol_map,
            execution_callbacks: parking_lot::RwLock::new(Vec::with_capacity(2)),
            start_amount: start_amount as f32,
            min_volume_multiplier: min_volume_multiplier as f32,
            symbol_precisions,
            slippage_buffer: 0.002,
            execution_delay_buffer: 0.001,
        })
    }

    #[inline(always)]
    fn apply_precision_f32(&self, value: f32, precision: u8) -> f32 {
        let factor = (10f32).powi(precision as i32);
        (value * factor).floor() / factor
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

    #[inline]
    fn estimate_slippage(&self, book: &Arc<OrderBook>, amount: f64, is_buy: bool) -> f32 {
        if
            let (Some(full_price), Some(top_price)) = (
                book.get_effective_price(amount, is_buy),
                book.get_effective_price(0.001, is_buy),
            )
        {
            ((full_price - top_price).abs() / top_price) as f32
        } else {
            0.01
        }
    }

    #[inline]
    fn check_full_path_liquidity(&self, path_data: &PathData, start_amount: f32) -> bool {
        let first_book = self.orderbook_manager.get_or_create_book_by_id(path_data.first_symbol_id);
        let second_book = self.orderbook_manager.get_or_create_book_by_id(
            path_data.second_symbol_id
        );
        let third_book = self.orderbook_manager.get_or_create_book_by_id(path_data.third_symbol_id);

        if !first_book.is_synced() || !second_book.is_synced() || !third_book.is_synced() {
            return false;
        }

        let mut amount = start_amount as f64;

        if !first_book.has_sufficient_liquidity(amount, path_data.first_is_base_to_quote()) {
            return false;
        }

        if
            let Some(first_price) = first_book.get_effective_price(
                amount,
                path_data.first_is_base_to_quote()
            )
        {
            amount = if path_data.first_is_base_to_quote() {
                amount * (first_price as f64) * (path_data.fee_multiplier() as f64)
            } else {
                (amount / (first_price as f64)) * (path_data.fee_multiplier() as f64)
            };
        } else {
            return false;
        }

        if !second_book.has_sufficient_liquidity(amount, path_data.second_is_base_to_quote()) {
            return false;
        }

        if
            let Some(second_price) = second_book.get_effective_price(
                amount,
                path_data.second_is_base_to_quote()
            )
        {
            amount = if path_data.second_is_base_to_quote() {
                amount * (second_price as f64) * (path_data.fee_multiplier() as f64)
            } else {
                (amount / (second_price as f64)) * (path_data.fee_multiplier() as f64)
            };
        } else {
            return false;
        }

        third_book.has_sufficient_liquidity(amount, path_data.third_is_base_to_quote())
    }

    #[inline(always)]
    fn calculate_portfolio(&self, path_idx: u16) -> Option<(f32, f32)> {
        unsafe {
            let path_data = self.path_data.get_unchecked(path_idx as usize);

            if !self.check_full_path_liquidity(path_data, self.start_amount) {
                return None;
            }

            let first_book = self.orderbook_manager.get_or_create_book_by_id(
                path_data.first_symbol_id
            );
            let second_book = self.orderbook_manager.get_or_create_book_by_id(
                path_data.second_symbol_id
            );
            let third_book = self.orderbook_manager.get_or_create_book_by_id(
                path_data.third_symbol_id
            );

            let (first_qty_prec, first_price_prec) = *self.symbol_precisions.get_unchecked(
                path_data.first_symbol_id as usize
            );
            let (second_qty_prec, second_price_prec) = *self.symbol_precisions.get_unchecked(
                path_data.second_symbol_id as usize
            );
            let (third_qty_prec, third_price_prec) = *self.symbol_precisions.get_unchecked(
                path_data.third_symbol_id as usize
            );

            let mut total_slippage = 0.0f32;
            let mut amount = self.start_amount;

            let first_amount_f64 = amount as f64;
            let first_price = first_book.get_effective_price(
                first_amount_f64,
                path_data.first_is_base_to_quote()
            )? as f32;

            total_slippage += self.estimate_slippage(
                &first_book,
                first_amount_f64,
                path_data.first_is_base_to_quote()
            );

            let precise_input = self.apply_precision_f32(amount, if
                path_data.first_is_base_to_quote()
            {
                first_qty_prec
            } else {
                first_price_prec
            });

            amount = if path_data.first_is_base_to_quote() {
                precise_input * first_price * path_data.fee_multiplier()
            } else {
                (precise_input / first_price) * path_data.fee_multiplier()
            };

            amount = self.apply_precision_f32(amount, if path_data.first_is_base_to_quote() {
                first_price_prec
            } else {
                first_qty_prec
            });

            let second_amount_f64 = amount as f64;
            let second_price = second_book.get_effective_price(
                second_amount_f64,
                path_data.second_is_base_to_quote()
            )? as f32;

            total_slippage += self.estimate_slippage(
                &second_book,
                second_amount_f64,
                path_data.second_is_base_to_quote()
            );

            let precise_input = self.apply_precision_f32(amount, if
                path_data.second_is_base_to_quote()
            {
                second_qty_prec
            } else {
                second_price_prec
            });

            amount = if path_data.second_is_base_to_quote() {
                precise_input * second_price * path_data.fee_multiplier()
            } else {
                (precise_input / second_price) * path_data.fee_multiplier()
            };

            amount = self.apply_precision_f32(amount, if path_data.second_is_base_to_quote() {
                second_price_prec
            } else {
                second_qty_prec
            });

            let third_amount_f64 = amount as f64;
            let third_price = third_book.get_effective_price(
                third_amount_f64,
                path_data.third_is_base_to_quote()
            )? as f32;

            total_slippage += self.estimate_slippage(
                &third_book,
                third_amount_f64,
                path_data.third_is_base_to_quote()
            );

            let precise_input = self.apply_precision_f32(amount, if
                path_data.third_is_base_to_quote()
            {
                third_qty_prec
            } else {
                third_price_prec
            });

            amount = if path_data.third_is_base_to_quote() {
                precise_input * third_price * path_data.fee_multiplier()
            } else {
                (precise_input / third_price) * path_data.fee_multiplier()
            };

            amount = self.apply_precision_f32(amount, if path_data.third_is_base_to_quote() {
                third_price_prec
            } else {
                third_qty_prec
            });

            let profit_ratio = amount / self.start_amount;

            total_slippage += self.slippage_buffer + self.execution_delay_buffer;

            if profit_ratio > path_data.min_threshold + total_slippage {
                Some((profit_ratio, total_slippage))
            } else {
                None
            }
        }
    }

    #[inline(always)]
    fn check_path(
        &self,
        path_idx: u16,
        profit_ratio: f32,
        estimated_slippage: f32
    ) -> Option<ArbitrageOpportunity> {
        unsafe {
            let path = self.all_paths.get_unchecked(path_idx as usize);
            let path_data = self.path_data.get_unchecked(path_idx as usize);

            Some(ArbitrageOpportunity {
                path: Arc::new(path.clone()),
                profit_ratio: profit_ratio as f64,
                start_amount: self.start_amount as f64,
                symbols_involved: [
                    path_data.first_symbol_id,
                    path_data.second_symbol_id,
                    path_data.third_symbol_id,
                ],
                estimated_slippage: estimated_slippage as f64,
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

            let max_paths_to_check = path_count.min(3);

            for i in 0..max_paths_to_check {
                let path_idx = *symbol_paths.indices.get_unchecked(i);

                if
                    let Some((profit_ratio, estimated_slippage)) =
                        self.calculate_portfolio(path_idx)
                {
                    if
                        let Some(opportunity) = self.check_path(
                            path_idx,
                            profit_ratio,
                            estimated_slippage
                        )
                    {
                        self.trigger_execution_callbacks(opportunity);
                    }
                }
            }
        }
    }

    #[inline]
    fn trigger_execution_callbacks(&self, opportunity: ArbitrageOpportunity) {
        let callbacks = self.execution_callbacks.read();
        for callback in callbacks.iter() {
            callback(opportunity.clone());
        }
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
    min_volume_multiplier: f64,
    symbol_map: &crate::models::symbol_map::SymbolMap
) -> Arc<ArbitrageDetector> {
    ArbitrageDetector::new(
        orderbook_manager,
        paths,
        shutdown,
        fee_rate,
        min_profit_threshold,
        start_amount,
        min_volume_multiplier,
        symbol_map
    )
}
