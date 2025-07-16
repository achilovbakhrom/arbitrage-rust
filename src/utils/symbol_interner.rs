use std::sync::Arc;
use std::sync::atomic::{ AtomicU16, Ordering };
use parking_lot::RwLock;
use ahash::HashMapExt;
use crate::utils::types::FastHashMap;

pub type SymbolId = u16;
const MAX_SYMBOLS: usize = 2048;

pub struct SymbolInterner {
    symbol_to_id: RwLock<FastHashMap<Arc<str>, SymbolId>>,
    id_to_symbol: Box<[RwLock<Option<Arc<str>>>; MAX_SYMBOLS]>,
    bytes_to_id: RwLock<FastHashMap<Box<[u8]>, SymbolId>>,
    next_id: AtomicU16,
}

impl SymbolInterner {
    pub fn new() -> Self {
        Self {
            symbol_to_id: RwLock::new(FastHashMap::with_capacity(MAX_SYMBOLS)),
            id_to_symbol: Box::new(std::array::from_fn(|_| RwLock::new(None))),
            bytes_to_id: RwLock::new(FastHashMap::with_capacity(MAX_SYMBOLS)),
            next_id: AtomicU16::new(0),
        }
    }

    #[inline(always)]
    pub fn get_id(&self, symbol: &str) -> Option<SymbolId> {
        self.symbol_to_id.read().get(symbol).copied()
    }

    #[inline(always)]
    pub fn get_id_from_bytes(&self, symbol_bytes: &[u8]) -> Option<SymbolId> {
        let actual_len = symbol_bytes
            .iter()
            .rposition(|&b| b != 0)
            .map(|i| i + 1)
            .unwrap_or(0);

        if actual_len == 0 {
            return None;
        }

        let trimmed = &symbol_bytes[..actual_len];
        self.bytes_to_id.read().get(trimmed).copied()
    }

    #[inline(always)]
    pub fn get_symbol(&self, id: SymbolId) -> Option<Arc<str>> {
        if (id as usize) >= MAX_SYMBOLS {
            return None;
        }
        self.id_to_symbol[id as usize].read().clone()
    }

    pub fn intern_batch(&self, symbols: &[String]) -> Vec<SymbolId> {
        let mut result = Vec::with_capacity(symbols.len());

        let mut to_intern = Vec::new();
        {
            let symbol_map = self.symbol_to_id.read();
            for symbol in symbols {
                if let Some(&id) = symbol_map.get(symbol.as_str()) {
                    result.push(id);
                } else {
                    to_intern.push((result.len(), symbol));
                    result.push(0);
                }
            }
        }

        if !to_intern.is_empty() {
            let mut symbol_map = self.symbol_to_id.write();
            let mut bytes_map = self.bytes_to_id.write();

            for (index, symbol) in to_intern {
                if let Some(&id) = symbol_map.get(symbol.as_str()) {
                    result[index] = id;
                    continue;
                }

                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                if id >= (MAX_SYMBOLS as u16) {
                    panic!("Symbol interner capacity exceeded");
                }

                let arc_symbol: Arc<str> = symbol.as_str().into();

                symbol_map.insert(Arc::clone(&arc_symbol), id);
                *self.id_to_symbol[id as usize].write() = Some(Arc::clone(&arc_symbol));

                let symbol_bytes = arc_symbol.as_bytes().to_vec().into_boxed_slice();
                bytes_map.insert(symbol_bytes, id);

                result[index] = id;
            }
        }

        result
    }

    pub fn get_all_mappings(&self) -> Vec<(String, SymbolId)> {
        self.symbol_to_id
            .read()
            .iter()
            .map(|(symbol, &id)| (symbol.to_string(), id))
            .collect()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.next_id.load(Ordering::Relaxed) as usize
    }
}

unsafe impl Sync for SymbolInterner {}
unsafe impl Send for SymbolInterner {}

lazy_static::lazy_static! {
    static ref GLOBAL_SYMBOL_INTERNER: SymbolInterner = SymbolInterner::new();
}

pub mod global {
    use super::*;

    #[inline(always)]
    pub fn get_id(symbol: &str) -> Option<SymbolId> {
        GLOBAL_SYMBOL_INTERNER.get_id(symbol)
    }

    #[inline(always)]
    pub fn get_id_from_bytes(symbol_bytes: &[u8]) -> Option<SymbolId> {
        GLOBAL_SYMBOL_INTERNER.get_id_from_bytes(symbol_bytes)
    }

    #[inline(always)]
    pub fn get_symbol(id: SymbolId) -> Option<Arc<str>> {
        GLOBAL_SYMBOL_INTERNER.get_symbol(id)
    }

    pub fn intern_batch(symbols: &[String]) -> Vec<SymbolId> {
        GLOBAL_SYMBOL_INTERNER.intern_batch(symbols)
    }

    pub fn get_all_mappings() -> Vec<(String, SymbolId)> {
        GLOBAL_SYMBOL_INTERNER.get_all_mappings()
    }

    #[inline(always)]
    pub fn len() -> usize {
        GLOBAL_SYMBOL_INTERNER.len()
    }
}
