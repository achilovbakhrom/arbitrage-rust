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

    #[inline]
    pub fn get_or_intern(&self, symbol: &str) -> SymbolId {
        let arc_symbol: Arc<str> = symbol.into();
        self.get_or_intern_arc(&arc_symbol)
    }

    #[inline]
    pub fn get_or_intern_arc(&self, symbol: &Arc<str>) -> SymbolId {
        {
            let symbol_map = self.symbol_to_id.read();
            if let Some(&id) = symbol_map.get(symbol) {
                return id;
            }
        }

        let mut symbol_map = self.symbol_to_id.write();
        if let Some(&id) = symbol_map.get(symbol) {
            return id;
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id >= (MAX_SYMBOLS as u16) {
            panic!("Symbol interner capacity exceeded");
        }

        symbol_map.insert(Arc::clone(symbol), id);
        *self.id_to_symbol[id as usize].write() = Some(Arc::clone(symbol));

        let symbol_bytes = symbol.as_bytes().to_vec().into_boxed_slice();
        self.bytes_to_id.write().insert(symbol_bytes, id);

        id
    }

    #[inline(always)]
    pub fn get_id(&self, symbol: &str) -> Option<SymbolId> {
        let arc_symbol: Arc<str> = symbol.into();
        self.symbol_to_id.read().get(&arc_symbol).copied()
    }

    #[inline(always)]
    pub fn get_id_from_bytes(&self, symbol_bytes: &[u8]) -> Option<SymbolId> {
        let trimmed_end = symbol_bytes
            .iter()
            .rposition(|&b| b != 0)
            .map(|i| i + 1)
            .unwrap_or(0);

        if trimmed_end == 0 {
            return None;
        }

        let trimmed = &symbol_bytes[..trimmed_end];
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
        symbols
            .iter()
            .map(|s| self.get_or_intern(s))
            .collect()
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
