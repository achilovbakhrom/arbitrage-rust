// src/utils/string_interner.rs
use std::sync::Arc;
use std::hash::{ Hash, Hasher };
use std::collections::hash_map::DefaultHasher;
use ahash::AHashMap;
use parking_lot::RwLock;

/// High-performance string interner for symbols
pub struct SymbolInterner {
    by_hash: RwLock<AHashMap<u64, Arc<str>>>,
    by_string: RwLock<AHashMap<Box<str>, Arc<str>>>,
}

impl SymbolInterner {
    pub fn new() -> Self {
        Self {
            by_hash: RwLock::new(AHashMap::with_capacity(5000)),
            by_string: RwLock::new(AHashMap::with_capacity(5000)),
        }
    }

    /// Intern a string with fast hash-based lookup
    #[inline]
    pub fn intern(&self, s: &str) -> Arc<str> {
        // Fast path: try hash lookup first
        let hash = self.hash_string(s);
        if let Some(cached) = self.by_hash.read().get(&hash) {
            return cached.clone();
        }

        // Slow path: check for collisions and intern
        let boxed = s.into();
        let interned = {
            let mut map = self.by_string.write();
            map.entry(boxed)
                .or_insert_with(|| Arc::from(s))
                .clone()
        };

        // Cache by hash for faster future lookups
        self.by_hash.write().insert(hash, interned.clone());
        interned
    }

    /// Fast string hashing
    #[inline]
    fn hash_string(&self, s: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    /// Pre-populate with known symbols
    pub fn preload(&self, symbols: &[String]) {
        for symbol in symbols {
            self.intern(symbol);
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> (usize, usize) {
        (self.by_hash.read().len(), self.by_string.read().len())
    }
}
