// src/utils/string_interner.rs
use std::sync::Arc;
use dashmap::DashMap;
use std::hash::{ Hash, Hasher };
use std::collections::hash_map::DefaultHasher;

/// High-performance string interner for symbols
pub struct SymbolInterner {
    /// Hash -> Arc<str> mapping for O(1) lookups
    by_hash: DashMap<u64, Arc<str>>,
    /// String -> Arc<str> mapping for deduplication
    by_string: DashMap<Box<str>, Arc<str>>,
}

impl SymbolInterner {
    pub fn new() -> Self {
        Self {
            by_hash: DashMap::with_capacity(5000),
            by_string: DashMap::with_capacity(5000),
        }
    }

    /// Intern a string with fast hash-based lookup
    #[inline]
    pub fn intern(&self, s: &str) -> Arc<str> {
        // Fast path: try hash lookup first
        let hash = self.hash_string(s);
        if let Some(cached) = self.by_hash.get(&hash) {
            return cached.clone();
        }

        // Slow path: check for collisions and intern
        let boxed = s.into();
        let interned = self.by_string
            .entry(boxed)
            .or_insert_with(|| Arc::from(s))
            .clone();

        // Cache by hash for faster future lookups
        self.by_hash.insert(hash, interned.clone());
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
        (self.by_hash.len(), self.by_string.len())
    }
}
