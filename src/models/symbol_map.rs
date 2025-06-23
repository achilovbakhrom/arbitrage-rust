use std::{ collections::HashSet, sync::Arc };

use crate::utils::types::FastHashMap;

use super::{ symbol::Symbol, triangular_path::TriangularPath };
use ahash::HashMapExt;
use parking_lot::RwLock;

#[derive(Debug)]
pub struct SymbolMap {
    symbols: RwLock<FastHashMap<Arc<str>, Symbol>>,
    by_base: RwLock<FastHashMap<Arc<str>, Vec<Symbol>>>,
    by_quote: RwLock<FastHashMap<Arc<str>, Vec<Symbol>>>,

    triangular_paths: Vec<TriangularPath>,
}

impl SymbolMap {
    #[inline]
    pub fn new() -> Self {
        Self {
            symbols: RwLock::new(FastHashMap::with_capacity(1000)),
            by_base: RwLock::new(FastHashMap::with_capacity(200)),
            by_quote: RwLock::new(FastHashMap::with_capacity(50)),
            triangular_paths: Vec::with_capacity(5000),
        }
    }

    #[inline]
    pub fn from_symbols(symbols: Vec<Symbol>) -> Self {
        let mut map = Self::new();
        for symbol in symbols {
            map.add_symbol(symbol);
        }
        map
    }

    #[inline]
    pub fn add_symbol(&mut self, symbol: Symbol) {
        let base = symbol.base_asset.clone();
        let quote = symbol.quote_asset.clone();
        let symbol_str = symbol.symbol.clone();

        self.by_base
            .write()
            .entry(base)
            .or_insert_with(|| Vec::with_capacity(10))
            .push(symbol.clone());

        self.by_quote
            .write()
            .entry(quote)
            .or_insert_with(|| Vec::with_capacity(10))
            .push(symbol.clone());

        self.symbols.write().insert(symbol_str, symbol);
    }

    #[inline]
    pub fn get(&self, symbol: &str) -> Option<Symbol> {
        self.symbols.read().get(symbol).cloned()
    }

    #[inline]
    pub fn get_by_base(&self, base_asset: &str) -> Vec<Symbol> {
        self.by_base.read().get(base_asset).cloned().unwrap_or_default()
    }

    #[inline]
    pub fn get_by_quote(&self, quote_asset: &str) -> Vec<Symbol> {
        self.by_quote.read().get(quote_asset).cloned().unwrap_or_default()
    }

    pub fn find_targeted_triangular_paths(
        &mut self,
        base_asset: &str,
        max_paths: usize,
        excluded_fiats: &[String]
    ) {
        let start = std::time::Instant::now();
        self.triangular_paths.clear();

        let excluded_fiats: HashSet<String> = excluded_fiats
            .iter()
            .map(|s| s.to_string())
            .collect();

        let should_exclude = |asset: &str| -> bool {
            asset != base_asset && excluded_fiats.contains(asset)
        };

        let mut all_paths = Vec::with_capacity(5000);
        let by_quote_borrowed = self.by_quote.read();

        let base_symbols = match by_quote_borrowed.get(base_asset) {
            Some(symbols) => symbols,
            None => {
                tracing::warn!("Base asset {} not found as quote in any symbol", base_asset);
                return;
            }
        };

        tracing::info!("Found {} symbols with {} as quote", base_symbols.len(), base_asset);

        for first_symbol in base_symbols.as_slice() {
            let second_asset = &first_symbol.base_asset;

            if should_exclude(second_asset.as_ref()) {
                continue;
            }

            let second_base_symbols = self.get_by_base(second_asset);
            for second_symbol in second_base_symbols {
                let third_asset = &second_symbol.quote_asset;

                if should_exclude(third_asset.as_ref()) {
                    continue;
                }

                let third_base_symbols = self.get_by_base(third_asset);
                for third_symbol in third_base_symbols {
                    if third_symbol.quote_asset.as_ref() == base_asset {
                        all_paths.push(TriangularPath {
                            first_symbol: first_symbol.symbol.clone(),
                            second_symbol: second_symbol.symbol.clone(),
                            third_symbol: third_symbol.symbol.clone(),
                            first_is_base_to_quote: false,
                            second_is_base_to_quote: true,
                            third_is_base_to_quote: true,
                            start_asset: base_asset.into(),
                            end_asset: base_asset.into(),
                        });
                    }
                }

                let third_quote_symbols = self.get_by_quote(third_asset);
                for third_symbol in third_quote_symbols {
                    if third_symbol.base_asset.as_ref() == base_asset {
                        all_paths.push(TriangularPath {
                            first_symbol: first_symbol.symbol.clone(),
                            second_symbol: second_symbol.symbol.clone(),
                            third_symbol: third_symbol.symbol.clone(),
                            first_is_base_to_quote: false,
                            second_is_base_to_quote: true,
                            third_is_base_to_quote: false,
                            start_asset: base_asset.into(),
                            end_asset: base_asset.into(),
                        });
                    }
                }
            }

            let second_quote_symbols = self.get_by_quote(second_asset);
            for second_symbol in second_quote_symbols {
                let third_asset = &second_symbol.base_asset;

                if should_exclude(third_asset.as_ref()) {
                    continue;
                }

                let third_base_symbols = self.get_by_base(third_asset);
                for third_symbol in third_base_symbols {
                    if third_symbol.quote_asset.as_ref() == base_asset {
                        all_paths.push(TriangularPath {
                            first_symbol: first_symbol.symbol.clone(),
                            second_symbol: second_symbol.symbol.clone(),
                            third_symbol: third_symbol.symbol.clone(),
                            first_is_base_to_quote: false,
                            second_is_base_to_quote: false,
                            third_is_base_to_quote: true,
                            start_asset: base_asset.into(),
                            end_asset: base_asset.into(),
                        });
                    }
                }

                let third_quote_symbols = self.get_by_quote(third_asset);
                for third_symbol in third_quote_symbols {
                    if third_symbol.base_asset.as_ref() == base_asset {
                        all_paths.push(TriangularPath {
                            first_symbol: first_symbol.symbol.clone(),
                            second_symbol: second_symbol.symbol.clone(),
                            third_symbol: third_symbol.symbol.clone(),
                            first_is_base_to_quote: false,
                            second_is_base_to_quote: false,
                            third_is_base_to_quote: false,
                            start_asset: base_asset.into(),
                            end_asset: base_asset.into(),
                        });
                    }
                }
            }
        }

        tracing::info!(
            "Found {} potential triangular paths starting and ending with {}",
            all_paths.len(),
            base_asset
        );

        if all_paths.len() > max_paths {
            use rand::seq::SliceRandom;
            let mut rng = rand::rng();

            all_paths.shuffle(&mut rng);
            all_paths.truncate(max_paths);

            self.triangular_paths = all_paths;

            tracing::info!("Randomly selected {} triangular paths", self.triangular_paths.len());
        } else {
            self.triangular_paths = all_paths;
        }

        tracing::info!("Processed triangular paths in {:?}", start.elapsed());
    }

    #[inline]
    pub fn get_triangular_paths(&self) -> &[TriangularPath] {
        &self.triangular_paths
    }

    #[inline]
    pub fn get_trangular_paths_as_arc(&self) -> Vec<Arc<TriangularPath>> {
        self.get_triangular_paths()
            .iter()
            .map(|p| Arc::new(p.clone()))
            .collect()
    }

    #[inline]
    pub fn get_all_symbols(&self) -> Vec<Symbol> {
        self.symbols
            .read()
            .iter()
            .map(|entry| entry.1.clone())
            .collect()
    }

    #[inline]
    pub fn get_unique_symbols(&self) -> Vec<String> {
        let mut result: Vec<String> = vec![];
        self.triangular_paths.iter().for_each(|path| {
            let first = path.first_symbol.to_string();
            let second = path.second_symbol.to_string();
            let third = path.third_symbol.to_string();
            if !result.contains(&first) {
                result.push(first);
            }
            if !result.contains(&second) {
                result.push(second);
            }
            if !result.contains(&third) {
                result.push(third);
            }
        });
        println!("Unique symbols: {:?}", result);

        return result;
    }
}
