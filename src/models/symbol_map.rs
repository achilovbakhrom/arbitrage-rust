use std::{ collections::HashSet, sync::Arc };

use super::{ symbol::Symbol, triangular_path::TriangularPath };
use dashmap::DashMap;
#[derive(Debug, Clone)]
pub struct SymbolMap {
    symbols: DashMap<Arc<str>, Symbol>,
    by_base: DashMap<Arc<str>, Vec<Symbol>>,
    by_quote: DashMap<Arc<str>, Vec<Symbol>>,
    /// Pre-computed triangular paths
    triangular_paths: Vec<TriangularPath>,
}

impl SymbolMap {
    #[inline]
    pub fn new() -> Self {
        Self {
            symbols: DashMap::with_capacity(1000), // Pre-allocate for typical exchange size
            by_base: DashMap::with_capacity(200),
            by_quote: DashMap::with_capacity(50),
            triangular_paths: Vec::with_capacity(5000), // Pre-allocate for paths
        }
    }

    /// Build from symbols with capacity optimization
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

        // Add to base asset map
        self.by_base
            .entry(base)
            .or_insert_with(|| Vec::with_capacity(10))
            .push(symbol.clone());

        // Add to quote asset map
        self.by_quote
            .entry(quote)
            .or_insert_with(|| Vec::with_capacity(10))
            .push(symbol.clone());

        // Add to symbols map
        self.symbols.insert(symbol_str, symbol);
    }

    #[inline]
    pub fn get(&self, symbol: &str) -> Option<Symbol> {
        // DashMap get returns a reference, so we need to clone
        self.symbols.get(symbol).map(|entry| entry.clone())
    }

    #[inline]
    pub fn get_by_base(&self, base_asset: &str) -> Vec<Symbol> {
        self.by_base
            .get(base_asset)
            .map(|entry| entry.clone())
            .unwrap_or_else(Vec::new)
    }

    #[inline]
    pub fn get_by_quote(&self, quote_asset: &str) -> Vec<Symbol> {
        // DashMap get, converting to Vec since we can't return a reference to internal data
        self.by_quote
            .get(quote_asset)
            .map(|entry| entry.clone())
            .unwrap_or_else(Vec::new)
    }

    /// Find triangular paths that start and end with a specific asset
    pub fn find_targeted_triangular_paths(
        &mut self,
        base_asset: &str,
        max_paths: usize,
        excluded_fiats: &[String]
    ) {
        let start = std::time::Instant::now();
        self.triangular_paths.clear();

        // Create a HashSet for faster lookups of excluded fiats
        let excluded_fiats: HashSet<String> = excluded_fiats
            .iter()
            .map(|s| s.to_string())
            .collect();

        // Don't exclude the base asset, even if it's a fiat
        let should_exclude = |asset: &str| -> bool {
            asset != base_asset && excluded_fiats.contains(asset)
        };

        // Track all possible paths before filtering
        let mut all_paths = Vec::with_capacity(5000);

        // Find all paths starting with base_asset (like USDT)
        let base_symbols = match self.by_quote.get(base_asset) {
            Some(symbols) => symbols,
            None => {
                tracing::warn!("Base asset {} not found as quote in any symbol", base_asset);
                return;
            }
        };

        tracing::info!("Found {} symbols with {} as quote", base_symbols.len(), base_asset);

        // For each starting pair (USDT → X)
        for first_symbol in base_symbols.as_slice() {
            let second_asset = &first_symbol.base_asset;

            // Skip if second asset is a fiat currency
            if should_exclude(second_asset.as_ref()) {
                continue;
            }

            // Find second leg paths (X → Y)
            let second_base_symbols = self.get_by_base(second_asset);
            for second_symbol in second_base_symbols {
                let third_asset = &second_symbol.quote_asset;

                // Skip if third asset is a fiat currency
                if should_exclude(third_asset.as_ref()) {
                    continue;
                }

                // Find paths back to base asset (Y → USDT)
                let third_base_symbols = self.get_by_base(third_asset);
                for third_symbol in third_base_symbols {
                    if third_symbol.quote_asset.as_ref() == base_asset {
                        all_paths.push(TriangularPath {
                            first_symbol: first_symbol.symbol.clone(),
                            second_symbol: second_symbol.symbol.clone(),
                            third_symbol: third_symbol.symbol.clone(),
                            first_is_base_to_quote: false, // USDT → X
                            second_is_base_to_quote: true, // X → Y
                            third_is_base_to_quote: true, // Y → USDT
                            start_asset: base_asset.into(),
                            end_asset: base_asset.into(),
                        });
                    }
                }

                // Check other path type (X → Y → USDT)
                let third_quote_symbols = self.get_by_quote(third_asset);
                for third_symbol in third_quote_symbols {
                    if third_symbol.base_asset.as_ref() == base_asset {
                        all_paths.push(TriangularPath {
                            first_symbol: first_symbol.symbol.clone(),
                            second_symbol: second_symbol.symbol.clone(),
                            third_symbol: third_symbol.symbol.clone(),
                            first_is_base_to_quote: false, // USDT → X
                            second_is_base_to_quote: true, // X → Y
                            third_is_base_to_quote: false, // Y → USDT
                            start_asset: base_asset.into(),
                            end_asset: base_asset.into(),
                        });
                    }
                }
            }

            // Check paths where second leg is quote → base
            let second_quote_symbols = self.get_by_quote(second_asset);
            for second_symbol in second_quote_symbols {
                let third_asset = &second_symbol.base_asset;

                // Skip if third asset is a fiat currency
                if should_exclude(third_asset.as_ref()) {
                    continue;
                }

                // Find paths back to base asset
                let third_base_symbols = self.get_by_base(third_asset);
                for third_symbol in third_base_symbols {
                    if third_symbol.quote_asset.as_ref() == base_asset {
                        all_paths.push(TriangularPath {
                            first_symbol: first_symbol.symbol.clone(),
                            second_symbol: second_symbol.symbol.clone(),
                            third_symbol: third_symbol.symbol.clone(),
                            first_is_base_to_quote: false, // USDT → X
                            second_is_base_to_quote: false, // X → Y
                            third_is_base_to_quote: true, // Y → USDT
                            start_asset: base_asset.into(),
                            end_asset: base_asset.into(),
                        });
                    }
                }

                // Check other path type
                let third_quote_symbols = self.get_by_quote(third_asset);
                for third_symbol in third_quote_symbols {
                    if third_symbol.base_asset.as_ref() == base_asset {
                        all_paths.push(TriangularPath {
                            first_symbol: first_symbol.symbol.clone(),
                            second_symbol: second_symbol.symbol.clone(),
                            third_symbol: third_symbol.symbol.clone(),
                            first_is_base_to_quote: false, // USDT → X
                            second_is_base_to_quote: false, // X → Y
                            third_is_base_to_quote: false, // Y → USDT
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

        // If we have more paths than needed, randomly select subset
        if all_paths.len() > max_paths {
            // Randomly select paths
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

    /// Fast access to precomputed paths
    #[inline]
    pub fn get_triangular_paths(&self) -> &[TriangularPath] {
        &self.triangular_paths
    }

    #[inline]
    pub fn get_all_assets(&self) -> Vec<Arc<str>> {
        let mut assets = HashSet::new();

        for entry in self.symbols.iter() {
            assets.insert(entry.base_asset.clone());
            assets.insert(entry.quote_asset.clone());
        }

        assets.into_iter().collect()
    }

    #[inline]
    pub fn get_all_symbols(&self) -> Vec<Symbol> {
        self.symbols
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.symbols.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.symbols.is_empty()
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
