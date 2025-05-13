// use std::collections::{ HashMap, HashSet };
// use rand::{ seq::SliceRandom, rng };
// use crate::constants::{ ASK, BID };
// use crate::models::{ symbol::Symbol, step::Step, triangle::Triangle };

// pub fn create_triangles_from_symbols(
//     symbols: &[Symbol],
//     limit: usize
// ) -> (Vec<Triangle>, Vec<Symbol>) {
//     // Build lookup tables: base → (quote → symbol), and quote → (base → symbol)
//     let mut pairs_by_base: HashMap<String, HashMap<String, String>> = HashMap::new();
//     let mut pairs_by_quote: HashMap<String, HashMap<String, String>> = HashMap::new();

//     for s in symbols {
//         pairs_by_base
//             .entry(s.base_asset.clone())
//             .or_default()
//             .insert(s.quote_asset.clone(), s.symbol.clone());
//         pairs_by_quote
//             .entry(s.quote_asset.clone())
//             .or_default()
//             .insert(s.base_asset.clone(), s.symbol.clone());
//     }

//     let mut result = Vec::new();
//     let mut used = HashSet::new();

//     // For each A/B symbol
//     for sym in symbols {
//         if sym.quote_asset != "USDT" {
//             continue;
//         }
//         if sym.base_asset == "USDC" || sym.base_asset == "FDUSD" {
//             continue;
//         }

//         let a = &sym.base_asset; // A
//         let b = &sym.quote_asset; // B
//         let ab = &sym.symbol; // "A/B"

//         // Find all C/B where B is quote
//         if let Some(c_map) = pairs_by_quote.get(a) {
//             for (c, cb_sym) in c_map {
//                 // Look for A/C
//                 if let Some(ac_sym) = pairs_by_base.get(c).and_then(|m| m.get(b)) {
//                     let triangle = Triangle::new(
//                         Step { pair: ab.to_lowercase(), side: ASK },
//                         Step { pair: cb_sym.to_lowercase(), side: ASK },
//                         Step { pair: ac_sym.to_lowercase(), side: BID }
//                     );
//                     result.push(triangle);
//                 }
//             }
//         }
//     }

//     // Apply limit with random shuffle
//     if limit != 0 && result.len() > limit {
//         result.shuffle(&mut rng());
//         result.truncate(limit);
//     }

//     // Collect used symbols from all triangles
//     for tri in &result {
//         for sym in tri.symbols() {
//             used.insert(sym.clone());
//         }
//     }

//     // Build Vec<Symbol> of unique symbols
//     let mut unique = Vec::with_capacity(used.len());
//     for sym_str in used {
//         if let Some(orig) = symbols.iter().find(|s| s.symbol.to_lowercase() == sym_str) {
//             unique.push(orig.clone());
//         }
//     }

//     (result, unique)
// }
