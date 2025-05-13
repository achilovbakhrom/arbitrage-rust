// use serde::Deserialize;

// #[derive(Deserialize, Clone, Debug)]
// pub struct Symbol {
//     pub symbol: String,
//     pub status: String,
//     #[serde(rename = "isSpotTradingAllowed")]
//     pub is_spot_trading_allowed: bool,
//     #[serde(rename = "baseAsset")]
//     pub base_asset: String,
//     #[serde(rename = "baseAssetPrecision")]
//     pub base_asset_precision: i64,
//     #[serde(rename = "quoteAsset")]
//     pub quote_asset: String,
//     #[serde(rename = "quotePrecision")]
//     pub quote_precision: i64,
//     #[serde(rename = "quoteAssetPrecision")]
//     pub quote_asset_precision: i64,
//     #[serde(rename = "baseCommissionPrecision")]
//     pub base_commission_precision: i32,
//     #[serde(rename = "quoteCommissionPrecision")]
//     pub quote_commission_precision: i32,
//     #[serde(rename = "orderTypes")]
//     pub order_types: Vec<String>,
//     #[serde(rename = "icebergAllowed")]
//     pub iceberg_allowed: bool,
//     #[serde(rename = "ocoAllowed")]
//     pub oco_allowed: bool,
//     #[serde(rename = "quoteOrderQtyMarketAllowed")]
//     pub quote_order_qty_market_allowed: bool,
//     #[serde(rename = "isMarginTradingAllowed")]
//     pub is_margin_trading_allowed: bool,
// }

// #[derive(Deserialize)]
// pub struct ExchangeInfo {
//     pub symbols: Vec<Symbol>,
// }
