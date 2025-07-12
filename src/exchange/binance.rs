use async_trait::async_trait;
use anyhow::{ Context, Result };
use reqwest::{ Client as HttpClient, Url };
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::{ debug, error, info, warn };
use std::time::{ Duration, Instant };
use std::sync::Arc;
use crate::models::level::Level;
use crate::models::symbol::Symbol;
use crate::exchange::client::{ DepthResponse, ExchangeClient };

lazy_static::lazy_static! {
    static ref HTTP_CLIENT: HttpClient = HttpClient::builder()
        .timeout(Duration::from_secs(10))
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .pool_idle_timeout(Some(Duration::from_secs(30)))
        .pool_max_idle_per_host(10)
        .build()
        .expect("Failed to create HTTP client");
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceDepthResponse {
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

pub struct BinanceClient {
    base_url: Url,
    api_key: Arc<str>,
    testnet: bool,
    symbols_cache: RwLock<Option<Vec<Symbol>>>,
    symbols_cache_updated_at: RwLock<Instant>,
    symbols_cache_ttl: Duration,
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    #[serde(rename = "symbols")]
    symbols: Vec<BinanceSymbol>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct BinanceSymbol {
    symbol: String,
    status: String,
    base_asset: String,
    quote_asset: String,
    is_spot_trading_allowed: bool,
    filters: Vec<BinanceSymbolFilter>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "filterType")]
enum BinanceSymbolFilter {
    #[serde(rename = "PRICE_FILTER")] #[serde(rename_all = "camelCase")] PriceFilter {
        min_price: String,
        max_price: String,
        tick_size: String,
    },
    #[serde(rename = "LOT_SIZE")] #[serde(rename_all = "camelCase")] LotSize {
        min_qty: String,
        max_qty: String,
        step_size: String,
    },
    #[serde(other)]
    Unknown,
}

impl BinanceClient {
    pub fn new(api_key: String, testnet: bool) -> Result<Self> {
        let base_url = Url::parse("https://api.binance.com/api/").context("Invalid API URL")?;

        Ok(Self {
            base_url,
            api_key: api_key.into(),
            testnet,
            symbols_cache: RwLock::new(None),
            symbols_cache_updated_at: RwLock::new(Instant::now()),
            symbols_cache_ttl: Duration::from_secs(300),
        })
    }

    #[inline]
    fn calculate_precision(step_size: &str) -> u8 {
        if let Some(decimal_idx) = step_size.find('.') {
            let decimal_part = &step_size[decimal_idx + 1..];

            if decimal_part == "1" || decimal_part == "01" {
                return decimal_part.len() as u8;
            }

            if let Some(non_zero_pos) = decimal_part.chars().position(|c| c != '0' && c != '1') {
                return non_zero_pos as u8;
            }

            decimal_part.len() as u8
        } else {
            0
        }
    }

    fn convert_symbol(binance_symbol: BinanceSymbol) -> Symbol {
        let mut price_precision = 8;
        let mut qty_precision = 8;

        for filter in &binance_symbol.filters {
            match filter {
                BinanceSymbolFilter::LotSize { step_size: ss, .. } => {
                    qty_precision = Self::calculate_precision(ss);
                }
                BinanceSymbolFilter::PriceFilter { tick_size: t, .. } => {
                    price_precision = Self::calculate_precision(t);
                }
                _ => {}
            }
        }

        Symbol {
            symbol: binance_symbol.symbol.into(),
            base_asset: binance_symbol.base_asset.into(),
            quote_asset: binance_symbol.quote_asset.into(),
            price_precision,
            qty_precision,
            is_trading: binance_symbol.status == "TRADING",
            is_spot_trading_allowed: binance_symbol.is_spot_trading_allowed,
        }
    }

    #[inline]
    async fn is_cache_valid(&self) -> bool {
        let cache_time = *self.symbols_cache_updated_at.read().await;
        let elapsed = cache_time.elapsed();

        if elapsed > self.symbols_cache_ttl {
            return false;
        }

        self.symbols_cache.read().await.is_some()
    }
}

#[async_trait]
impl ExchangeClient for BinanceClient {
    fn name(&self) -> &str {
        if self.testnet { "Binance Testnet" } else { "Binance" }
    }

    async fn get_all_symbols(&self) -> Result<Vec<Symbol>> {
        if self.is_cache_valid().await {
            if let Some(symbols) = self.symbols_cache.read().await.as_ref() {
                debug!("Returning {} symbols from cache", symbols.len());
                return Ok(symbols.clone());
            }
        }

        let start = Instant::now();
        debug!("Fetching all symbols from Binance");

        let url = self.base_url.join("v3/exchangeInfo").context("Failed to build URL")?;

        let response = HTTP_CLIENT.get(url)
            .header("X-MBX-APIKEY", self.api_key.as_ref())
            .send().await
            .context("Failed to send request to Binance")?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            error!("Binance API error: {} - {}", status, text);
            anyhow::bail!("Binance API error: {} - {}", status, text);
        }

        let exchange_info: BinanceExchangeInfo = response
            .json().await
            .context("Failed to parse Binance response")?;

        let symbols_count = exchange_info.symbols.len();
        let mut symbols = Vec::with_capacity(symbols_count);

        for binance_symbol in exchange_info.symbols {
            symbols.push(Self::convert_symbol(binance_symbol));
        }

        let elapsed = start.elapsed();
        info!("Fetched {} symbols from Binance in {:.2?}", symbols.len(), elapsed);

        *self.symbols_cache.write().await = Some(symbols.clone());
        *self.symbols_cache_updated_at.write().await = Instant::now();

        Ok(symbols)
    }

    async fn get_active_spot_symbols(&self) -> Result<Vec<Symbol>> {
        let all_symbols = self.get_all_symbols().await.unwrap();

        let mut spot_symbols = Vec::with_capacity(all_symbols.len() / 2);

        for symbol in all_symbols {
            if symbol.is_trading && symbol.is_spot_trading_allowed {
                spot_symbols.push(symbol);
            }
        }

        spot_symbols.shrink_to_fit();
        Ok(spot_symbols)
    }

    async fn is_operational(&self) -> Result<bool> {
        let url = self.base_url.join("v3/ping").context("Failed to build URL")?;

        let response = HTTP_CLIENT.get(url).timeout(Duration::from_secs(2)).send().await;

        match response {
            Ok(res) => Ok(res.status().is_success()),
            Err(_) => Ok(false),
        }
    }

    async fn fetch_depth(&self, symbol: &str, limit: usize) -> Result<DepthResponse> {
        if symbol.trim().is_empty() {
            return Err(anyhow::anyhow!("Symbol parameter cannot be empty"));
        }

        let url = self.base_url
            .join(&format!("v3/depth?symbol={symbol}&limit={limit}"))
            .context("Failed to build depth API URL")?;

        debug!(symbol = %symbol, limit = limit, url = %url.as_str(), "Fetching orderbook depth");

        let start = Instant::now();

        let response = HTTP_CLIENT.get(url.clone())
            .header("X-MBX-APIKEY", self.api_key.as_ref())
            .timeout(Duration::from_secs(5))
            .send().await
            .context(format!("Failed to fetch depth from Binance for symbol {symbol}"))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            error!(symbol = %symbol, status = %status, response = %text, "Binance depth API error");
            anyhow::bail!("Binance depth API error for {symbol}: {status} - {text}");
        }

        let depth: BinanceDepthResponse = response
            .json().await
            .context(format!("Failed to parse Binance depth response for {symbol}"))?;

        let bids = depth.bids
            .iter()
            .filter_map(|bid| {
                match (bid[0].parse::<f64>(), bid[1].parse::<f64>()) {
                    (Ok(price), Ok(qty)) => Some(Level { price, quantity: qty }),
                    _ => {
                        warn!(symbol = %symbol, price = %bid[0], qty = %bid[1], "Failed to parse bid");
                        None
                    }
                }
            })
            .collect::<Vec<Level>>();

        let asks = depth.asks
            .iter()
            .filter_map(|ask| {
                match (ask[0].parse::<f64>(), ask[1].parse::<f64>()) {
                    (Ok(price), Ok(qty)) => Some(Level { price, quantity: qty }),
                    _ => {
                        warn!(symbol = %symbol, price = %ask[0], qty = %ask[1], "Failed to parse ask");
                        None
                    }
                }
            })
            .collect::<Vec<Level>>();

        let elapsed = start.elapsed();

        debug!(
            symbol = %symbol,
            last_update_id = depth.last_update_id,
            bid_count = bids.len(),
            ask_count = asks.len(),
            elapsed_ms = %elapsed.as_millis(),
            "Successfully fetched orderbook depth"
        );

        Ok(DepthResponse {
            bids,
            asks,
            last_update_id: depth.last_update_id,
        })
    }
}
