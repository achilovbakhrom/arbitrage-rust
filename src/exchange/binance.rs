use async_trait::async_trait;
use anyhow::{ Context, Result };
use reqwest::{ Client as HttpClient, Url };
use serde::{ Deserialize, Serialize };
use tokio::sync::RwLock;
use tracing::{ debug, error, info };
use std::time::{ Duration, Instant };
use std::sync::Arc;
// use tokio::::RwLock;
use rust_decimal::Decimal;

use crate::models::symbol::Symbol;
use crate::exchange::client::ExchangeClient;

// Shared singleton client for connection pooling
lazy_static::lazy_static! {
    static ref HTTP_CLIENT: HttpClient = HttpClient::builder()
        .timeout(Duration::from_secs(10))
        .tcp_nodelay(true) // Disable Nagle's algorithm for low latency
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .pool_idle_timeout(Some(Duration::from_secs(30)))
        .pool_max_idle_per_host(10)
        .build()
        .expect("Failed to create HTTP client");
}
pub struct BinanceClient {
    /// Base URL for API requests
    base_url: Url,

    /// API key for authenticated requests
    api_key: Arc<str>,

    /// API secret for request signing
    api_secret: Arc<str>,

    /// Whether to use the testnet
    testnet: bool,

    /// Cache for symbols to avoid repeated API calls
    symbols_cache: RwLock<Option<Vec<Symbol>>>,

    /// Cache expiration time
    symbols_cache_updated_at: RwLock<Instant>,

    /// Cache expiration duration (5 minutes)
    symbols_cache_ttl: Duration,
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    #[serde(rename = "symbols")]
    symbols: Vec<BinanceSymbol>,
}

#[derive(Debug, Deserialize, Clone)]
struct BinanceSymbol {
    symbol: String,
    status: String,
    baseAsset: String,
    quoteAsset: String,
    isSpotTradingAllowed: bool,
    filters: Vec<BinanceSymbolFilter>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "filterType")]
enum BinanceSymbolFilter {
    #[serde(rename = "PRICE_FILTER")] PriceFilter {
        minPrice: String,
        maxPrice: String,
        tickSize: String,
    },
    #[serde(rename = "LOT_SIZE")] LotSize {
        minQty: String,
        maxQty: String,
        stepSize: String,
    },
    #[serde(other)]
    Unknown,
}

impl BinanceClient {
    /// Create a new Binance client
    pub fn new(api_key: String, api_secret: String, testnet: bool) -> Result<Self> {
        // Set the base URL based on whether testnet is enabled
        let base_url = if testnet {
            Url::parse("https://testnet.binance.vision/api/").context("Invalid testnet URL")?
        } else {
            Url::parse("https://api.binance.com/api/").context("Invalid API URL")?
        };

        Ok(Self {
            base_url,
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            testnet,
            symbols_cache: RwLock::new(None),
            symbols_cache_updated_at: RwLock::new(Instant::now()),
            symbols_cache_ttl: Duration::from_secs(300), // 5 minutes
        })
    }

    /// Calculate precision from step size - optimized version
    #[inline]
    fn calculate_precision(step_size: &str) -> u8 {
        if let Some(decimal_idx) = step_size.find('.') {
            let decimal_part = &step_size[decimal_idx + 1..];

            // Fast path for common cases
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

    /// Convert BinanceSymbol to our Symbol model
    fn convert_symbol(binance_symbol: BinanceSymbol) -> Symbol {
        let mut min_qty = None;
        let mut max_qty = None;
        let mut price_precision = 8; // Default values
        let mut qty_precision = 8;

        // Extract filter information
        for filter in &binance_symbol.filters {
            match filter {
                BinanceSymbolFilter::LotSize { minQty, maxQty, stepSize } => {
                    min_qty = minQty.parse::<Decimal>().ok();
                    max_qty = maxQty.parse::<Decimal>().ok();
                    qty_precision = Self::calculate_precision(stepSize);
                }
                BinanceSymbolFilter::PriceFilter { tickSize, .. } => {
                    price_precision = Self::calculate_precision(tickSize);
                }
                _ => {}
            }
        }

        Symbol {
            symbol: binance_symbol.symbol.into(),
            base_asset: binance_symbol.baseAsset.into(),
            quote_asset: binance_symbol.quoteAsset.into(),
            min_qty,
            max_qty,
            price_precision,
            qty_precision,
            is_trading: binance_symbol.status == "TRADING",
            is_spot_trading_allowed: binance_symbol.isSpotTradingAllowed,
        }
    }

    /// Check if the symbols cache is valid
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
        // Fast path: return cached symbols if available and not expired
        if self.is_cache_valid().await {
            if let Some(symbols) = self.symbols_cache.read().await.as_ref() {
                debug!("Returning {} symbols from cache", symbols.len());
                return Ok(symbols.clone());
            }
        }

        // Cache miss: fetch from API
        let start = Instant::now();
        debug!("Fetching all symbols from Binance");

        // Build the URL for the exchange info endpoint
        let url = self.base_url.join("v3/exchangeInfo").context("Failed to build URL")?;

        // Make the request
        let response = HTTP_CLIENT.get(url)
            .header("X-MBX-APIKEY", self.api_key.as_ref())
            .send().await
            .context("Failed to send request to Binance")?;

        // Check if the request was successful
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            error!("Binance API error: {} - {}", status, text);
            anyhow::bail!("Binance API error: {} - {}", status, text);
        }

        // Parse the response with minimal allocations
        let exchange_info: BinanceExchangeInfo = response
            .json().await
            .context("Failed to parse Binance response")?;

        // Convert and optimize with pre-allocated capacity
        let symbols_count = exchange_info.symbols.len();
        let mut symbols = Vec::with_capacity(symbols_count);

        for binance_symbol in exchange_info.symbols {
            symbols.push(Self::convert_symbol(binance_symbol));
        }

        let elapsed = start.elapsed();
        info!("Fetched {} symbols from Binance in {:.2?}", symbols.len(), elapsed);

        // Update cache
        *self.symbols_cache.write().await = Some(symbols.clone());
        *self.symbols_cache_updated_at.write().await = Instant::now();

        Ok(symbols)
    }

    async fn get_active_spot_symbols(&self) -> Result<Vec<Symbol>> {
        let all_symbols = self.get_all_symbols().await.unwrap();

        // Pre-allocate for efficiency
        let mut spot_symbols = Vec::with_capacity(all_symbols.len() / 2);

        // Filter symbols to include only active spot trading symbols
        for symbol in all_symbols {
            if symbol.is_trading && symbol.is_spot_trading_allowed {
                spot_symbols.push(symbol);
            }
        }

        // Shrink to fit to conserve memory
        spot_symbols.shrink_to_fit();

        Ok(spot_symbols)
    }

    async fn get_symbol(&self, symbol: &str) -> Result<Symbol> {
        // Try to find in cache first for better performance
        if self.is_cache_valid().await {
            if let Some(symbols) = self.symbols_cache.read().await.as_ref() {
                if let Some(found) = symbols.iter().find(|s| s.symbol.as_ref() == symbol) {
                    return Ok(found.clone());
                }
            }
        }

        // Fall back to fetching all symbols if not in cache
        let all_symbols = self.get_all_symbols().await?;

        all_symbols
            .into_iter()
            .find(|s| s.symbol.as_ref() == symbol)
            .ok_or_else(|| anyhow::anyhow!("Symbol not found: {}", symbol))
    }

    async fn is_operational(&self) -> Result<bool> {
        // Use cached client for connection reuse
        let url = self.base_url.join("v3/ping").context("Failed to build URL")?;

        // Make the request with minimal overhead
        let response = HTTP_CLIENT.get(url)
            .timeout(Duration::from_secs(2)) // Short timeout for ping
            .send().await;

        // Check if the request was successful
        match response {
            Ok(res) => Ok(res.status().is_success()),
            Err(_) => Ok(false),
        }
    }
}
