// use crate::constants;
// use crate::models::symbols::{ ExchangeInfo, Symbol };
// use reqwest::Client;
// use tracing::instrument;
// use std::time::Duration;
// use std::error::Error;

// #[derive(Debug)]
// pub struct RestApiClient {
//     client: Client,
// }

// impl RestApiClient {
//     pub fn new() -> Result<Self, Box<dyn Error>> {
//         let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
//         Ok(Self { client })
//     }
// }

// impl RestApiClient {
//     #[instrument]
//     pub async fn fetch_all_trading_spot_symbols(&self) -> Result<Vec<Symbol>, Box<dyn Error>> {
//         let resp = self.client.get(constants::BINANCE_URL).send().await?.error_for_status()?; // ensure 2xx

//         let info: ExchangeInfo = resp.json().await?;

//         let symbols = info.symbols
//             .into_iter()
//             .filter(|s| s.status == "TRADING" && s.is_spot_trading_allowed)
//             .collect();

//         Ok(symbols)
//     }
// }
