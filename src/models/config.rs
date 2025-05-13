use std::env;
use serde::Serialize;
use tracing::info;

#[derive(Debug, Clone, Serialize)]
pub struct Config {
    pub debug: bool,
    pub sbe_key: String,
    pub fix_key: String,
    pub fix_secret: String,
    pub threshold: f64,
    pub pairs_count: usize,
    pub log_level: String,
}

impl Config {
    pub fn new() -> Self {
        dotenv::dotenv().ok();
        let debug = env::var("DEBUG").unwrap_or_default();
        let sbe_api_key = env::var("SBE_API_KEY").expect("Missing SBE_API_KEY");
        let fix_api_key = env::var("FIX_API").expect("Missing FIX_API");
        let fix_secret_key = env::var("FIX_SECRET").expect("Missing FIX_SECRET");
        let threshold = env
            ::var("THRESHOLD")
            .expect("Missing THRESHOLD")
            .parse::<f64>()
            .expect("THRESHOLD: Failed to parse string as f64");
        let pairs_count = env
            ::var("PAIRS_COUNT")
            .expect("Missing PAIRS_COUNT")
            .parse::<usize>()
            .expect("PAIRS_COUNT: Failed to parse string as u16");
        let log_level = env::var("LOG_LEVEL").unwrap_or(String::from("info"));

        Self {
            debug: &debug == "true",
            sbe_key: sbe_api_key,
            fix_key: fix_api_key,
            fix_secret: fix_secret_key,
            threshold: threshold,
            pairs_count: pairs_count,
            log_level: log_level,
        }
    }
}

impl Config {
    pub fn print(&self) {
        let json = serde_json::to_string_pretty(self).expect("Failed to serialize Config");
        info!("Config: {}", json)
    }
}
