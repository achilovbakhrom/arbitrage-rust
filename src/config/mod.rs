use anyhow::{ Context, Result };
use dotenv::dotenv;
use serde::{ Deserialize, Serialize };
use std::env;
use std::path::PathBuf;
use tracing::Level;
use crate::utils::serde_helpers::{ serialize_level, deserialize_level };

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub debug: bool,
    pub sbe_api_key: String,
    pub fix_api: String,
    pub fix_secret: String,
    pub threshold: f64,
    pub max_triangles: usize,

    #[serde(serialize_with = "serialize_level", deserialize_with = "deserialize_level")]
    pub log_level: Level,
    pub log_config: LogConfig,

    pub base_asset: String,
    pub excluded_fiats: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    pub directory: PathBuf,
    pub filename_prefix: String,
    pub rotation: LogRotation,
    pub max_files: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LogRotation {
    Hourly,
    Daily,
    Never,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        // Load environment variables from .env file
        dotenv().ok();

        // Parse DEBUG
        let debug = env
            ::var("DEBUG")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .context("Failed to parse DEBUG environment variable")?;

        // Parse SBE_API_KEY
        let sbe_api_key = env
            ::var("SBE_API_KEY")
            .context("SBE_API_KEY environment variable not set")?;

        // Parse FIX_API
        let fix_api = env::var("FIX_API").context("FIX_API environment variable not set")?;

        // Parse FIX_SECRET
        let fix_secret = env::var("FIX_SECRET").context("FIX_SECRET environment variable not set")?;

        // Parse THRESHOLD
        let threshold = env
            ::var("THRESHOLD")
            .unwrap_or_else(|_| "0.5".to_string())
            .parse::<f64>()
            .context("Failed to parse THRESHOLD environment variable")?;

        // Parse PAIRS_COUNT
        let max_triangles: usize = env
            ::var("MAX_TRIANGLES")
            .unwrap_or_else(|_| "15".to_string())
            .parse::<usize>()
            .context("Failed to parse MAX_TRIANGLES environment variable")?;

        // Parse LOG_LEVEL
        let log_level_str = env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());

        let log_level = match log_level_str.to_lowercase().as_str() {
            "trace" => Level::TRACE,
            "debug" => Level::DEBUG,
            "info" => Level::INFO,
            "warn" => Level::WARN,
            "error" => Level::ERROR,
            _ => Level::INFO,
        };

        // Set up logging configuration
        let log_dir = env::var("LOG_DIRECTORY").unwrap_or_else(|_| "logs".to_string());

        let log_prefix = env
            ::var("LOG_FILENAME_PREFIX")
            .unwrap_or_else(|_| "triangular_arbitrage".to_string());

        let log_rotation_str = env::var("LOG_ROTATION").unwrap_or_else(|_| "daily".to_string());

        let log_rotation = match log_rotation_str.to_lowercase().as_str() {
            "hourly" => LogRotation::Hourly,
            "never" => LogRotation::Never,
            _ => LogRotation::Daily,
        };

        let max_files = env
            ::var("LOG_MAX_FILES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok());

        let log_config = LogConfig {
            directory: PathBuf::from(log_dir),
            filename_prefix: log_prefix,
            rotation: log_rotation,
            max_files,
        };

        let base_asset = env::var("BASE_ASSET").unwrap_or_else(|_| "USDT".to_string());

        let default_fiats = "USD,EUR,GBP,JPY,AUD,CAD,CHF,CNY,RUB,TRY";
        let excluded_fiats = env
            ::var("EXCLUDED_FIATS")
            .unwrap_or_else(|_| default_fiats.to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        Ok(Config {
            debug,
            sbe_api_key,
            fix_api,
            fix_secret,
            threshold,
            max_triangles,
            log_level,
            log_config,
            base_asset,
            excluded_fiats,
        })
    }
}
