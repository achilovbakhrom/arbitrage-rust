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
    pub depth: usize,
    pub max_triangles: usize,
    pub trade_amount: f64,
    pub fee: f64,

    #[serde(serialize_with = "serialize_level", deserialize_with = "deserialize_level")]
    pub log_level: Level,
    pub log_config: LogConfig,

    pub base_asset: String,
    pub excluded_coins: Vec<String>,

    pub min_volume_multiplier: f64,
    pub volume_depth_check: usize,
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
        match dotenv() {
            Ok(path) => {
                println!("✅ Loaded .env file from: {}", path.display());
            }
            Err(e) => {
                println!("⚠️  Warning: Could not load .env file: {}", e);
                println!("   Make sure .env file exists in the current directory");
            }
        }

        // Debug: Print current working directory
        if let Ok(cwd) = env::current_dir() {
            println!("📁 Current working directory: {}", cwd.display());
        }

        // Debug: Check if .env file exists
        if std::path::Path::new(".env").exists() {
            println!("✅ .env file found in current directory");
        } else {
            println!("❌ .env file NOT found in current directory");
        }

        // Parse DEBUG
        let debug = env
            ::var("TAA_DEBUG")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .context("Failed to parse TAA_DEBUG environment variable")?;

        // Parse SBE_API_KEY
        let sbe_api_key = env
            ::var("TAA_SBE_API_KEY")
            .context("SBE_API_KEY environment variable not set")?;

        // Parse FIX_API
        let fix_api = env::var("TAA_FIX_API").context("TAA_FIX_API environment variable not set")?;

        // Parse FIX_SECRET
        let fix_secret = env
            ::var("TAA_FIX_SECRET")
            .context("TAA_FIX_SECRET environment variable not set")?;

        // Parse THRESHOLD
        let threshold = env
            ::var("TAA_THRESHOLD")
            .unwrap_or_else(|_| "0.5".to_string())
            .parse::<f64>()
            .context("Failed to parse TAA_THRESHOLD environment variable")?;

        // Parse DEPTH
        let depth = env
            ::var("TAA_DEPTH")
            .unwrap_or_else(|_| "20".to_string())
            .parse::<usize>()
            .context("Failed to parse TAA_DEPTH environment variable")?;

        // Parse PAIRS_COUNT
        let max_triangles: usize = env
            ::var("TAA_MAX_TRIANGLES")
            .unwrap_or_else(|_| "15".to_string())
            .parse::<usize>()
            .context("Failed to parse TAA_MAX_TRIANGLES environment variable")?;

        // Parse TRADE_AMOUNT
        let trade_amount: f64 = env
            ::var("TAA_TRADE_AMOUNT")
            .unwrap_or_else(|_| "100.0".to_string())
            .parse::<f64>()
            .context("Failed to parse TAA_TRADE_AMOUNT environment variable")?;

        // Parse TRADE_AMOUNT
        let fee: f64 = env
            ::var("TAA_FEE")
            .unwrap_or_else(|_| "0.001".to_string())
            .parse::<f64>()
            .context("Failed to parse TAA_FEE environment variable")?;

        // Parse LOG_LEVEL
        let log_level_str = env::var("TAA_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());

        let log_level = match log_level_str.to_lowercase().as_str() {
            "trace" => Level::TRACE,
            "debug" => Level::DEBUG,
            "info" => Level::INFO,
            "warn" => Level::WARN,
            "error" => Level::ERROR,
            _ => Level::INFO,
        };

        // Set up logging configuration
        let log_dir = env::var("TAA_LOG_DIRECTORY").unwrap_or_else(|_| "logs".to_string());

        let log_prefix = env
            ::var("TAA_LOG_FILENAME_PREFIX")
            .unwrap_or_else(|_| "triangular_arbitrage".to_string());

        let log_rotation_str = env::var("TAA_LOG_ROTATION").unwrap_or_else(|_| "daily".to_string());

        let log_rotation = match log_rotation_str.to_lowercase().as_str() {
            "hourly" => LogRotation::Hourly,
            "never" => LogRotation::Never,
            _ => LogRotation::Daily,
        };

        let max_files = env
            ::var("TAA_LOG_MAX_FILES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok());

        let log_config = LogConfig {
            directory: PathBuf::from(log_dir),
            filename_prefix: log_prefix,
            rotation: log_rotation,
            max_files,
        };

        let base_asset = env::var("TAA_BASE_ASSET").unwrap_or_else(|_| "USDT".to_string());

        let default_fiats = "USD,EUR,GBP,JPY,AUD,CAD,CHF,CNY,RUB,TRY";
        let excluded_coins = env
            ::var("TAA_EXCLUDED_COINS")
            .unwrap_or_else(|_| default_fiats.to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let min_volume_multiplier = env
            ::var("TAA_MIN_VOLUME_MULTIPLIER")
            .unwrap_or_else(|_| "2.0".to_string())
            .parse::<f64>()?;

        let volume_depth_check = env
            ::var("TAA_VOLUME_DEPTH_CHECK")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<usize>()?;

        Ok(Config {
            debug,
            sbe_api_key,
            fix_api,
            fix_secret,
            threshold,
            depth,
            max_triangles,
            log_level,
            log_config,
            base_asset,
            excluded_coins,
            trade_amount,
            fee,
            min_volume_multiplier,
            volume_depth_check,
        })
    }
}
