// for the quick data allocation on memory
#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

mod app;
mod config;
mod exchange;
mod utils;
mod models;
mod orderbook;
mod arbitrage;
mod performance;

use std::time::Duration;

use config::Config;
use anyhow::{ Context, Result };

use utils::logging;

const API_TIMEOUT: Duration = Duration::from_secs(5);
const WEBSOCKET_BUFFER_SIZE: usize = 100;

// Define command line arguments enum
#[derive(Debug)]
enum Command {
    Run,
    PerformanceTest,
}

fn main() -> Result<()> {
    // Parse command line arguments
    let command = if std::env::args().nth(1).as_deref() == Some("perf-test") {
        Command::PerformanceTest
    } else {
        Command::Run
    };

    // Load configuration with helpful error messages
    let mut config = Config::from_env().context(
        "Failed to load configuration from environment. Make sure you have a .env file with required variables."
    )?;

    // Override debug mode for performance tests
    if matches!(command, Command::PerformanceTest) {
        config.debug = false; // Force disable console logging for performance tests
        config.max_triangles = 50;
    }

    // Initialize logging system
    logging
        ::init_logging(config.log_level, config.debug, &config.log_config)
        .context("Failed to initialize logging system")?;

    match command {
        Command::Run => app::normal_mode::run_normal_mode(config)?,
        Command::PerformanceTest => app::perf_mode::run_performance_test(config)?,
    }

    Ok(())
}
