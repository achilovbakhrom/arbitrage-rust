#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

mod app;
mod config;
mod exchange;
mod executor;
mod utils;
mod models;
mod orderbook;
mod arbitrage;
mod performance;

use std::time::Duration;
use config::Config;
use anyhow::{ Context, Result };
use utils::logging;
use crate::{ models::command::Command, utils::helpers::get_run_command };

const API_TIMEOUT: Duration = Duration::from_secs(5);
const WEBSOCKET_BUFFER_SIZE: usize = 100;

fn main() -> Result<()> {
    let command = get_run_command();
    let config = Config::from_env().context("Failed to load configuration")?;

    logging
        ::init_logging(config.log_level, config.debug, &config.log_config)
        .context("Failed to initialize logging")?;

    match command {
        Command::Run => app::normal_mode::run_normal_mode(config)?,
        Command::PerformanceTest => app::perf_mode::run_performance_test(config)?,
        Command::TestExecution => app::test_execution::run_test_execution(config)?,
    }

    Ok(())
}
