#[global_allocator]
static ALLOC: rpmalloc::RpMalloc = rpmalloc::RpMalloc;

mod app;
mod config;
mod exchange;
mod executor;
mod utils;
mod models;
mod orderbook_manager;
mod arbitrage;
mod performance;

use std::time::Duration;
use config::Config;
use anyhow::{ Context, Result };
use utils::logging;
use crate::{ models::command::Command, utils::helpers::get_run_command };
use std::sync::atomic::{ AtomicBool, Ordering };
use std::sync::Arc;
use tracing::{ info, error };

const API_TIMEOUT: Duration = Duration::from_secs(5);
const WEBSOCKET_BUFFER_SIZE: usize = 100;

fn main() -> Result<()> {
    let command = get_run_command();
    let config = Config::from_env().context("Failed to load configuration")?;

    logging
        ::init_logging(config.log_level, &config.log_config)
        .context("Failed to initialize logging")?;

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown_flag);

    let result = match command {
        Command::Run => {
            let shutdown_clone_for_handler = Arc::clone(&shutdown_clone);
            ctrlc
                ::set_handler(move || {
                    info!("Received Ctrl+C signal, initiating graceful shutdown...");
                    shutdown_clone_for_handler.store(true, Ordering::Relaxed);
                })
                .context("Failed to set Ctrl+C handler")?;

            app::normal_mode::run_normal_mode(config, shutdown_clone)
        }
        Command::PerformanceTest => {
            let shutdown_clone_for_handler = Arc::clone(&shutdown_clone);
            ctrlc
                ::set_handler(move || {
                    info!("Received Ctrl+C signal, stopping performance test...");
                    shutdown_clone_for_handler.store(true, Ordering::Relaxed);
                })
                .context("Failed to set Ctrl+C handler")?;

            app::perf_mode::run_performance_test(config, shutdown_clone)
        }
        Command::TestExecution => {
            let shutdown_clone_for_handler = Arc::clone(&shutdown_clone);
            ctrlc
                ::set_handler(move || {
                    info!("Received Ctrl+C signal, stopping test execution...");
                    shutdown_clone_for_handler.store(true, Ordering::Relaxed);
                })
                .context("Failed to set Ctrl+C handler")?;

            app::test_execution::run_test_execution(config, Some(shutdown_clone))
        }
    };

    match result {
        Ok(_) => {
            info!("Application completed successfully");
            std::process::exit(0);
        }
        Err(e) => {
            error!("Application failed: {}", e);
            std::process::exit(1);
        }
    }
}
