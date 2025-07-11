use std::io;
use std::path::Path;
use chrono::Local;
use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{
    fmt::{ self },
    prelude::*,
    EnvFilter,
    filter::LevelFilter,
    layer::SubscriberExt,
};
use tracing_appender::rolling::{ RollingFileAppender, Rotation };
use tracing_appender::non_blocking::WorkerGuard;

use crate::config::{ LogConfig, LogRotation };

struct LogGuards {
    _file_guard: WorkerGuard,
    _console_guard: Option<WorkerGuard>,
}

static mut LOG_GUARDS: Option<LogGuards> = None;

pub fn init_logging(level: Level, log_config: &LogConfig) -> io::Result<()> {
    if !log_config.directory.exists() {
        std::fs::create_dir_all(&log_config.directory).map_err(|e| {
            eprintln!("Failed to create log directory: {e}");
            e
        })?;
    }

    let now = Local::now();
    let timestamp = now.format("%Y%m%d");
    let filename = format!("{}_{}.log", log_config.filename_prefix, timestamp);

    let rotation = match log_config.rotation {
        LogRotation::Hourly => Rotation::HOURLY,
        LogRotation::Daily => Rotation::DAILY,
        LogRotation::Never => Rotation::NEVER,
    };

    let file_appender = RollingFileAppender::new(rotation, log_config.directory.clone(), filename);

    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = fmt
        ::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::CLOSE);

    let level_filter = match level {
        Level::TRACE => LevelFilter::TRACE,
        Level::DEBUG => LevelFilter::DEBUG,
        Level::INFO => LevelFilter::INFO,
        Level::WARN => LevelFilter::WARN,
        Level::ERROR => LevelFilter::ERROR,
    };

    let filter = EnvFilter::from_default_env().add_directive(level_filter.into());

    let (console_writer, console_guard) = tracing_appender::non_blocking(io::stdout());

    let console_layer = fmt
        ::layer()
        .with_writer(console_writer)
        .with_ansi(true)
        .with_target(true)
        .with_span_events(FmtSpan::CLOSE)
        .pretty();

    tracing_subscriber::registry().with(filter).with(file_layer).with(console_layer).init();

    unsafe {
        LOG_GUARDS = Some(LogGuards {
            _file_guard: file_guard,
            _console_guard: Some(console_guard),
        });
    }

    if let Some(max_files) = log_config.max_files {
        if
            let Err(e) = cleanup_old_logs(
                &log_config.directory,
                &log_config.filename_prefix,
                max_files
            )
        {
            eprintln!("Failed to clean up old log files: {e}");
        }
    }

    tracing::info!(
        log_dir = %log_config.directory.display(),
        log_prefix = %log_config.filename_prefix,
        "Asynchronous logging initialized at level: {:?}", 
        level
    );

    Ok(())
}

fn cleanup_old_logs(log_dir: &Path, prefix: &str, max_files: usize) -> io::Result<()> {
    let entries = std::fs
        ::read_dir(log_dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();

            if path.is_file() && path.file_name()?.to_string_lossy().starts_with(prefix) {
                if let Ok(metadata) = entry.metadata() {
                    return Some((path, metadata.modified().ok()?));
                }
            }
            None
        })
        .collect::<Vec<_>>();

    if entries.len() > max_files {
        let mut sorted_entries = entries;
        sorted_entries.sort_by(|a, b| b.1.cmp(&a.1));

        for (path, _) in sorted_entries.iter().skip(max_files) {
            std::fs::remove_file(path)?;
        }
    }

    Ok(())
}
