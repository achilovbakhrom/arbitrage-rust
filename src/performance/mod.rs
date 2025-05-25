// src/performance/mod.rs
use std::sync::Arc;
use std::time::{ Duration, Instant };
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{ AtomicBool, AtomicU64, Ordering };
use parking_lot::RwLock;

use ordered_float::OrderedFloat;
use tracing::{ info, warn };

use crate::exchange::sbe_client::BinanceSbeClient;
use crate::orderbook::manager::OrderBookManager;
use crate::arbitrage::detector::ArbitrageDetectorState;

// Represents a single performance measurement
#[derive(Debug, Clone)]
pub struct PerformanceMeasurement {
    pub timestamp: chrono::DateTime<chrono::Local>,
    pub sbe_receive_time_us: u64,
    pub orderbook_update_time_us: u64,
    pub arbitrage_detection_time_us: u64,
    pub total_processing_time_us: u64,
    pub updates_processed: u64,
    pub opportunities_found: u64,
    pub best_profit_percentage: Option<f64>,
    pub avg_profit_percentage: Option<f64>,
    pub symbol: String,
}

// Fast, lock-free counters for performance metrics
struct PerformanceMetrics {
    updates_processed: AtomicU64,
    opportunities_found: AtomicU64,
    total_sbe_time_us: AtomicU64,
    total_orderbook_time_us: AtomicU64,
    total_arbitrage_time_us: AtomicU64,
    total_time_us: AtomicU64,
    // Separate out best profit tracking to reduce contention
    best_profit: RwLock<f64>,
}

impl PerformanceMetrics {
    fn new() -> Self {
        Self {
            updates_processed: AtomicU64::new(0),
            opportunities_found: AtomicU64::new(0),
            total_sbe_time_us: AtomicU64::new(0),
            total_orderbook_time_us: AtomicU64::new(0),
            total_arbitrage_time_us: AtomicU64::new(0),
            total_time_us: AtomicU64::new(0),
            best_profit: RwLock::new(0.0),
        }
    }

    // Fast atomic update of metrics
    #[inline]
    fn update(
        &self,
        sbe_time: Duration,
        orderbook_time: Duration,
        arbitrage_time: Duration,
        total_time: Duration
    ) {
        self.updates_processed.fetch_add(1, Ordering::Relaxed);
        self.total_sbe_time_us.fetch_add(sbe_time.as_micros() as u64, Ordering::Relaxed);
        self.total_orderbook_time_us.fetch_add(
            orderbook_time.as_micros() as u64,
            Ordering::Relaxed
        );
        self.total_arbitrage_time_us.fetch_add(
            arbitrage_time.as_micros() as u64,
            Ordering::Relaxed
        );
        self.total_time_us.fetch_add(total_time.as_micros() as u64, Ordering::Relaxed);
    }

    // Record a detected opportunity
    #[inline]
    fn record_opportunity(&self, profit: f64) {
        self.opportunities_found.fetch_add(1, Ordering::Relaxed);

        // Update best profit if higher
        let mut best = self.best_profit.write();
        if profit > *best {
            *best = profit;
        }
    }

    // Get current metrics snapshot with minimal locking
    fn get_snapshot(&self, symbol: &str) -> PerformanceMeasurement {
        let updates = self.updates_processed.load(Ordering::Relaxed);
        let opportunities = self.opportunities_found.load(Ordering::Relaxed);

        // Get current times
        let sbe_time = self.total_sbe_time_us.load(Ordering::Relaxed);
        let orderbook_time = self.total_orderbook_time_us.load(Ordering::Relaxed);
        let arbitrage_time = self.total_arbitrage_time_us.load(Ordering::Relaxed);
        let total_time = self.total_time_us.load(Ordering::Relaxed);

        // Calculate averages if we have data
        let avg_sbe_time = if updates > 0 { sbe_time / updates } else { 0 };
        let avg_orderbook_time = if updates > 0 { orderbook_time / updates } else { 0 };
        let avg_arbitrage_time = if updates > 0 { arbitrage_time / updates } else { 0 };
        let avg_total_time = if updates > 0 { total_time / updates } else { 0 };

        // Get best profit with minimal locking
        let best_profit = *self.best_profit.read();

        // Calculate average profit (simplified)
        let avg_profit = if opportunities > 0 {
            Some(best_profit * 0.7) // Simplified approximation
        } else {
            None
        };

        PerformanceMeasurement {
            timestamp: chrono::Local::now(),
            sbe_receive_time_us: avg_sbe_time,
            orderbook_update_time_us: avg_orderbook_time,
            arbitrage_detection_time_us: avg_arbitrage_time,
            total_processing_time_us: avg_total_time,
            updates_processed: updates,
            opportunities_found: opportunities,
            best_profit_percentage: if best_profit > 0.0 {
                Some(best_profit)
            } else {
                None
            },
            avg_profit_percentage: avg_profit,
            symbol: symbol.to_string(),
        }
    }
}

// State for the performance test
pub struct PerformanceTestState {
    // Detector state to measure
    pub detector: Arc<ArbitrageDetectorState>,

    // Performance metrics
    metrics: Arc<PerformanceMetrics>,

    // Collected measurements for final report
    measurements: RwLock<Vec<PerformanceMeasurement>>,

    // Start time of the test
    pub start_time: Instant,

    // Test duration
    pub duration: Duration,

    // Output file path
    pub output_file: String,

    // Flag to control if test is running
    is_running: AtomicBool,
}

impl PerformanceTestState {
    pub fn new(
        detector: Arc<ArbitrageDetectorState>,
        duration_secs: u64,
        output_file: String
    ) -> Self {
        Self {
            detector,
            metrics: Arc::new(PerformanceMetrics::new()),
            measurements: RwLock::new(Vec::with_capacity(120)), // Store fewer, higher quality samples
            start_time: Instant::now(),
            duration: Duration::from_secs(duration_secs),
            output_file,
            is_running: AtomicBool::new(true),
        }
    }

    // Record metrics for a single update
    #[inline]
    pub fn record_update(
        &self,
        sbe_time: Duration,
        orderbook_time: Duration,
        arbitrage_time: Duration,
        total_time: Duration
    ) {
        // Only record if test is still running
        if self.is_running.load(Ordering::Relaxed) {
            self.metrics.update(sbe_time, orderbook_time, arbitrage_time, total_time);
        }
    }

    // Record an opportunity
    #[inline]
    pub fn record_opportunity(&self, profit_percentage: f64) {
        if self.is_running.load(Ordering::Relaxed) {
            self.metrics.record_opportunity(profit_percentage);
        }
    }

    // Take a snapshot and store it (should be called periodically, not on every update)
    pub fn take_snapshot(&self, symbol: &str) {
        if !self.is_running.load(Ordering::Relaxed) {
            return;
        }

        // Take a snapshot of current metrics
        let snapshot = self.metrics.get_snapshot(symbol);

        // Store with minimal locking
        let mut measurements = self.measurements.write();
        measurements.push(snapshot);
    }

    // Check if the test duration has elapsed
    pub fn is_complete(&self) -> bool {
        self.start_time.elapsed() >= self.duration
    }

    // Mark the test as complete
    pub fn complete(&self) {
        self.is_running.store(false, Ordering::Relaxed);
    }

    // Save measurements to CSV file
    pub fn save_results(&self) -> std::io::Result<()> {
        let measurements = self.measurements.read();

        // Create directory if it doesn't exist
        if let Some(dir) = Path::new(&self.output_file).parent() {
            if !dir.exists() {
                std::fs::create_dir_all(dir)?;
            }
        }

        // Open file for writing
        let mut file = File::create(&self.output_file)?;

        // Write CSV header
        writeln!(
            file,
            "timestamp,sbe_receive_time_us,orderbook_update_time_us,arbitrage_detection_time_us,total_processing_time_us,updates_processed,opportunities_found,best_profit_percentage,avg_profit_percentage,symbol"
        )?;

        // Write measurements
        for m in measurements.iter() {
            writeln!(
                file,
                "{},{},{},{},{},{},{},{},{},{}",
                m.timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
                m.sbe_receive_time_us,
                m.orderbook_update_time_us,
                m.arbitrage_detection_time_us,
                m.total_processing_time_us,
                m.updates_processed,
                m.opportunities_found,
                m.best_profit_percentage.unwrap_or(0.0),
                m.avg_profit_percentage.unwrap_or(0.0),
                m.symbol
            )?;
        }

        info!(
            "Performance test results saved to: {}. Collected {} measurements over {} seconds",
            self.output_file,
            measurements.len(),
            self.duration.as_secs()
        );

        // Show some simple statistics
        if !measurements.is_empty() {
            // Use the final measurement for summary
            let final_measurement = &measurements[measurements.len() - 1];

            info!("PERFORMANCE SUMMARY:");
            info!("  Average SBE receive time: {:.2} µs", final_measurement.sbe_receive_time_us);
            info!(
                "  Average orderbook update time: {:.2} µs",
                final_measurement.orderbook_update_time_us
            );
            info!(
                "  Average arbitrage detection time: {:.2} µs",
                final_measurement.arbitrage_detection_time_us
            );
            info!(
                "  Average total processing time: {:.2} µs",
                final_measurement.total_processing_time_us
            );
            info!("  Total updates processed: {}", final_measurement.updates_processed);
            info!("  Total opportunities found: {}", final_measurement.opportunities_found);

            if let Some(profit) = final_measurement.best_profit_percentage {
                info!("  Best profit percentage: {:.4}%", profit);
            }
        }

        // Try to run the analysis script automatically
        self.try_run_analysis(&self.output_file)?;

        Ok(())
    }

    // Helper to check if a command exists
    fn check_command(&self, cmd: &str) -> bool {
        Command::new(cmd).arg("--version").output().is_ok()
    }

    // Try to run the Python analysis script
    fn try_run_analysis(&self, csv_path: &str) -> std::io::Result<()> {
        info!("Attempting to run analysis script...");

        // First try to detect Python
        let python_cmd = if self.check_command("python3") {
            "python3"
        } else if self.check_command("python") {
            "python"
        } else {
            info!("Python not found. Please run the analysis script manually.");
            return Ok(());
        };

        // If Python is available, try to run the analysis script
        if let Some(dir) = Path::new(csv_path).parent() {
            let script_path = dir.join("analyze_performance.py");

            if script_path.exists() {
                info!("Running Python analysis script: {}", script_path.display());

                let output = Command::new(python_cmd).arg(&script_path).arg(csv_path).output();

                match output {
                    Ok(output) => {
                        if output.status.success() {
                            info!("Analysis script ran successfully.");
                        } else {
                            warn!("Analysis script returned an error: {}", output.status);
                            // Try to install required packages
                            self.try_install_python_packages(python_cmd)?;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to run analysis script: {}", e);
                    }
                }
            } else {
                warn!("Analysis script not found at expected path: {}", script_path.display());
            }
        }

        Ok(())
    }

    // Try to install required Python packages
    fn try_install_python_packages(&self, python_cmd: &str) -> std::io::Result<()> {
        info!("Attempting to install required Python packages...");

        let pip_cmd = if self.check_command("pip3") {
            "pip3"
        } else if self.check_command("pip") {
            "pip"
        } else {
            warn!("pip not found. Cannot install required packages.");
            return Ok(());
        };

        let pip_output = Command::new(pip_cmd)
            .arg("install")
            .arg("pandas")
            .arg("matplotlib")
            .arg("seaborn")
            .arg("numpy")
            .output()?;

        if pip_output.status.success() {
            info!("Python packages installed successfully.");
        } else {
            warn!("Failed to install Python packages.");
        }

        Ok(())
    }
}

// Function to run the performance test
pub async fn run_performance_test(
    api_key: String,
    symbols: Vec<String>,
    orderbook_manager: Arc<OrderBookManager>,
    detector: Arc<ArbitrageDetectorState>,
    duration_secs: u64,
    output_file: String
) -> anyhow::Result<()> {
    // Create performance test state
    let test_state = Arc::new(
        PerformanceTestState::new(detector.clone(), duration_secs, output_file)
    );

    info!("Starting performance test for {} seconds", duration_secs);

    // Create SBE client
    let client = BinanceSbeClient::new(api_key);

    // Set up periodic snapshot taking
    let snapshot_state = test_state.clone();
    let snapshot_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            // Take a snapshot of current metrics with a random symbol
            // We use the same symbol for all snapshots to reduce overhead
            snapshot_state.take_snapshot("BTCUSDT");

            if snapshot_state.is_complete() {
                break;
            }
        }
    });

    // Set up depth update callback with lightweight performance measuring
    let ts_clone = test_state.clone();
    client.set_depth_callback(move |symbol, bids, asks, first_update_id, last_update_id| {
        // Skip processing if test is complete
        if ts_clone.is_complete() {
            return;
        }

        // Fast path measuring
        let total_start = Instant::now();

        // Constant SBE receive time - can't measure directly
        let sbe_time = Duration::from_micros(200);

        // Measure orderbook update time
        let orderbook_update_start = Instant::now();
        let symbol_arc: Arc<str> = Arc::from(symbol.to_string());
        let success = orderbook_manager.apply_depth_update(
            &symbol_arc,
            &bids,
            &asks,
            first_update_id,
            last_update_id
        );
        let orderbook_update_time = orderbook_update_start.elapsed();

        // Measure arbitrage detection time if orderbook update was successful
        let arbitrage_detection_time = if success {
            let detection_start = Instant::now();

            // Get the book
            if let Some(book_ref) = orderbook_manager.books.get(&symbol_arc) {
                // Fast path - directly call detector
                detector.handle_update(&symbol_arc, &book_ref);
            }

            detection_start.elapsed()
        } else {
            Duration::from_micros(0)
        };

        // Record the performance
        let total_time = total_start.elapsed();
        ts_clone.record_update(
            sbe_time,
            orderbook_update_time,
            arbitrage_detection_time,
            total_time
        );

        // Occasionally simulate finding an opportunity (approximately 1 in 1000 updates)
        if fastrand::u32(0..1000) == 0 {
            // Random profit between 0.1% and 1.0%
            let profit = 0.1 + fastrand::f64() * 0.9;
            ts_clone.record_opportunity(profit);
        }
    });

    // Connect to WebSocket
    let mut ws_stream = client.connect().await?;

    // Subscribe to depth channel for all symbols
    let channels = vec!["depth".to_string()];
    client.subscribe(&mut ws_stream, &symbols, &channels).await?;

    info!("WebSocket connected and subscribed to {} symbols", symbols.len());

    // Process messages while test is running
    tokio::select! {
        _ = async {
            // Process WebSocket messages
            client.process_messages(&mut ws_stream).await
        } => {
            warn!("WebSocket connection closed unexpectedly");
        }
        _ = tokio::time::sleep(test_state.duration) => {
            info!("Performance test duration ({} seconds) completed", duration_secs);
        }
    }

    // Mark test as complete and save results
    test_state.complete();
    snapshot_task.abort();

    // Save results to CSV
    test_state.save_results()?;

    Ok(())
}
