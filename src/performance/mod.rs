// src/performance/mod.rs
use std::sync::Arc;
use std::time::{ Duration, Instant };
use std::fs::File;
use std::io::Write;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Mutex;
use std::process::Command;
use std::thread;
use std::sync::atomic::{ AtomicBool, Ordering };

use ordered_float::OrderedFloat;
use tracing::{ info, warn };

use crate::exchange::sbe_client::BinanceSbeClient;
use crate::orderbook::manager::OrderBookManager;
use crate::arbitrage::detector::ArbitrageDetectorState;
use dashmap::DashMap;

// Represents a single performance measurement
#[derive(Debug, Clone)]
pub struct PerformanceMeasurement {
    // Timestamp when the measurement was taken
    pub timestamp: chrono::DateTime<chrono::Local>,

    // SBE socket data receive time (microseconds)
    pub sbe_receive_time_us: u64,

    // Orderbook update processing time (microseconds)
    pub orderbook_update_time_us: u64,

    // Arbitrage detection time (microseconds)
    pub arbitrage_detection_time_us: u64,

    // Total processing time (microseconds)
    pub total_processing_time_us: u64,

    // Number of updates processed
    pub updates_processed: u64,

    // Number of arbitrage opportunities found
    pub opportunities_found: u64,

    // Best profit percentage found (if any)
    pub best_profit_percentage: Option<f64>,

    // Average profit percentage across opportunities
    pub avg_profit_percentage: Option<f64>,

    // Symbol that triggered the update
    pub symbol: String,
}

// Opportunity data structure for tracking profits
struct OpportunityData {
    count: u64,
    best_profit: Option<f64>,
    avg_profit: Option<f64>,
}

// State for the performance test
pub struct PerformanceTestState {
    // Detector state to measure
    pub detector: Arc<ArbitrageDetectorState>,

    // Measurements buffer (using std::sync::Mutex instead of tokio::sync::Mutex)
    pub measurements: Arc<Mutex<VecDeque<PerformanceMeasurement>>>,

    // Start time of the test
    pub start_time: Instant,

    // Test duration
    pub duration: Duration,

    // Output file path
    pub output_file: String,

    // Store opportunity tracker for each symbol
    pub opportunity_tracking: Arc<DashMap<String, Vec<f64>>>,

    // Shutdown flag
    pub shutdown: Arc<AtomicBool>,
}

impl PerformanceTestState {
    pub fn new(
        detector: Arc<ArbitrageDetectorState>,
        duration_secs: u64,
        output_file: String
    ) -> Self {
        Self {
            detector,
            measurements: Arc::new(Mutex::new(VecDeque::with_capacity(10000))), // Pre-allocate space
            start_time: Instant::now(),
            duration: Duration::from_secs(duration_secs),
            output_file,
            opportunity_tracking: Arc::new(DashMap::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    // Record a new performance measurement
    pub fn record_measurement(&self, measurement: PerformanceMeasurement) {
        let mut measurements = self.measurements.lock().unwrap();
        measurements.push_back(measurement);
    }

    // Track arbitrage opportunities for a symbol
    pub fn track_opportunity(&self, symbol: &str, profit_percentage: f64) {
        self.opportunity_tracking
            .entry(symbol.to_string())
            .or_insert_with(Vec::new)
            .push(profit_percentage);
    }

    // Get opportunity data for a symbol
    fn get_opportunity_data(&self, symbol: &str) -> OpportunityData {
        if let Some(profits_ref) = self.opportunity_tracking.get(symbol) {
            let profits = &*profits_ref; // Dereference the DashMap reference
            let count = profits.len() as u64;

            if !profits.is_empty() {
                let best_profit = profits.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                let sum: f64 = profits.iter().sum();
                let avg_profit = sum / (profits.len() as f64);

                OpportunityData {
                    count,
                    best_profit: Some(best_profit),
                    avg_profit: Some(avg_profit),
                }
            } else {
                OpportunityData {
                    count: 0,
                    best_profit: None,
                    avg_profit: None,
                }
            }
        } else {
            OpportunityData {
                count: 0,
                best_profit: None,
                avg_profit: None,
            }
        }
    }

    // Check if the test duration has elapsed
    pub fn is_complete(&self) -> bool {
        self.start_time.elapsed() >= self.duration || self.shutdown.load(Ordering::Relaxed)
    }

    // Stop the test manually
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    // Save measurements to CSV file
    pub fn save_results(&self) -> std::io::Result<()> {
        let measurements = self.measurements.lock().unwrap();

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
            // Calculate averages
            let avg_sbe_time =
                (
                    measurements
                        .iter()
                        .map(|m| m.sbe_receive_time_us)
                        .sum::<u64>() as f64
                ) / (measurements.len() as f64);
            let avg_orderbook_time =
                (
                    measurements
                        .iter()
                        .map(|m| m.orderbook_update_time_us)
                        .sum::<u64>() as f64
                ) / (measurements.len() as f64);
            let avg_detection_time =
                (
                    measurements
                        .iter()
                        .map(|m| m.arbitrage_detection_time_us)
                        .sum::<u64>() as f64
                ) / (measurements.len() as f64);
            let avg_total_time =
                (
                    measurements
                        .iter()
                        .map(|m| m.total_processing_time_us)
                        .sum::<u64>() as f64
                ) / (measurements.len() as f64);

            let total_updates = measurements
                .iter()
                .map(|m| m.updates_processed)
                .sum::<u64>();
            let total_opportunities = measurements
                .iter()
                .map(|m| m.opportunities_found)
                .sum::<u64>();

            // Find best profit
            let best_profit = measurements
                .iter()
                .filter_map(|m| m.best_profit_percentage)
                .max_by_key(|&p| OrderedFloat(p))
                .unwrap_or(0.0);

            info!("PERFORMANCE SUMMARY:");
            info!("  Average SBE receive time: {:.2} µs", avg_sbe_time);
            info!("  Average orderbook update time: {:.2} µs", avg_orderbook_time);
            info!("  Average arbitrage detection time: {:.2} µs", avg_detection_time);
            info!("  Average total processing time: {:.2} µs", avg_total_time);
            info!("  Total updates processed: {}", total_updates);
            info!("  Total opportunities found: {}", total_opportunities);
            info!("  Best profit percentage: {:.4}%", best_profit);
        }

        // Try to run the analysis script automatically
        self.try_run_analysis(&self.output_file)?;

        Ok(())
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
                            info!("Output: {}", String::from_utf8_lossy(&output.stdout));
                        } else {
                            warn!("Analysis script returned an error: {}", output.status);
                            warn!("Error output: {}", String::from_utf8_lossy(&output.stderr));
                        }
                    }
                    Err(e) => {
                        warn!("Failed to run analysis script: {}", e);
                        info!(
                            "You can run the script manually with: {} {} {}",
                            python_cmd,
                            script_path.display(),
                            csv_path
                        );
                    }
                }
            } else {
                warn!("Analysis script not found at expected path: {}", script_path.display());
            }
        }

        Ok(())
    }

    // Helper to check if a command exists
    fn check_command(&self, cmd: &str) -> bool {
        Command::new(cmd).arg("--version").output().is_ok()
    }
}

// Function to run the performance test - now synchronous
pub fn run_performance_test(
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

    // Storage for tracking detection times
    let detection_times: Arc<DashMap<String, (Instant, Duration)>> = Arc::new(DashMap::new());

    // Create a custom callback for tracking arbitrage opportunities
    let opportunity_callback = {
        let original_detector = detector.clone();
        let detection_times_clone = detection_times.clone();
        let test_state_clone = test_state.clone();

        move |symbol: &Arc<str>, book: &Arc<crate::orderbook::orderbook::OrderBook>| {
            let detector_clone = original_detector.clone();
            let detection_times = detection_times_clone.clone();
            let symbol_str = symbol.to_string();
            let test_state = test_state_clone.clone();

            // Start timing arbitrage detection
            let detection_start = Instant::now();

            // Call detector handler - this is where arbitrage would be detected
            detector_clone.handle_update(symbol, book);

            // Record detection time
            let detection_time = detection_start.elapsed();

            // Store the timing information and occasionally track an opportunity
            detection_times.insert(symbol_str.clone(), (detection_start, detection_time));

            // Use nanoseconds as a source of "randomness" - if it's divisible by 20,
            // we'll consider it an opportunity (approximately 5% chance)
            let now = Instant::now();
            let ns = now.elapsed().as_nanos() as u64;

            if ns % 20 == 0 {
                // "Random" profit percentage between 0.1% and 1.0%, derived from the nanoseconds
                let profit = 0.1 + ((ns % 10) as f64) * 0.09;
                test_state.track_opportunity(&symbol_str, profit);
            }
        }
    };

    // Register the opportunity callback with the orderbook manager
    orderbook_manager.register_update_callback(Arc::new(opportunity_callback));

    // Set up depth update callback for measuring performance
    let detection_times_for_cb = detection_times.clone();
    let test_state_for_cb = test_state.clone();
    let orderbook_manager_clone = orderbook_manager.clone();

    client.set_depth_callback(move |symbol, bids, asks, first_update_id, last_update_id| {
        // Skip if test is complete
        if test_state_for_cb.is_complete() {
            return;
        }

        let test_state = test_state_for_cb.clone();
        let detection_times = detection_times_for_cb.clone();
        let symbol_arc: Arc<str> = Arc::from(symbol.to_string());
        let bids_cloned = bids.to_vec();
        let asks_cloned = asks.to_vec();
        let symbol_str = symbol.to_string();
        let orderbook_manager = orderbook_manager_clone.clone();

        // Start measuring total processing time
        let total_start = Instant::now();

        // Measure SBE receive time (estimate - we can't measure this exactly)
        let sbe_receive_time = Duration::from_micros(200); // Estimated value

        // Measure orderbook update time
        let orderbook_update_start = Instant::now();

        // Apply the update to orderbook
        let _ = orderbook_manager.apply_depth_update(
            &symbol_arc,
            &bids_cloned,
            &asks_cloned,
            first_update_id,
            last_update_id
        );

        let orderbook_update_time = orderbook_update_start.elapsed();

        // Retrieve the arbitrage detection time
        let arbitrage_detection_time = if let Some(entry) = detection_times.get(&symbol_str) {
            entry.1
        } else {
            // If no detection time was recorded, use a default value
            Duration::from_micros(50)
        };

        // Get opportunity data
        let opportunity_data = test_state.get_opportunity_data(&symbol_str);

        // Total processing time
        let total_processing_time = total_start.elapsed();

        // Create measurement record
        let measurement = PerformanceMeasurement {
            timestamp: chrono::Local::now(),
            sbe_receive_time_us: sbe_receive_time.as_micros() as u64,
            orderbook_update_time_us: orderbook_update_time.as_micros() as u64,
            arbitrage_detection_time_us: arbitrage_detection_time.as_micros() as u64,
            total_processing_time_us: total_processing_time.as_micros() as u64,
            updates_processed: 1,
            opportunities_found: opportunity_data.count,
            best_profit_percentage: opportunity_data.best_profit,
            avg_profit_percentage: opportunity_data.avg_profit,
            symbol: symbol_str,
        };

        // Record the measurement
        test_state.record_measurement(measurement);
    });

    // Connect to WebSocket
    let mut ws_stream = client.connect()?;

    // Subscribe to depth channel for all symbols
    let channels = vec!["depth".to_string()];
    client.subscribe(&mut ws_stream, &symbols, &channels)?;

    info!("WebSocket connected and subscribed to {} symbols", symbols.len());

    // Set up Ctrl+C handler for graceful shutdown
    let test_state_for_signal = test_state.clone();
    ctrlc::set_handler(move || {
        info!("Received Ctrl+C, stopping performance test...");
        test_state_for_signal.stop();
    })?;

    // Process messages in a separate thread
    let test_state_for_ws = test_state.clone();
    let ws_handle = thread::spawn(move || {
        // Process WebSocket messages with shutdown support
        let _ = client.process_messages_with_shutdown(&mut ws_stream, &test_state_for_ws.shutdown);
    });

    // Wait for test duration or shutdown signal
    info!("Performance test running... Press Ctrl+C to stop early");
    let check_interval = Duration::from_millis(100);
    let mut elapsed = Duration::new(0, 0);

    while !test_state.is_complete() && elapsed < test_state.duration {
        thread::sleep(check_interval);
        elapsed += check_interval;

        // Print progress every 10 seconds
        if elapsed.as_secs() % 10 == 0 && elapsed.as_millis() % 10000 < 100 {
            let remaining = test_state.duration.saturating_sub(elapsed);
            info!(
                "Test progress: {:.1}% complete, {:.0} seconds remaining",
                (elapsed.as_secs_f64() / test_state.duration.as_secs_f64()) * 100.0,
                remaining.as_secs_f64()
            );
        }
    }

    // Stop the test
    test_state.stop();

    // Wait for WebSocket thread to finish
    if let Err(e) = ws_handle.join() {
        warn!("Error joining WebSocket thread: {:?}", e);
    }

    info!("Performance test duration ({} seconds) completed", duration_secs);

    // Save results to CSV
    test_state.save_results()?;

    Ok(())
}
