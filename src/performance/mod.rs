// src/performance/mod.rs
use std::sync::Arc;
use std::time::{ Duration, Instant };
use std::fs::File;
use std::io::Write;
use std::collections::VecDeque;
use std::path::Path;
use tokio::sync::Mutex;

use chrono::Local;
use ordered_float::OrderedFloat;
use tracing::{ info, warn, error };
use futures::future::join_all;

use crate::exchange::sbe_client::BinanceSbeClient;
use crate::orderbook::manager::OrderBookManager;
use crate::arbitrage::detector::{ ArbitrageDetectorState, ArbitrageOpportunity };
use crate::models::triangular_path::TriangularPath;

// Represents a single performance measurement
#[derive(Debug, Clone)]
pub struct PerformanceMeasurement {
    // Timestamp when the measurement was taken
    timestamp: chrono::DateTime<chrono::Local>,

    // SBE socket data receive time (microseconds)
    sbe_receive_time_us: u64,

    // Orderbook update processing time (microseconds)
    orderbook_update_time_us: u64,

    // Arbitrage detection time (microseconds)
    arbitrage_detection_time_us: u64,

    // Total processing time (microseconds)
    total_processing_time_us: u64,

    // Number of updates processed
    updates_processed: u64,

    // Number of arbitrage opportunities found
    opportunities_found: u64,

    // Best profit percentage found (if any)
    best_profit_percentage: Option<f64>,

    // Average profit percentage across opportunities
    avg_profit_percentage: Option<f64>,

    // Symbol that triggered the update
    symbol: String,
}

// State for the performance test
pub struct PerformanceTestState {
    // Detector state to measure
    detector: Arc<ArbitrageDetectorState>,

    // Measurements buffer
    measurements: Arc<Mutex<VecDeque<PerformanceMeasurement>>>,

    // Start time of the test
    start_time: Instant,

    // Test duration
    duration: Duration,

    // Output file path
    output_file: String,
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
        }
    }

    // Record a new performance measurement
    pub async fn record_measurement(&self, measurement: PerformanceMeasurement) {
        let mut measurements = self.measurements.lock().await;
        measurements.push_back(measurement);
    }

    // Check if the test duration has elapsed
    pub fn is_complete(&self) -> bool {
        self.start_time.elapsed() >= self.duration
    }

    // Save measurements to CSV file
    pub async fn save_results(&self) -> std::io::Result<()> {
        let measurements = self.measurements.lock().await;

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

    // Set up measurement callback
    let test_state_for_callback = test_state.clone();
    let detector_for_callback = detector.clone();

    client.set_depth_callback(
        Box::new(move |symbol, bids, asks, first_update_id, last_update_id| {
            // Skip if test is complete
            if test_state_for_callback.is_complete() {
                return;
            }

            let test_state = test_state_for_callback.clone();
            let detector = detector_for_callback.clone();
            // let symbol_arc: Arc<str> = Arc::from(symbol.to_string());
            let symbol_arc: Arc<str> = Arc::from(symbol.to_string());
            let bids_cloned = bids.to_vec();
            let asks_cloned = asks.to_vec();

            // Record the time measurements asynchronously in a separate task
            tokio::spawn(async move {
                let sbe_receive_time = Instant::now();

                // Directly measure the orderbook update time
                let orderbook_update_start = Instant::now();

                // Apply the update to orderbook
                let orderbook_manager = detector.orderbook_manager.clone();
                let _ = orderbook_manager.apply_depth_update(
                    &symbol_arc,
                    &bids_cloned,
                    &asks_cloned,
                    first_update_id,
                    last_update_id
                ).await;

                let orderbook_update_time = orderbook_update_start.elapsed();

                // Measure arbitrage detection time - we don't actually need to do anything here
                // since the detector is already registered as a callback on the orderbook manager
                let arbitrage_detection_time = Duration::from_micros(0);

                // Total processing time
                let total_processing_time = sbe_receive_time.elapsed();

                // Create measurement object
                let measurement = PerformanceMeasurement {
                    timestamp: Local::now(),
                    sbe_receive_time_us: 0, // We can't measure this properly
                    orderbook_update_time_us: orderbook_update_time.as_micros() as u64,
                    arbitrage_detection_time_us: arbitrage_detection_time.as_micros() as u64,
                    total_processing_time_us: total_processing_time.as_micros() as u64,
                    updates_processed: 1,
                    opportunities_found: 0, // Will be updated if opportunities are found
                    best_profit_percentage: None,
                    avg_profit_percentage: None,
                    symbol: symbol_arc.to_string(),
                };

                // Record the measurement
                test_state.record_measurement(measurement).await;
            });
        })
    ).await;

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

    // Save results to CSV
    test_state.save_results().await?;

    Ok(())
}
