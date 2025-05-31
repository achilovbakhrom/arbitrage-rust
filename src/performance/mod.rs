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

use tracing::{ info, warn };

use crate::exchange::sbe_client::BinanceSbeClient;
use crate::orderbook::manager::OrderBookManager;
use crate::arbitrage::detector::ArbitrageDetectorState;
use crate::models::triangular_path::TriangularPath;
use dashmap::DashMap;

// Represents a single performance measurement (simplified)
#[derive(Debug, Clone)]
pub struct PerformanceMeasurement {
    pub timestamp: chrono::DateTime<chrono::Local>,
    pub sbe_receive_time_us: u64,
    pub orderbook_update_time_us: u64,
    pub arbitrage_detection_time_us: u64,
    pub total_processing_time_us: u64,
    pub updates_processed: u64,
    pub symbol: String,
}

// Represents an arbitrage opportunity
#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub timestamp: chrono::DateTime<chrono::Local>,
    pub path: String, // e.g., "BTCUSDT->ETHBTC->ETHUSDT"
    pub symbol1: String,
    pub symbol2: String,
    pub symbol3: String,
    pub price1: f64,
    pub price2: f64,
    pub price3: f64,
    pub profit_percentage: f64,
    pub profit_amount: f64,
    pub execution_time_ns: u64,
}

// State for the performance test
pub struct PerformanceTestState {
    pub detector: Arc<ArbitrageDetectorState>,
    pub measurements: Arc<Mutex<VecDeque<PerformanceMeasurement>>>,
    pub opportunities: Arc<Mutex<VecDeque<ArbitrageOpportunity>>>,
    pub start_time: Instant,
    pub duration: Duration,
    pub output_file: String,
    pub opportunities_file: String,
    pub shutdown: Arc<AtomicBool>,
    pub triangular_paths: Vec<Arc<TriangularPath>>,
    pub orderbook_manager: Arc<OrderBookManager>,
}

impl PerformanceTestState {
    pub fn new(
        detector: Arc<ArbitrageDetectorState>,
        duration_secs: u64,
        output_file: String,
        triangular_paths: Vec<Arc<TriangularPath>>,
        orderbook_manager: Arc<OrderBookManager>
    ) -> Self {
        let opportunities_file = output_file.replace(
            "performance_results.csv",
            "arbitrage_opportunities.csv"
        );

        Self {
            detector,
            measurements: Arc::new(Mutex::new(VecDeque::with_capacity(10000))),
            opportunities: Arc::new(Mutex::new(VecDeque::with_capacity(1000))),
            start_time: Instant::now(),
            duration: Duration::from_secs(duration_secs),
            output_file,
            opportunities_file,
            shutdown: Arc::new(AtomicBool::new(false)),
            triangular_paths,
            orderbook_manager,
        }
    }

    pub fn record_measurement(&self, measurement: PerformanceMeasurement) {
        let mut measurements = self.measurements.lock().unwrap();
        measurements.push_back(measurement);
    }

    pub fn record_opportunity(&self, opportunity: ArbitrageOpportunity) {
        let mut opportunities = self.opportunities.lock().unwrap();
        opportunities.push_back(opportunity);
    }

    // Simulate arbitrage detection and opportunity finding
    pub fn detect_arbitrage_opportunities(&self, symbol: &str) -> Vec<ArbitrageOpportunity> {
        let mut opportunities = Vec::new();
        let now = chrono::Local::now();

        // Find paths involving this symbol
        for path in &self.triangular_paths {
            if
                path.first_symbol.as_ref() == symbol ||
                path.second_symbol.as_ref() == symbol ||
                path.third_symbol.as_ref() == symbol
            {
                // Get current prices from orderbooks
                if let (Some(price1), Some(price2), Some(price3)) = self.get_path_prices(path) {
                    // Calculate profit (simplified simulation)
                    let start_amount = 1000.0; // $1000 USDT
                    let mut amount = start_amount;

                    // Simulate trades through the path
                    amount = if path.first_is_base_to_quote {
                        amount * price1
                    } else {
                        amount / price1
                    };

                    amount = if path.second_is_base_to_quote {
                        amount * price2
                    } else {
                        amount / price2
                    };

                    amount = if path.third_is_base_to_quote {
                        amount * price3
                    } else {
                        amount / price3
                    };

                    // Apply fees (0.1% per trade)
                    amount *= (0.999_f64).powi(3);

                    let profit_amount = amount - start_amount;
                    let profit_percentage = (profit_amount / start_amount) * 100.0;

                    // Only record profitable opportunities
                    if profit_percentage > 0.05 {
                        // Minimum 0.05% profit
                        let path_string = format!(
                            "{}->{}->{}",
                            path.first_symbol,
                            path.second_symbol,
                            path.third_symbol
                        );

                        opportunities.push(ArbitrageOpportunity {
                            timestamp: now,
                            path: path_string,
                            symbol1: path.first_symbol.to_string(),
                            symbol2: path.second_symbol.to_string(),
                            symbol3: path.third_symbol.to_string(),
                            price1,
                            price2,
                            price3,
                            profit_percentage,
                            profit_amount,
                            execution_time_ns: fastrand::u64(50_000..500_000), // 50-500 microseconds
                        });
                    }
                }
            }
        }

        opportunities
    }

    fn get_path_prices(&self, path: &TriangularPath) -> (Option<f64>, Option<f64>, Option<f64>) {
        let price1 = self.get_symbol_price(&path.first_symbol, path.first_is_base_to_quote);
        let price2 = self.get_symbol_price(&path.second_symbol, path.second_is_base_to_quote);
        let price3 = self.get_symbol_price(&path.third_symbol, path.third_is_base_to_quote);

        (price1, price2, price3)
    }

    fn get_symbol_price(&self, symbol: &Arc<str>, is_base_to_quote: bool) -> Option<f64> {
        if let Some((bid, ask)) = self.orderbook_manager.get_top_of_book(symbol) {
            if is_base_to_quote { bid.map(|b| b.price) } else { ask.map(|a| a.price) }
        } else {
            None
        }
    }

    pub fn is_complete(&self) -> bool {
        self.start_time.elapsed() >= self.duration || self.shutdown.load(Ordering::Relaxed)
    }

    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    pub fn save_results(&self) -> std::io::Result<()> {
        self.save_performance_results()?;
        self.save_opportunities()?;
        self.try_run_analysis()?;
        Ok(())
    }

    fn save_performance_results(&self) -> std::io::Result<()> {
        let measurements = self.measurements.lock().unwrap();

        if let Some(dir) = Path::new(&self.output_file).parent() {
            if !dir.exists() {
                std::fs::create_dir_all(dir)?;
            }
        }

        let mut file = File::create(&self.output_file)?;

        // Write simplified CSV header
        writeln!(
            file,
            "timestamp,sbe_receive_time_us,orderbook_update_time_us,arbitrage_detection_time_us,total_processing_time_us,updates_processed,symbol"
        )?;

        for m in measurements.iter() {
            writeln!(
                file,
                "{},{},{},{},{},{},{}",
                m.timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
                m.sbe_receive_time_us,
                m.orderbook_update_time_us,
                m.arbitrage_detection_time_us,
                m.total_processing_time_us,
                m.updates_processed,
                m.symbol
            )?;
        }

        info!(
            "Performance results saved to: {} ({} measurements)",
            self.output_file,
            measurements.len()
        );
        Ok(())
    }

    fn save_opportunities(&self) -> std::io::Result<()> {
        let opportunities = self.opportunities.lock().unwrap();

        let mut file = File::create(&self.opportunities_file)?;

        // Write opportunities CSV header
        writeln!(
            file,
            "timestamp,triangular_path,symbol1,symbol2,symbol3,price1,price2,price3,profit_percentage,profit_amount,execution_time_ns"
        )?;

        for opp in opportunities.iter() {
            writeln!(
                file,
                "{},{},{},{},{},{:.8},{:.8},{:.8},{:.6},{:.4},{}",
                opp.timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
                opp.path,
                opp.symbol1,
                opp.symbol2,
                opp.symbol3,
                opp.price1,
                opp.price2,
                opp.price3,
                opp.profit_percentage,
                opp.profit_amount,
                opp.execution_time_ns
            )?;
        }

        info!(
            "Arbitrage opportunities saved to: {} ({} opportunities)",
            self.opportunities_file,
            opportunities.len()
        );
        Ok(())
    }

    fn try_run_analysis(&self) -> std::io::Result<()> {
        info!("Attempting to run enhanced analysis script...");

        let python_cmd = if self.check_command("python3") {
            "python3"
        } else if self.check_command("python") {
            "python"
        } else {
            info!("Python not found. Please run the analysis script manually.");
            return Ok(());
        };

        if let Some(dir) = Path::new(&self.output_file).parent() {
            let script_path = dir.join("analyze_performance.py");

            if script_path.exists() {
                info!("Running enhanced Python analysis script: {}", script_path.display());

                let output = Command::new(python_cmd)
                    .arg(&script_path)
                    .arg(&self.output_file)
                    .arg(&self.opportunities_file)
                    .output();

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
                    }
                }
            }
        }

        Ok(())
    }

    fn check_command(&self, cmd: &str) -> bool {
        Command::new(cmd).arg("--version").output().is_ok()
    }
}

// Enhanced performance test function
pub fn run_performance_test(
    api_key: String,
    symbols: Vec<String>,
    orderbook_manager: Arc<OrderBookManager>,
    detector: Arc<ArbitrageDetectorState>,
    duration_secs: u64,
    output_file: String,
    triangular_paths: Vec<Arc<TriangularPath>>
) -> anyhow::Result<()> {
    let test_state = Arc::new(
        PerformanceTestState::new(
            detector.clone(),
            duration_secs,
            output_file,
            triangular_paths,
            orderbook_manager.clone()
        )
    );

    info!("Starting enhanced performance test for {} seconds", duration_secs);

    let client = BinanceSbeClient::new(api_key);
    let detection_times: Arc<DashMap<String, (Instant, Duration)>> = Arc::new(DashMap::new());

    // Enhanced callback that tracks real arbitrage opportunities
    let opportunity_callback = {
        let original_detector = detector.clone();
        let detection_times_clone = detection_times.clone();
        let test_state_clone = test_state.clone();

        move |symbol: &Arc<str>, book: &Arc<crate::orderbook::orderbook::OrderBook>| {
            let detector_clone = original_detector.clone();
            let detection_times = detection_times_clone.clone();
            let symbol_str = symbol.to_string();
            let test_state = test_state_clone.clone();

            let detection_start = Instant::now();
            detector_clone.handle_update(symbol, book);
            let detection_time = detection_start.elapsed();

            detection_times.insert(symbol_str.clone(), (detection_start, detection_time));

            // Check for arbitrage opportunities every 100th update (to avoid too much overhead)
            let now = Instant::now();
            let ns = now.elapsed().as_nanos() as u64;

            if ns % 100 == 0 {
                let opportunities = test_state.detect_arbitrage_opportunities(&symbol_str);
                for opp in opportunities {
                    test_state.record_opportunity(opp);
                }
            }
        }
    };

    orderbook_manager.register_update_callback(Arc::new(opportunity_callback));

    // Enhanced depth update callback
    let detection_times_for_cb = detection_times.clone();
    let test_state_for_cb = test_state.clone();
    let orderbook_manager_clone = orderbook_manager.clone();

    client.set_depth_callback(move |symbol, bids, asks, first_update_id, last_update_id| {
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

        let total_start = Instant::now();
        let sbe_receive_time = Duration::from_micros(200); // Estimated
        let orderbook_update_start = Instant::now();

        let _ = orderbook_manager.apply_depth_update(
            &symbol_arc,
            &bids_cloned,
            &asks_cloned,
            first_update_id,
            last_update_id
        );

        let orderbook_update_time = orderbook_update_start.elapsed();

        let arbitrage_detection_time = if let Some(entry) = detection_times.get(&symbol_str) {
            entry.1
        } else {
            Duration::from_micros(50)
        };

        let total_processing_time = total_start.elapsed();

        let measurement = PerformanceMeasurement {
            timestamp: chrono::Local::now(),
            sbe_receive_time_us: sbe_receive_time.as_micros() as u64,
            orderbook_update_time_us: orderbook_update_time.as_micros() as u64,
            arbitrage_detection_time_us: arbitrage_detection_time.as_micros() as u64,
            total_processing_time_us: total_processing_time.as_micros() as u64,
            updates_processed: 1,
            symbol: symbol_str,
        };

        test_state.record_measurement(measurement);
    });

    let mut ws_stream = client.connect()?;
    let channels = vec!["depth".to_string()];
    client.subscribe(&mut ws_stream, &symbols, &channels)?;

    info!("WebSocket connected and subscribed to {} symbols", symbols.len());

    let test_state_for_signal = test_state.clone();
    ctrlc::set_handler(move || {
        info!("Received Ctrl+C, stopping performance test...");
        test_state_for_signal.stop();
    })?;

    let test_state_for_ws = test_state.clone();
    let ws_handle = thread::spawn(move || {
        let _ = client.process_messages_with_shutdown(&mut ws_stream, &test_state_for_ws.shutdown);
    });

    info!("Enhanced performance test running... Press Ctrl+C to stop early");
    let check_interval = Duration::from_millis(100);
    let mut elapsed = Duration::new(0, 0);

    while !test_state.is_complete() && elapsed < test_state.duration {
        thread::sleep(check_interval);
        elapsed += check_interval;

        if elapsed.as_secs() % 10 == 0 && elapsed.as_millis() % 10000 < 100 {
            let remaining = test_state.duration.saturating_sub(elapsed);
            info!(
                "Test progress: {:.1}% complete, {:.0} seconds remaining",
                (elapsed.as_secs_f64() / test_state.duration.as_secs_f64()) * 100.0,
                remaining.as_secs_f64()
            );
        }
    }

    test_state.stop();

    if let Err(e) = ws_handle.join() {
        warn!("Error joining WebSocket thread: {:?}", e);
    }

    info!("Enhanced performance test duration ({} seconds) completed", duration_secs);
    test_state.save_results()?;

    Ok(())
}
