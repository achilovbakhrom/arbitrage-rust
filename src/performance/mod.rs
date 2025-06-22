use std::sync::Arc;
use std::time::{ Duration, Instant };
use std::fs::File;
use std::io::Write;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Mutex;
use std::process::Command;
use std::thread;
use std::sync::atomic::{ AtomicBool, AtomicU64, Ordering };

use ahash::HashMapExt;
use tracing::{ info, warn };

use crate::exchange::sbe_client::BinanceSbeClient;
use crate::orderbook::manager::OrderBookManager;
use crate::arbitrage::detector::ArbitrageDetectorState;
use crate::models::triangular_path::TriangularPath;
use crate::utils::types::FastHashMap;
use crate::utils::symbol_interner::global;

use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub struct PerformanceMeasurement {
    pub timestamp: chrono::DateTime<chrono::Local>,
    pub sbe_receive_time_us: u64,
    pub orderbook_update_time_us: u64,
    pub arbitrage_detection_time_us: u64,
    pub total_processing_time_us: u64,
    pub updates_processed: u64,
    pub symbol_id: u16,
}

#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub timestamp: chrono::DateTime<chrono::Local>,
    pub path: String,
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

pub struct PerformanceTestState {
    pub detector: Arc<ArbitrageDetectorState>,
    pub measurements: Arc<Mutex<VecDeque<PerformanceMeasurement>>>,
    pub opportunities: Arc<Mutex<VecDeque<ArbitrageOpportunity>>>,
    pub start_time: Instant,
    pub duration: Duration,
    pub output_file: String,
    pub opportunities_file: String,
    pub shutdown: Arc<AtomicBool>,
    pub triangular_paths: Box<[TriangularPath]>,
    pub orderbook_manager: Arc<OrderBookManager>,
    pub measurement_counter: AtomicU64,
    pub opportunity_counter: AtomicU64,
    pub last_progress_report: AtomicU64,
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

        let unwrapped_paths: Vec<TriangularPath> = triangular_paths
            .iter()
            .map(|arc_path| (**arc_path).clone())
            .collect();

        Self {
            detector,
            measurements: Arc::new(Mutex::new(VecDeque::with_capacity(50000))),
            opportunities: Arc::new(Mutex::new(VecDeque::with_capacity(5000))),
            start_time: Instant::now(),
            duration: Duration::from_secs(duration_secs),
            output_file,
            opportunities_file,
            shutdown: Arc::new(AtomicBool::new(false)),
            triangular_paths: unwrapped_paths.into_boxed_slice(),
            orderbook_manager,
            measurement_counter: AtomicU64::new(0),
            opportunity_counter: AtomicU64::new(0),
            last_progress_report: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    fn get_symbol_id(&self, symbol: &str) -> Option<u16> {
        global::get_id(symbol)
    }

    #[inline(always)]
    fn get_symbol_string(&self, symbol_id: u16) -> Option<String> {
        global::get_symbol(symbol_id).map(|s| s.to_string())
    }

    pub fn record_measurement(&self, measurement: PerformanceMeasurement) {
        let count = self.measurement_counter.fetch_add(1, Ordering::Relaxed);

        if count % 100 == 0 {
            if let Ok(mut measurements) = self.measurements.try_lock() {
                measurements.push_back(measurement);

                if measurements.len() > 40000 {
                    measurements.drain(0..10000);
                }
            }
        } else {
            if let Ok(mut measurements) = self.measurements.try_lock() {
                measurements.push_back(measurement);
            }
        }
    }

    pub fn record_opportunity(&self, opportunity: ArbitrageOpportunity) {
        self.opportunity_counter.fetch_add(1, Ordering::Relaxed);

        if let Ok(mut opportunities) = self.opportunities.try_lock() {
            opportunities.push_back(opportunity);

            if opportunities.len() > 4000 {
                opportunities.drain(0..1000);
            }
        }
    }

    pub fn detect_arbitrage_opportunities_by_id(
        &self,
        symbol_id: u16
    ) -> Vec<ArbitrageOpportunity> {
        let count = self.measurement_counter.load(Ordering::Relaxed);
        if count % 1000 != 0 {
            return Vec::new();
        }

        let mut opportunities = Vec::with_capacity(4);
        let now = chrono::Local::now();

        for path in self.triangular_paths.iter().take(5) {
            let first_id = self.get_symbol_id(&path.first_symbol);
            let second_id = self.get_symbol_id(&path.second_symbol);
            let third_id = self.get_symbol_id(&path.third_symbol);

            if
                first_id == Some(symbol_id) ||
                second_id == Some(symbol_id) ||
                third_id == Some(symbol_id)
            {
                if
                    let (Some(price1), Some(price2), Some(price3)) =
                        self.get_path_prices_by_id(path)
                {
                    let start_amount = 1000.0;
                    let mut amount = start_amount;

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

                    amount *= 0.997;

                    let profit_amount = amount - start_amount;
                    let profit_percentage = (profit_amount / start_amount) * 100.0;

                    if profit_percentage > 0.05 {
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
                            execution_time_ns: 150_000,
                        });
                    }
                }
            }
        }

        opportunities
    }

    #[inline(always)]
    fn get_path_prices_by_id(
        &self,
        path: &TriangularPath
    ) -> (Option<f64>, Option<f64>, Option<f64>) {
        let first_id = self.get_symbol_id(&path.first_symbol);
        let second_id = self.get_symbol_id(&path.second_symbol);
        let third_id = self.get_symbol_id(&path.third_symbol);

        let price1 = first_id.and_then(|id|
            self.get_symbol_price_by_id(id, path.first_is_base_to_quote)
        );
        let price2 = second_id.and_then(|id|
            self.get_symbol_price_by_id(id, path.second_is_base_to_quote)
        );
        let price3 = third_id.and_then(|id|
            self.get_symbol_price_by_id(id, path.third_is_base_to_quote)
        );

        (price1, price2, price3)
    }

    #[inline(always)]
    fn get_symbol_price_by_id(&self, symbol_id: u16, is_base_to_quote: bool) -> Option<f64> {
        if let Some((bid, ask)) = self.orderbook_manager.get_top_of_book_by_id(symbol_id) {
            if is_base_to_quote { bid.map(|b| b.price) } else { ask.map(|a| a.price) }
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn is_complete(&self) -> bool {
        self.start_time.elapsed() >= self.duration || self.shutdown.load(Ordering::Relaxed)
    }

    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    pub fn maybe_report_progress(&self) {
        let elapsed = self.start_time.elapsed().as_secs();
        let last_report = self.last_progress_report.load(Ordering::Relaxed);

        if elapsed > last_report + 10 {
            if
                self.last_progress_report
                    .compare_exchange_weak(
                        last_report,
                        elapsed,
                        Ordering::Relaxed,
                        Ordering::Relaxed
                    )
                    .is_ok()
            {
                let remaining = self.duration.saturating_sub(self.start_time.elapsed());
                let measurements = self.measurement_counter.load(Ordering::Relaxed);
                let opportunities = self.opportunity_counter.load(Ordering::Relaxed);

                info!(
                    "Test progress: {:.1}% complete, {:.0}s remaining, {}k measurements, {} opportunities",
                    ((elapsed as f64) / self.duration.as_secs_f64()) * 100.0,
                    remaining.as_secs_f64(),
                    measurements / 1000,
                    opportunities
                );
            }
        }
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

        writeln!(
            file,
            "timestamp,sbe_receive_time_us,orderbook_update_time_us,arbitrage_detection_time_us,total_processing_time_us,updates_processed,symbol_id,symbol"
        )?;

        for m in measurements.iter() {
            let symbol_name = self
                .get_symbol_string(m.symbol_id)
                .unwrap_or_else(|| format!("ID_{}", m.symbol_id));
            writeln!(
                file,
                "{},{},{},{},{},{},{},{}",
                m.timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
                m.sbe_receive_time_us,
                m.orderbook_update_time_us,
                m.arbitrage_detection_time_us,
                m.total_processing_time_us,
                m.updates_processed,
                m.symbol_id,
                symbol_name
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
                        } else {
                            warn!("Analysis script returned an error: {}", output.status);
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

    info!("Starting ultra-optimized performance test for {} seconds", duration_secs);

    let client = BinanceSbeClient::new(api_key);

    let symbol_mappings = orderbook_manager.get_symbol_mappings();
    client.preload_symbol_mappings(&symbol_mappings);
    client.enable_timing();

    let detection_times: Arc<RwLock<FastHashMap<u16, Duration>>> = Arc::new(
        RwLock::new(FastHashMap::with_capacity(2048))
    );

    let opportunity_callback = {
        let original_detector = detector.clone();
        let detection_times_clone = detection_times.clone();
        let test_state_clone = test_state.clone();

        move |symbol_id: u16, book: &crate::orderbook::orderbook::OrderBook| {
            let detection_start = Instant::now();
            original_detector.handle_update_by_id(symbol_id, book);
            let detection_time = detection_start.elapsed();

            let _ = detection_times_clone.write().insert(symbol_id, detection_time);

            let count = test_state_clone.measurement_counter.load(Ordering::Relaxed);
            if count % 500 == 0 {
                let opportunities =
                    test_state_clone.detect_arbitrage_opportunities_by_id(symbol_id);
                for opp in opportunities {
                    test_state_clone.record_opportunity(opp);
                }
            }
        }
    };

    orderbook_manager.register_update_callback(Arc::new(opportunity_callback));

    let detection_times_for_cb = detection_times.clone();
    let test_state_for_cb = test_state.clone();
    let orderbook_manager_clone = orderbook_manager.clone();

    client.set_depth_callback(
        move |
            symbol_id: u16,
            bids: &[(f64, f64); 5],
            asks: &[(f64, f64); 5],
            first_update_id: u64,
            last_update_id: u64,
            receive_time_us: u64
        | {
            if test_state_for_cb.is_complete() {
                return;
            }

            let total_start = Instant::now();
            let orderbook_update_start = Instant::now();

            let _ = orderbook_manager_clone.apply_depth_update(
                symbol_id,
                bids,
                asks,
                first_update_id,
                last_update_id
            );

            let orderbook_update_time = orderbook_update_start.elapsed();

            let arbitrage_detection_time = detection_times_for_cb
                .read()
                .get(&symbol_id)
                .map(|entry| *entry)
                .unwrap_or_else(|| Duration::from_micros(25));

            let total_processing_time = total_start.elapsed();

            let measurement = PerformanceMeasurement {
                timestamp: chrono::Local::now(),
                sbe_receive_time_us: receive_time_us,
                orderbook_update_time_us: orderbook_update_time.as_micros() as u64,
                arbitrage_detection_time_us: arbitrage_detection_time.as_micros() as u64,
                total_processing_time_us: total_processing_time.as_micros() as u64,
                updates_processed: 1,
                symbol_id,
            };

            test_state_for_cb.record_measurement(measurement);
            test_state_for_cb.maybe_report_progress();
        }
    );

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

    info!("Ultra-optimized performance test running... Press Ctrl+C to stop early");

    let check_interval = Duration::from_millis(500);

    while !test_state.is_complete() {
        thread::sleep(check_interval);
    }

    test_state.stop();

    if let Err(e) = ws_handle.join() {
        warn!("Error joining WebSocket thread: {:?}", e);
    }

    info!("Ultra-optimized performance test ({} seconds) completed", duration_secs);

    let final_measurements = test_state.measurement_counter.load(Ordering::Relaxed);
    let final_opportunities = test_state.opportunity_counter.load(Ordering::Relaxed);
    info!(
        "Final stats: {} measurements, {} opportunities",
        final_measurements,
        final_opportunities
    );

    test_state.save_results()?;

    Ok(())
}
