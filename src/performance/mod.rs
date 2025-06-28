use std::sync::Arc;
use std::time::{ Duration, Instant };
use std::fs::File;
use std::io::Write;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Mutex;
use std::thread;
use std::sync::atomic::{ AtomicBool, AtomicU64, Ordering };
use ahash::HashMapExt;
use tracing::{ info, warn };
use crate::exchange::sbe_client::BinanceSbeClient;
use crate::orderbook::manager::OrderBookManager;
use crate::arbitrage::detector::ArbitrageDetector;
use crate::utils::types::FastHashMap;
use crate::utils::symbol_interner::global;
use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub struct PerformanceMeasurement {
    pub timestamp: chrono::DateTime<chrono::Local>,
    pub sbe_parse_time_us: u64,
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
    pub detector: Arc<ArbitrageDetector>,
    pub measurements: Arc<Mutex<VecDeque<PerformanceMeasurement>>>,
    pub opportunities: Arc<Mutex<VecDeque<ArbitrageOpportunity>>>,
    pub start_time: Instant,
    pub duration: Duration,
    pub output_file: String,
    pub opportunities_file: String,
    pub shutdown: Arc<AtomicBool>,
    pub orderbook_manager: Arc<OrderBookManager>,
    pub measurement_counter: AtomicU64,
    pub opportunity_counter: AtomicU64,
    pub last_progress_report: AtomicU64,
}

impl PerformanceTestState {
    pub fn new(
        orderbook_manager: Arc<OrderBookManager>,
        detector: Arc<ArbitrageDetector>,
        output_file: String,
        duration_secs: u64,
        shutdown: Arc<AtomicBool>
    ) -> Self {
        let opportunities_file = output_file.replace(
            "performance_results.csv",
            "arbitrage_opportunities.csv"
        );

        Self {
            detector,
            measurements: Arc::new(Mutex::new(VecDeque::with_capacity(50000))),
            opportunities: Arc::new(Mutex::new(VecDeque::with_capacity(5000))),
            start_time: Instant::now(),
            duration: Duration::from_secs(duration_secs),
            output_file,
            opportunities_file,
            shutdown: shutdown,
            orderbook_manager,
            measurement_counter: AtomicU64::new(0),
            opportunity_counter: AtomicU64::new(0),
            last_progress_report: AtomicU64::new(0),
        }
    }

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
        } else if let Ok(mut measurements) = self.measurements.try_lock() {
            measurements.push_back(measurement);
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
            "timestamp,sbe_parse_time_us,orderbook_update_time_us,arbitrage_detection_time_us,total_processing_time_us,updates_processed,symbol_id,symbol"
        )?;

        for m in measurements.iter() {
            let symbol_name = self
                .get_symbol_string(m.symbol_id)
                .unwrap_or_else(|| format!("ID_{}", m.symbol_id));
            writeln!(
                file,
                "{},{},{},{},{},{},{},{}",
                m.timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
                m.sbe_parse_time_us,
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
}

pub fn run_performance_test(
    api_key: String,
    symbols: Vec<String>,
    orderbook_manager: Arc<OrderBookManager>,
    detector: Arc<ArbitrageDetector>,
    duration_secs: u64,
    output_file: String,
    shutdown: Arc<AtomicBool>
) -> anyhow::Result<()> {
    let test_state = Arc::new(
        PerformanceTestState::new(
            Arc::clone(&orderbook_manager),
            Arc::clone(&detector),
            output_file,
            duration_secs,
            shutdown
        )
    );

    info!("Starting performance test for {} seconds", duration_secs);

    let client = BinanceSbeClient::new(api_key);
    let symbol_mappings = orderbook_manager.get_symbol_mappings();
    client.preload_symbol_mappings(&symbol_mappings);
    client.enable_timing();

    let detection_times: Arc<RwLock<FastHashMap<u16, Duration>>> = Arc::new(
        RwLock::new(FastHashMap::with_capacity(2048))
    );

    // let test_state_opp_clone = test_state.clone();
    // thread::spawn(move || {
    //     let mut counter = 0u64;
    //     loop {
    //         if test_state_opp_clone.is_complete() {
    //             break;
    //         }

    //         thread::sleep(Duration::from_millis(100));
    //         counter += 1;

    //         if counter % 10 == 0 {
    //             for symbol_id in 0..10u16 {
    //                 let opportunities =
    //                     test_state_opp_clone.detect_arbitrage_opportunities_by_id(symbol_id);
    //                 for opp in opportunities {
    //                     test_state_opp_clone.record_opportunity(opp);
    //                 }
    //             }
    //         }
    //     }
    // });

    orderbook_manager.register_update_callback(
        Arc::new({
            let detector_clone = detector.clone();
            let detection_times_clone = detection_times.clone();

            move |symbol_id: u16, _book: &crate::orderbook::orderbook::OrderBook| {
                let detection_start = Instant::now();
                detector_clone.process_symbol_update(symbol_id);
                let detection_time = detection_start.elapsed();

                let _ = detection_times_clone.write().insert(symbol_id, detection_time);
            }
        })
    );

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
            sbe_parse_time_us: u64
        | {
            if test_state_for_cb.is_complete() {
                return;
            }

            let orderbook_update_start = Instant::now();
            let _ = orderbook_manager_clone.apply_depth_update_pure(
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
                .copied()
                .unwrap_or_else(|| Duration::from_nanos(0));

            let total_processing_time =
                Duration::from_micros(sbe_parse_time_us) +
                orderbook_update_time +
                arbitrage_detection_time;

            test_state_for_cb.record_measurement(PerformanceMeasurement {
                timestamp: chrono::Local::now(),
                sbe_parse_time_us,
                orderbook_update_time_us: orderbook_update_time.as_micros() as u64,
                arbitrage_detection_time_us: arbitrage_detection_time.as_micros() as u64,
                total_processing_time_us: total_processing_time.as_micros() as u64,
                updates_processed: 1,
                symbol_id,
            });

            orderbook_manager_clone.trigger_arbitrage_detection(symbol_id);
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
        let _ = client.process_messages(&mut ws_stream, &test_state_for_ws.shutdown);
    });

    info!("Performance test running... Press Ctrl+C to stop early");

    let check_interval = Duration::from_millis(500);
    while !test_state.is_complete() {
        thread::sleep(check_interval);
    }

    test_state.stop();

    if let Err(e) = ws_handle.join() {
        warn!("Error joining WebSocket thread: {:?}", e);
    }

    info!("Performance test ({} seconds) completed", duration_secs);

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
