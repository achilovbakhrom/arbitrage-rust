use std::sync::Arc;
use std::time::{ Duration, Instant };
use std::fs::File;
use std::io::Write;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Mutex;
use std::thread;
use std::sync::atomic::{ AtomicBool, Ordering };
use tracing::{ error, info, warn };
use crate::exchange::sbe_client::BinanceSbeClient;
use crate::orderbook_manager::manager::OrderBookManager;
use crate::utils::types::FastHashMap;
use crate::utils::symbol_interner::global;
use parking_lot::RwLock;
use anyhow::Result;

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
    pub measurements: Arc<Mutex<VecDeque<PerformanceMeasurement>>>,
    pub opportunities: Arc<Mutex<VecDeque<ArbitrageOpportunity>>>,
    pub start_time: Instant,
    pub duration: Duration,
    pub output_file: String,
    pub opportunities_file: String,
    pub shutdown: Arc<AtomicBool>,
}

impl PerformanceTestState {
    pub fn new(output_file: String, duration_secs: u64, shutdown: Arc<AtomicBool>) -> Self {
        let opportunities_file = output_file.replace(
            "performance_results.csv",
            "arbitrage_opportunities.csv"
        );

        Self {
            measurements: Arc::new(Mutex::new(VecDeque::with_capacity(50000))),
            opportunities: Arc::new(Mutex::new(VecDeque::with_capacity(5000))),
            start_time: Instant::now(),
            duration: Duration::from_secs(duration_secs),
            output_file,
            opportunities_file,
            shutdown: shutdown,
        }
    }

    fn get_symbol_string(&self, symbol_id: u16) -> Option<String> {
        global::get_symbol(symbol_id).map(|s| s.to_string())
    }

    pub fn record_measurement(&self, measurement: PerformanceMeasurement) {
        if let Ok(mut measurements) = self.measurements.try_lock() {
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
    duration_secs: u64,
    output_file: String,
    shutdown: Arc<AtomicBool>,
    detection_times: Arc<RwLock<FastHashMap<u16, Duration>>>
) -> anyhow::Result<()> {
    let test_state = Arc::new(
        PerformanceTestState::new(output_file, duration_secs, Arc::clone(&shutdown))
    );

    info!("Starting performance test for {} seconds", duration_secs);

    let detection_times_for_cb = Arc::clone(&detection_times);
    let test_state_for_cb = Arc::clone(&test_state);

    let ws_handler = start_websocket_thread(
        api_key,
        symbols,
        Arc::clone(&orderbook_manager),
        shutdown,
        test_state_for_cb,
        detection_times_for_cb
    )?;

    let test_state_for_signal = Arc::clone(&test_state);
    ctrlc::set_handler(move || {
        info!("Received Ctrl+C, stopping performance test...");
        test_state_for_signal.stop();
    })?;

    info!("Performance test running... Press Ctrl+C to stop early");

    let check_interval = Duration::from_millis(500);
    while !test_state.is_complete() {
        thread::sleep(check_interval);
    }

    test_state.stop();

    if let Err(e) = ws_handler.join() {
        warn!("Error joining WebSocket thread: {:?}", e);
    }

    info!("Performance test ({} seconds) completed", duration_secs);

    test_state.save_results()?;

    Ok(())
}

pub fn start_websocket_thread(
    sbe_api_key: String,
    symbols: Vec<String>,
    orderbook_manager: Arc<OrderBookManager>,
    shutdown: Arc<AtomicBool>,
    test_state_for_cb: Arc<PerformanceTestState>,
    detection_times_for_cb: Arc<RwLock<FastHashMap<u16, Duration>>>
) -> Result<thread::JoinHandle<()>> {
    let handle = thread::Builder
        ::new()
        .name("websocket-thread".to_string())
        .spawn(move || {
            let client = BinanceSbeClient::new(sbe_api_key);
            client.enable_timing();

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
                    orderbook_manager.apply_depth_update_pure(
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

                    orderbook_manager.trigger_arbitrage_detection(symbol_id);
                }
            );

            let mut ws_stream = loop {
                if shutdown.load(Ordering::Relaxed) {
                    return;
                }

                match client.connect() {
                    Ok(stream) => {
                        break stream;
                    }
                    Err(err) => {
                        warn!("Connection failed: {}, retrying in 5 seconds...", err);
                        thread::sleep(Duration::from_secs(5));
                    }
                }
            };
            let channels = ["depth".to_string()];

            match client.subscribe(&mut ws_stream, &symbols, channels.iter().as_slice()) {
                Ok(_) => info!("Successfully subscribed to {} symbols", symbols.len()),
                Err(e) => {
                    error!("Failed to subscribe: {}", e);
                    return;
                }
            }

            if let Err(e) = client.process_messages(&mut ws_stream, &shutdown) {
                error!("WebSocket processing error: {}", e);
            }

            while !shutdown.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        })?;

    Ok(handle)
}
