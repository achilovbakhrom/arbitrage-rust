use std::sync::{ Arc, atomic::{ AtomicBool, Ordering } };
use std::thread::{ self, JoinHandle };
use std::time::Duration;
use tracing::{ info, error };
use crate::arbitrage::detector::{ ArbitrageDetector, ArbitrageOpportunity, ExecutionCallback };
use crate::orderbook_manager::manager::OrderBookManager;

pub fn start_detection_thread(
    detector: Arc<ArbitrageDetector>,
    orderbook_manager: Arc<OrderBookManager>,
    shutdown: Arc<AtomicBool>
) -> JoinHandle<()> {
    thread::Builder
        ::new()
        .name("arbitrage-detector".to_string())
        .spawn(move || {
            info!("Arbitrage detection thread started");

            let update_callback = {
                let detector = Arc::clone(&detector);
                Arc::new(
                    move |symbol_id: u16, _book: &crate::orderbook_manager::orderbook::OrderBook| {
                        detector.process_symbol_update(symbol_id);
                    }
                )
            };

            orderbook_manager.register_update_callback(update_callback);

            while !shutdown.load(Ordering::Relaxed) && !detector.is_shutdown() {
                thread::sleep(Duration::from_millis(100));
            }

            info!("Arbitrage detection thread stopped");
        })
        .expect("Failed to spawn arbitrage detection thread")
}

pub fn start_execution_thread(
    detector: Arc<ArbitrageDetector>,
    execution_callback: Option<ExecutionCallback>,
    shutdown: Arc<AtomicBool>
) -> JoinHandle<()> {
    thread::Builder
        ::new()
        .name("execution".to_string())
        .spawn(move || {
            if let Some(callback) = execution_callback {
                detector.register_execution_callback(
                    Arc::new(move |opportunity: ArbitrageOpportunity| {
                        info!(
                            "Execution opportunity: {} with {:.4}% profit",
                            opportunity.display(),
                            opportunity.profit_percentage()
                        );
                        callback(opportunity);
                    })
                );

                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_secs(1));
                }
            }
        })
        .expect("Failed to spawn execution thread")
}

pub struct ThreadManager {
    handles: Vec<JoinHandle<()>>,
}

impl ThreadManager {
    pub fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    pub fn add_handle(&mut self, handle: JoinHandle<()>) {
        self.handles.push(handle);
    }

    pub fn join_all(self) {
        for handle in self.handles {
            if let Err(e) = handle.join() {
                error!("Error joining thread: {:?}", e);
            }
        }
    }
}
