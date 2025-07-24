use std::sync::{ Arc, atomic::{ AtomicBool, Ordering } };
use std::sync::mpsc;
use std::thread::{ self, JoinHandle };
use std::time::Duration;
use tracing::{ info, error, debug };
use crate::arbitrage::detector::{ ArbitrageDetector, ArbitrageOpportunity, ExecutionCallback };
use crate::orderbook_manager::manager::OrderBookManager;

pub fn start_detection_thread(
    detector: Arc<ArbitrageDetector>,
    orderbook_manager: Arc<OrderBookManager>,
    shutdown: Arc<AtomicBool>
) -> JoinHandle<()> {
    let shutdown_clone = Arc::clone(&shutdown);
    thread::Builder
        ::new()
        .name("arbitrage-detector".to_string())
        .spawn(move || {
            info!("Arbitrage detection thread started");

            let update_callback = {
                let detector = Arc::clone(&detector);
                Arc::new(
                    move |symbol_id: u16, _book: &crate::orderbook_manager::orderbook::OrderBook| {
                        if shutdown_clone.load(Ordering::Relaxed) {
                            return;
                        }
                        detector.process_symbol_update(symbol_id);
                    }
                )
            };

            orderbook_manager.register_update_callback(update_callback);

            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(1000));
            }

            debug!("Arbitrage detection thread stopped");
        })
        .expect("Failed to spawn arbitrage detection thread")
}

/// Optimized event-driven execution thread that eliminates polling delays
pub fn start_execution_thread(
    detector: Arc<ArbitrageDetector>,
    execution_callback: Option<ExecutionCallback>,
    shutdown: Arc<AtomicBool>
) -> JoinHandle<()> {
    thread::Builder
        ::new()
        .name("execution".to_string())
        .spawn(move || {
            debug!("🚀 OPTIMIZED: Event-driven execution thread started");

            if let Some(callback) = execution_callback {
                // Create a channel for immediate opportunity notifications
                let (tx, rx) = mpsc::channel::<ArbitrageOpportunity>();

                let wrapped_callback = {
                    let shutdown_ref = Arc::clone(&shutdown);
                    Arc::new(move |opportunity: ArbitrageOpportunity| {
                        if shutdown_ref.load(Ordering::Relaxed) {
                            debug!("Skipping execution due to shutdown");
                            return;
                        }

                        // Send opportunity through channel for immediate processing
                        if let Err(_) = tx.send(opportunity) {
                            debug!("Execution channel closed, stopping callback");
                        }
                    })
                };

                detector.register_execution_callback(wrapped_callback);

                // Event-driven loop - no sleep delays!
                loop {
                    // Use select-like behavior with timeout for shutdown check
                    match rx.recv_timeout(Duration::from_millis(10)) {
                        Ok(opportunity) => {
                            // Process opportunity immediately - zero delay!
                            info!(
                                "⚡ FAST EXECUTION: {} with {:.4}% profit (no polling delay)",
                                opportunity.display(),
                                opportunity.profit_percentage()
                            );
                            callback(opportunity);
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            // Check shutdown without blocking execution
                            if shutdown.load(Ordering::Relaxed) {
                                break;
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            debug!("Execution channel disconnected");
                            break;
                        }
                    }
                }
            } else {
                // If no callback, just wait for shutdown
                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(100));
                }
            }

            debug!("Execution thread stopped");
        })
        .expect("Failed to spawn execution thread")
}

pub struct ThreadManager {
    handles: Vec<(String, JoinHandle<()>)>,
}

impl ThreadManager {
    pub fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    pub fn add_handle(&mut self, handle: JoinHandle<()>) {
        let thread_name = handle.thread().name().unwrap_or("unnamed").to_string();
        self.handles.push((thread_name, handle));
    }

    pub fn join_all(self) {
        info!("Waiting for {} threads to complete...", self.handles.len());

        for (name, handle) in self.handles {
            debug!("Waiting for thread '{}' to complete", name);

            match handle.join() {
                Ok(_) => debug!("Thread '{}' completed successfully", name),
                Err(e) => error!("Thread '{}' panicked: {:?}", name, e),
            }
        }

        info!("All threads completed");
    }
}
