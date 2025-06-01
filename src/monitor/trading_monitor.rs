// src/monitor/trading_monitor.rs
use std::sync::{ Arc, Mutex };
use std::collections::VecDeque;
use std::time::{ Duration, Instant, SystemTime, UNIX_EPOCH };
use serde::{ Serialize, Deserialize };
use tracing::{ info, warn, error };
use std::fs::File;
use std::io::Write;

use crate::executor::fix_executor::{ TriangularExecutionResult, ExecutionStats };
use crate::arbitrage::detector::ArbitrageOpportunity;

/// Trade execution record for monitoring and analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub timestamp: u64, // Unix timestamp in milliseconds
    pub execution_id: u64,
    pub symbol_path: String, // e.g., "BTCUSDT->ETHBTC->ETHUSDT"
    pub expected_profit_percentage: f64,
    pub actual_profit_amount: f64,
    pub execution_time_us: u64,
    pub trades_completed: u8,
    pub success: bool,
    pub slippage_percentage: f64,
    pub error_message: Option<String>,
}

/// Real-time trading statistics
#[derive(Debug, Clone)]
pub struct TradingStats {
    pub total_executions: u64,
    pub successful_executions: u64,
    pub total_profit: f64,
    pub average_execution_time_us: u64,
    pub success_rate: f64,
    pub average_profit_per_trade: f64,
    pub last_execution_time: Option<SystemTime>,
}

/// Configuration for trading monitor
#[derive(Debug, Clone)]
pub struct MonitorConfig {
    pub max_records: usize,
    pub auto_save_interval: Duration,
    pub auto_save_path: Option<String>,
    pub alert_on_failure: bool,
    pub alert_on_low_success_rate: f64, // Alert if success rate drops below this
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            max_records: 10000,
            auto_save_interval: Duration::from_secs(300), // 5 minutes
            auto_save_path: Some("trading_records.json".to_string()),
            alert_on_failure: true,
            alert_on_low_success_rate: 0.7, // 70%
        }
    }
}

/// Comprehensive trading execution monitor
pub struct TradingMonitor {
    config: MonitorConfig,
    records: Arc<Mutex<VecDeque<TradeRecord>>>,
    stats: Arc<Mutex<TradingStats>>,
    last_save_time: Arc<Mutex<Instant>>,
    execution_counter: Arc<std::sync::atomic::AtomicU64>,
}

impl TradingMonitor {
    /// Create a new trading monitor
    pub fn new(config: MonitorConfig) -> Self {
        Self {
            config: config.clone(),
            records: Arc::new(Mutex::new(VecDeque::with_capacity(config.max_records))),
            stats: Arc::new(
                Mutex::new(TradingStats {
                    total_executions: 0,
                    successful_executions: 0,
                    total_profit: 0.0,
                    average_execution_time_us: 0,
                    success_rate: 0.0,
                    average_profit_per_trade: 0.0,
                    last_execution_time: None,
                })
            ),
            last_save_time: Arc::new(Mutex::new(Instant::now())),
            execution_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Record a trading execution result
    pub fn record_execution(
        &self,
        opportunity: &ArbitrageOpportunity,
        result: &TriangularExecutionResult
    ) {
        let execution_id = self.execution_counter.fetch_add(
            1,
            std::sync::atomic::Ordering::Relaxed
        );
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Create symbol path string
        let symbol_path = format!(
            "{}->{}->{}",
            opportunity.path.first_symbol,
            opportunity.path.second_symbol,
            opportunity.path.third_symbol
        );

        // Calculate slippage (simplified)
        let expected_profit = opportunity.end_amount - opportunity.start_amount;
        let slippage_percentage = if expected_profit > 0.0 {
            ((expected_profit - result.actual_profit) / expected_profit) * 100.0
        } else {
            0.0
        };

        // Create trade record
        let record = TradeRecord {
            timestamp,
            execution_id,
            symbol_path: symbol_path.clone(),
            expected_profit_percentage: opportunity.profit_percentage(),
            actual_profit_amount: result.actual_profit,
            execution_time_us: result.execution_time_us,
            trades_completed: result.trades_executed,
            success: result.success,
            slippage_percentage,
            error_message: result.error.clone(),
        };

        // Add to records
        {
            let mut records = self.records.lock().unwrap();
            records.push_back(record.clone());

            // Trim if too many records
            while records.len() > self.config.max_records {
                records.pop_front();
            }
        }

        // Update statistics
        self.update_stats(&record);

        // Log the execution
        if result.success {
            info!(
                "Trade executed: {} | Profit: ${:.4} ({:.4}%) | Time: {}μs | Slippage: {:.3}%",
                symbol_path,
                result.actual_profit,
                opportunity.profit_percentage(),
                result.execution_time_us,
                slippage_percentage
            );
        } else {
            warn!(
                "Trade failed: {} | Expected: {:.4}% | Completed: {}/{} trades | Error: {:?}",
                symbol_path,
                opportunity.profit_percentage(),
                result.trades_executed,
                3,
                result.error
            );

            if self.config.alert_on_failure {
                self.send_failure_alert(&record);
            }
        }

        // Auto-save if needed
        self.maybe_auto_save();
    }

    /// Update internal statistics
    fn update_stats(&self, record: &TradeRecord) {
        let mut stats = self.stats.lock().unwrap();

        stats.total_executions += 1;
        if record.success {
            stats.successful_executions += 1;
            stats.total_profit += record.actual_profit_amount;
        }

        stats.success_rate = (stats.successful_executions as f64) / (stats.total_executions as f64);
        stats.average_profit_per_trade = if stats.successful_executions > 0 {
            stats.total_profit / (stats.successful_executions as f64)
        } else {
            0.0
        };

        // Update average execution time
        let total_time =
            stats.average_execution_time_us * (stats.total_executions - 1) +
            record.execution_time_us;
        stats.average_execution_time_us = total_time / stats.total_executions;

        stats.last_execution_time = Some(SystemTime::now());

        // Check for alerts
        if
            stats.success_rate < self.config.alert_on_low_success_rate &&
            stats.total_executions >= 10
        {
            warn!(
                "Low success rate alert: {:.2}% ({}/{})",
                stats.success_rate * 100.0,
                stats.successful_executions,
                stats.total_executions
            );
        }
    }

    /// Get current trading statistics
    pub fn get_stats(&self) -> TradingStats {
        self.stats.lock().unwrap().clone()
    }

    /// Get recent trade records
    pub fn get_recent_records(&self, count: usize) -> Vec<TradeRecord> {
        let records = self.records.lock().unwrap();
        records.iter().rev().take(count).cloned().collect()
    }

    /// Get all trade records
    pub fn get_all_records(&self) -> Vec<TradeRecord> {
        let records = self.records.lock().unwrap();
        records.iter().cloned().collect()
    }

    /// Save records to file
    pub fn save_to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let records = self.get_all_records();
        let json = serde_json::to_string_pretty(&records)?;

        let mut file = File::create(path)?;
        file.write_all(json.as_bytes())?;

        info!("Saved {} trade records to {}", records.len(), path);
        Ok(())
    }

    /// Load records from file
    pub fn load_from_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let loaded_records: Vec<TradeRecord> = serde_json::from_str(&content)?;

        let mut records = self.records.lock().unwrap();
        records.clear();
        for record in loaded_records {
            records.push_back(record);
        }

        info!("Loaded {} trade records from {}", records.len(), path);
        Ok(())
    }

    /// Auto-save if interval has passed
    fn maybe_auto_save(&self) {
        if let Some(ref path) = self.config.auto_save_path {
            let mut last_save = self.last_save_time.lock().unwrap();
            if last_save.elapsed() >= self.config.auto_save_interval {
                if let Err(e) = self.save_to_file(path) {
                    error!("Auto-save failed: {}", e);
                } else {
                    *last_save = Instant::now();
                }
            }
        }
    }

    /// Send failure alert (placeholder for actual alerting system)
    fn send_failure_alert(&self, record: &TradeRecord) {
        error!(
            "TRADING FAILURE ALERT: {} | Expected: {:.4}% | Trades: {}/3 | Error: {:?}",
            record.symbol_path,
            record.expected_profit_percentage,
            record.trades_completed,
            record.error_message
        );

        // Here you could integrate with:
        // - Slack/Discord webhooks
        // - Email notifications
        // - SMS alerts
        // - Monitoring systems (DataDog, New Relic, etc.)
    }

    /// Generate performance report
    pub fn generate_report(&self, duration: Duration) -> TradingReport {
        let stats = self.get_stats();
        let records = self.get_all_records();

        // Filter records by time
        let cutoff_time =
            (SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64) -
            (duration.as_millis() as u64);

        let recent_records: Vec<_> = records
            .into_iter()
            .filter(|r| r.timestamp >= cutoff_time)
            .collect();

        TradingReport::new(stats, recent_records, duration)
    }

    /// Print live statistics to console
    pub fn print_live_stats(&self) {
        let stats = self.get_stats();
        let recent_records = self.get_recent_records(10);

        println!("\n═══════════════════════════════════════════════════════════");
        println!("📊 LIVE TRADING STATISTICS");
        println!("═══════════════════════════════════════════════════════════");
        println!("Total Executions: {}", stats.total_executions);
        println!(
            "Successful: {} ({:.2}%)",
            stats.successful_executions,
            stats.success_rate * 100.0
        );
        println!("Total Profit: ${:.4}", stats.total_profit);
        println!("Avg Profit/Trade: ${:.4}", stats.average_profit_per_trade);
        println!("Avg Execution Time: {}μs", stats.average_execution_time_us);

        if let Some(last_time) = stats.last_execution_time {
            let elapsed = SystemTime::now().duration_since(last_time).unwrap_or_default();
            println!("Last Execution: {:.1}s ago", elapsed.as_secs_f64());
        }

        if !recent_records.is_empty() {
            println!("\n📋 RECENT TRADES:");
            for (i, record) in recent_records.iter().take(5).enumerate() {
                let status = if record.success { "✅" } else { "❌" };
                println!(
                    "  {}. {} {} | ${:.4} | {}μs",
                    i + 1,
                    status,
                    record.symbol_path,
                    record.actual_profit_amount,
                    record.execution_time_us
                );
            }
        }
        println!("═══════════════════════════════════════════════════════════\n");
    }
}

/// Comprehensive trading report
#[derive(Debug)]
pub struct TradingReport {
    pub stats: TradingStats,
    pub records: Vec<TradeRecord>,
    pub duration: Duration,
    pub profit_distribution: Vec<f64>,
    pub execution_time_distribution: Vec<u64>,
    pub most_profitable_path: Option<String>,
    pub most_executed_path: Option<String>,
}

impl TradingReport {
    fn new(stats: TradingStats, records: Vec<TradeRecord>, duration: Duration) -> Self {
        let profit_distribution: Vec<f64> = records
            .iter()
            .map(|r| r.actual_profit_amount)
            .collect();

        let execution_time_distribution: Vec<u64> = records
            .iter()
            .map(|r| r.execution_time_us)
            .collect();

        // Find most profitable path
        let mut path_profits: std::collections::HashMap<
            String,
            f64
        > = std::collections::HashMap::new();
        let mut path_counts: std::collections::HashMap<
            String,
            u32
        > = std::collections::HashMap::new();

        for record in &records {
            if record.success {
                *path_profits.entry(record.symbol_path.clone()).or_insert(0.0) +=
                    record.actual_profit_amount;
            }
            *path_counts.entry(record.symbol_path.clone()).or_insert(0) += 1;
        }

        let most_profitable_path = path_profits
            .into_iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(path, _)| path);

        let most_executed_path = path_counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(path, _)| path);

        Self {
            stats,
            records,
            duration,
            profit_distribution,
            execution_time_distribution,
            most_profitable_path,
            most_executed_path,
        }
    }

    /// Print detailed report
    pub fn print(&self) {
        println!("\n🔍 TRADING PERFORMANCE REPORT");
        println!("Duration: {:.1} minutes", self.duration.as_secs_f64() / 60.0);
        println!("───────────────────────────────────────────────────────────");

        println!("📈 EXECUTION SUMMARY:");
        println!("  Total Executions: {}", self.stats.total_executions);
        println!(
            "  Successful: {} ({:.2}%)",
            self.stats.successful_executions,
            self.stats.success_rate * 100.0
        );
        println!("  Failed: {}", self.stats.total_executions - self.stats.successful_executions);

        println!("\n💰 PROFIT ANALYSIS:");
        println!("  Total Profit: ${:.4}", self.stats.total_profit);
        println!("  Average per Trade: ${:.4}", self.stats.average_profit_per_trade);

        if !self.profit_distribution.is_empty() {
            let min_profit = self.profit_distribution.iter().fold(f64::INFINITY, |a, &b| a.min(b));
            let max_profit = self.profit_distribution
                .iter()
                .fold(f64::NEG_INFINITY, |a, &b| a.max(b));
            println!("  Min Profit: ${:.4}", min_profit);
            println!("  Max Profit: ${:.4}", max_profit);
        }

        println!("\n⏱️  TIMING ANALYSIS:");
        println!("  Average Execution Time: {}μs", self.stats.average_execution_time_us);

        if !self.execution_time_distribution.is_empty() {
            let min_time = *self.execution_time_distribution.iter().min().unwrap_or(&0);
            let max_time = *self.execution_time_distribution.iter().max().unwrap_or(&0);
            println!("  Fastest Execution: {}μs", min_time);
            println!("  Slowest Execution: {}μs", max_time);
        }

        if let Some(ref path) = self.most_profitable_path {
            println!("\n🏆 BEST PERFORMING PATH:");
            println!("  {}", path);
        }

        if let Some(ref path) = self.most_executed_path {
            println!("\n🔄 MOST ACTIVE PATH:");
            println!("  {}", path);
        }

        println!("───────────────────────────────────────────────────────────\n");
    }
}
