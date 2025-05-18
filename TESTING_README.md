Triangular Arbitrage Performance Testing
This document explains how to run and interpret the performance tests for the triangular arbitrage system.
Overview
The performance test measures the efficiency of the arbitrage detection system by tracking:

WebSocket data reception time
Orderbook update processing time
Arbitrage detection time
Total end-to-end processing time
Number of updates processed
Number of arbitrage opportunities found
Profit percentages of opportunities

The test runs for exactly 2 minutes, after which results are saved to a CSV file for analysis.
Prerequisites

Rust toolchain (1.75+ recommended)
Environment file (.env) with required configuration
Python 3.7+ with pandas, matplotlib, seaborn, and numpy (for analysis)

Running the Performance Test
Option 1: Using the Built-in Command
The easiest way to run the performance test is to use the built-in command:
bash# Build the project
cargo build --release

# Run the performance test
./target/release/rust-arb-optimized perf-test
This will:

Initialize the system
Run the test for 2 minutes
Save results to a timestamped CSV file in the performance_results directory

Option 2: Using the Analysis Script
To analyze the test results, you can use the provided Python script:
bash# First run the performance test as above
./target/release/rust-arb-optimized perf-test

# Then analyze the most recent results
python3 analyze_performance.py ./performance_results/YYYYMMDD_HHMMSS/performance_results.csv
Option 3: Using the Convenience Script
For a complete workflow, you can use the provided shell script:
bash# Make script executable
chmod +x run_performance_test.sh

# Run the test and automatic analysis
./run_performance_test.sh
Understanding the Results
The performance test creates:

A CSV file with raw timing data
A set of visualization plots (if analysis script is run)

CSV Output Columns

timestamp: Exact time of the measurement
sbe_receive_time_us: Time to receive data from SBE socket (microseconds)
orderbook_update_time_us: Time to update the orderbook (microseconds)
arbitrage_detection_time_us: Time to detect arbitrage opportunities (microseconds)
total_processing_time_us: Total end-to-end processing time (microseconds)
updates_processed: Number of updates processed
opportunities_found: Number of arbitrage opportunities found
best_profit_percentage: Best profit percentage found (if any)
avg_profit_percentage: Average profit percentage (if multiple opportunities)
symbol: Symbol that triggered the update

Analysis Visualizations
The analysis script generates several visualization plots:

Timing Distribution: Box plots showing the distribution of processing times
Timing Histograms: Detailed histograms for each timing metric
Timing Over Time: Line graph showing how processing times change during the test
Updates Per Second: Number of updates processed each second
Opportunities Per Second: Number of arbitrage opportunities found each second
Profit Percentages: Scatter plot of profit percentages over time
Symbol Distribution: Bar chart showing the most frequently updated symbols

Performance Optimization Tips
When analyzing results, focus on:

99th Percentile Processing Time: This indicates worst-case performance
Updates Per Second: Higher is better for real-time processing
Orderbook Update Time: Often the bottleneck in the system
Arbitrage Detection Time: Should remain low even with many triangular paths

If performance is not satisfactory:

Reduce the number of symbols monitored
Reduce the depth of orderbooks
Limit the number of triangular paths
Consider higher-performance hardware
Profile specific functions for optimization opportunities

Interpreting Common Patterns

Increasing latency over time: May indicate memory growth or garbage collection issues
Periodic spikes: Could be related to system scheduling or background processes
Consistent high latency: May indicate fundamental algorithmic inefficiencies

Performance Targets
For production use, the system should aim for:

Average total processing time < 1000 microseconds (1 ms)
99th percentile total processing time < 10 ms
Updates per second > 100
Minimal variance in processing times

Meeting these targets ensures the system can detect arbitrage opportunities before market conditions change significantly.
Troubleshooting
If the performance test fails:

Check the .env file has all required configuration
Ensure your API key has sufficient permissions
Verify network connectivity to the exchange
Examine system resource usage (memory, CPU)
Check the logs for specific error messages

For help interpreting results, reference the detailed comments in the analysis script.