# Rust Binance Triangular Arbitrage

A low-latency Rust application for discovering triangular arbitrage paths on Binance Spot markets, maintaining live order books from Binance SBE market data, and optionally executing three-leg market-order sequences through Binance FIX.

This project is built for experimentation, performance testing, and controlled trading research. Real trading is disabled unless `TAA_ENABLE_TRADING=true` and `TAA_DEBUG=false` are both set.

## What It Does

- Fetches active Binance Spot symbols and exchange filters from the REST API.
- Builds triangular paths that start and end with a configured base asset, usually `USDT`.
- Subscribes to Binance SBE depth updates for symbols used in those paths.
- Maintains compact in-memory order books and recovers books from REST snapshots when update sequences fall out of sync.
- Calculates path profitability after fees, depth, precision, and liquidity checks.
- Runs in simulation-only mode by default.
- Can run mock FIX execution for integration testing.
- Can execute real FIX market orders when explicitly configured for real trading.
- Records performance CSVs for market-data parsing, order-book updates, and detection timing.

## Important Safety Notice

This software can place real market orders when real trading is enabled. Market orders can lose money because of latency, slippage, fees, partial execution, stale order books, account limits, and exchange-side rejects.

Before enabling real trading, read [docs/TRADING_SAFETY.md](docs/TRADING_SAFETY.md). Start in simulation mode, then mock mode, then very small real amounts only after validating credentials, filters, logs, and account permissions.

## Requirements

- Rust stable toolchain with Cargo.
- Network access to Binance REST, SBE WebSocket, and FIX endpoints.
- Binance API key for market data and exchange information.
- Binance FIX API key and Ed25519 private key for real FIX execution.
- Python 3 with `pandas`, `matplotlib`, `seaborn`, and `numpy` only if you want to run performance analysis charts.

## Quick Start

1. Create a local `.env` file from the example:

```bash
cp .env.example .env
```

2. Edit `.env` and add your Binance keys. Keep `TAA_ENABLE_TRADING=false` for the first run.

3. Build the optimized binary:

```bash
cargo build --release
```

4. Run continuous detection in simulation mode:

```bash
cargo run --release
```

5. Stop with `Ctrl+C`. Logs are written to the configured log directory, `logs/` by default.

## Run Modes

The first command-line argument selects the application mode:

| Command | Mode | Purpose |
| --- | --- | --- |
| `cargo run --release` | Normal mode | Continuous market-data ingestion, order-book maintenance, arbitrage detection, and optional execution. |
| `cargo run --release -- perf-test` | Performance test | Runs for `TAA_PERFORMANCE_TEST_DURATION` seconds and writes timing CSVs to `performance_results/<timestamp>/`. |
| `cargo run --release -- test-exec` | Execution test | Builds a sample triangular path and tests the configured FIX execution path. Requires `TAA_ENABLE_TRADING=true`. |

Helper scripts:

```bash
./run_perf.sh
./analyze_performance.sh performance_results/<timestamp>/performance_results.csv
./simple_test.sh
```

## Trading Modes

| Settings | Result |
| --- | --- |
| `TAA_ENABLE_TRADING=false` | Simulation only. Opportunities are detected and logged; no orders are placed. |
| `TAA_ENABLE_TRADING=true` and `TAA_DEBUG=true` | Mock FIX mode. Orders are simulated through the local mock client. |
| `TAA_ENABLE_TRADING=true` and `TAA_DEBUG=false` | Real FIX mode. The app connects to Binance FIX and can place real market orders. |

## Documentation

- [Configuration](docs/CONFIGURATION.md): environment variables, defaults, and example settings.
- [Architecture](docs/ARCHITECTURE.md): runtime flow, modules, threads, and data model.
- [Operations](docs/OPERATIONS.md): build, run, logs, performance analysis, and troubleshooting.
- [Trading Safety](docs/TRADING_SAFETY.md): real-money risk controls and rollout checklist.

## Repository Layout

```text
src/
  app/                 Run modes: normal, performance test, execution test
  arbitrage/           Profit calculation and opportunity callbacks
  config/              Environment-driven configuration
  exchange/            Binance REST, SBE WebSocket, FIX, mock FIX, user-data WebSocket
  models/              Symbols, order-book levels, commands, triangular paths
  orderbook_manager/   Order-book state, snapshots, depth updates, recovery
  performance/         Performance measurement and CSV output
  utils/               Logging, validation, threading, execution, order helpers
```

## Disclaimer

This project is provided for technical and educational use. It is not financial advice, does not guarantee profit, and does not remove exchange, market, operational, or software risk.
