# Architecture

The application is a Binance Spot triangular-arbitrage engine with a synchronous Rust entry point, Tokio-backed REST operations, dedicated OS threads for market data and detection, and optional FIX execution.

## Runtime Flow

1. `src/main.rs` reads the command argument and loads `Config::from_env()`.
2. Logging is initialized with file and console layers.
3. A shared shutdown flag is installed for `Ctrl+C`.
4. The selected mode runs:
   - `Command::Run` -> `src/app/normal_mode.rs`
   - `Command::PerformanceTest` -> `src/app/perf_mode.rs`
   - `Command::TestExecution` -> `src/app/test_execution.rs`

## Normal Mode

Normal mode performs the full application workflow:

1. Validate startup configuration.
2. Create a `BinanceClient` and check `/api/v3/ping`.
3. Fetch active Binance Spot symbols from `/api/v3/exchangeInfo`.
4. Build a `SymbolMap` and find triangular paths that start and end with `TAA_BASE_ASSET`.
5. Intern selected symbols into the global symbol interner.
6. Create an `OrderBookManager`.
7. Create an `ArbitrageDetector`.
8. Create a FIX executor only when trading is enabled.
9. Start the detection, execution, and SBE WebSocket threads.
10. Run until shutdown, then disconnect FIX and join threads.

## Main Components

### Configuration

`src/config/mod.rs` owns all environment parsing and default values. The most important methods are:

- `Config::from_env()`: loads `.env` and parses all `TAA_*` values.
- `should_enable_real_trading()`: true only when trading is enabled and debug is false.
- `should_enable_mock_trading()`: true only when trading is enabled and debug is true.
- `get_fix_client_config()`: converts app config into FIX client config.

### Exchange Clients

`src/exchange/` contains all exchange-facing code:

- `client.rs`: shared `ExchangeClient` trait.
- `binance.rs`: Binance REST client for ping, exchange info, and depth snapshots.
- `sbe_client.rs`: Binance SBE WebSocket client and binary depth parser.
- `fix_api.rs`: real Binance FIX order-entry client.
- `mock_fix_api.rs`: mock execution client for safe local testing.
- `user_data_ws.rs`: order-status WebSocket support used by real execution when REST credentials are available.

### Symbol Map and Path Builder

`src/models/symbol_map.rs` indexes symbols by base asset, quote asset, and symbol name. It builds targeted triangular paths from symbols quoted in the base asset and filters excluded assets.

Each `TriangularPath` stores:

- The three exchange symbols.
- The trade direction for each leg.
- The start and end asset.

### Symbol Interner

`src/utils/symbol_interner.rs` maps symbol strings to compact `u16` IDs. This lets hot paths use fixed-size arrays instead of repeated string hashing.

### Order Books

`src/orderbook_manager/` owns market-data state:

- `orderbook.rs` stores a compact top-of-book depth structure with bid and ask levels.
- `manager.rs` maps symbol IDs to order books, applies SBE updates, triggers detection callbacks, and recovers out-of-sync books from REST snapshots.

The in-memory book currently stores up to 5 levels, even if `TAA_DEPTH` is configured higher.

### Arbitrage Detector

`src/arbitrage/detector.rs` receives symbol update notifications. For each changed symbol, it checks a bounded number of related triangular paths, calculates effective prices through the current order books, applies fee and precision handling, and emits an `ArbitrageOpportunity` when the resulting profit ratio is greater than the configured threshold.

### Execution

Execution code is split across:

- `src/utils/thread_ops.rs`: event-driven execution-thread registration.
- `src/utils/fix_ops.rs`: FIX client selection and three-leg execution sequence.
- `src/utils/order_ops.rs`: market order creation, precision handling, step-size adjustment, and exchange-filter validation.
- `src/utils/execution_guard.rs`: prevents concurrent arbitrage execution sequences.
- `src/utils/simulation_logger.rs`: simulates an opportunity before execution and logs simulated-vs-actual results.

When execution is enabled, the app places three market orders in sequence. The output amount from one leg becomes the input amount for the next leg.

## Threads

Normal mode uses these main threads:

| Thread | Responsibility |
| --- | --- |
| Main thread | Startup, shutdown flag, lifecycle coordination. |
| `arbitrage-detector` | Registers order-book update callback and invokes detector processing. |
| `execution` | Receives opportunities and runs execution callbacks without polling delay. |
| `websocket-thread` | Connects to Binance SBE, subscribes to depth streams, parses messages, and updates order books. |

The order-book manager also creates or uses a Tokio runtime for REST snapshot recovery tasks.

## Performance Mode

`perf-test` measures:

- SBE parse time.
- Order-book update time.
- Arbitrage detection time.
- Total processing time.

It writes CSV files to `performance_results/<timestamp>/`:

- `performance_results.csv`
- `arbitrage_opportunities.csv`

The Python analyzer generates charts in an `analysis/` directory beside the CSVs.

## Data Outputs

| Output | Location |
| --- | --- |
| Application logs | `logs/` by default, configurable through `TAA_LOG_DIRECTORY`. |
| Performance CSVs | `performance_results/<timestamp>/`. |
| Performance charts | `performance_results/<timestamp>/analysis/` after running `analyze_performance.py`. |
