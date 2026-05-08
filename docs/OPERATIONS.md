# Operations

This page covers building, running, observing, and troubleshooting the application.

## Build

Development build:

```bash
cargo build
```

Optimized build:

```bash
cargo build --release
```

The release profile enables high optimization, link-time optimization, one codegen unit, stripped binaries, and abort-on-panic behavior.

## Run Continuous Detection

```bash
cargo run --release
```

This starts normal mode. With `TAA_ENABLE_TRADING=false`, the app only detects and logs opportunities. Stop with `Ctrl+C`.

## Run a Performance Test

```bash
cargo run --release -- perf-test
```

Or use the helper:

```bash
./run_perf.sh
```

Results are written to:

```text
performance_results/<timestamp>/performance_results.csv
performance_results/<timestamp>/arbitrage_opportunities.csv
```

Analyze results:

```bash
./analyze_performance.sh performance_results/<timestamp>/performance_results.csv
```

The analyzer creates PNG charts under:

```text
performance_results/<timestamp>/analysis/
```

## Run a FIX Execution Test

```bash
cargo run --release -- test-exec
```

Or use:

```bash
./simple_test.sh
```

`test-exec` requires `TAA_ENABLE_TRADING=true`. With `TAA_DEBUG=true`, it uses the mock FIX client. With `TAA_DEBUG=false`, it connects to real Binance FIX and can execute real orders.

## Logs

By default, logs are written to:

```text
logs/triangular_arbitrage_<date>.log
```

Control logging with:

```dotenv
TAA_LOG_LEVEL=info
TAA_LOG_DIRECTORY=logs
TAA_LOG_FILENAME_PREFIX=triangular_arbitrage
TAA_LOG_ROTATION=daily
TAA_LOG_MAX_FILES=10
```

Use `TAA_LOG_LEVEL=debug` while setting up credentials, FIX connectivity, and path selection. Use `info` for routine runs.

## Environment Loading

The app loads `.env` automatically. You can also export variables manually:

```bash
export $(grep -v '^#' .env | xargs)
```

Keep `.env` private. It can contain API keys and private key material.

## Common Checks

Before normal mode:

```bash
cargo build --release
cargo run --release
```

Before mock execution:

```dotenv
TAA_ENABLE_TRADING=true
TAA_DEBUG=true
```

Before real execution:

```dotenv
TAA_ENABLE_TRADING=true
TAA_DEBUG=false
TAA_TRADE_AMOUNT=10
TAA_THRESHOLD=0.002
```

Confirm:

- API key is active.
- IP restrictions allow the machine running the app.
- Spot trading is enabled for the key.
- FIX API access is enabled.
- `TAA_FIX_PRIVATE_KEY_BASE64` matches the FIX key registered with Binance.
- Account has enough balances for all three legs.

## Troubleshooting

### `SBE API key is required`

Set `TAA_SBE_API_KEY` in `.env`.

### `FIX API key is required`

Set `TAA_FIX_API` in `.env`.

### No triangular paths found

Check:

- `TAA_BASE_ASSET` exists as a quote asset on Binance, for example `USDT`.
- `TAA_EXCLUDED_COINS` is not filtering too much.
- `TAA_MAX_TRIANGLES` is greater than zero.
- Binance REST `/api/v3/exchangeInfo` is reachable.

### WebSocket connection failures

Check network access to `wss://stream-sbe.binance.com:9443/ws`, API key validity, and local firewall or proxy settings. The WebSocket thread retries with backoff.

### Repeated order-book recovery

This usually means SBE update sequences are missing or stale. The manager recovers with REST depth snapshots and rate limits those snapshot requests.

### Real FIX logon fails

Check:

- `TAA_FIX_ENDPOINT`.
- `TAA_FIX_TARGET_COMP_ID`.
- `TAA_FIX_API`.
- `TAA_FIX_PRIVATE_KEY_BASE64`.
- API key permissions and IP whitelist.
- System clock accuracy.

### Orders rejected by filters

The execution path validates Binance symbol filters before placing orders. Rejections can happen when trade sizes are below `minQty`, below `minNotional`, above `maxQty`, above `maxNotional`, or not aligned to `stepSize`.

Increase `TAA_TRADE_AMOUNT` carefully, or choose a base asset and path set with enough notional value.

## Maintenance Commands

Run Clippy with the repository's performance-oriented lint set:

```bash
cargo clippy -- \
  -W clippy::perf \
  -W clippy::large_enum_variant \
  -W clippy::inefficient_to_string \
  -W clippy::clone_on_ref_ptr \
  -W clippy::redundant_allocation \
  -W clippy::vec_box \
  -W clippy::box_collection \
  -W clippy::rc_buffer
```
