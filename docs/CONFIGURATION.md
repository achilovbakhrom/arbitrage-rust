# Configuration

Configuration is loaded from environment variables. A local `.env` file is supported through `dotenv`, and `.env` is intentionally ignored by git.

Start from:

```bash
cp .env.example .env
```

## Required Variables

| Variable | Required When | Description |
| --- | --- | --- |
| `TAA_SBE_API_KEY` | All run modes | Binance API key used for the SBE WebSocket connection. |
| `TAA_FIX_API` | All run modes | Binance API key used by the Binance REST client and FIX client configuration. |
| `TAA_FIX_PRIVATE_KEY_BASE64` | Real trading only | Base64 encoded Ed25519 private key or base64 encoded PEM/PKCS#8 private key material for FIX logon signing. |

`test-exec` also requires `TAA_ENABLE_TRADING=true` because that mode exercises the FIX execution path.

## Core Strategy Settings

| Variable | Default | Description |
| --- | --- | --- |
| `TAA_BASE_ASSET` | `USDT` | Asset the triangular path starts and ends with. The path builder searches symbols quoted in this asset first. |
| `TAA_EXCLUDED_COINS` | `USD,EUR,GBP,JPY,AUD,CAD,CHF,CNY,RUB,TRY` | Comma-separated assets to exclude from intermediate legs, except the configured base asset. |
| `TAA_MAX_TRIANGLES` | `15` | Maximum triangular paths selected for subscription and detection. If more paths exist, the app randomly truncates to this number. |
| `TAA_THRESHOLD` | `0.5` | Minimum profit ratio delta. For example, `0.002` means 0.2 percent. The code default is `0.5`, which means 50 percent. |
| `TAA_TRADE_AMOUNT` | `100.0` | Starting amount in the base asset used for simulation and execution sizing. |
| `TAA_FEE` | `0.001` | Per-trade fee rate used in profitability simulation. `0.001` means 0.1 percent. |
| `TAA_DEPTH` | `20` | Requested order-book depth setting. The current in-memory book stores up to 5 levels internally. |
| `TAA_MIN_VOLUME_MULTIPLIER` | `2.0` | Configured liquidity multiplier. The current detection path primarily uses effective price and sufficient-liquidity checks. |

## Trading Settings

| Variable | Default | Description |
| --- | --- | --- |
| `TAA_ENABLE_TRADING` | `false` | Enables execution plumbing. When false, the app detects opportunities but does not place orders. |
| `TAA_DEBUG` | `false` | Chooses mock execution when trading is enabled. `true` means mock FIX, `false` means real FIX. |
| `TAA_MAX_SLIPPAGE_PERCENTAGE` | `0.5` | Configured slippage limit. Current execution logs simulation-vs-execution differences and validates exchange filters. |
| `TAA_MIN_EXECUTION_PROFIT_THRESHOLD` | `0.1` | Configured execution profit threshold. Use this as an operational policy alongside `TAA_THRESHOLD`. |

## FIX API Settings

| Variable | Default | Description |
| --- | --- | --- |
| `TAA_FIX_TARGET_COMP_ID` | `SPOT` | FIX target component ID. |
| `TAA_FIX_HEARTBEAT_INTERVAL` | `30` | FIX heartbeat interval in seconds. |
| `TAA_FIX_ENDPOINT` | `fix-oe.binance.com:9000` | Binance FIX order-entry endpoint. |
| `TAA_API_KEY` | empty | REST API key used for order status checking. Optional but recommended for real execution. |
| `TAA_API_SECRET` | empty | REST API secret used for order status checking. Optional but recommended for real execution. |

## Logging Settings

| Variable | Default | Description |
| --- | --- | --- |
| `TAA_LOG_LEVEL` | `info` | One of `trace`, `debug`, `info`, `warn`, or `error`. |
| `TAA_LOG_DIRECTORY` | `logs` | Directory for rolling log files. |
| `TAA_LOG_FILENAME_PREFIX` | `triangular_arbitrage` | Prefix used for log filenames. |
| `TAA_LOG_ROTATION` | `daily` | `hourly`, `daily`, or `never`. |
| `TAA_LOG_MAX_FILES` | unset | Optional number of recent log files to retain. |

## Performance Test Settings

| Variable | Default | Description |
| --- | --- | --- |
| `TAA_PERFORMANCE_TEST_DURATION` | `300` | Duration in seconds for `perf-test`. Results are written under `performance_results/<timestamp>/`. |

## Example Profiles

Simulation-only detection:

```dotenv
TAA_ENABLE_TRADING=false
TAA_DEBUG=true
TAA_BASE_ASSET=USDT
TAA_TRADE_AMOUNT=100
TAA_THRESHOLD=0.002
```

Mock FIX execution:

```dotenv
TAA_ENABLE_TRADING=true
TAA_DEBUG=true
TAA_TRADE_AMOUNT=25
TAA_THRESHOLD=0.002
```

Real FIX execution:

```dotenv
TAA_ENABLE_TRADING=true
TAA_DEBUG=false
TAA_TRADE_AMOUNT=10
TAA_THRESHOLD=0.002
TAA_FIX_PRIVATE_KEY_BASE64=base64_encoded_key_material
```

Use very small trade amounts for real execution until you have verified the full operational path.
