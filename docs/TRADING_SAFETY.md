# Trading Safety

This application can place real Binance Spot market orders. Treat real mode as production trading infrastructure.

## Mode Gates

Real trading requires both:

```dotenv
TAA_ENABLE_TRADING=true
TAA_DEBUG=false
```

Any other combination avoids real FIX order placement:

- `TAA_ENABLE_TRADING=false`: detection and simulation only.
- `TAA_ENABLE_TRADING=true` with `TAA_DEBUG=true`: mock FIX execution only.

## Real Trading Risks

Triangular arbitrage can lose money even when the detector sees a profitable path. Common causes include:

- Market data latency between SBE depth updates and FIX execution.
- Slippage from market orders.
- Exchange fees differing from `TAA_FEE`.
- Partial fills or rejected orders.
- Symbol filter changes, minimum notional limits, and step sizes.
- Balance changes across the three legs.
- Out-of-sync order books.
- Network delay, reconnects, or local process stalls.
- Other traders consuming the same liquidity first.

## Suggested Rollout

1. Run normal mode with `TAA_ENABLE_TRADING=false`.
2. Confirm symbols, paths, thresholds, and logs look correct.
3. Run `perf-test` and review processing latency.
4. Run `test-exec` with `TAA_ENABLE_TRADING=true` and `TAA_DEBUG=true`.
5. Confirm mock order sequencing and precision handling.
6. Configure real FIX credentials, but keep `TAA_TRADE_AMOUNT` very small.
7. Run one controlled `test-exec` in real mode.
8. Review Binance account history and application logs.
9. Increase size only after repeated successful low-size runs.

## Operational Guardrails

Use conservative settings while testing:

```dotenv
TAA_TRADE_AMOUNT=10
TAA_THRESHOLD=0.002
TAA_MAX_TRIANGLES=5
TAA_LOG_LEVEL=debug
```

Prefer:

- A high enough threshold to cover fees and slippage.
- A small path count until the system is stable.
- API keys with the minimum permissions required.
- IP whitelisting for all trading keys.
- Separate keys for market data and execution when possible.
- Frequent log review after every real-mode run.

## Execution Behavior

When enabled, execution runs three market orders in sequence. The filled result from each leg determines the input amount for the next leg.

The execution path includes:

- A single-execution guard to avoid concurrent three-leg sequences.
- Precision and step-size formatting.
- Binance filter validation where order-book price estimates are available.
- Pre-execution simulation logging.
- Simulation-vs-actual result comparison logging.

These checks reduce operational mistakes but do not guarantee profit.

## Stop Procedure

Use `Ctrl+C` for graceful shutdown. The app sets the shutdown flag, stops background threads, and disconnects the FIX executor if present.

If you run scripts with timeouts or wrappers, confirm child processes are also stopped before starting another real-mode run.
