#!/bin/bash
# test_execution.sh
# Script to run the test execution mode for triangular arbitrage

echo "==================================================="
echo "  Triangular Arbitrage Test Execution Runner"
echo "==================================================="
echo

if [ ! -f .env ]; then
    echo "Error: .env file not found. Please create one with the required configuration."
    exit 1
fi

echo "[1/2] Building in release mode..."
cargo build --release
if [ $? -ne 0 ]; then
    echo "Error: Failed to build binary."
    exit 1
fi

echo
echo "[2/2] Running test execution..."
echo "This will execute hardcoded triangular arbitrage opportunities"
echo

RUSTFLAGS="-C target-cpu=apple-m1"
./target/release/rust-arb-optimized test-exec

echo
echo "==================================================="
echo "  Test execution completed!"
echo "==================================================="