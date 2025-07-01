#!/bin/bash
# simple_test.sh
# Simple script to test single triangular arbitrage path

echo "==================================================="
echo "  Single Path FIX API Test"
echo "==================================================="
echo

if [ ! -f .env ]; then
    echo "Error: .env file not found. Please create one with the required configuration."
    exit 1
fi

echo "Testing single triangular arbitrage path: BTC/USDT → ETH/BTC → ETH/USDT"
echo "This will verify if your FIX API configuration is working correctly."
echo

echo "[1/2] Building in release mode..."
cargo build
if [ $? -ne 0 ]; then
    echo "Error: Failed to build binary."
    exit 1
fi

echo
echo "[2/2] Running single path test..."
echo

RUSTFLAGS="-C target-cpu=apple-m1"
./target/debug/rust-arb-optimized test-exec

if [ $? -eq 0 ]; then
    echo
    echo "✅ Test completed successfully!"
    echo "Your FIX API configuration is working."
else
    echo
    echo "❌ Test failed!"
    echo "Check your FIX API configuration in .env file."
fi

echo
echo "==================================================="
echo "  Single path test completed!"
echo "==================================================="