#!/bin/bash
# run_performance_test.sh
# A script to run the triangular arbitrage performance test and analyze the results

# Display an informational header
echo "==================================================="
echo "  Triangular Arbitrage Performance Test Runner"
echo "==================================================="
echo

# Ensure the environment is set up
if [ ! -f .env ]; then
    echo "Error: .env file not found. Please create one with the required configuration."
    exit 1
fi

# Create results directory if it doesn't exist
mkdir -p performance_results

# Build the performance test binary in release mode
echo "[1/4] Building in release mode..."
cargo build --release
if [ $? -ne 0 ]; then
    echo "Error: Failed to build binary."
    exit 1
fi

# Run the performance test
echo
echo "[2/4] Running performance test (duration: 300 seconds)..."
echo "This will take approximately 5 minutes to complete."
echo

# Execute the performance test 
./target/release/rust-arb-optimized perf-test
if [ $? -ne 0 ]; then
    echo "Error: Performance test failed."
    exit 1
fi

# Find the most recent results file
RESULTS_FILE=$(find performance_results -name "performance_results.csv" -type f -print0 | xargs -0 ls -t | head -1)

if [ -z "$RESULTS_FILE" ]; then
    echo "Error: Could not find performance results file."
    exit 1
fi

echo
echo "[3/4] Performance test completed successfully."
echo "Results saved to: $RESULTS_FILE"

# Check if Python is available for analysis
if command -v python3 &> /dev/null; then
    echo
    echo "[4/4] Running performance analysis..."

    python -m venv analyze

    source .analyze/bin/activate
    
    # Check for required Python packages
    PIP_CMD="pip3"
    if ! command -v $PIP_CMD &> /dev/null; then
        PIP_CMD="pip"
    fi
    
    # Install required packages if needed
    PACKAGES="pandas matplotlib seaborn numpy"
    for pkg in $PACKAGES; do
        $PIP_CMD install $pkg -q
    done
    
    # Run the analysis script
    python3 analyze_performance.py "$RESULTS_FILE"
    
    echo
    echo "Analysis complete! Check the 'analysis' folder inside the results directory."
    
    deactivate
else
    echo
    echo "[4/4] Python not found. Skipping automatic analysis."
    echo "To analyze results manually, run: python3 analyze_performance.py $RESULTS_FILE"
fi

echo
echo "==================================================="
echo "  Performance test process completed!"
echo "==================================================="