#!/bin/bash
# analyze_performance.sh
# A script to run the Python analysis script on performance results

# Check if the performance results file was provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <performance_results.csv>"
    exit 1
fi

RESULTS_FILE=$1

# Check if the file exists
if [ ! -f "$RESULTS_FILE" ]; then
    echo "Error: Results file $RESULTS_FILE not found."
    exit 1
fi

# Check if Python is available
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
else
    echo "Python not found. Please install Python to run the analysis."
    exit 1
fi

# Check for required Python packages
$PYTHON_CMD << EOF
try:
    import pandas
    import matplotlib
    import seaborn
    import numpy
    print("All required packages are installed.")
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Installing required packages...")
    import sys
    sys.exit(1)
EOF

# If packages are missing, try to install them
if [ $? -ne 0 ]; then
    echo "Attempting to install required packages..."
    
    # Check if pip is available
    if command -v pip3 &> /dev/null; then
        PIP_CMD="pip3"
    elif command -v pip &> /dev/null; then
        PIP_CMD="pip"
    else
        echo "Error: pip not found. Please install pip to install the required packages."
        exit 1
    fi
    
    # Install the required packages
    $PIP_CMD install pandas matplotlib seaborn numpy
    
    # Check if installation was successful
    if [ $? -ne 0 ]; then
        echo "Error: Failed to install the required packages."
        exit 1
    fi
fi

# Run the analysis script
echo "Running performance analysis on $RESULTS_FILE..."
$PYTHON_CMD analyze_performance.py "$RESULTS_FILE"

# Check if analysis was successful
if [ $? -eq 0 ]; then
    echo "Analysis completed successfully."
else
    echo "Error: Analysis failed."
    exit 1
fi