# set -e  # Exit on any error

# # Colors for output
# RED='\033[0;31m'
# GREEN='\033[0;32m'
# YELLOW='\033[1;33m'
# BLUE='\033[0;34m'
# NC='\033[0m' # No Color

# # Function to print colored output
# print_status() {
#     echo -e "${BLUE}[INFO]${NC} $1"
# }

# print_success() {
#     echo -e "${GREEN}[SUCCESS]${NC} $1"
# }

# print_warning() {
#     echo -e "${YELLOW}[WARNING]${NC} $1"
# }

# print_error() {
#     echo -e "${RED}[ERROR]${NC} $1"
# }

# print_header() {
#     echo "==================================================="
#     echo -e "${BLUE}  Enhanced FIX API Triangular Arbitrage Test${NC}"
#     echo "==================================================="
#     echo
# }

# # Function to check if required environment variables are set
# check_env_vars() {
#     print_status "Checking environment variables..."
    
#     local missing_vars=()
    
#     if [ -z "${TAA_SBE_API_KEY:-}" ]; then
#         missing_vars+=("TAA_SBE_API_KEY")
#     fi
    
#     if [ -z "${TAA_FIX_API:-}" ]; then
#         missing_vars+=("TAA_FIX_API")
#     fi
    
#     if [ -z "${TAA_FIX_PRIVATE_KEY_BASE64:-}" ]; then
#         missing_vars+=("TAA_FIX_PRIVATE_KEY_BASE64")
#     fi
    
#     if [ -z "${TAA_ENABLE_TRADING:-}" ]; then
#         missing_vars+=("TAA_ENABLE_TRADING")
#     fi
    
#     if [ ${#missing_vars[@]} -gt 0 ]; then
#         print_error "Missing required environment variables:"
#         for var in "${missing_vars[@]}"; do
#             echo "  - $var"
#         done
#         echo
#         print_error "Please ensure your .env file contains all required variables."
#         echo "Example .env content:"
#         echo "TAA_SBE_API_KEY=your_api_key_here"
#         echo "TAA_FIX_API=your_api_key_here"
#         echo "TAA_FIX_PRIVATE_KEY_BASE64=your_base64_private_key_here"
#         echo "TAA_ENABLE_TRADING=true"
#         echo "TAA_DEBUG=true  # for mock trading"
#         return 1
#     fi
    
#     print_success "All required environment variables are set"
    
#     # Show configuration summary
#     echo
#     print_status "Configuration Summary:"
#     echo "  - API Key: ${TAA_FIX_API:0:8}..."
#     echo "  - Trading Enabled: ${TAA_ENABLE_TRADING}"
#     echo "  - Debug Mode: ${TAA_DEBUG:-false}"
#     echo "  - Base Asset: ${TAA_BASE_ASSET:-USDT}"
#     echo "  - Trade Amount: ${TAA_TRADE_AMOUNT:-100}"
#     echo "  - Threshold: ${TAA_THRESHOLD:-0.002}"
#     echo
# }

# # Function to check if .env file exists
# check_env_file() {
#     if [ ! -f .env ]; then
#         print_error ".env file not found!"
#         echo
#         echo "Please create a .env file in the project root with the following variables:"
#         echo
#         cat << 'EOF'
# # Required for SBE WebSocket (market data)
# TAA_SBE_API_KEY=your_binance_api_key

# # Required for FIX API (trading)
# TAA_FIX_API=your_binance_api_key
# TAA_FIX_PRIVATE_KEY_BASE64=your_base64_encoded_private_key

# # Trading configuration
# TAA_ENABLE_TRADING=true
# TAA_DEBUG=true

# # Optional configuration
# TAA_BASE_ASSET=USDT
# TAA_TRADE_AMOUNT=100
# TAA_THRESHOLD=0.002
# TAA_DEPTH=5
# TAA_MAX_TRIANGLES=10
# TAA_FEE=0.001
# TAA_MIN_VOLUME_MULTIPLIER=2.0

# # FIX API Configuration
# TAA_FIX_TARGET_COMP_ID=SPOT
# TAA_FIX_HEARTBEAT_INTERVAL=30
# TAA_FIX_ENDPOINT=fix-oe.binance.com:9000

# # Trading limits and safety
# TAA_MAX_SLIPPAGE_PERCENTAGE=0.5
# TAA_MIN_EXECUTION_PROFIT_THRESHOLD=0.1

# # Logging
# TAA_LOG_LEVEL=debug
# EOF
#         echo
#         print_error "Exiting..."
#         return 1
#     fi
    
#     print_success ".env file found"
# }

# # Function to build the project
# build_project() {
#     print_status "Building project in release mode for optimal performance..."
    
#     if ! cargo build --release; then
#         print_error "Failed to build project"
#         echo
#         echo "Common build issues:"
#         echo "  - Missing Rust toolchain: Install from https://rustup.rs/"
#         echo "  - Missing dependencies: Run 'cargo clean' and try again"
#         echo "  - Network issues: Check internet connection"
#         return 1
#     fi
    
#     print_success "Project built successfully"
# }

# # Function to run the test
# run_test() {
#     print_status "Running enhanced triangular arbitrage test..."
#     echo
#     print_warning "This will test FIX API connectivity and execute sample trades"
    
#     if [ "${TAA_DEBUG:-false}" = "true" ]; then
#         print_status "Running in MOCK mode (safe for testing)"
#     else
#         print_warning "Running in REAL trading mode - real trades will be executed!"
#         echo -n "Are you sure you want to continue? (y/N): "
#         read -r response
#         if [[ ! "$response" =~ ^[Yy]$ ]]; then
#             print_status "Test cancelled by user"
#             return 0
#         fi
#     fi
    
#     echo
#     print_status "Starting test execution..."
#     echo
    
#     # Set additional environment for better error reporting
#     export RUST_BACKTRACE=1
#     export RUST_LOG=debug
    
#     # Run the test with timeout
#     if timeout 300 ./target/release/rust-arb-optimized test-exec; then
#         echo
#         print_success "🎉 Test completed successfully!"
#         print_success "Your FIX API configuration is working correctly"
#         echo
#         print_status "Next steps:"
#         echo "  1. You can now run the full arbitrage system with: cargo run"
#         echo "  2. Monitor logs for performance and opportunities"
#         echo "  3. Adjust configuration in .env as needed"
#     else
#         exit_code=$?
#         echo
#         if [ $exit_code -eq 124 ]; then
#             print_error "❌ Test timed out after 5 minutes"
#             echo "This might indicate:"
#             echo "  - Network connectivity issues"
#             echo "  - FIX API connection problems"
#             echo "  - Account permission issues"
#         else
#             print_error "❌ Test failed with exit code $exit_code"
#             echo "Common failure reasons:"
#             echo "  - Invalid API credentials"
#             echo "  - IP address not whitelisted"
#             echo "  - Insufficient account permissions"
#             echo "  - Rate limits exceeded"
#             echo "  - Network connectivity issues"
#         fi
        
#         echo
#         print_status "Troubleshooting steps:"
#         echo "  1. Verify your API key and private key in .env"
#         echo "  2. Check if your IP is whitelisted in Binance"
#         echo "  3. Ensure your account has spot trading permissions"
#         echo "  4. Check Binance API status: https://binance.statuspage.io/"
#         echo "  5. Review the logs above for specific error messages"
        
#         return $exit_code
#     fi
# }

# # Function to check system requirements
# check_requirements() {
#     print_status "Checking system requirements..."
    
#     # Check if cargo is installed
#     if ! command -v cargo &> /dev/null; then
#         print_error "Cargo (Rust) is not installed"
#         echo "Please install Rust from: https://rustup.rs/"
#         return 1
#     fi
    
#     # Check if openssl is available (required for TLS)
#     if ! command -v openssl &> /dev/null; then
#         print_warning "OpenSSL not found - might cause TLS issues"
#     fi
    
#     print_success "System requirements check passed"
# }

# # Function to show post-test information
# show_post_test_info() {
#     echo
#     print_status "📚 Additional Information:"
#     echo
#     echo "Understanding the test results:"
#     echo "  ✅ Success: All triangular trades executed correctly"
#     echo "  ⚠️  Partial: Some trades succeeded, others failed"
#     echo "  ❌ Failure: Connection or configuration issues"
#     echo
#     echo "Log files location:"
#     echo "  - Application logs: ./logs/"
#     echo "  - Rust logs: Set RUST_LOG=debug for detailed output"
#     echo
#     echo "Performance tuning:"
#     echo "  - Adjust TAA_THRESHOLD for sensitivity"
#     echo "  - Modify TAA_TRADE_AMOUNT for position size"
#     echo "  - Update TAA_MIN_VOLUME_MULTIPLIER for liquidity requirements"
#     echo
#     echo "Safety features:"
#     echo "  - Set TAA_DEBUG=true for mock trading"
#     echo "  - Use TAA_MAX_SLIPPAGE_PERCENTAGE to limit slippage"
#     echo "  - Configure TAA_MIN_EXECUTION_PROFIT_THRESHOLD for minimum profit"
# }

# # Main execution
# main() {
#     print_header
    
#     print_status "Initializing enhanced triangular arbitrage test..."
#     echo
    
#     # Run all checks
#     check_requirements || exit 1
#     echo
    
#     check_env_file || exit 1
#     echo
    
#     check_env_vars || exit 1
#     echo
    
#     build_project || exit 1
#     echo
    
#     run_test
#     test_result=$?
    
#     show_post_test_info
    
#     echo
#     echo "==================================================="
#     if [ $test_result -eq 0 ]; then
#         print_success "Enhanced triangular arbitrage test completed successfully!"
#     else
#         print_error "Enhanced triangular arbitrage test failed!"
#     fi
#     echo "==================================================="
    
#     exit $test_result
# }

# # Run main function
# main "$@"

#!/bin/bash

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo "==================================================="
    echo -e "${BLUE}  Enhanced FIX API Triangular Arbitrage Test${NC}"
    echo "==================================================="
    echo
}

# Function to check if required environment variables are set
check_env_vars() {
    print_status "Checking environment variables..."
    
    local missing_vars=()
    
    if [ -z "${TAA_SBE_API_KEY:-}" ]; then
        missing_vars+=("TAA_SBE_API_KEY")
    fi
    
    if [ -z "${TAA_FIX_API:-}" ]; then
        missing_vars+=("TAA_FIX_API")
    fi
    
    if [ -z "${TAA_FIX_PRIVATE_KEY_BASE64:-}" ]; then
        missing_vars+=("TAA_FIX_PRIVATE_KEY_BASE64")
    fi
    
    if [ -z "${TAA_ENABLE_TRADING:-}" ]; then
        missing_vars+=("TAA_ENABLE_TRADING")
    fi
    
    if [ ${#missing_vars[@]} -gt 0 ]; then
        print_error "Missing required environment variables:"
        for var in "${missing_vars[@]}"; do
            echo "  - $var"
        done
        echo
        print_error "Please ensure your .env file contains all required variables."
        echo "Example .env content:"
        echo "TAA_SBE_API_KEY=your_api_key_here"
        echo "TAA_FIX_API=your_api_key_here"
        echo "TAA_FIX_PRIVATE_KEY_BASE64=your_base64_private_key_here"
        echo "TAA_ENABLE_TRADING=true"
        echo "TAA_DEBUG=true  # for mock trading"
        return 1
    fi
    
    print_success "All required environment variables are set"
    
    # Show configuration summary
    echo
    print_status "Configuration Summary:"
    echo "  - API Key: ${TAA_FIX_API:0:8}..."
    echo "  - Trading Enabled: ${TAA_ENABLE_TRADING}"
    echo "  - Debug Mode: ${TAA_DEBUG:-false}"
    echo "  - Base Asset: ${TAA_BASE_ASSET:-USDT}"
    echo "  - Trade Amount: ${TAA_TRADE_AMOUNT:-100}"
    echo "  - Threshold: ${TAA_THRESHOLD:-0.002}"
    echo
}

# Function to check if .env file exists
check_env_file() {
    if [ ! -f .env ]; then
        print_error ".env file not found!"
        echo
        echo "Please create a .env file in the project root with the following variables:"
        echo
        cat << 'EOF'
# Required for SBE WebSocket (market data)
TAA_SBE_API_KEY=your_binance_api_key

# Required for FIX API (trading)
TAA_FIX_API=your_binance_api_key
TAA_FIX_PRIVATE_KEY_BASE64=your_base64_encoded_private_key

# Trading configuration
TAA_ENABLE_TRADING=true
TAA_DEBUG=true

# Optional configuration
TAA_BASE_ASSET=USDT
TAA_TRADE_AMOUNT=100
TAA_THRESHOLD=0.002
TAA_DEPTH=5
TAA_MAX_TRIANGLES=10
TAA_FEE=0.001
TAA_MIN_VOLUME_MULTIPLIER=2.0

# FIX API Configuration
TAA_FIX_TARGET_COMP_ID=SPOT
TAA_FIX_HEARTBEAT_INTERVAL=30
TAA_FIX_ENDPOINT=fix-oe.binance.com:9000

# Trading limits and safety
TAA_MAX_SLIPPAGE_PERCENTAGE=0.5
TAA_MIN_EXECUTION_PROFIT_THRESHOLD=0.1

# Logging
TAA_LOG_LEVEL=debug
EOF
        echo
        print_error "Exiting..."
        return 1
    fi
    
    print_success ".env file found"
}

# Function to build the project
build_project() {
    print_status "Building project in release mode for optimal performance..."
    
    if ! cargo build --release; then
        print_error "Failed to build project"
        echo
        echo "Common build issues:"
        echo "  - Missing Rust toolchain: Install from https://rustup.rs/"
        echo "  - Missing dependencies: Run 'cargo clean' and try again"
        echo "  - Network issues: Check internet connection"
        return 1
    fi
    
    print_success "Project built successfully"
}

# Function to run the test
run_test() {
    print_status "Running enhanced triangular arbitrage test..."
    echo
    print_warning "This will test FIX API connectivity and execute a sample trade"
    
    if [ "${TAA_DEBUG:-false}" = "true" ]; then
        print_status "Running in MOCK mode (safe for testing)"
    else
        print_warning "Running in REAL trading mode - a real trade will be executed!"
        echo -n "Are you sure you want to continue? (y/N): "
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            print_status "Test cancelled by user"
            return 0
        fi
    fi
    
    echo
    print_status "Starting test execution..."
    echo
    
    # Set additional environment for better error reporting
    export RUST_BACKTRACE=1
    export RUST_LOG=info
    
    # Run the test with timeout and proper signal handling
    local test_pid
    timeout 120 ./target/release/rust-arb-optimized test-exec &
    test_pid=$!
    
    # Function to handle cleanup on script termination
    cleanup() {
        print_warning "Cleaning up test process..."
        if kill -0 $test_pid 2>/dev/null; then
            kill -TERM $test_pid 2>/dev/null || true
            sleep 2
            if kill -0 $test_pid 2>/dev/null; then
                kill -KILL $test_pid 2>/dev/null || true
            fi
        fi
    }
    
    # Set trap for cleanup
    trap cleanup EXIT INT TERM
    
    # Wait for the test to complete
    if wait $test_pid; then
        echo
        print_success "🎉 Test completed successfully!"
        print_success "Your FIX API configuration is working correctly"
        echo
        print_status "Next steps:"
        echo "  1. You can now run the full arbitrage system with: cargo run"
        echo "  2. Monitor logs for performance and opportunities"
        echo "  3. Adjust configuration in .env as needed"
        echo "  4. Consider enabling real trading by setting TAA_DEBUG=false"
        
        # Reset trap
        trap - EXIT INT TERM
        return 0
    else
        exit_code=$?
        echo
        
        if [ $exit_code -eq 124 ]; then
            print_error "❌ Test timed out after 2 minutes"
            echo "This might indicate:"
            echo "  - Network connectivity issues"
            echo "  - FIX API connection problems"
            echo "  - Account permission issues"
            echo "  - Invalid API credentials"
        elif [ $exit_code -eq 130 ]; then
            print_warning "Test interrupted by user (Ctrl+C)"
            # Reset trap
            trap - EXIT INT TERM
            return 0
        else
            print_error "❌ Test failed with exit code $exit_code"
            echo "Common failure reasons:"
            echo "  - Invalid API credentials"
            echo "  - IP address not whitelisted"
            echo "  - Insufficient account permissions"
            echo "  - Rate limits exceeded"
            echo "  - Network connectivity issues"
            echo "  - Missing required trading pairs"
        fi
        
        echo
        print_status "Troubleshooting steps:"
        echo "  1. Verify your API key and private key in .env"
        echo "  2. Check if your IP is whitelisted in Binance"
        echo "  3. Ensure your account has spot trading permissions"
        echo "  4. Check Binance API status: https://binance.statuspage.io/"
        echo "  5. Review the logs above for specific error messages"
        echo "  6. Try running with TAA_DEBUG=true for mock trading first"
        
        # Reset trap
        trap - EXIT INT TERM
        return $exit_code
    fi
}

# Function to check system requirements
check_requirements() {
    print_status "Checking system requirements..."
    
    # Check if cargo is installed
    if ! command -v cargo &> /dev/null; then
        print_error "Cargo (Rust) is not installed"
        echo "Please install Rust from: https://rustup.rs/"
        return 1
    fi
    
    # Check if openssl is available (required for TLS)
    if ! command -v openssl &> /dev/null; then
        print_warning "OpenSSL not found - might cause TLS issues"
    fi
    
    print_success "System requirements check passed"
}

# Function to show post-test information
show_post_test_info() {
    echo
    print_status "📚 Additional Information:"
    echo
    echo "Understanding the test results:"
    echo "  ✅ Success: Test order executed correctly"
    echo "  ⚠️  Warning: Minor issues but test completed"
    echo "  ❌ Failure: Connection or configuration issues"
    echo
    echo "Log files location:"
    echo "  - Application logs: ./logs/"
    echo "  - Rust logs: Set RUST_LOG=debug for detailed output"
    echo
    echo "Configuration options:"
    echo "  - TAA_DEBUG=true: Enable mock trading (safe for testing)"
    echo "  - TAA_DEBUG=false: Enable real trading (uses real money!)"
    echo "  - TAA_TRADE_AMOUNT: Amount to trade (default: 100 USDT)"
    echo "  - TAA_THRESHOLD: Profit threshold (default: 0.002 = 0.2%)"
    echo
    echo "Safety features:"
    echo "  - Mock mode executes simulated trades only"
    echo "  - Real mode executes actual trades on Binance"
    echo "  - Test uses small amounts (1% of configured trade amount)"
    echo "  - All trades are market orders for immediate execution"
}

# Main execution
main() {
    print_header
    
    print_status "Initializing enhanced triangular arbitrage test..."
    echo
    
    # Run all checks
    check_requirements || exit 1
    echo
    
    check_env_file || exit 1
    echo
    
    check_env_vars || exit 1
    echo
    
    build_project || exit 1
    echo
    
    run_test
    test_result=$?
    
    show_post_test_info
    
    echo
    echo "==================================================="
    if [ $test_result -eq 0 ]; then
        print_success "Enhanced triangular arbitrage test completed successfully!"
    else
        print_error "Enhanced triangular arbitrage test failed!"
    fi
    echo "==================================================="
    
    exit $test_result
}

# Ensure the script is executable
if [ ! -x "$0" ]; then
    chmod +x "$0"
fi

# Run main function
main "$@"