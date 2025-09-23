#!/bin/bash

# GoMsg Test Runner Script
# This script runs all tests for the GoMsg project

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
GOMSG_ADDR=${GOMSG_TEST_ADDR:-"localhost:9000"}
TEST_TIMEOUT=${TEST_TIMEOUT:-"30s"}
VERBOSE=${VERBOSE:-false}

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

# Function to check if server is running
check_server() {
    print_status "Checking if GoMsg server is running at $GOMSG_ADDR..."
    
    if timeout 5 bash -c "</dev/tcp/${GOMSG_ADDR%:*}/${GOMSG_ADDR#*:}"; then
        print_success "Server is running"
        return 0
    else
        print_error "Server is not running at $GOMSG_ADDR"
        return 1
    fi
}

# Function to start server if needed
start_server() {
    print_status "Starting GoMsg server..."
    
    # Check if binary exists
    if [ ! -f "../bin/gomsg" ]; then
        print_error "GoMsg binary not found. Please build the project first:"
        echo "  cd .. && make build"
        exit 1
    fi
    
    # Start server in background
    ../bin/gomsg --data-dir=./test-data --port=9000 > gomsg.log 2>&1 &
    SERVER_PID=$!
    
    # Wait for server to start
    for i in {1..10}; do
        if check_server; then
            print_success "Server started (PID: $SERVER_PID)"
            return 0
        fi
        sleep 1
    done
    
    print_error "Failed to start server"
    return 1
}

# Function to stop server
stop_server() {
    if [ ! -z "$SERVER_PID" ]; then
        print_status "Stopping server (PID: $SERVER_PID)..."
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
        print_success "Server stopped"
    fi
}

# Function to run tests
run_tests() {
    local test_type=$1
    local test_path=$2
    
    print_status "Running $test_type tests..."
    
    # Set test flags
    local flags="-timeout=$TEST_TIMEOUT"
    if [ "$VERBOSE" = "true" ]; then
        flags="$flags -v"
    fi
    
    # Set environment variables
    export GOMSG_TEST_ADDR=$GOMSG_ADDR
    
    # Run tests
    if go test $flags ./$test_path/...; then
        print_success "$test_type tests passed"
        return 0
    else
        print_error "$test_type tests failed"
        return 1
    fi
}

# Function to run benchmarks
run_benchmarks() {
    print_status "Running performance benchmarks..."
    
    export GOMSG_TEST_ADDR=$GOMSG_ADDR
    
    if go test -bench=. -benchmem -timeout=5m ./performance/...; then
        print_success "Benchmarks completed"
        return 0
    else
        print_error "Benchmarks failed"
        return 1
    fi
}

# Function to generate test report
generate_report() {
    print_status "Generating test report..."
    
    export GOMSG_TEST_ADDR=$GOMSG_ADDR
    
    # Run tests with coverage
    go test -coverprofile=coverage.out -covermode=atomic ./unit/... ./integration/...
    
    # Generate HTML coverage report
    go tool cover -html=coverage.out -o coverage.html
    
    print_success "Test report generated: coverage.html"
}

# Function to clean up test data
cleanup() {
    print_status "Cleaning up..."
    
    # Stop server if we started it
    stop_server
    
    # Clean up test data
    rm -rf ./test-data
    rm -f gomsg.log
    
    print_success "Cleanup completed"
}

# Trap to ensure cleanup on exit
trap cleanup EXIT

# Main execution
main() {
    echo "======================================"
    echo "       GoMsg Test Runner"
    echo "======================================"
    echo ""
    
    # Parse command line arguments
    case "${1:-all}" in
        "unit")
            TEST_TYPES="unit"
            ;;
        "integration")
            TEST_TYPES="integration"
            ;;
        "performance")
            TEST_TYPES="performance"
            ;;
        "all")
            TEST_TYPES="unit integration performance"
            ;;
        "report")
            TEST_TYPES="report"
            ;;
        "help"|"-h"|"--help")
            echo "Usage: $0 [unit|integration|performance|all|report|help]"
            echo ""
            echo "Options:"
            echo "  unit         Run unit tests only"
            echo "  integration  Run integration tests only"
            echo "  performance  Run performance benchmarks only"
            echo "  all          Run all tests (default)"
            echo "  report       Generate test coverage report"
            echo "  help         Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  GOMSG_TEST_ADDR  Server address (default: localhost:9000)"
            echo "  TEST_TIMEOUT     Test timeout (default: 30s)"
            echo "  VERBOSE          Verbose output (default: false)"
            echo ""
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use '$0 help' for usage information"
            exit 1
            ;;
    esac
    
    # Check if server is running, start if needed
    if ! check_server; then
        print_warning "Server not running, attempting to start..."
        if ! start_server; then
            print_error "Could not start server. Please start GoMsg server manually:"
            echo "  cd .. && ./bin/gomsg --port=9000"
            exit 1
        fi
        STARTED_SERVER=true
    fi
    
    # Run tests
    local failed=0
    
    for test_type in $TEST_TYPES; do
        case $test_type in
            "unit")
                run_tests "Unit" "unit" || failed=1
                ;;
            "integration")
                run_tests "Integration" "integration" || failed=1
                ;;
            "performance")
                run_benchmarks || failed=1
                ;;
            "report")
                generate_report || failed=1
                ;;
        esac
        echo ""
    done
    
    # Summary
    echo "======================================"
    if [ $failed -eq 0 ]; then
        print_success "All tests completed successfully!"
    else
        print_error "Some tests failed!"
        exit 1
    fi
}

# Change to tests directory
cd "$(dirname "$0")"

# Run main function
main "$@"
