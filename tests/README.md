# GoMsg Test Suite

Comprehensive test suite for GoMsg - covering unit tests, integration tests, and performance benchmarks.

## 🚀 Quick Start

```bash
# Run all tests
make test

# Run specific test types
make test-unit           # Unit tests only
make test-integration    # Integration tests only
make test-performance    # Performance benchmarks
make test-report        # Generate coverage report
```

## 📁 Test Structure

```
tests/
├── unit/                    # Unit tests for individual components
│   ├── kv_test.go          # Key/Value operations tests
│   ├── queue_test.go       # Queue operations tests
│   └── stream_test.go      # Stream operations tests
├── integration/             # Integration tests for workflows
│   └── full_workflow_test.go # End-to-end workflow tests
├── performance/             # Performance benchmarks
│   └── benchmark_test.go    # Throughput and latency benchmarks
├── smoke_test.go           # Basic smoke test
├── test_config.go          # Test configuration and utilities
└── run_tests.sh           # Test runner script
```

## 🧪 Test Categories

### Unit Tests
- **KV Tests**: SET, GET, INCR, DECR, EXISTS, DEL, TTL, MSET, MGET
- **Queue Tests**: PUSH, POP, PEEK, ACK, NACK, STATS, PURGE, LIST
- **Stream Tests**: CREATE, PUBLISH, READ, SUBSCRIBE, SEEK, DELETE

### Integration Tests
- **User Registration Workflow**: Complete user signup flow using KV + Queue + Stream
- **Concurrent Operations**: Multi-worker stress testing
- **Data Consistency**: ACID properties and ordering guarantees

### Performance Tests
- **Throughput Benchmarks**: Operations per second
- **Latency Benchmarks**: Response time percentiles (P95, P99)
- **Mixed Workload**: Real-world usage patterns

## 🔧 Running Tests

### Prerequisites
1. **GoMsg Server Running**: The server must be running on `localhost:9000`
2. **Go 1.21+**: Required for running tests
3. **Generated Protobuf**: Run `make proto` if needed

### Manual Test Execution

```bash
# Start GoMsg server (in separate terminal)
./bin/gomsg --port=9000

# Run tests
cd tests

# Smoke test (quick verification)
go test -v ./smoke_test.go

# Unit tests
go test -v ./unit/...

# Integration tests  
go test -v ./integration/...

# Performance benchmarks
go test -bench=. -benchmem ./performance/...
```

### Automated Test Execution

```bash
# Use the test runner (handles server startup)
./tests/run_tests.sh all

# Or use Makefile targets
make test                # All tests
make test-unit          # Unit tests only
make test-integration   # Integration tests only
make test-performance   # Benchmarks only
```

## 📊 Test Results

### Expected Performance (Single Node)
- **KV Operations**: 50,000+ ops/sec
- **Queue Operations**: 30,000+ ops/sec  
- **Stream Operations**: 25,000+ ops/sec
- **Latency P99**: <5ms for all operations

### Coverage Goals
- **Unit Tests**: >90% code coverage
- **Integration Tests**: All major workflows covered
- **Performance Tests**: Baseline metrics established

## 🐛 Troubleshooting

### Common Issues

**Server Not Running**
```bash
Error: dial tcp [::1]:9000: connect: connection refused
```
Solution: Start the GoMsg server first:
```bash
./bin/gomsg --port=9000
```

**Import Path Issues**
```bash
Error: package gomsg/api/generated/kv is not in GOPATH
```
Solution: Run from project root and ensure protobuf files are generated:
```bash
make proto
```

**Test Timeouts**
```bash
Error: test timed out after 30s
```
Solution: Increase timeout or check server performance:
```bash
export TEST_TIMEOUT=60s
```

### Environment Variables

- `GOMSG_TEST_ADDR`: Server address (default: localhost:9000)
- `TEST_TIMEOUT`: Test timeout duration (default: 30s)
- `VERBOSE`: Enable verbose output (default: false)

## 📈 Continuous Integration

### GitHub Actions Example

```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v3
        with:
          go-version: '1.21'
      
      - name: Build GoMsg
        run: make build
      
      - name: Run Tests
        run: make test
      
      - name: Upload Coverage
        uses: codecov/codecov-action@v3
        with:
          file: ./tests/coverage.out
```

## 🎯 Test Development Guidelines

### Writing New Tests

1. **Follow Naming Convention**: `TestFeatureName` for tests, `BenchmarkFeatureName` for benchmarks
2. **Use Subtests**: Group related test cases with `t.Run()`
3. **Clean Up**: Always clean up test data
4. **Error Handling**: Use `t.Fatalf()` for critical failures, `t.Errorf()` for non-critical
5. **Timeouts**: Set appropriate timeouts for operations

### Example Test Structure

```go
func TestNewFeature(t *testing.T) {
    client := setupClient(t)
    ctx := context.Background()
    
    t.Run("BasicOperation", func(t *testing.T) {
        // Test basic functionality
    })
    
    t.Run("ErrorHandling", func(t *testing.T) {
        // Test error conditions
    })
    
    t.Run("EdgeCases", func(t *testing.T) {
        // Test edge cases
    })
}
```

## 📝 Test Reports

Generate detailed test reports:

```bash
# Coverage report
make test-report

# View coverage in browser
open tests/coverage.html

# Benchmark comparison
go test -bench=. -count=5 ./performance/... | tee benchmark.txt
```

## 🔍 Debugging Tests

### Verbose Output
```bash
go test -v ./tests/unit/kv_test.go
```

### Run Single Test
```bash
go test -run TestKVSetGet ./tests/unit/
```

### Debug with Delve
```bash
dlv test ./tests/unit/ -- -test.run TestKVSetGet
```

---

**Your GoMsg test suite is comprehensive and ready for development!** 🚀
