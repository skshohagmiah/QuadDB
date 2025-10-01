GO_VERSION := 1.24
PROTO_VERSION := 25.1
BINARY_NAME := gomsg
CLI_BINARY_NAME := gomsg-cli

.PHONY: help build test clean install proto docker deps setup check-tools quick-start

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}\' $(MAKEFILE_LIST)

deps: ## Install dependencies
	go mod download
	go mod tidy

setup: ## One-time setup - install all required tools
	@echo "🔧 Setting up development environment..."
	@echo "📦 Installing protobuf Go plugins..."
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "✅ Setup complete! You can now run 'make build' to build the project."

check-tools: ## Check if required tools are installed
	@echo "🔍 Checking required tools..."
	@command -v protoc >/dev/null 2>&1 || { echo "❌ protoc is not installed. Please install Protocol Buffers compiler."; echo "   macOS: brew install protobuf"; echo "   Ubuntu: apt-get install protobuf-compiler"; exit 1; }
	@export PATH="$(shell go env GOPATH)/bin:$$PATH"; command -v protoc-gen-go >/dev/null 2>&1 || { echo "⚠️  protoc-gen-go not found. Installing..."; go install google.golang.org/protobuf/cmd/protoc-gen-go@latest; }
	@export PATH="$(shell go env GOPATH)/bin:$$PATH"; command -v protoc-gen-go-grpc >/dev/null 2>&1 || { echo "⚠️  protoc-gen-go-grpc not found. Installing..."; go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest; }
	@echo "✅ All tools are available!"

quick-start: setup build ## First-time setup and build (recommended for new users)
	@echo ""
	@echo "🎉 GoMsg is ready to use!"
	@echo ""
	@echo "Quick commands:"
	@echo "  make run          - Start the server"
	@echo "  make test-quick   - Run quick tests"
	@echo "  ./bin/$(CLI_BINARY_NAME) --help - See CLI options"
	@echo ""

proto: check-tools ## Generate protobuf code
	@echo "Generating protobuf code..."
	@mkdir -p api/generated/{common,kv,queue,stream,cluster,document}
	@export PATH="$(shell go env GOPATH)/bin:$$PATH"; \
	protoc --go_out=. --go_opt=module=github.com/skshohagmiah/gomsg \
		--go-grpc_out=. --go-grpc_opt=module=github.com/skshohagmiah/gomsg \
		api/proto/common.proto
	@export PATH="$(shell go env GOPATH)/bin:$$PATH"; \
	protoc --go_out=. --go_opt=module=github.com/skshohagmiah/gomsg \
		--go-grpc_out=. --go-grpc_opt=module=github.com/skshohagmiah/gomsg \
		-I api/proto api/proto/kv.proto
	@export PATH="$(shell go env GOPATH)/bin:$$PATH"; \
	protoc --go_out=. --go_opt=module=github.com/skshohagmiah/gomsg \
		--go-grpc_out=. --go-grpc_opt=module=github.com/skshohagmiah/gomsg \
		-I api/proto api/proto/queue.proto
	@export PATH="$(shell go env GOPATH)/bin:$$PATH"; \
	protoc --go_out=. --go_opt=module=github.com/skshohagmiah/gomsg \
		--go-grpc_out=. --go-grpc_opt=module=github.com/skshohagmiah/gomsg \
		-I api/proto api/proto/stream.proto
	@export PATH="$(shell go env GOPATH)/bin:$$PATH"; \
	protoc --go_out=. --go_opt=module=github.com/skshohagmiah/gomsg \
		--go-grpc_out=. --go-grpc_opt=module=github.com/skshohagmiah/gomsg \
		-I api/proto api/proto/cluster.proto
	@export PATH="$(shell go env GOPATH)/bin:$$PATH"; \
	protoc --go_out=. --go_opt=module=github.com/skshohagmiah/gomsg \
		--go-grpc_out=. --go-grpc_opt=module=github.com/skshohagmiah/gomsg \
		-I api/proto api/proto/db.proto

build: deps proto ## Build the server and CLI binaries
	@echo "🔨 Building server..."
	@go build -ldflags="-s -w" -o bin/$(BINARY_NAME) ./cmd/gomsg
	@echo "🔨 Building CLI..."
	@go build -ldflags="-s -w" -o bin/$(CLI_BINARY_NAME) ./cmd/cli
	@echo "✅ Build complete! Binaries are in ./bin/"
	@echo "   Server: ./bin/$(BINARY_NAME)"
	@echo "   CLI:    ./bin/$(CLI_BINARY_NAME)"

test: ## Run all tests
	cd tests && ./run_tests.sh all

test-unit: ## Run unit tests only
	cd tests && ./run_tests.sh unit

test-integration: ## Run integration tests only
	cd tests && ./run_tests.sh integration

test-performance: ## Run performance benchmarks
	cd tests && ./run_tests.sh performance

test-report: ## Generate test coverage report
	cd tests && ./run_tests.sh report

test-quick: ## Run quick tests (unit only)
	go test -v -race ./tests/unit/...

bench: ## Run benchmarks
	go test -bench=. -benchmem ./...

clean: ## Clean build artifacts
	rm -rf bin/
	rm -rf api/generated/
	rm -f coverage.out
	rm -rf data/
	rm -rf cluster/
	rm -rf backups/

install: build ## Install binaries to $GOPATH/bin
	cp bin/$(BINARY_NAME) $(GOPATH)/bin/
	cp bin/$(CLI_BINARY_NAME) $(GOPATH)/bin/

docker: ## Build Docker image
	./scripts/docker-build.sh

docker-push: ## Push Docker image to Docker Hub
	docker push shohag2100/gomsg:latest

docker-run: ## Run Docker container
	docker run -d -p 9000:9000 -p 7000:7000 -v gomsg-data:/data --name gomsg shohag2100/gomsg:latest

docker-compose: ## Start with docker-compose (single node)
	docker-compose -f docker-compose.simple.yml up -d

docker-cluster: ## Start cluster with docker-compose
	docker-compose up -d

docker-stop: ## Stop all docker services
	docker stop gomsg 2>/dev/null || true
	docker-compose down 2>/dev/null || true
	docker-compose -f docker-compose.simple.yml down 2>/dev/null || true

docker-clean: ## Clean up Docker resources
	docker stop gomsg 2>/dev/null || true
	docker rm gomsg 2>/dev/null || true
	docker-compose down -v 2>/dev/null || true
	docker-compose -f docker-compose.simple.yml down -v 2>/dev/null || true

run: build ## Run the server locally
	./bin/$(BINARY_NAME) --data-dir=./data --port=9000

run-cluster: build ## Run a 3-node cluster locally
	@echo "Starting 3-node cluster..."
	@mkdir -p data/{node1,node2,node3} cluster/{node1,node2,node3}
	@echo "Starting node1 (bootstrap)..."
	@./bin/$(BINARY_NAME) --cluster --node-id=node1 --port=9000 --data-dir=./data/node1 --bootstrap &
	@sleep 2
	@echo "Starting node2..."
	@./bin/$(BINARY_NAME) --cluster --node-id=node2 --port=9001 --data-dir=./data/node2 --join=localhost:9000 &
	@sleep 2
	@echo "Starting node3..."
	@./bin/$(BINARY_NAME) --cluster --node-id=node3 --port=9002 --data-dir=./data/node3 --join=localhost:9000 &
	@echo "Cluster started! Press Ctrl+C to stop."
	@wait

lint: ## Run linter
	golangci-lint run

fmt: ## Format code
	go fmt ./...

mod: ## Update go modules
	go mod tidy
	go mod verify

release: ## Build release binaries
	@echo "Building release binaries..."
	@mkdir -p releases
	GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o releases/$(BINARY_NAME)-linux-amd64 cmd/fluxdl/main.go
	GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o releases/$(BINARY_NAME)-darwin-amd64 cmd/fluxdl/main.go
	GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o releases/$(BINARY_NAME)-windows-amd64.exe cmd/fluxdl/main.go
	GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o releases/$(CLI_BINARY_NAME)-linux-amd64 cmd/cli/*.go
	GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o releases/$(CLI_BINARY_NAME)-darwin-amd64 cmd/cli/*.go
	GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o releases/$(CLI_BINARY_NAME)-windows-amd64.exe cmd/cli/*.go

# Development targets
dev: build ## Start development server with file watching
	@echo "Starting development server..."
	./bin/$(BINARY_NAME) --data-dir=./dev-data --port=9000 &
	@echo "Server started on localhost:9000"

stop: ## Stop all running fluxdl processes
	@echo "Stopping fluxdl processes..."
	-pkill -f "$(BINARY_NAME)"

# Database targets
backup: ## Create database backup
	./bin/$(CLI_BINARY_NAME) admin backup --output=./backups/backup-$(shell date +%Y%m%d-%H%M%S).db

restore: ## Restore database from backup (requires BACKUP_FILE)
	@if [ -z "$(BACKUP_FILE)" ]; then echo "Usage: make restore BACKUP_FILE=path/to/backup.db"; exit 1; fi
	./bin/$(CLI_BINARY_NAME) admin restore --input=$(BACKUP_FILE)