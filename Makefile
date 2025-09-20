GO_VERSION := 1.21
PROTO_VERSION := 25.1
BINARY_NAME := gomsg
CLI_BINARY_NAME := gomsg-cli

.PHONY: help build test clean install proto docker deps

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}\' $(MAKEFILE_LIST)

deps: ## Install dependencies
	go mod download
	go mod tidy

proto: ## Generate protobuf code
	@echo "Generating protobuf code..."
	@mkdir -p api/generated/{common,kv,queue,stream,cluster}
	protoc --go_out=api/generated/common --go_opt=paths=source_relative \
		--go-grpc_out=api/generated/common --go-grpc_opt=paths=source_relative \
		api/proto/common.proto
	protoc --go_out=api/generated/kv --go_opt=paths=source_relative \
		--go-grpc_out=api/generated/kv --go-grpc_opt=paths=source_relative \
		-I api/proto api/proto/kv.proto
	protoc --go_out=api/generated/queue --go_opt=paths=source_relative \
		--go-grpc_out=api/generated/queue --go-grpc_opt=paths=source_relative \
		-I api/proto api/proto/queue.proto
	protoc --go_out=api/generated/stream --go_opt=paths=source_relative \
		--go-grpc_out=api/generated/stream --go-grpc_opt=paths=source_relative \
		-I api/proto api/proto/stream.proto
	protoc --go_out=api/generated/cluster --go_opt=paths=source_relative \
		--go-grpc_out=api/generated/cluster --go-grpc_opt=paths=source_relative \
		-I api/proto api/proto/cluster.proto

build: deps proto ## Build the server and CLI binaries
	@echo "Building server..."
	go build -ldflags="-s -w" -o bin/$(BINARY_NAME) cmd/gomsg/main.go
	@echo "Building CLI..."
	go build -ldflags="-s -w" -o bin/$(CLI_BINARY_NAME) cmd/cli/*.go

test: ## Run tests
	go test -v -race -coverprofile=coverage.out ./...

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
	docker build -t gomsg:latest -f docker/Dockerfile .

docker-compose: ## Start with docker-compose
	docker-compose -f docker/docker-compose.yml up -d

docker-cluster: ## Start cluster with docker-compose
	docker-compose -f docker/docker-compose.cluster.yml up -d

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
	GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o releases/$(BINARY_NAME)-linux-amd64 cmd/gomsg/main.go
	GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o releases/$(BINARY_NAME)-darwin-amd64 cmd/gomsg/main.go
	GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o releases/$(BINARY_NAME)-windows-amd64.exe cmd/gomsg/main.go
	GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o releases/$(CLI_BINARY_NAME)-linux-amd64 cmd/cli/*.go
	GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o releases/$(CLI_BINARY_NAME)-darwin-amd64 cmd/cli/*.go
	GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o releases/$(CLI_BINARY_NAME)-windows-amd64.exe cmd/cli/*.go

# Development targets
dev: build ## Start development server with file watching
	@echo "Starting development server..."
	./bin/$(BINARY_NAME) --data-dir=./dev-data --port=9000 &
	@echo "Server started on localhost:9000"

stop: ## Stop all running gomsg processes
	@echo "Stopping gomsg processes..."
	-pkill -f "$(BINARY_NAME)"

# Database targets
backup: ## Create database backup
	./bin/$(CLI_BINARY_NAME) admin backup --output=./backups/backup-$(shell date +%Y%m%d-%H%M%S).db

restore: ## Restore database from backup (requires BACKUP_FILE)
	@if [ -z "$(BACKUP_FILE)" ]; then echo "Usage: make restore BACKUP_FILE=path/to/backup.db"; exit 1; fi
	./bin/$(CLI_BINARY_NAME) admin restore --input=$(BACKUP_FILE)