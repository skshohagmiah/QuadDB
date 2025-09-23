# Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make protoc protobuf-dev

# Install protobuf Go plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Set working directory
WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Generate protobuf code
RUN make proto

# Build both server and CLI with optimizations
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -o gomsg ./cmd/gomsg
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -o gomsg-cli ./cmd/cli

# Runtime stage
FROM alpine:latest

# Add metadata labels
LABEL maintainer="GoMsg Team"
LABEL description="GoMsg - Distributed data platform (Redis + RabbitMQ + Kafka replacement)"
LABEL version="1.0.0"

# Install runtime dependencies
RUN apk --no-cache add ca-certificates curl tzdata && \
    rm -rf /var/cache/apk/*

# Create non-root user
RUN addgroup -g 1001 gomsg && \
    adduser -D -s /bin/sh -u 1001 -G gomsg gomsg

# Set working directory
WORKDIR /app

# Copy binaries from builder stage
COPY --from=builder /app/gomsg .
COPY --from=builder /app/gomsg-cli .

# Create data directory with proper permissions
RUN mkdir -p /data && chown -R gomsg:gomsg /data /app

# Switch to non-root user
USER gomsg

# Expose ports (gRPC API and Raft)
EXPOSE 9000 7000

# Health check (gRPC service - using process check)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD pgrep gomsg || exit 1

# Environment variables with defaults
ENV GOMSG_DATA_DIR=/data
ENV GOMSG_BIND_ADDR=0.0.0.0:9000
ENV GOMSG_NODE_ID=""
ENV GOMSG_BOOTSTRAP=false
ENV GOMSG_PARTITIONS=32
ENV GOMSG_REPLICATION_FACTOR=1

# Default command
CMD ["./gomsg", "--data-dir=/data", "--host=0.0.0.0", "--port=9000"]
