# Build stage
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make protoc protobuf-dev

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Generate protobuf code
RUN make proto

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o gomsg ./cmd/gomsg

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates curl

# Create non-root user
RUN addgroup -g 1001 gomsg && \
    adduser -D -s /bin/sh -u 1001 -G gomsg gomsg

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/gomsg .

# Create data directory
RUN mkdir -p /data && chown gomsg:gomsg /data

# Switch to non-root user
USER gomsg

# Expose ports
EXPOSE 8080 7000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# Default command
CMD ["./gomsg"]
