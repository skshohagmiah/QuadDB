# fluxdl Installation Guide

Complete step-by-step guide to install and run fluxdl with all available SDKs.

## 📋 Prerequisites

- **Docker** (recommended) or Go 1.21+
- **Git** for cloning repositories
- Language-specific tools for SDKs:
  - **Go**: Go 1.21+
  - **Node.js**: Node.js 16+ and npm/yarn
  - **Python**: Python 3.8+ and pip

## 🚀 Quick Start (Docker - Recommended)

### 1. Run fluxdl Server

```bash
# Pull and run the latest fluxdl Docker image
docker run -d \
  --name fluxdl \
  -p 9000:9000 \
  -v fluxdl-data:/data \
  shohag2100/fluxdl:latest

# Verify it's running
docker logs fluxdl
```

### 2. Test Connection

```bash
# Check if fluxdl is responding
curl -f http://localhost:9000/health || echo "fluxdl is running on gRPC port 9000"
```

Now jump to the [SDK Installation](#-sdk-installation) section for your preferred language.

## 🔧 Manual Installation (From Source)

### 1. Clone the Repository

```bash
git clone https://github.com/skshohagmiah/fluxdl.git
cd fluxdl
```

### 2. Build fluxdl Server

```bash
# Install dependencies and build
make deps
make build

# Or build with Docker
make docker
```

### 3. Run fluxdl Server

```bash
# Run single node
./bin/fluxdl server --port 9000 --data-dir ./data

# Or with Docker Compose
make docker-compose
```

### 4. Verify Installation

```bash
# Test with CLI
./bin/fluxdl-cli --server localhost:9000 kv set test "Hello fluxdl"
./bin/fluxdl-cli --server localhost:9000 kv get test
```

## 📚 SDK Installation

Choose your preferred programming language:

### 🐹 Go SDK

#### Installation

```bash
# Create a new Go project
mkdir my-fluxdl-app && cd my-fluxdl-app
go mod init my-fluxdl-app

# Install the Go SDK
go get github.com/skshohagmiah/fluxdl-go-sdk
```

#### Quick Example

Create `main.go`:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    fluxdl "github.com/skshohagmiah/fluxdl-go-sdk"
)

func main() {
    // Connect to fluxdl
    client, err := fluxdl.NewClient(&fluxdl.Config{
        Address: "localhost:9000",
        Timeout: 30 * time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // Key-Value operations
    client.KV.Set(ctx, "user:1", "John Doe")
    value, _ := client.KV.Get(ctx, "user:1")
    fmt.Printf("Retrieved: %s\n", value)

    // Queue operations
    client.Queue.Push(ctx, "tasks", "process-payment")
    message, _ := client.Queue.Pop(ctx, "tasks")
    fmt.Printf("Task: %s\n", message)

    // Stream operations
    client.Stream.CreateStream(ctx, "events", 3)
    client.Stream.Publish(ctx, "events", "user-login")
    fmt.Println("✅ fluxdl operations completed!")
}
```

#### Run the Example

```bash
go run main.go
```

### 🟢 Node.js/TypeScript SDK

#### Installation

```bash
# Create a new Node.js project
mkdir my-fluxdl-app && cd my-fluxdl-app
npm init -y

# Install the Node.js SDK
npm install @skshohagmiah/fluxdl-nodejs-sdk

# For TypeScript projects
npm install -D typescript @types/node
npx tsc --init
```

#### Quick Example

Create `index.js` (or `index.ts` for TypeScript):

```javascript
const { fluxdlClient } = require('@skshohagmiah/fluxdl-nodejs-sdk');
// For TypeScript: import { fluxdlClient } from '@skshohagmiah/fluxdl-nodejs-sdk';

async function main() {
    // Connect to fluxdl
    const client = await fluxdlClient.connect({
        address: 'localhost:9000',
        timeout: 10000
    });

    try {
        // Key-Value operations
        await client.kv.set('user:1', 'John Doe');
        const user = await client.kv.get('user:1');
        console.log(`Retrieved: ${user}`);

        // Queue operations
        await client.queue.push('tasks', 'process-payment');
        const task = await client.queue.pop('tasks');
        console.log(`Task: ${task}`);

        // Stream operations
        await client.stream.createStream('events', 3);
        await client.stream.publish('events', 'user-login');
        
        console.log('✅ fluxdl operations completed!');
    } finally {
        await client.disconnect();
    }
}

main().catch(console.error);
```

#### Run the Example

```bash
node index.js
# Or for TypeScript: npx ts-node index.ts
```

### 🐍 Python SDK

#### Installation

```bash
# Create a new Python project
mkdir my-fluxdl-app && cd my-fluxdl-app

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the Python SDK
pip install fluxdl-python-sdk
```

#### Quick Example

Create `main.py`:

```python
import asyncio
from fluxdl_sdk import fluxdlClient

async def main():
    # Connect to fluxdl
    client = await fluxdlClient.create(
        address="localhost:9000",
        timeout=10.0
    )

    try:
        # Key-Value operations
        await client.kv.set("user:1", "John Doe")
        user = await client.kv.get("user:1")
        print(f"Retrieved: {user}")

        # Queue operations
        await client.queue.push("tasks", "process-payment")
        task = await client.queue.pop("tasks")
        print(f"Task: {task}")

        # Stream operations
        await client.stream.create_stream("events", 3)
        await client.stream.publish("events", "user-login")
        
        print("✅ fluxdl operations completed!")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```

#### Run the Example

```bash
python main.py
```

## 🐳 Docker Development Setup

For development with Docker Compose:

### 1. Create docker-compose.yml

```yaml
version: '3.8'
services:
  fluxdl:
    image: shohag2100/fluxdl:latest
    ports:
      - "9000:9000"
    volumes:
      - fluxdl-data:/data
    environment:
      - fluxdl_LOG_LEVEL=info
    healthcheck:
      test: ["CMD", "fluxdl-cli", "--server", "localhost:9000", "ping"]
      interval: 30s
      timeout: 10s
      retries: 3

  # Your application
  app:
    build: .
    depends_on:
      - fluxdl
    environment:
      - fluxdl_ADDRESS=fluxdl:9000

volumes:
  fluxdl-data:
```

### 2. Run with Docker Compose

```bash
docker-compose up -d
```

## 🔧 Configuration Options

### Environment Variables

```bash
# Server configuration
export fluxdl_PORT=9000
export fluxdl_DATA_DIR=/data
export fluxdl_LOG_LEVEL=info

# Client configuration
export fluxdl_ADDRESS=localhost:9000
export fluxdl_TIMEOUT=30s
```

### Configuration Files

Create `fluxdl.yaml`:

```yaml
server:
  port: 9000
  data_dir: "./data"
  log_level: "info"
  
cluster:
  enabled: false
  nodes: []
  
storage:
  sync_writes: true
  compression: true
```

## 🧪 Testing Your Installation

### 1. Health Check

```bash
# Using Docker
docker exec fluxdl fluxdl-cli ping

# Using binary
./bin/fluxdl-cli --server localhost:9000 ping
```

### 2. Basic Operations Test

```bash
# Key-Value test
./bin/fluxdl-cli --server localhost:9000 kv set test "Hello World"
./bin/fluxdl-cli --server localhost:9000 kv get test

# Queue test
./bin/fluxdl-cli --server localhost:9000 queue push myqueue "test message"
./bin/fluxdl-cli --server localhost:9000 queue pop myqueue

# Stream test
./bin/fluxdl-cli --server localhost:9000 stream create mystream 3
./bin/fluxdl-cli --server localhost:9000 stream publish mystream "test event"
```

## 🚨 Troubleshooting

### Common Issues

#### 1. Connection Refused
```bash
# Check if fluxdl is running
docker ps | grep fluxdl
# or
ps aux | grep fluxdl

# Check port availability
netstat -tlnp | grep 9000
```

#### 2. Permission Denied (Data Directory)
```bash
# Fix data directory permissions
sudo chown -R $(whoami) ./data
chmod 755 ./data
```

#### 3. Docker Issues
```bash
# Clean up Docker resources
docker system prune -f
docker volume prune -f

# Restart fluxdl container
docker restart fluxdl
```

#### 4. SDK Import Issues

**Go:**
```bash
go mod tidy
go mod download
```

**Node.js:**
```bash
npm cache clean --force
npm install
```

**Python:**
```bash
pip install --upgrade pip
pip install --force-reinstall fluxdl-python-sdk
```

## 📖 Next Steps

1. **Read the Documentation**: Check individual SDK README files in `./sdks/`
2. **Explore Examples**: Look at example projects in each SDK directory
3. **Join the Community**: 
   - 🐛 [Report Issues](https://github.com/shohag2100/fluxdl/issues)
   - 💬 [Discussions](https://github.com/shohag2100/fluxdl/discussions)
   - 📖 [Wiki](https://github.com/shohag2100/fluxdl/wiki)

## 🤝 Contributing

Want to contribute? Check out our [Contributing Guide](CONTRIBUTING.md) and [Development Setup](docs/DEVELOPMENT.md).

---

**Need Help?** 
- 📧 Email: support@fluxdl.io
- 💬 Discord: [fluxdl Community](https://discord.gg/fluxdl)
- 📖 Docs: [docs.fluxdl.io](https://docs.fluxdl.io)
