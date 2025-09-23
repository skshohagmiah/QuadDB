# GoMsg Docker Quick Start

Simple guide to run GoMsg with Docker.

## Quick Start

### 1. Pull from Docker Hub (easiest)
```bash
docker pull shohag2100/gomsg:latest
```

### 2. Run a single node
```bash
make docker-run
# or directly: docker run -d -p 9000:9000 -v gomsg-data:/data --name gomsg shohag2100/gomsg:latest
```

### 3. Run with Docker Compose (recommended)
```bash
make docker-compose
# or directly: docker-compose -f docker-compose.simple.yml up -d
```

### 4. Build your own image (optional)
```bash
make docker
# or directly: ./scripts/docker-build.sh
```

## What you get

- **Single node**: Perfect for development and small deployments
- **API endpoint**: http://localhost:9000
- **Persistent data**: Stored in Docker volume `gomsg-data`
- **Health checks**: Built-in container health monitoring

## Basic commands

```bash
# Pull from Docker Hub
docker pull shohag2100/gomsg:latest

# Build image (if you want to build locally)
make docker

# Push to Docker Hub (for maintainers)
make docker-push

# Run single container
make docker-run

# Run with compose (recommended)
make docker-compose

# Stop everything
make docker-stop

# Clean up
make docker-clean

# View logs
docker logs gomsg
# or: docker-compose -f docker-compose.simple.yml logs -f
```

## For production cluster

If you need a 3-node cluster:
```bash
make docker-cluster
```

This gives you:
- Node 1: http://localhost:9000
- Node 2: http://localhost:9001  
- Node 3: http://localhost:9002

## Using the published image

Anyone can now use GoMsg directly from Docker Hub:

```bash
# Quick start
docker run -d -p 9000:9000 -v gomsg-data:/data --name gomsg shohag2100/gomsg:latest

# With custom tag
docker run -d -p 9000:9000 -v gomsg-data:/data --name gomsg shohag2100/gomsg:v1.0.0
```

That's it! Keep it simple. 🚀
