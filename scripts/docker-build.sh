#!/bin/bash

# Simple Docker build script for fluxdl

set -e

# Configuration
IMAGE_NAME=${IMAGE_NAME:-"shohag2100/fluxdl"}
VERSION=${VERSION:-"latest"}

echo "🐳 Building fluxdl Docker image..."
echo "Image: ${IMAGE_NAME}:${VERSION}"

# Build the image
docker build -t "${IMAGE_NAME}:${VERSION}" .

echo "✅ Build completed!"
echo ""
echo "To run:"
echo "  docker run -d -p 9000:9000 --name fluxdl ${IMAGE_NAME}:${VERSION}"
echo ""
echo "To run with persistent storage:"
echo "  docker run -d -p 9000:9000 -v fluxdl-data:/data --name fluxdl ${IMAGE_NAME}:${VERSION}"
echo ""
echo "To push to Docker Hub:"
echo "  docker push ${IMAGE_NAME}:${VERSION}"
echo ""
echo "To push with custom tag:"
echo "  docker tag ${IMAGE_NAME}:${VERSION} ${IMAGE_NAME}:your-tag"
echo "  docker push ${IMAGE_NAME}:your-tag"
