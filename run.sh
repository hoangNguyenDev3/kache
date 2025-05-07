#!/bin/bash

# Stop and remove any existing container
if docker ps -a | grep -q kache; then
  echo "Stopping and removing existing container..."
  docker stop kache
  docker rm kache
fi

# Build the Docker image
echo "Building Docker image..."
docker build -t kache .

# Run the Kache container
echo "Starting Kache container..."
docker run -d \
  --name kache \
  -p 6379:6379 \
  -p 8080:8080 \
  -v kache-data:/data \
  kache

echo "Container started! Connect using:"
echo "  redis-cli -p 6379"
echo ""
echo "HTTP API available at http://localhost:8080" 