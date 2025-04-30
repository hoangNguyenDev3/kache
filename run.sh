#!/bin/bash

# Stop and remove any existing container
if docker ps -a | grep -q redis-clone; then
  echo "Stopping and removing existing container..."
  docker stop redis-clone
  docker rm redis-clone
fi

# Build the Docker image
echo "Building Docker image..."
docker build -t redis-clone .

# Run the Redis clone container
echo "Starting Redis clone container..."
docker run -d \
  --name redis-clone \
  -p 6379:6379 \
  -p 8080:8080 \
  -v redis-data:/data \
  redis-clone

echo "Container started! Connect using:"
echo "  redis-cli -p 6379"
echo ""
echo "HTTP API available at http://localhost:8080" 