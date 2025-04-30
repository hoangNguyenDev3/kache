#!/bin/bash

# Build and run the Redis clone
# Usage: ./run.sh [server]

# Ensure script exits on any error
set -e

# Variables
GO_CMD=go
BUILD_CMD="$GO_CMD build -o redis-clone"
RUN_CMD="./redis-clone"

# Ensure proper error messages
function error_exit {
    echo "Error: $1" >&2
    exit 1
}

# Build the application
echo "Building Redis clone..."
$BUILD_CMD || error_exit "Failed to build"
echo "Build successful!"

# Run the application
if [ "$1" == "server" ]; then
    echo "Starting Redis clone server..."
    $RUN_CMD server
else
    echo "Starting Redis clone..."
    $RUN_CMD
fi 