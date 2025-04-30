# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod ./
COPY go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o redis-clone

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/redis-clone .

# Create data directory for persistence
RUN mkdir -p /data

# Expose RESP and HTTP ports
EXPOSE 6379 8080

# Set environment variables
ENV REDIS_CLONE_RESP_PORT=6379 \
    REDIS_CLONE_HTTP_PORT=8080 \
    REDIS_CLONE_RDB_PATH="/data/dump.rdb"

# Run the server
CMD ["./redis-clone", "server"] 