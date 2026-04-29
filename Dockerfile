# Build stage
FROM golang:1.25-alpine AS builder

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
RUN CGO_ENABLED=0 GOOS=linux go build -o kache

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/kache .

# Create data directory for persistence
RUN mkdir -p /data

# Expose RESP and HTTP ports
EXPOSE 6379 8080

# Set environment variables
ENV KACHE_RESP_PORT=6379 \
    KACHE_HTTP_PORT=8080 \
    KACHE_RDB_PATH="/data/dump.rdb"

# Run the server
CMD ["./kache", "server"] 