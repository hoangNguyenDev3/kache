FROM golang:1.16-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum files first for better caching
COPY go.mod .
COPY go.sum .
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o redis-clone

# Use a smaller base image for the final stage
FROM alpine:latest

# Install necessary runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/redis-clone /app/redis-clone

# Expose the ports for the Redis clone
EXPOSE 6379
EXPOSE 8080

# Command to run when the container starts
ENTRYPOINT ["/app/redis-clone"]
CMD ["server"] 