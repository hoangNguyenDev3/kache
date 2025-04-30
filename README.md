# Redis Clone

A production-grade Redis clone implemented in Go.

## Features

- In-memory key-value store
- RESP protocol support
- HTTP API
- Basic Redis commands (GET, SET, INCR, etc.)
- Hash data type (HSET, HGET, etc.)
- Key expiration (EXPIRE, TTL)
- Persistence (RDB/AOF)

## Getting Started

### Prerequisites

- Go 1.16 or later

### Building

```
go build -o redis-clone
```

Or use the provided script:

```
./run.sh
```

### Running

```
./redis-clone server
```

Or:

```
./run.sh server
```

## Configuration

The server can be configured using command-line flags:

```
# Start with custom ports
./redis-clone server --resp-port 6380 --http-port 8081

# Use a configuration file
./redis-clone server --config config.yaml
```

## Command-line Options

```
  --resp-port int       RESP protocol port (default 6379)
  --http-port int       HTTP API port (default 8080)
  --auth-token string   Authentication token for HTTP API
  --rdb-enabled         Enable RDB persistence (default true)
  --rdb-path string     Path to RDB file (default "dump.rdb")
  --aof-enabled         Enable AOF persistence (default true)
  --aof-path string     Path to AOF file (default "appendonly.aof")
  --log-level string    Logging level (debug, info, warn, error) (default "info")
```

## Supported Commands

### String Operations
- SET
- GET
- DEL
- INCR

### Hash Operations
- HSET
- HGET
- HDEL
- HGETALL
- HLEN

### Key Management
- EXPIRE
- TTL
- KEYS

## Architecture

The Redis clone is composed of several components:

- Command-line interface (CLI) - Using Cobra and Viper for configuration
- TCP server - Implements the RESP protocol
- HTTP server - RESTful API
- Store - In-memory data store with support for different data types
- Persistence - RDB and AOF persistence mechanisms