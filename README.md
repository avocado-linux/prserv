# PR Server (prserv)

A high-performance Package Revision (PR) server for BitBake builds, rewritten in Rust with immediate SQLite persistence for reliability.

## Features

- **Configurable sync modes** - Choose between immediate, periodic, or on-shutdown persistence for optimal performance/safety balance
- **Async TCP server** - High-performance async networking with tokio
- **BitBake compatibility** - Drop-in replacement for the Python prserv
- **History and no-history modes** - Supports both PR increment modes
- **Read-only mode** - Safe querying without modifications
- **Structured logging** - Comprehensive logging with tracing
- **Type-safe errors** - Rust's error handling for reliability

## Architecture Improvements

This Rust implementation addresses key issues in the original Python prserv:

1. **Configurable persistence**: Choose sync mode based on your needs - immediate for safety, periodic for balance, or on-shutdown for speed
2. **Better concurrency**: Uses SQLite WAL mode for improved concurrent access
3. **Memory safety**: Rust's ownership system prevents common memory issues
4. **Performance**: Compiled binary with optimized SQLite operations and buffered writes

## Installation

Build from source:
```bash
cargo build --release
```

The binary will be available at `target/release/prserv`.

## Usage

### Starting the Server

```bash
# Start server with default settings (localhost:8585, prserv.db)
prserv server

# Custom database and port
prserv server --database /path/to/prserv.db --bind 0.0.0.0:9090

# Read-only mode
prserv server --read-only

# No-history mode (no PR decrements)
prserv server --nohist

# Different sync modes
prserv server --sync-mode immediate        # Default: safest, writes immediately
prserv server --sync-mode periodic:30     # Balanced: flush every 30 seconds
prserv server --sync-mode on_shutdown     # Fastest: only sync on shutdown
```

### Client Commands

```bash
# Get a PR value (creates if doesn't exist)
prserv client get-pr "1.0" "x86_64" "abc123hash"

# Test if PR exists
prserv client test-pr "1.0" "x86_64" "abc123hash"

# Check if package exists
prserv client test-package "1.0" "x86_64"

# Get maximum PR for package
prserv client max-package-pr "1.0" "x86_64"

# Import a PR value
prserv client import-one "1.0" "x86_64" "abc123hash" 42

# Export data
prserv client export --colinfo

# Check server status
prserv client ping
prserv client is-readonly
```

## Database Schema

The server uses SQLite with the following table structure:

```sql
CREATE TABLE PRMAIN_hist (
    version TEXT NOT NULL,
    pkgarch TEXT NOT NULL,
    checksum TEXT NOT NULL,
    value INTEGER NOT NULL,
    PRIMARY KEY (version, pkgarch, checksum)
);

CREATE TABLE PRMAIN_nohist (
    version TEXT NOT NULL,
    pkgarch TEXT NOT NULL,
    checksum TEXT NOT NULL,
    value INTEGER NOT NULL,
    PRIMARY KEY (version, pkgarch, checksum)
);
```

## Configuration

The server supports JSON/YAML configuration files:

```yaml
# config.yaml
server:
  database: "/var/lib/prserv/prserv.db"
  bind_addr: "0.0.0.0:8585"
  read_only: false
  nohist: false
  sync_mode: immediate  # or periodic: { interval_secs: 30 } or on_shutdown
client:
  server_addr: "localhost:8585"
```

## Protocol

The server implements a JSON-RPC style protocol over TCP:

### Request Format
```json
{
    "id": "unique-request-id",
    "method": "get-pr",
    "params": {
        "version": "1.0",
        "pkgarch": "x86_64",
        "checksum": "abc123hash"
    }
}
```

### Response Format
```json
{
    "id": "unique-request-id",
    "result": {"value": 42},
    "error": null
}
```

### Supported Methods

- `get-pr`: Get/create PR value
- `test-pr`: Test if PR exists
- `test-package`: Test if package exists
- `max-package-pr`: Get maximum PR for package
- `import-one`: Import PR value
- `export`: Export PR data
- `is-readonly`: Check server mode
- `ping`: Health check

## Differences from Python Version

1. **Configurable sync modes**: Choose between immediate, periodic, or on-shutdown persistence
2. **WAL mode**: Uses SQLite WAL journaling for better concurrency
3. **Buffered writes**: Periodic and on-shutdown modes use intelligent write batching
4. **Better error handling**: Type-safe error propagation
5. **Async architecture**: Non-blocking I/O for better performance

## Sync Mode Details

### Immediate Mode (Default)
- **Safety**: ⭐⭐⭐⭐⭐ Highest - Every write is immediately committed to disk
- **Performance**: ⭐⭐ Slowest - Each operation waits for disk I/O
- **Use case**: Production builds where data integrity is critical

### Periodic Mode
- **Safety**: ⭐⭐⭐⭐ High - Writes are flushed every N seconds
- **Performance**: ⭐⭐⭐⭐ Balanced - Good throughput with regular safety checkpoints
- **Use case**: Development/CI environments with moderate safety requirements
- **Configuration**: `--sync-mode periodic:30` (flush every 30 seconds)

### On-Shutdown Mode
- **Safety**: ⭐⭐ Lowest - Data only written on graceful shutdown
- **Performance**: ⭐⭐⭐⭐⭐ Fastest - All writes are buffered in memory
- **Use case**: Testing or temporary builds where speed matters most
- **Risk**: Data loss if process crashes or is killed ungracefully

## Development

### Building
```bash
cargo build
```

### Testing
```bash
cargo test
```

### Running with logs
```bash
RUST_LOG=debug cargo run -- server
```

### Environment Variables

You can configure the server using environment variables:

```bash
# Server configuration
export PRSERV_DATABASE="/var/lib/prserv/prserv.db"
export PRSERV_BIND_ADDR="0.0.0.0:8585"
export PRSERV_READ_ONLY="false"
export PRSERV_NOHIST="false"
export PRSERV_SYNC_MODE="periodic:60"  # or "immediate" or "on_shutdown"

# Client configuration
export PRSERV_SERVER_ADDR="localhost:8585"

# Start server with environment config
prserv server
```

## License

Apache-2.0
