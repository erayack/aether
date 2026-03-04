# Aether

Aether is a high-performance, embedded key-value storage engine written in Rust, based on a **LSM-tree** architecture. It provides crash-recoverable durability, low-latency lookups, and background data maintenance.

Currently evolving into a high-throughput read engine (**Iteration v3**) with pinned snapshots and shared caching.

## Features

- **LSM-Tree Architecture**: Efficient write handling via MemTables and tiered SSTables.
- **Crash Recovery**: Write-Ahead Log (WAL) with CRC32 integrity checks and automatic tail repair.
- **Atomic Operations**: Support for multi-key **Write Batches** with all-or-nothing durability.
- **Range Scans**: Efficient forward scans with deterministic visibility across all levels.
- **Space Efficiency**: **Prefix Delta Encoding** and block-level compression (**Snappy**, **Zstd**).
- **Read Optimization**: Per-table **Bloom Filters**, sparse indices, and a shared **LRU Block Cache** (v3).
- **Pinned Snapshots**: Consistent read-only views pinned to a sequence number (v3).
- **High-Throughput IO**: Zero-copy **Memory-Mapped (mmap)** SST reads (v3).

## Architecture

1. **Write Path**: `WriteBatch` → `WAL` (Append) → `MemTable` (In-memory BTreeMap).
2. **Read Path**: `Snapshot` → `MemTable` → `Immutable MemTable` → `L0 SSTables` → `L1 SSTables`.
3. **Compaction**: Merges L0 tables into L1 to reclaim space, maintaining the non-overlapping L1 invariant.
4. **Consistency**: MANIFEST-based state tracking with atomic commits and sequence-based visibility.

## Usage

### Installation

```bash
cargo build --release
```

### CLI Commands

The `aether` binary provides a comprehensive interface for database operations:

```bash
# Put a key-value pair
./target/release/aether put --db ./data --key "user:1" --value "Alice"

# Get a value (supports hex: or 0x prefix for binary data)
./target/release/aether get --db ./data --key "user:1"

# Delete a key
./target/release/aether delete --db ./data --key "user:1"

# Range scan with bounds and limits
./target/release/aether scan --db ./data --start "user:1" --end "user:9" --limit 10

# Atomic Write Batch (ingest from JSON file)
./target/release/aether write-batch --db ./data --ops-json batch.json

# Force a manual flush of the memtable to disk
./target/release/aether flush --db ./data
```

## Configuration

Engine behavior can be tuned via `EngineOptions` (see `src/config.rs`) or global CLI flags:

- `memtable_max_bytes`: Threshold for rotating memtables.
- `fsync_policy`: Control WAL durability (`Always`, `EveryMillis`, or `Never`).
- `l0_compaction_trigger`: Number of L0 tables before compaction starts.
- `compression_codec`: SST block compression (`None`, `Snappy`, `Zstd`).
- `prefix_restart_interval`: Restart interval for prefix-delta encoded blocks.
- `enable_mmap_reads`: Enable memory-mapped file access for SSTables (v3).
- `block_cache_capacity_bytes`: Shared LRU cache size for decoded blocks (v3).
- `max_open_snapshots`: Hard limit on concurrent pinned snapshots (v3).

## License

This project is licensed under the Apache License 2.0.
