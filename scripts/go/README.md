# Qdrant Export Tools (Go)

Go tools for exporting and downloading Qdrant vector data to/from cloud storage (Cloudflare R2 or Supabase Storage).

## Features

- **Two-pass parallel export**: Fast ID collection, then parallel vector retrieval
- **Memory efficient**: Streams data with bounded memory usage
- **Multiple strategies**: NDJSON+DuckDB or direct Parquet writing
- **HTTP and gRPC support**: Use `--http` when gRPC port is blocked
- **Resume support**: Can resume from existing NDJSON or Parquet files
- **Debug mode**: Keep temp files, confirm before upload
- **Streaming upload**: Upload directly to R2 without disk storage
- **Native FLOAT[] arrays**: Vectors stored as queryable float32 arrays in Parquet

## Prerequisites

1. **Go 1.21+**
   ```bash
   winget install GoLang.Go
   go version
   ```

2. **DuckDB CLI** (only for `ndjson-gz` strategy)
   ```bash
   winget install DuckDB.cli
   duckdb --version
   ```

## Setup

1. Navigate to this directory:
   ```bash
   cd scripts/go
   ```

2. Download dependencies:
   ```bash
   go mod tidy
   ```

3. Create a `.env` file (or use environment variables):
   ```bash
   # Qdrant
   QDRANT_URL=localhost:6334
   QDRANT_API_KEY=your-api-key
   QDRANT_COLLECTION_NAME=embeddings
   QDRANT_PORT=6333  # Optional, for HTTP mode

   # R2/S3
   R2_ACCESS_KEY_ID=your-access-key
   R2_SECRET_ACCESS_KEY=your-secret-key
   R2_ENDPOINT=https://xxx.r2.cloudflarestorage.com
   R2_BUCKET=your-bucket-name
   ```

   Note: The tool also looks for `.env` in parent directories (`../`, `../../`), so you can use the project root's `.env`.

## Usage

### Run directly with Go

```bash
# Basic export (uses gRPC, default strategy)
go run qdrant_exporter_r2.go

# Use HTTP instead of gRPC (when gRPC port is blocked)
go run qdrant_exporter_r2.go --http

# With options
go run qdrant_exporter_r2.go --parallel 8 --batch-size 5000

# Debug mode (keep files, ask before upload)
go run qdrant_exporter_r2.go --debug

# Resume from previous run
go run qdrant_exporter_r2.go --resume

# Dry run (show what would be exported)
go run qdrant_exporter_r2.go --dry-run

# Direct parquet export (faster, no DuckDB needed)
go run qdrant_exporter_r2.go --strategy parquet-med --http
```

### Build and run as executable

```bash
# Build R2 exporter
go build -o qdrant_exporter_r2.exe qdrant_exporter_r2.go

# Build Supabase exporter
go build -o qdrant_exporter_supabase.exe qdrant_exporter_supabase.go

# Run
./qdrant_exporter_r2.exe --parallel 4
./qdrant_exporter_supabase.exe --parallel 4
```

### Cross-compile for Linux

```bash
# R2 exporter
GOOS=linux GOARCH=amd64 go build -o qdrant_exporter_r2 qdrant_exporter_r2.go

# Supabase exporter
GOOS=linux GOARCH=amd64 go build -o qdrant_exporter_supabase qdrant_exporter_supabase.go
```

## Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `--collection` | Qdrant collection name | from env |
| `--batch-size` | Points per scroll batch | 10000 |
| `--retrieve-batch` | Points per retrieve batch | batch-size / 2 |
| `--parallel` | Number of parallel workers | NumCPU |
| `--resume` | Resume from existing files | false |
| `--debug` | Keep files, ask before upload | false |
| `--dry-run` | Show what would be done | false |
| `--strategy` | Export strategy (see below) | ndjson-gz |
| `--row-group-size` | Override row group size | strategy default |
| `--stream-upload` | Stream directly to R2 (no disk) | false |
| `--http` | Use HTTP REST API instead of gRPC | false |

## Export Strategies

The exporter supports multiple strategies for different tradeoffs:

| Strategy | Row Group Size | Memory Usage | Disk Usage | Speed | Use Case |
|----------|---------------|--------------|------------|-------|----------|
| `ndjson-gz` | N/A | **Lowest** (~500MB) | Medium | Medium | Default, safest for any data size |
| `parquet-small` | 1,000 | Low (~1GB) | Larger | Slower | Memory-constrained, frequent flushes |
| `parquet-med` | 10,000 | Medium (~2-4GB) | Medium | Medium | Balanced approach |
| `parquet-large` | 100,000 | High (~10GB+) | Smallest | Fastest | High-memory machines only |

### Strategy Details

**ndjson-gz (default)** - Memory-safe streaming
- Writes gzipped NDJSON, then uses DuckDB CLI to convert to Parquet
- Memory usage is bounded regardless of data size
- Requires DuckDB CLI installed
- Best for large datasets where you must stay under a memory limit
- Vectors stored as `FLOAT[]` (32-bit) in final Parquet

**parquet-small/med/large** - Direct Parquet writing
- Writes directly to Parquet using parquet-go library
- No DuckDB dependency required
- Memory usage = row_group_size × vector_dim × 4 bytes + overhead
- Faster than ndjson-gz (no conversion step)
- Vectors stored as native `FLOAT[]` arrays (queryable)
- For `parquet-small` with 1024-dim vectors: ~1000 × 1024 × 4 = ~4MB per row group

### Output Format

Both strategies produce Parquet files with the same schema:

| Column | Type | Description |
|--------|------|-------------|
| `key` | VARCHAR | Point identifier (from payload or point ID) |
| `metadata` | VARCHAR | JSON string of point metadata |
| `vector` | FLOAT[] | Native float32 array (1024 elements) |

The `FLOAT[]` format allows direct SQL operations:
```sql
SELECT key, vector[1:5] FROM read_parquet('export.parquet') LIMIT 10;
```

### Choosing a Strategy

```bash
# Safe default for any data size (requires DuckDB)
go run qdrant_exporter_r2.go --strategy ndjson-gz

# Direct parquet - faster, no DuckDB needed
go run qdrant_exporter_r2.go --strategy parquet-med

# Testing memory usage with small row groups
go run qdrant_exporter_r2.go --strategy parquet-small

# Custom row group size
go run qdrant_exporter_r2.go --strategy parquet-med --row-group-size 5000

# High-memory machine, maximum speed
go run qdrant_exporter_r2.go --strategy parquet-large 

# High-memory machine, maximum speed, stream as it comes
go run qdrant_exporter_r2.go --strategy parquet-large --stream-upload
```

## HTTP vs gRPC

By default, the exporter uses gRPC (port 6334). If the gRPC port is blocked (common in some cloud setups), use HTTP:

```bash
# Use HTTP REST API instead of gRPC
go run qdrant_exporter_r2.go --http

# HTTP uses QDRANT_PORT env var (default: 6333)
QDRANT_PORT=8080 go run qdrant_exporter_r2.go --http
```

## Streaming Upload (No Disk)

For parquet strategies, you can stream directly to R2 **without storing anything on disk**:

```bash
# Stream parquet directly to R2 - zero disk usage!
go run qdrant_exporter_r2.go --strategy parquet-small --stream-upload

# With custom row group size
go run qdrant_exporter_r2.go --strategy parquet-med --row-group-size 5000 --stream-upload
```

**How it works:**
```
[Qdrant] → [Parquet Writer] → [io.Pipe] → [S3 Multipart Upload] → [R2]
```

The parquet library writes to an `io.Pipe()`, which the S3 uploader reads from concurrently. This means:
- Zero disk I/O
- Memory usage depends only on row group size + upload buffer
- Network becomes the bottleneck (which is usually fine)

**Limitations:**
- Only works with parquet strategies (not ndjson-gz)
- No resume capability (if upload fails, must restart)
- Debug mode confirmation not supported

## Architecture: Go-Style Pipeline

This Go version uses a **streaming pipeline** with channels:

```
[ID Producer] --idChan--> [Retrieve Workers] --dataChan--> [Single Writer]
     |                          |                               |
  Scroll API              N goroutines                    Parquet/NDJSON
  (no vectors)            (parallel HTTP/gRPC)            (single file)
```

### Pipeline Features

| Aspect | Description |
|--------|-------------|
| Memory | Streaming, bounded by channel buffers |
| Backpressure | Built-in via channel blocking |
| Cancellation | Context propagation via errgroup |
| Error handling | errgroup cancels all on first error |
| Retries | HTTP client retries with exponential backoff |

### Go-Specific Optimizations

1. **Channels for backpressure** - If the writer is slow, workers block automatically
2. **errgroup** - Proper error propagation and automatic cancellation
3. **Single writer** - No file locking, no merge step needed
4. **Buffered I/O** - 64KB buffer reduces syscalls
5. **Buffer reuse** - `strings.Builder` reused per worker to reduce GC pressure
6. **Context cancellation** - Ctrl+C gracefully stops all goroutines
7. **NumCPU default** - Automatically uses all CPU cores
8. **json.Number** - Preserves precision for large integer IDs (Twitter snowflake IDs)

## Performance

Typical throughput:
- ID scroll: ~50,000+ IDs/sec (no vectors = fast)
- Vector retrieval: ~3,000-4,000 pts/sec (HTTP, depends on network)
- File write: Not a bottleneck (buffered, single writer)

Example: 6.7M vectors exported in ~29 minutes (~3,850 pts/sec)

## Troubleshooting

### "failed to connect to Qdrant"
- For gRPC: Check `QDRANT_URL` - should be the gRPC port (default: 6334)
- For HTTP: Use `--http` flag and check `QDRANT_PORT` (default: 6333)
- Verify Qdrant is running: `curl http://localhost:6333/collections`

### "duckdb: command not found"
- Install DuckDB CLI: `winget install DuckDB.cli`
- Restart terminal after installation
- Or use `--strategy parquet-med` to skip DuckDB entirely

### Low hit rate / missing points
- If using large integer IDs (like Twitter snowflake IDs), ensure you're using the latest version
- The tool uses `json.Number` to preserve precision for IDs > 2^53

### API key issues
- If using Qdrant Cloud, set `QDRANT_API_KEY`
- For local Qdrant without auth, leave it empty

## Downloading from R2

Use the `qdrant_downloader.go` script to download exported parquet files from R2:

```bash
# List all files in the bucket
go run qdrant_downloader.go --list

# Download the latest export for the default collection
go run qdrant_downloader.go

# Download a specific collection's export
go run qdrant_downloader.go --collection my-vectors

# Download to a custom path
go run qdrant_downloader.go --output ./data/vectors.parquet
```

### Download Options

| Flag | Description | Default |
|------|-------------|---------|
| `--list` | List all files in bucket | false |
| `--collection` | Collection name | from QDRANT_COLLECTION_NAME env |
| `--output` | Output file path | ./{collection}/latest.parquet |

### Build download tool

```bash
go build -o qdrant_downloader.exe qdrant_downloader.go
./qdrant_downloader.exe --list
```

## Comparison with TypeScript Version

| Aspect | TypeScript (Bun) | Go |
|--------|-----------------|-----|
| Runtime | Requires Bun | Single binary |
| Startup | ~100ms | ~10ms |
| Memory | Higher (V8) | Lower |
| Concurrency | Workers | Goroutines |
| Dependencies | node_modules | Compiled in |
| HTTP/gRPC | HTTP only | Both supported |
