# CA_Embed Scripts

## Semantic Search Script

**Standalone script for semantic search** - Query your vector database using natural language text. Automatically generates embeddings and returns similar results without vector data.

### Quick Start

```bash
# Basic search
bun run scripts/semantic-search.ts "machine learning"

# Or use the npm script
bun run search "artificial intelligence"

# With options
bun run scripts/semantic-search.ts "neural networks" --k=5 --threshold=0.8

# Using the CLI command (recommended)
bun run cli search "deep learning" --k 10 --threshold 0.7
```

### Usage

```bash
bun run scripts/semantic-search.ts "your search query" [options]
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--url=<url>` | Server URL | `http://localhost:3000` |
| `--k=<number>` | Number of results to return | `10` |
| `--threshold=<number>` | Minimum similarity threshold (0.0-1.0) | none |
| `--filter=<json>` | Metadata filter as JSON (Qdrant only) | none |

### Examples

```bash
# Find top 5 similar items
bun run scripts/semantic-search.ts "quantum computing" --k=5

# High precision search (>80% similarity)
bun run scripts/semantic-search.ts "blockchain technology" --threshold=0.8

# Search with metadata filter (Qdrant)
bun run scripts/semantic-search.ts "javascript" --filter='{"category":"tutorial"}'

# Custom server URL
bun run scripts/semantic-search.ts "AI research" --url=http://production:3000
```

### What It Does

1. **Converts text to embedding** using your configured embedding provider (e.g., DeepInfra)
2. **Searches vector database** for similar items using cosine similarity
3. **Returns results without vectors** for cleaner output
4. **Pretty prints results** with colors and formatting
5. **Shows statistics** including similarity scores and search latency

### Output

```
🔍 Semantic Search
──────────────────────────────────────────────────
Query: machine learning
Server: http://localhost:3000
Results (k): 10
──────────────────────────────────────────────────

✅ Search completed in 245ms
Found 3 result(s)

─── Result 1 ───
Key: article_123
Distance: 0.9542
Similarity: 95.42%
Metadata:
  title: Introduction to Machine Learning
  category: technology

...
```

### Prerequisites

- Server must be running: `bun start`
- Embedding generation must be enabled in `.env`:
  ```bash
  EMBEDDING_GENERATION_ENABLED=true
  EMBEDDING_API_KEY=your_api_key
  ```

### CLI Command Alternative

For more features and better integration, use the CLI command:

```bash
# Pretty output (default)
bun run cli search "your query" --k 5 --threshold 0.8

# Raw JSON output
bun run cli search "your query" --no-pretty

# With metadata filter
bun run cli search "web dev" --filter '{"language":"javascript"}'

# Help
bun run cli search --help
```

**See [SEMANTIC_SEARCH_GUIDE.md](../SEMANTIC_SEARCH_GUIDE.md) for complete documentation.**

---

## Qdrant Import Script

**High-performance parallel importer** for transferring embeddings from pending parquet queue files directly into Qdrant vector store. Features real-time visual progress tracking and optimized batch processing.

### Quick Start

```bash
# Run with default settings (localhost:6333)
bun run scripts/import-to-qdrant.ts

# Or use the npm script
bun run import-to-qdrant

# Test with limited files (useful for testing)
# Bash/Unix:
MAX_FILES=10 bun run import-to-qdrant

# PowerShell:
$env:MAX_FILES="10"; bun run import-to-qdrant
```

### Advanced Usage

**Bash/Unix:**
```bash
# Custom configuration with parallelization
QDRANT_URL=http://localhost:6333 \
QDRANT_COLLECTION_NAME=my_embeddings \
BATCH_SIZE=200 \
MAX_PARALLEL_FILES=10 \
MAX_PARALLEL_BATCHES=5 \
bun run scripts/import-to-qdrant.ts

# Process only first 100 files (useful for testing or staged imports)
MAX_FILES=100 bun run scripts/import-to-qdrant.ts

# For Qdrant Cloud with high throughput
QDRANT_URL=https://your-cluster.qdrant.io \
QDRANT_API_KEY=your-api-key \
QDRANT_COLLECTION_NAME=embeddings \
MAX_PARALLEL_FILES=8 \
bun run scripts/import-to-qdrant.ts
```

**PowerShell:**
```powershell
# Custom configuration with parallelization
$env:QDRANT_URL="http://localhost:6333"; $env:QDRANT_COLLECTION_NAME="my_embeddings"; $env:BATCH_SIZE="200"; $env:MAX_PARALLEL_FILES="10"; $env:MAX_PARALLEL_BATCHES="5"; bun run scripts/import-to-qdrant.ts

# Process only first 100 files (useful for testing or staged imports)
$env:MAX_FILES="100"; bun run scripts/import-to-qdrant.ts

# For Qdrant Cloud with high throughput
$env:QDRANT_URL="https://your-cluster.qdrant.io"; $env:QDRANT_API_KEY="your-api-key"; $env:QDRANT_COLLECTION_NAME="embeddings"; $env:MAX_PARALLEL_FILES="8"; bun run scripts/import-to-qdrant.ts
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `QDRANT_URL` | `http://localhost:6333` | Qdrant server URL |
| `QDRANT_API_KEY` | (none) | API key for Qdrant Cloud (optional) |
| `QDRANT_COLLECTION_NAME` | `embeddings` | Name of the collection to import into |
| `QUEUE_DIR` | `./data/embedding-calls/embedding-queue` | Path to queue directory |
| `BATCH_SIZE` | `1000` | Number of vectors to upload per batch |
| `VECTOR_DIMENSION` | `1024` | Vector dimension size |
| `MAX_PARALLEL_FILES` | `8` | Number of files to process simultaneously |
| `MAX_PARALLEL_BATCHES` | `4` | Number of batches to upload in parallel per file |
| `SHARD_NUMBER` | `4` | Number of shards for parallel upload |
| `MAX_FILES` | (none) | Limit number of files to process (processes all if not set) |
| `OPTIMIZE_FOR_BULK` | `true` | Enable bulk upload optimizations (low RAM) |
| `ENABLE_INDEXING_AFTER` | `true` | Re-enable indexing after upload completes |
| `ERROR_LOG_FILE` | `./qdrant-import-errors.log` | Path to error log file |

### What It Does

1. **Connects to Qdrant** at the specified URL
2. **Creates collection if needed** with bulk upload optimizations:
   - Defers HNSW indexing (`m: 0`) to minimize RAM during upload
   - Uses disk storage (`on_disk: true`) to keep vectors on disk
   - Configures multiple shards for parallel upload
3. **Scans queue directory** for all `.pending.parquet` files
4. **Estimates total embeddings** efficiently (reads only last file metadata)
5. **Processes files in parallel** - up to `MAX_PARALLEL_FILES` simultaneously
6. **Reads embeddings** from each parquet file (flattened format with v0, v1, v2... columns)
7. **Uploads in parallel batches** - up to `MAX_PARALLEL_BATCHES` per file for maximum throughput
8. **Real-time progress display** with visual indicators showing:
   - Overall file and embedding progress with percentages
   - Upload rate (embeddings/sec) and ETA
   - Active workers with individual progress bars
   - Live status updates for each worker
   - Recent errors (last 5) displayed in real-time
9. **Re-enables indexing** after upload completes (builds HNSW in background)
10. **Does NOT move/delete files** - leaves them for the queue processor
11. **Comprehensive final report** with statistics and Qdrant collection info
12. **Error logging** - all errors saved to log file for debugging

### Performance Features

- ⚡ **Multi-level parallelization**: Process multiple files while uploading multiple batches per file
- 💾 **RAM optimized**: Bulk upload mode minimizes memory usage ([Qdrant docs](https://qdrant.tech/documentation/database-tutorials/bulk-upload/))
  - Defers HNSW indexing during upload (saves CPU & RAM)
  - Stores vectors directly on disk (uses mmap for minimal RAM)
  - Multiple shards for parallel WAL operations
- 📊 **Real-time progress tracking**: Beautiful visual display with progress bars, statistics, and live errors
- 🎯 **Optimized batch sizes**: Configurable batch sizes for optimal network utilization
- 🔄 **Graceful error handling**: Failed files don't stop the entire import
- 📝 **Error logging**: All errors logged to file with full stack traces
- 🏗️ **Background indexing**: HNSW index built in background after upload completes

### Sample Output

```
╔════════════════════════════════════════════════════════════════╗
║           QDRANT IMPORT - PARALLEL PROCESSING              ║
╚════════════════════════════════════════════════════════════════╝

📊 Overall Progress:
   Files:      12/50 completed (24.0%) 
   Embeddings: 125,430/520,000 uploaded (24.1%)
   Rate:       2,450 embeddings/sec
   Elapsed:    51s
   ETA:        2m 41s

🔄 Active Workers (5/5):
   a3f8c2b1 ███████████████░░░░░░░░░░░░░░░  52% ...queue-2025-10-31-xyz789.pending.parquet
            Uploading batches (1300/2500)
   7d4e9a2c ████████████████████████░░░░░░  82% ...queue-2025-10-31-abc456.pending.parquet
            Uploading batches (2050/2500)
   c1b8f3e7 ████████░░░░░░░░░░░░░░░░░░░░░░  28% ...queue-2025-10-31-def123.pending.parquet
            Uploading batches (700/2500)
   2f6a9d4b ███████████████████░░░░░░░░░░░  65% ...queue-2025-10-31-ghi789.pending.parquet
            Uploading batches (1625/2500)
   9e3c5b8a ██████████████████████████████ 100% ...queue-2025-10-31-jkl456.pending.parquet
            Uploading batches (2500/2500)

╔════════════════════════════════════════════════════════════════╗
║                    IMPORT COMPLETE                         ║
╚════════════════════════════════════════════════════════════════╝

📈 Final Statistics:
   ✓ Successful:  50/50 files
   📦 Embeddings:  520,000 uploaded
   ⏱️  Duration:    3m 32s
   ⚡ Avg Rate:    2,453 embeddings/sec

📊 Qdrant Collection:
   Total vectors: 520,000
   Status:        green
```

### Performance Tuning

Adjust these settings based on your setup:

**For Docker with NVMe SSD (recommended - optimized defaults):**
```bash
SHARD_NUMBER=4 MAX_PARALLEL_FILES=8 MAX_PARALLEL_BATCHES=4 BATCH_SIZE=1000
```
*Best for: Local Docker with fast storage. Expected: 3000-4000 embeddings/sec*

**For Local Qdrant (basic):**
```bash
SHARD_NUMBER=2 MAX_PARALLEL_FILES=5 MAX_PARALLEL_BATCHES=3 BATCH_SIZE=1000
```
*Best for: Standard local setup. Expected: 1500-2000 embeddings/sec*

**For Qdrant Cloud (high bandwidth):**
```bash
SHARD_NUMBER=6 MAX_PARALLEL_FILES=10 MAX_PARALLEL_BATCHES=5 BATCH_SIZE=1000
```
*Best for: Cloud with good network. Expected: 4000-6000 embeddings/sec*

**For Very High Throughput (more RAM):**
```bash
OPTIMIZE_FOR_BULK=false SHARD_NUMBER=6 MAX_PARALLEL_FILES=12 BATCH_SIZE=2500
```
*Best for: When RAM is not a concern. Expected: 5000-8000 embeddings/sec*

**For Large Files:**
```bash
SHARD_NUMBER=4 MAX_PARALLEL_FILES=3 MAX_PARALLEL_BATCHES=5 BATCH_SIZE=1000
```
*Best for: Few large files with many embeddings each*

**For Many Small Files:**
```bash
SHARD_NUMBER=4 MAX_PARALLEL_FILES=10 MAX_PARALLEL_BATCHES=2 BATCH_SIZE=1000
```
*Best for: Many files with fewer embeddings each*

### Notes

- ✅ **Idempotent**: Uses the `key` field as the Qdrant point ID - safe to re-run
- 🔢 **ID Format**: Keys are converted to **64-bit unsigned integers** for Qdrant
  - Uses JavaScript BigInt for proper handling of large integers (e.g., Twitter IDs like `850461037455343617`)
  - Converts to Number for Qdrant client, which handles them as u64 on the backend
  - Original string keys are preserved in the payload for reference
  - Works correctly even for IDs beyond JavaScript's safe integer range (2^53-1)
- 💾 **Metadata preserved**: All queue metadata stored in Qdrant payload
- 📁 **Non-destructive**: Files remain in place for the queue processor
- 🎯 **Deduplication**: Duplicate keys are automatically handled by Qdrant upsert
- ⚡ **Fast**: Achieves 2000-5000+ embeddings/sec with proper tuning
- 🔧 **Bulk optimizations**: Auto-enabled for new collections (disable with `OPTIMIZE_FOR_BULK=false`)
- 🔍 **Error visibility**: Live error display + detailed error log for debugging
- 🏗️ **Indexing**: For new collections, HNSW builds in background after upload (monitor in Qdrant dashboard)

---

## Queue Items Analysis

This script analyzes queue items (failed or completed) and checks which ones already exist in the CoreNN database.

### Usage

```bash
# Analyze failed queue items (default)
bun run analyze-failed-queue
# or
bun run scripts/analyze-failed-queue-items.ts --type failed

# Analyze completed queue items
bun run analyze-completed-queue
# or
bun run scripts/analyze-failed-queue-items.ts --type completed

# Show help
bun run scripts/analyze-failed-queue-items.ts --help
```

### Environment Variables

You can customize the paths using environment variables:

```bash
# Custom database path
CORENN_DB_PATH=./data/embeddings.db bun run scripts/analyze-failed-queue-items.ts

# Custom queue directory
QUEUE_DIR=./data/embedding-calls/embedding-queue bun run scripts/analyze-failed-queue-items.ts

# Custom vector dimension
VECTOR_DIMENSION=1024 bun run scripts/analyze-failed-queue-items.ts
```

### What It Does

1. **Connects to your CoreNN database**
2. **Finds all queue files** (`.failed.jsonl` or `.completed.jsonl`) in your queue directory
3. **Extracts all embeddings** (keys + vectors) from queue items
4. **Uses vector similarity search** to check if each embedding exists in the database
   - Queries the database with each vector
   - Considers it a match if distance < 0.0001 and key matches
5. **Generates a comprehensive report** showing:
   - Per-file statistics (how many embeddings from each file are already in DB)
   - Overall statistics across all queue files
   - Sample existing/missing keys for debugging
   - Analysis specific to the queue type (failed vs completed)

### Sample Output

**For Failed Items:**
```
📊 ❌ FAILED QUEUE ITEMS ANALYSIS REPORT
================================================================================

🔢 OVERALL STATISTICS:
   Total failed files: 40
   Total failed items: 767
   Items already in DB: 650 (84.7%)
   Items missing from DB: 117 (15.3%)

💡 ANALYSIS:
   🎯 HIGH DUPLICATION: 84.7% of failed items already exist in DB
   📋 This suggests a double-processing issue - items are being processed successfully
      but then retried and failing on the retry attempt.
```

**For Completed Items:**
```
📊 ✅ COMPLETED QUEUE ITEMS ANALYSIS REPORT
================================================================================

🔢 OVERALL STATISTICS:
   Total completed files: 40
   Total completed items: 767
   Items already in DB: 767 (100.0%)
   Items missing from DB: 0 (0.0%)

💡 ANALYSIS:
   ✅ EXCELLENT: 100.0% of completed items are properly stored in DB
   📋 The queue processing is working correctly for completed items.
```

### Interpreting Results

**For Failed Items:**
- **High % in DB (>80%)**: Double-processing issue - items succeeded initially but failed on retry
- **Low % in DB (<20%)**: Genuine processing failures - items are failing to insert initially  
- **Mixed pattern**: Combination of both issues

**For Completed Items:**
- **High % in DB (>95%)**: Queue is working correctly - completed items are properly stored
- **Low % in DB (<80%)**: Concerning - completed items are not being stored in database
- **Mixed pattern**: Some completed items are missing from database

The script will also save a detailed JSON report with all the data for further analysis.


docker run -p 6333:6333 -p 6334:6334    -v "$(pwd)/qdrant_storage:/qdrant/storage:z"  qdrant/qdrant