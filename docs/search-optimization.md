# Search API - How It Works

This document explains how the embeddings search API works internally, including the optimizations that enable fast vector similarity search at scale.

## Overview

The search API finds semantically similar content by comparing vector embeddings. With 9+ million vectors, naive search would be too slow, so we use several optimizations:

1. **HNSW Index** - Approximate nearest neighbor graph for fast traversal
2. **Binary Quantization** - Compressed vectors stored in RAM
3. **Two-Phase Search** - Fast filter + accurate rescore

## How Search Works

### Two-Phase Search Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PHASE 1: FAST FILTER                        │
│                    (using quantized vectors in RAM)                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Query Vector ──► Quantize ──► Compare with quantized vectors      │
│                                (binary: 1024 bits = 128 bytes)     │
│                                                                     │
│  HNSW graph traversal using quantized similarities                 │
│  Result: Top N×oversampling candidates (e.g., 20 if k=10, 2x)     │
│                                                                     │
│  Speed: ~1-2ms (everything in RAM, bitwise operations)             │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                       PHASE 2: RESCORE                              │
│                  (using original vectors from disk)                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Load original vectors for only the top candidates                 │
│  Compute exact cosine similarity                                   │
│  Re-rank and return top k results                                  │
│                                                                     │
│  Speed: ~1-3ms (loading only 20 vectors instead of scanning 9M)   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Why Two Phases?

- **Phase 1** uses compressed vectors (128 bytes vs 4KB) stored entirely in RAM
- Binary comparison uses hardware-accelerated XOR + POPCOUNT operations
- This is 100-1000x faster than full vector comparison
- **Phase 2** ensures accuracy by re-ranking with original vectors
- Only loads ~20-30 vectors from disk instead of traversing millions

## Search Parameters

The API uses optimized default parameters:

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `hnsw_ef` | 128 | Search beam width (balances speed vs recall) |
| `rescore` | true | Re-rank results using original vectors |
| `oversampling` | 2.0 | Fetch 2x candidates before rescoring |

### What These Mean for You

- **~99.6% recall** - Results are nearly identical to exhaustive search
- **~2-5ms latency** - Fast enough for real-time applications
- **Accurate ranking** - Top results are re-scored with full precision vectors

## API Usage

### Search by Text (Recommended)

The API generates an embedding from your search term:

```json
POST /embeddings/search
{
  "searchTerm": "your search query here",
  "k": 10,
  "threshold": 0.5,
  "filter": {
    "must": [
      { "key": "username", "match": { "value": "someuser" } }
    ]
  }
}
```

### Search by Vector (Advanced)

If you have a pre-computed embedding:

```json
POST /embeddings/search
{
  "vector": [0.123, -0.456, ...],  // 1024-dimensional float array
  "k": 10,
  "threshold": 0.5
}
```

### Parameters

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `searchTerm` | string | Either this or `vector` | Text to search for |
| `vector` | number[] | Either this or `searchTerm` | Pre-computed 1024-dim embedding |
| `k` | number | No (default: 10) | Number of results to return |
| `threshold` | number | No | Minimum similarity score (0-1) |
| `filter` | object | No | Metadata filters (see below) |

### Filtering

Filters narrow results before vector similarity is computed:

```json
{
  "filter": {
    "must": [
      { "key": "username", "match": { "value": "someuser" } },
      { "key": "source", "match": { "value": "twitter" } }
    ]
  }
}
```

**Note**: Filter keys should NOT include `metadata.` prefix - the API adds it automatically.

Available filter fields:
- `username` - Exact match on username
- `source` - Content source (e.g., "twitter")
- `text` - Full-text search within content
- `original_text` - Full-text search in original text

## Response Format

```json
{
  "results": [
    {
      "key": "1234567890123456789",
      "distance": 0.892,
      "metadata": {
        "username": "someuser",
        "text": "The matched content...",
        "source": "twitter"
      }
    }
  ],
  "count": 10,
  "query_time_ms": 3.2
}
```

### Response Fields

| Field | Description |
|-------|-------------|
| `key` | Unique identifier for the matched item |
| `distance` | Cosine similarity score (0-1, higher = more similar) |
| `metadata` | Associated metadata for the matched item |

## Performance Expectations

| Metric | Value |
|--------|-------|
| Latency (p50) | ~2-3ms |
| Latency (p95) | ~5-10ms |
| Latency (p99) | ~15-20ms |
| Recall | ~99.6% |
| Throughput | ~100-200 req/s |

**Note**: Latency may increase with complex filters or during high load.

## Memory Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           RAM (~4-5 GB)                            │
├─────────────────────────────────────────────────────────────────────┤
│  HNSW Graph Index         ~2.4 GB                                  │
│  Quantized Vectors        ~1.1 GB (binary, 1-bit per dimension)   │
│  Payload Indexes          ~0.5 GB                                  │
├─────────────────────────────────────────────────────────────────────┤
│                           DISK (~40 GB)                            │
├─────────────────────────────────────────────────────────────────────┤
│  Original Vectors         ~35 GB (only loaded for rescoring)      │
│  Payloads                 ~5 GB                                    │
└─────────────────────────────────────────────────────────────────────┘
```

## Quantization Explained

### What is Quantization?

Quantization compresses vectors to use less memory:

| Type | Bits per Dimension | Compression | Memory (9M vectors) |
|------|-------------------|-------------|---------------------|
| Original | 32 (float) | 1x | ~35 GB |
| Scalar INT8 | 8 | 4x | ~8.8 GB |
| Binary 2-bit | 2 | 16x | ~2.2 GB |
| Binary 1-bit | 1 | 32x | ~1.1 GB |

### Why Binary Quantization?

We use 1-bit binary quantization because:

1. **Fits in RAM** - 1.1 GB for 9M vectors
2. **Hardware accelerated** - XOR + POPCOUNT are single CPU instructions
3. **High recall with rescoring** - ~99.6% accuracy

### The Tradeoff

Binary quantization alone has poor recall (~57%), but combined with rescoring using original vectors, we achieve ~99.6% recall while maintaining fast search.

## Technical Details

### HNSW (Hierarchical Navigable Small World)

HNSW is a graph-based approximate nearest neighbor algorithm:

- Vectors are nodes in a multi-layer graph
- Each node connects to its nearest neighbors
- Search starts at top layer, descends to find candidates
- `ef` parameter controls how many nodes to explore

### Binary Comparison

Binary vector comparison is extremely fast:

```
Original (Cosine Similarity):
  - 1024 multiplications
  - 1024 additions
  - Square root, division
  - ~100 CPU cycles

Binary (Hamming Distance):
  - XOR: 1 instruction
  - POPCOUNT: 1 instruction
  - ~2 CPU cycles
```

This 50x speedup, combined with RAM access (vs disk I/O), enables millisecond search.

## Troubleshooting

### Slow Searches

If searches are slow (>100ms):
1. Check if Qdrant is under memory pressure
2. Verify quantization is enabled (`quantization_config` in collection)
3. Check for complex filters that bypass indexes

### Low Recall / Missing Results

If expected results aren't returned:
1. Lower the `threshold` parameter
2. Increase `k` to get more candidates

### Connection Timeouts

If you get "operation was aborted" errors:
1. Check Qdrant server health
2. Verify network connectivity
3. Check if collection is still indexing (status should be "green")
