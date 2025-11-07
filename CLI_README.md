# CA_Embed CLI Tool

A command-line tool for processing parquet files and generating embeddings using the CA_Embed service.

## Features

- 📁 Read parquet files and extract text content
- 🔄 Skip records that already have embeddings (deduplication)
- 📊 Batch processing with configurable batch sizes
- 🎯 Progress tracking with ETA calculations
- 💾 Resume functionality for large datasets
- 🔍 Dry-run mode to preview what will be processed
- 🎨 Colored output with progress bars
- ⚠️ Comprehensive error handling
- 🚀 **Intelligent Caching System** - Automatically caches processed tweet data to dramatically speed up subsequent runs
- 💾 **Memory Efficient Streaming** - Processes large parquet files without loading everything into memory

## Installation

The CLI tool is included with the CA_Embed project. Make sure you have:

1. Bun installed
2. CA_Embed service running
3. Embedding generation enabled in your environment

## Usage

### Explore Parquet File Structure

Before processing, you can inspect the parquet file structure:

```bash
# Show headers and schema information
bun run cli process-parquet -f ./data/enriched_tweets.parquet --show-headers

# Check with custom column expectations
bun run cli process-parquet -f ./data/enriched_tweets.parquet --text-column "content" --id-column "id" --show-headers
```

### Basic Usage

```bash
# Process a parquet file
bun run cli process-parquet -f ./data/enriched_tweets.parquet

# Specify custom column names
bun run cli process-parquet -f ./data/enriched_tweets.parquet --text-column "content" --id-column "id"

# Use a different server URL
bun run cli process-parquet -f ./data/enriched_tweets.parquet -u http://localhost:8080

# Reprocess existing embeddings (normally skipped automatically)
bun run cli process-parquet -f ./data/enriched_tweets.parquet --reprocess-existing
```

### Advanced Options

```bash
# Process with custom batch size
bun run cli process-parquet -f ./data/enriched_tweets.parquet -b 50

# Default behavior: skip existing embeddings automatically
bun run cli process-parquet -f ./data/enriched_tweets.parquet

# Resume from previous run
bun run cli process-parquet -f ./data/enriched_tweets.parquet --resume-file ./state.json

# Dry run to see what would be processed
bun run cli process-parquet -f ./data/enriched_tweets.parquet --dry-run

# Limit number of records
bun run cli process-parquet -f ./data/enriched_tweets.parquet --max-records 1000


bun run cli process-parquet -f data/enriched_tweets.parquet --batch-size 1000 --parallel 20 --clear-cache
```

## Command Line Options

### `process-parquet`

| Option | Description | Default |
|--------|-------------|---------|
| `-f, --file <path>` | Path to the parquet file | **Required** |
| `-u, --url <url>` | CA_Embed server URL | `http://localhost:3000` |
| `-b, --batch-size <size>` | Batch size for processing | `1000` |
| `-p, --parallel <count>` | Number of parallel batches to process | `10` |
| `--text-column <name>` | Name of the text column | `full_text` |
| `--id-column <name>` | Name of the ID column | `tweet_id` |
| `--reprocess-existing` | Reprocess records that already have embeddings (skips by default) | `false` |
| `--resume-file <path>` | Resume from a previous run using a state file | |
| `--dry-run` | Show what would be processed without making API calls | `false` |
| `--max-records <count>` | Maximum number of records to process | |
| `--show-headers` | Show parquet file headers/schema and exit | `false` |
| `--no-context` | Disable conversation context processing (enabled by default) | `false` |
| `--max-text-length <length>` | Maximum length of processed text | `1024` |
| `--include-date` | Include creation date in processed text | `false` |
| `--export-html [path]` | Export HTML validation report (auto-named if no path provided) | |
| `--export-html-limit <limit>` | Limit number of tweets in HTML export for memory efficiency | `1000` |
| `--export-embeddings [path]` | Export embeddings to file (auto-saves to `./data/export-embeddings/` if no path provided) | |
| `--export-format <format>` | Format for embedding export: jsonl, csv, npy | `jsonl` |
| `--clear-cache` | Clear processing cache for this file before processing | `false` |
| `--disable-cache` | Disable reading from and writing to processing cache | `false` |
| `--show-cache-stats` | Show cache statistics and exit | `false` |

## Examples

### Explore File Structure First

```bash
# Before processing, check the file structure
bun run cli process-parquet --file ./data/enriched_tweets.parquet --show-headers
```

**Example Output:**
```
📋 Parquet File Schema:
📁 File: twitter_./data/enriched_tweets.parquet
🔢 Total columns: 15
📊 Total rows: 50000

📝 Column Details:
  📝 full_text - string (nullable)
  🔑 tweet_id - int64 (required)
     user_id - int64 (nullable)
     created_at - timestamp (nullable)
     retweet_count - int32 (nullable)
     ...

🔍 Sample Data (first row):
  📝 full_text: "This is an example tweet about machine learning..."
  🔑 tweet_id: 1234567890
     user_id: 987654321
     created_at: "2023-12-01T10:30:00Z"
     ...

💡 Usage Hints:
  ✅ Text column 'full_text' found
  ✅ ID column 'tweet_id' found

🚀 Ready to process! Remove --show-headers to start processing.
```

### Twitter Data Processing

```bash
# Process Twitter data with tweet-specific columns
bun run cli process-parquet \\
  --file twitter_./data/enriched_tweets.parquet \\
  --text-column "full_text" \\
  --id-column "tweet_id" \\
  --batch-size 200 \\
  --skip-existing
```

### Resume Large Dataset

```bash
# Start processing a large dataset
bun run cli process-parquet \\
  --file large_dataset.parquet \\
  --resume-file ./processing_state.json \\
  --batch-size 50

# If interrupted, resume from where you left off
bun run cli process-parquet \\
  --file large_dataset.parquet \\
  --resume-file ./processing_state.json
```

### Preview Processing

```bash
# See what would be processed without making API calls
bun run cli process-parquet \\
  --file ./data/enriched_tweets.parquet \\
  --dry-run \\
  --max-records 100
```

### Conversation Context Processing

```bash
# Default processing with conversation context (enabled by default)
bun run cli process-parquet \\
  --file twitter_./data/enriched_tweets.parquet \\
  --max-text-length 2048 \\
  --include-date \\
  --batch-size 100

# Disable conversation context processing if not needed
bun run cli process-parquet \\
  --file ./data/enriched_tweets.parquet \\
  --text-column "full_text" \\
  --id-column "tweet_id" \\
  --no-context \\
  --max-text-length 1500

# High-performance processing with larger batches and more parallelism
bun run cli process-parquet \\
  --file large_dataset.parquet \\
  --batch-size 1000 \\
  --parallel 20

# Force reprocessing of existing embeddings
bun run cli process-parquet \\
  --file ./data/enriched_tweets.parquet \\
  --reprocess-existing
```

### HTML Validation Export

```bash
# Generate HTML validation report for processed tweets (conversation context enabled by default)
bun run cli process-parquet \\
  --file ./data/enriched_tweets.parquet \\
  --export-html \\
  --dry-run \\
  --max-records 100

# Custom HTML output path
bun run cli process-parquet \\
  --file ./data/enriched_tweets.parquet \\
  --export-html "./validation-report.html" \\
  --dry-run

# Process tweets and generate validation report
bun run cli process-parquet \\
  --file ./data/enriched_tweets.parquet \\
  --export-html \\
  --batch-size 50

# Export more tweets for larger validation samples
bun run cli process-parquet \\
  --file ./data/enriched_tweets.parquet \\
  --export-html \\
  --export-html-limit 5000 \\
  --dry-run

# Disable conversation context if not needed
bun run cli process-parquet \\
  --file ./data/enriched_tweets.parquet \\
  --no-context \\
  --export-html \\
  --dry-run
```

### Embedding Export

```bash
# Export embeddings to JSONL format (saves to ./data/export-embeddings/)
bun run cli process-parquet \\
  --file ./data/enriched_tweets.parquet \\
  --export-embeddings

# Export to specific file in CSV format
bun run cli process-parquet \\
  --file ./data/enriched_tweets.parquet \\
  --export-embeddings "./my_embeddings.csv" \\
  --export-format csv

# Export embeddings in NumPy-compatible format
bun run cli process-parquet \\
  --file ./data/enriched_tweets.parquet \\
  --export-embeddings "./embeddings.npy" \\
  --export-format npy

# Process and export both HTML validation and embeddings
bun run cli process-parquet \\
  --file ./data/enriched_tweets.parquet \\
  --export-html \\
  --export-embeddings \\
  --export-format jsonl
```

## Output Example

```
✓ Loaded 10000 records from parquet file

📊 Processing Summary:
  📁 Total records in file: 10000
  ✅ Records to process: 8500
  ⏭️  Skipped (empty/invalid): 300
  🔄 Skipped (already processed): 1200

🚀 Processing 8500 records in 85 batches...

[██████████████████████████████] 100.0% (8500/8500) [156.2/s, ETA: 0s]

✨ Processing Complete!
  📊 Total processed: 8500
  ❌ Total errors: 0
  📈 Success rate: 100.0%
  💾 State saved to: ./processing_state.json
```

## Error Handling

The CLI tool includes comprehensive error handling:

- **Network errors**: Automatic retries with exponential backoff
- **Invalid data**: Skips records with missing or invalid content
- **API errors**: Continues processing remaining batches
- **Resume capability**: Save state to recover from interruptions

## Requirements

### Server Configuration

Make sure your CA_Embed server has embedding generation enabled:

```bash
# Environment variables
EMBEDDING_GENERATION_ENABLED=true
EMBEDDING_PROVIDER=deepinfra
EMBEDDING_MODEL=Qwen/Qwen3-Embedding-4B
EMBEDDING_API_KEY=your_api_key_here
```

### Parquet File Format

Your parquet file should contain:
- A text column (default: `full_text`) with the content to embed
- An ID column (default: `tweet_id`) with unique identifiers

For conversation context processing, additional fields are required:
- `conversation_id` - Groups tweets into conversations
- `reply_to_tweet_id` - Links replies to parent tweets
- `quoted_tweet_id` - Links quoted tweets
- `created_at` - Tweet creation timestamp
- `username` - Tweet author username

## Conversation Context Processing

By default, the CLI creates rich contextual embeddings by:

1. **Building conversation maps** - Groups tweets by conversation and builds parent-child relationships
2. **Assembling conversation threads** - Creates chronological conversation flows
3. **Including quoted content** - Adds quoted tweet text inline
4. **Smart truncation** - Prioritizes current tweet, immediate context, then conversation root
5. **Text cleaning** - Removes URLs, normalizes whitespace, and handles mentions

**Example processed output:**
```
[root] This is the original tweet that started the conversation
[context] This is a reply in the middle of the thread
[current] This is the current tweet being embedded
[quoted] Content from a quoted tweet is included here
```

**Benefits:**
- Better embeddings for replies and conversation context
- Preserved quoted tweet content
- Chronological conversation flow
- Smart truncation maintains most important context

## HTML Validation Export

The `--export-html` option generates a beautiful, interactive HTML report that lets you validate the text processing results:

### Features:
- **Side-by-side comparison** of original vs processed text
- **Visual indicators** for context inclusion, quotes, and truncation
- **Statistics dashboard** showing processing metrics
- **Responsive design** that works on desktop and mobile
- **Character count comparisons** to see text expansion/reduction
- **Conversation markers** highlighting [root], [context], [current], [quoted] parts

### Usage:
```bash
# Auto-generate timestamped filename
--export-html

# Specify custom path
--export-html "./my-validation-report.html"
```

The HTML report includes:
- **Tweet cards** with original and processed text
- **Processing badges** (Context, Quotes, Truncated, Simple)
- **Character count differences** with color coding
- **Conversation flow indicators** when context processing is enabled
- **Summary statistics** at the top of the report

## Embedding Export Formats

The CLI supports multiple output formats for embedding data. When using `--export-embeddings` without specifying a path, files are automatically saved to `./data/export-embeddings/` with timestamped filenames.

### JSONL (JSON Lines) - Default
- **File extension**: `.jsonl`
- **Format**: One JSON object per line
- **Easily readable**: Can be processed line-by-line
- **Appendable**: Easy to add new embeddings
- **Example**:
```json
{"key":"tweet_123","vector":[0.1,0.2,-0.3,...],"metadata":{"source":"parquet_import"},"timestamp":"2023-12-01T12:00:00.000Z"}
{"key":"tweet_124","vector":[0.2,0.1,-0.1,...],"metadata":{"source":"parquet_import"},"timestamp":"2023-12-01T12:00:01.000Z"}
```

### CSV (Comma-Separated Values)
- **File extension**: `.csv`
- **Format**: Traditional spreadsheet format
- **Headers**: `key,timestamp,dim_0,dim_1,...,metadata`
- **Excel compatible**: Easy to view in spreadsheet applications
- **Example**:
```csv
key,timestamp,dim_0,dim_1,dim_2,metadata
"tweet_123","2023-12-01T12:00:00.000Z",0.1,0.2,-0.3,"{\"source\":\"parquet_import\"}"
```

### NumPy (.npy)
- **File extension**: `.npy`
- **Format**: Binary format optimized for numerical data
- **Companion metadata**: Creates `_metadata.jsonl` file alongside
- **Python compatible**: Load with `numpy.load()`
- **Most efficient**: Smallest file size for large datasets

### Memory Efficiency
For large datasets (millions of tweets), the HTML export automatically limits to the first 1000 tweets by default to prevent excessive memory usage. You can adjust this with `--export-html-limit`:

```bash
# Default: Export first 1000 tweets
--export-html

# Export first 5000 tweets
--export-html --export-html-limit 5000

# Export first 100 tweets for quick validation
--export-html --export-html-limit 100
```

## Intelligent Caching System

The CLI features an advanced caching system that dramatically improves performance for repeated processing runs, especially when using conversation context processing.

### How It Works

1. **Hash-Based Validation**: Each parquet file is hashed along with processing options (text column, conversation context, max length, etc.)
2. **Automatic Caching**: When conversation context processing is enabled, processed tweet data is automatically cached to `./data/parquet-cache/`
3. **Smart Cache Invalidation**: Cache is automatically invalidated if:
   - Source parquet file changes
   - Processing options change (column names, max text length, etc.)
   - Cached files are corrupted or missing

### Performance Benefits

- **First Run**: Normal processing speed (builds conversation maps, processes all tweets)
- **Subsequent Runs**: 10-100x faster (loads from cache, skips conversation processing)
- **Memory Efficiency**: Cached data uses minimal memory compared to full conversation map building

### Cache Management Commands

```bash
# View cache statistics
bun run cli process-parquet --show-cache-stats

# Clear cache for a specific file before processing
bun run cli process-parquet -f ./data/enriched_tweets.parquet --clear-cache

# Disable caching completely (not recommended for conversation processing)
bun run cli process-parquet -f ./data/enriched_tweets.parquet --disable-cache
```

### Cache Storage

```
./data/parquet-cache/
├── my-tweets/                    # Cache for my-tweets.parquet
│   ├── cache-manifest.json      # Hash and metadata
│   ├── processed-tweets.jsonl   # Cached processed data
│   └── processed-tweets.metadata.json
└── large-dataset/               # Cache for large-dataset.parquet
    ├── cache-manifest.json
    ├── processed-tweets.jsonl
    └── processed-tweets.metadata.json
```

### Memory Considerations

- **With Caching**: Memory usage scales with batch size only (very efficient)
- **Without Caching**: Memory usage scales with full file size for conversation processing
- Use `--max-records` to limit memory usage during testing

## Performance Tips

1. **Batch Size**: Use 1000 (maximum) for best throughput with DeepInfra API
2. **Parallel Processing**: Increase `--parallel` up to 100 for faster processing (default: 10)
3. **Skip Existing**: Existing embeddings are skipped automatically (use `--reprocess-existing` to force reprocessing)
4. **Resume**: Use `--resume-file` for large datasets that might be interrupted
5. **Network**: Run CLI on the same machine as the CA_Embed server for best performance
6. **API Optimization**: Requests automatically use 1024 dimensions and normalization

## Troubleshooting

### Common Issues

1. **"Parquet file not found"**
   - Check the file path is correct
   - Ensure the file has `.parquet` extension

2. **"Column not found in parquet file"**
   - Use `--show-headers` to see all available columns in the file
   - Use `--text-column` and `--id-column` to specify correct column names
   - Check column names with a parquet viewer tool

3. **"Connection refused"**
   - Ensure CA_Embed server is running
   - Check the server URL with `--url` option

4. **"Embedding generation not enabled"**
   - Check server configuration
   - Ensure `EMBEDDING_GENERATION_ENABLED=true`

### Debug Mode

Add `LOG_LEVEL=debug` to see detailed processing information:

```bash
LOG_LEVEL=debug bun run cli process-parquet -f ./data/enriched_tweets.parquet
```

## State File Format

The resume state file contains:

```json
{
  "startTime": "2023-12-01T12:00:00.000Z",
  "lastUpdate": "2023-12-01T12:30:00.000Z",
  "processedKeys": ["id1", "id2", "id3"],
  "totalProcessed": 1500,
  "errors": []
}
```

This allows the CLI to skip already processed records on resume.