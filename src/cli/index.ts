#!/usr/bin/env bun

import { Command } from 'commander';
import { config } from 'dotenv';
import { processParquetCommand } from './commands/process-parquet.js';
import { searchCommand } from './commands/search.js';

config();

const program = new Command();

program
  .name('ca_embed-cli')
  .description('CLI tools for CA_Embed - Process parquet files and generate embeddings')
  .version('1.0.0');

program
  .command('process-parquet')
  .description('Process a parquet file and generate embeddings for text content using batch files (default)')
  .requiredOption('-f, --file <path>', 'Path to the parquet file')
  .option('-u, --url <url>', 'CA_Embed server URL', 'http://localhost:3000')
  .option('--api-key <key>', 'API key for authentication (or set EMBED_SERVICE_CLIENT_API env var)', process.env.EMBED_SERVICE_CLIENT_API)
  .option('-b, --batch-size <size>', 'Batch size for processing (records per batch file)', '1000')
  .option('-p, --parallel <count>', 'Number of parallel batches to process simultaneously', '10')
  .option('--text-column <name>', 'Name of the text column', 'full_text')
  .option('--id-column <name>', 'Name of the ID column', 'tweet_id')
  .option('--reprocess-existing', 'Reprocess records that already have embeddings (skips by default)', false)
  .option('--resume-file <path>', 'Resume from a previous run using a state file (legacy streaming mode only)')
  .option('--dry-run', 'Show what would be processed without making API calls', false)
  .option('--max-records <count>', 'Maximum number of records to process')
  .option('--show-headers', 'Show parquet file headers/schema and exit', false)
  .option('--no-context', 'Disable conversation context processing (enabled by default)', false)
  .option('--max-text-length <length>', 'Maximum length of processed text', '1024')
  .option('--include-date', 'Include creation date in processed text', false)
  .option('--export-html [path]', 'Export HTML validation report (auto-named if no path provided)')
  .option('--export-html-limit <limit>', 'Limit number of tweets in HTML export for memory efficiency', '1000')
  .option('--export-embeddings [path]', 'Export embeddings to file (auto-saves to ./data/export-embeddings/ if no path provided)')
  .option('--export-format <format>', 'Format for embedding export: jsonl, csv, npy', 'jsonl')
  .option('--clear-cache', 'Clear processing cache for this file before processing', false)
  .option('--disable-cache', 'Disable reading from and writing to processing cache', false)
  .option('--show-cache-stats', 'Show cache statistics and exit', false)
  .option('--use-streaming', 'Use legacy streaming mode instead of batch files (not recommended for large datasets)', false)
  .option('--batch-file-dir <path>', 'Directory to store/load batch files (default: next to parquet file)')
  .option('--skip-confirmation', 'Skip user confirmation for sending data to DeepInfra', false)
  .option('--manifest-save-interval <ms>', 'Milliseconds between manifest saves (default: 30000, min: 5000)', '30000')
  .option('--disable-progress-ui', 'Disable live progress UI for maximum performance', false)
  .option('--skip-qdrant-check', 'Skip checking Qdrant for existing records (faster startup for fresh processing)', false)
  .action(processParquetCommand);

program
  .command('search <text>')
  .description('Perform semantic search using text query')
  .option('-u, --url <url>', 'CA_Embed server URL', 'http://localhost:3000')
  .option('--api-key <key>', 'API key for authentication (or set EMBED_SERVICE_CLIENT_API env var)', process.env.EMBED_SERVICE_CLIENT_API)
  .option('-k, --k <number>', 'Number of results to return', parseInt, 10)
  .option('-t, --threshold <number>', 'Minimum similarity threshold (0.0-1.0)')
  .option('-f, --filter <json>', 'Metadata filter as JSON string (Qdrant only)')
  .option('--pretty', 'Pretty print results with colors', true)
  .option('--no-pretty', 'Output raw JSON')
  .action(searchCommand);

program.parse();