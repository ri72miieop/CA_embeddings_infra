#!/usr/bin/env bun
/**
 * Update metadata for existing embeddings in Qdrant
 *
 * This script reads metadata from JSONL batch files (created by process-parquet.ts)
 * and updates Qdrant payloads without reprocessing embeddings.
 *
 * NOTE: Local parquet file updates are deprecated. Use the R2 export script for backups.
 * The --update-parquet flag can be used for legacy parquet file updates if needed.
 *
 * Usage:
 *   bun run scripts/update-metadata.ts --batch-dir <path>
 *
 * CLI Arguments:
 *   --batch-dir      Path to JSONL batch directory (e.g., .ca_embed-batches/enriched_tweets/)
 *   --archive-dirs   Comma-separated paths to archived parquet directories (only needed with --update-parquet)
 *   --batch-size     Records per API call (default: 1000)
 *   --api-url        Embedding service URL (default: http://localhost:3000)
 *   --api-key        Authentication key (or use EMBED_SERVICE_CLIENT_API env var)
 *   --skip-qdrant    Skip Qdrant updates
 *   --update-parquet Enable parquet file updates (deprecated - use R2 export instead)
 *   --dry-run        Show what would be updated without making changes
 */

import fs from 'fs/promises';
import path from 'path';
import chalk from 'chalk';
import { parquetRead } from 'hyparquet';
import { ByteWriter, parquetWrite } from 'hyparquet-writer';
import { config } from 'dotenv';

// Load environment variables
config();

// ============================================================================
// CLI Argument Parsing
// ============================================================================

function parseArgs(): {
  batchDir: string;
  archiveDirs: string[];
  batchSize: number;
  apiUrl: string;
  apiKey: string;
  skipQdrant: boolean;
  skipParquet: boolean;
  dryRun: boolean;
  parallel: number;
} {
  const args = process.argv.slice(2);

  let batchDir = '';
  let archiveDirs: string[] = [];
  let batchSize = 1000;
  let apiUrl = process.env.EMBEDDING_SERVICE_URL || 'http://localhost:3000';
  let apiKey = process.env.EMBED_SERVICE_CLIENT_API || '';
  let skipQdrant = false;
  let skipParquet = true; // Default to skip parquet - use R2 export instead
  let dryRun = false;
  let parallel = 8; // Default parallel file processing

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const nextArg = args[i + 1];

    switch (arg) {
      case '--batch-dir':
        batchDir = nextArg || '';
        i++;
        break;
      case '--archive-dirs':
        archiveDirs = (nextArg || '').split(',').map(d => d.trim()).filter(Boolean);
        i++;
        break;
      case '--batch-size':
        batchSize = parseInt(nextArg || '1000', 10);
        i++;
        break;
      case '--api-url':
        apiUrl = nextArg || apiUrl;
        i++;
        break;
      case '--api-key':
        apiKey = nextArg || apiKey;
        i++;
        break;
      case '--parallel':
        parallel = parseInt(nextArg || '8', 10);
        i++;
        break;
      case '--skip-qdrant':
        skipQdrant = true;
        break;
      case '--skip-parquet':
        // Legacy flag - same as default now
        skipParquet = true;
        break;
      case '--update-parquet':
        // Explicitly enable parquet updates (deprecated)
        skipParquet = false;
        console.warn(chalk.yellow('\nWARNING: --update-parquet is deprecated. Use R2 export (scripts/export-to-r2.ts) for backups.\n'));
        break;
      case '--dry-run':
        dryRun = true;
        break;
      case '--help':
      case '-h':
        printUsage();
        process.exit(0);
    }
  }

  // Validate required arguments
  if (!batchDir) {
    console.error(chalk.red('Error: --batch-dir is required'));
    printUsage();
    process.exit(1);
  }

  // Archive dirs only required if updating parquet files
  if (!skipParquet && archiveDirs.length === 0) {
    console.error(chalk.red('Error: --archive-dirs is required when using --update-parquet'));
    printUsage();
    process.exit(1);
  }

  if (!apiKey && !skipQdrant) {
    console.error(chalk.red('Error: --api-key or EMBED_SERVICE_CLIENT_API environment variable is required'));
    process.exit(1);
  }

  return { batchDir, archiveDirs, batchSize, apiUrl, apiKey, skipQdrant, skipParquet, dryRun, parallel };
}

function printUsage(): void {
  console.log(`
${chalk.bold('Usage:')}
  bun run scripts/update-metadata.ts --batch-dir <path>

${chalk.bold('Required Arguments:')}
  --batch-dir      Path to JSONL batch directory (e.g., .ca_embed-batches/enriched_tweets/)

${chalk.bold('Optional Arguments:')}
  --batch-size     Records per API call (default: 1000)
  --parallel       Number of files to process in parallel (default: 8)
  --api-url        Embedding service URL (default: http://localhost:3000)
  --api-key        Authentication key (or use EMBED_SERVICE_CLIENT_API env var)
  --skip-qdrant    Skip Qdrant updates
  --dry-run        Show what would be updated without making changes
  --help, -h       Show this help message

${chalk.bold('Deprecated (use R2 export instead):')}
  --update-parquet Enable parquet file updates (requires --archive-dirs)
  --archive-dirs   Comma-separated paths to archived parquet directories

${chalk.bold('Example:')}
  bun run scripts/update-metadata.ts \\
    --batch-dir .ca_embed-batches/enriched_tweets/

${chalk.bold('Note:')}
  Local parquet file updates are deprecated. For backups, use:
  bun run scripts/export-to-r2.ts
`);
}

// ============================================================================
// Types
// ============================================================================

interface BatchRecord {
  key: string;
  content: string;
  metadata: {
    original_text: string | null;
    row_index: number;
    batch_file: string;
    processing_metadata?: {
      has_context: boolean;
      has_quotes: boolean;
      is_truncated: boolean;
      character_difference: number;
      text: string;
    };
  };
}

interface UpdateState {
  lastUpdatedAt: string;
  filesProcessed: Array<{
    file: string;
    directory: string;
    qdrantUpdated: boolean;
    parquetUpdated: boolean;
    keysUpdated: number;
    updatedAt: string;
  }>;
}

interface ProgressState {
  totalFiles: number;
  processedFiles: number;
  skippedFiles: number;
  totalKeysUpdated: number;
  qdrantUpdates: number;
  parquetUpdates: number;
  errors: Array<{ file: string; error: string }>;
  startTime: number;
}

// ============================================================================
// State Management
// ============================================================================

const STATE_FILE = '.metadata-update-state.json';

async function loadState(): Promise<UpdateState> {
  try {
    const content = await fs.readFile(STATE_FILE, 'utf-8');
    return JSON.parse(content);
  } catch {
    return {
      lastUpdatedAt: '',
      filesProcessed: []
    };
  }
}

async function saveState(state: UpdateState): Promise<void> {
  await fs.writeFile(STATE_FILE, JSON.stringify(state, null, 2));
}

function isFileProcessed(state: UpdateState, filePath: string, skipQdrant: boolean, skipParquet: boolean): boolean {
  const processed = state.filesProcessed.find(f => f.file === path.basename(filePath));
  if (!processed) return false;
  
  const qdrantDone = skipQdrant || processed.qdrantUpdated;
  const parquetDone = skipParquet || processed.parquetUpdated;
  
  return qdrantDone && parquetDone;
}

// ============================================================================
// JSONL Batch Loading
// ============================================================================

async function loadBatchMetadata(batchDir: string): Promise<Map<string, Record<string, any>>> {
  console.log(chalk.cyan(`\n📂 Loading metadata from batch directory: ${batchDir}`));
  
  const metadataMap = new Map<string, Record<string, any>>();
  
  // Find all batch_*.jsonl files
  const files = await fs.readdir(batchDir);
  const batchFiles = files.filter(f => f.startsWith('batch_') && f.endsWith('.jsonl')).sort();
  
  if (batchFiles.length === 0) {
    throw new Error(`No batch files found in ${batchDir}`);
  }
  
  console.log(chalk.gray(`   Found ${batchFiles.length} batch files`));
  
  for (const batchFile of batchFiles) {
    const filePath = path.join(batchDir, batchFile);
    const content = await fs.readFile(filePath, 'utf-8');
    const lines = content.trim().split('\n');
    
    for (const line of lines) {
      if (!line.trim()) continue;
      
      try {
        const record: BatchRecord = JSON.parse(line);
        
        // Build the metadata object that will be stored in Qdrant
        const metadata: Record<string, any> = {
          source: 'parquet_import',
          original_text: record.metadata.original_text,
        };
        
        // Add processing metadata if available
        if (record.metadata.processing_metadata) {
          Object.assign(metadata, record.metadata.processing_metadata);
        }
        
        metadataMap.set(record.key, metadata);
      } catch (error) {
        console.warn(chalk.yellow(`   Warning: Failed to parse line in ${batchFile}`));
      }
    }
  }
  
  console.log(chalk.green(`   ✓ Loaded metadata for ${metadataMap.size.toLocaleString()} keys`));
  
  return metadataMap;
}

// ============================================================================
// API Health Check
// ============================================================================

async function checkApiHealth(apiUrl: string, apiKey: string): Promise<{ ok: boolean; error?: string }> {
  try {
    // Test authentication by sending a trial update with a dummy non-existent key
    // This verifies both connectivity and auth without affecting real data
    console.log(chalk.gray('   Testing API connection with trial update...'));
    
    const testResponse = await fetch(`${apiUrl}/embeddings/update-metadata`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`
      },
      body: JSON.stringify({ 
        items: [{ 
          key: '__health_check_dummy_key_999999999999__', 
          metadata: { test: true } 
        }] 
      })
    });
    
    if (!testResponse.ok) {
      const errorText = await testResponse.text();
      
      // Parse the error to give a helpful message
      try {
        const errorJson = JSON.parse(errorText);
        
        // Check for various auth-related errors
        if (errorJson.error === 'Unauthorized' || errorJson.message?.includes('Invalid or missing API key')) {
          return { 
            ok: false, 
            error: `Authentication Failed: Invalid API key\n\n` +
                   `   Your API key doesn't match the server's configured keys.\n` +
                   `   Check that:\n` +
                   `   1. API_KEYS is set in your server's .env file\n` +
                   `   2. The key you're using (${apiKey.substring(0, 4)}...) matches one in API_KEYS\n` +
                   `   3. Both the server and this script are using the same key`
          };
        }
        
        if (errorJson.message?.includes('API key authentication is required but no API keys are configured')) {
          return { 
            ok: false, 
            error: `Server Configuration Error: No API keys configured on server\n\n` +
                   `   Add API_KEYS=your-secret-key to your server's .env file\n` +
                   `   Then restart the embedding service.`
          };
        }
        
        return { ok: false, error: `API Error: ${errorJson.message || errorJson.error || errorText}` };
      } catch {
        return { ok: false, error: `API Error (${testResponse.status}): ${errorText}` };
      }
    }
    
    // If we got a 200, auth works (the dummy key won't match any real records, which is fine)
    console.log(chalk.gray('   ✓ API connection and authentication verified'));
    return { ok: true };
    
  } catch (error) {
    if (error instanceof Error) {
      if (error.message.includes('ECONNREFUSED') || error.message.includes('fetch failed')) {
        return { 
          ok: false, 
          error: `Cannot connect to API at ${apiUrl}\n` +
                 `   Make sure the embedding service is running.`
        };
      }
      return { ok: false, error: error.message };
    }
    return { ok: false, error: 'Unknown error checking API health' };
  }
}

// ============================================================================
// Qdrant API Updates
// ============================================================================

async function updateQdrantMetadata(
  apiUrl: string,
  apiKey: string,
  updates: Array<{ key: string; metadata: Record<string, any> }>,
  batchSize: number
): Promise<{ updated: number; failed: number }> {
  let totalUpdated = 0;
  let totalFailed = 0;
  
  // Process in batches
  for (let i = 0; i < updates.length; i += batchSize) {
    const batch = updates.slice(i, i + batchSize);
    
    try {
      const response = await fetch(`${apiUrl}/embeddings/update-metadata`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${apiKey}`
        },
        body: JSON.stringify({ items: batch })
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`API error (${response.status}): ${errorText}`);
      }
      
      const result = await response.json() as { updated: number; failed: number };
      totalUpdated += result.updated;
      totalFailed += result.failed;
      
      // Progress indicator
      if ((i + batchSize) % (batchSize * 10) === 0 || i + batchSize >= updates.length) {
        const progress = Math.min(i + batchSize, updates.length);
        console.log(chalk.gray(`      Qdrant progress: ${progress}/${updates.length} (${totalUpdated} updated, ${totalFailed} failed)`));
      }
    } catch (error) {
      console.error(chalk.red(`      Batch ${i}-${i + batch.length} failed: ${error}`));
      totalFailed += batch.length;
    }
  }
  
  return { updated: totalUpdated, failed: totalFailed };
}

// ============================================================================
// Parquet Updates with hyparquet (pure in-memory, no file locks)
// ============================================================================

async function updateParquetMetadata(
  parquetPath: string,
  metadataMap: Map<string, Record<string, any>>
): Promise<{ keysUpdated: number }> {
  // Read entire file into memory - file handle released immediately after read
  const buffer = await fs.readFile(parquetPath);
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength) as ArrayBuffer;
  
  // Parse parquet into rows
  let rows: any[] = [];
  await parquetRead({
    file: arrayBuffer,
    rowFormat: 'object',
    onComplete: (data: any[]) => {
      rows = data;
    }
  });
  
  if (rows.length === 0) {
    return { keysUpdated: 0 };
  }
  
  // Find which keys need updating
  const keysToUpdateSet = new Set<string>();
  for (const row of rows) {
    const key = String(row.key);
    if (metadataMap.has(key)) {
      keysToUpdateSet.add(key);
    }
  }
  
  if (keysToUpdateSet.size === 0) {
    console.log(chalk.gray(`      No matching keys to update in this file`));
    return { keysUpdated: 0 };
  }
  
  console.log(chalk.gray(`      Found ${keysToUpdateSet.size} keys to update`));
  
  // Get column names from first row
  const columnNames = Object.keys(rows[0]!);
  
  // Build column arrays, updating metadata as we go
  const columnArrays: Record<string, any[]> = {};
  for (const col of columnNames) {
    columnArrays[col] = [];
  }
  
  let keysUpdated = 0;
  for (const row of rows) {
    const key = String(row.key);
    const shouldUpdate = keysToUpdateSet.has(key);
    
    for (const col of columnNames) {
      let value = row[col];
      
      if (col === 'metadata' && shouldUpdate) {
        value = metadataMap.get(key);
        keysUpdated++;
        
        if (keysUpdated % 1000 === 0) {
          console.log(chalk.gray(`      Updated ${keysUpdated}/${keysToUpdateSet.size} keys...`));
        }
      }
      
      columnArrays[col]!.push(value);
    }
  }
  
  // Build column data for parquet writer
  const columnData = columnNames.map(name => ({
    name,
    data: columnArrays[name]!
  }));
  
  // Write directly to original file (no locks held, no temp files)
  const writer = new ByteWriter();
  parquetWrite({
    writer,
    columnData,
    compressed: true,
    rowGroupSize: 10000,
  });
  
  await fs.writeFile(parquetPath, Buffer.from(writer.getBuffer()));
  
  return { keysUpdated };
}

// ============================================================================
// User Confirmation
// ============================================================================

type PromptAnswer = 'yes' | 'skip' | 'quit' | 'batch5' | 'batch10' | 'all';

async function promptConfirmation(message: string, remainingFiles?: number): Promise<PromptAnswer> {
  const readline = await import('readline');
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  
  // Build options string
  let options = '[y]es / [s]kip / [q]uit / [5] next 5 / [10] next 10';
  if (remainingFiles !== undefined) {
    options += ` / [a]ll (${remainingFiles} remaining)`;
  }
  
  return new Promise((resolve) => {
    rl.question(`${message} ${options}: `, (answer) => {
      rl.close();
      const normalized = answer.trim().toLowerCase();
      
      if (normalized === 'y' || normalized === 'yes') {
        resolve('yes');
      } else if (normalized === 's' || normalized === 'skip') {
        resolve('skip');
      } else if (normalized === 'q' || normalized === 'quit') {
        resolve('quit');
      } else if (normalized === '5') {
        resolve('batch5');
      } else if (normalized === '10') {
        resolve('batch10');
      } else if (normalized === 'a' || normalized === 'all') {
        resolve('all');
      } else {
        // Default to skip for invalid input
        resolve('skip');
      }
    });
  });
}

// ============================================================================
// Main Processing
// ============================================================================

interface BatchContext {
  autoProcessRemaining: number;
  remainingFilesInQueue: number;
}

/**
 * Process a single parquet file silently (for parallel execution)
 * No interactive prompts, minimal logging
 */
async function processParquetFileSilent(
  filePath: string,
  directory: string,
  metadataMap: Map<string, Record<string, any>>,
  config: ReturnType<typeof parseArgs>,
  state: UpdateState,
  progress: ProgressState
): Promise<void> {
  const fileName = path.basename(filePath);
  
  try {
    // Read parquet to get key count
    const buffer = await fs.readFile(filePath);
    const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength) as ArrayBuffer;
    
    let matchingKeys: string[] = [];
    
    await parquetRead({
      file: arrayBuffer,
      rowFormat: 'object',
      onComplete: (rows: any[]) => {
        for (const row of rows) {
          if (row.key && metadataMap.has(String(row.key))) {
            matchingKeys.push(String(row.key));
          }
        }
      }
    });
    
    if (matchingKeys.length === 0) {
      progress.skippedFiles++;
      return;
    }
    
    // Prepare updates
    const updates = matchingKeys.map(key => ({
      key,
      metadata: metadataMap.get(key)!
    }));
    
    let qdrantUpdated = false;
    let parquetUpdated = false;
    let keysUpdated = 0;
    
    // Update Qdrant
    if (!config.skipQdrant) {
      try {
        const result = await updateQdrantMetadata(config.apiUrl, config.apiKey, updates, config.batchSize);
        qdrantUpdated = true;
        progress.qdrantUpdates += result.updated;
      } catch (error) {
        progress.errors.push({ file: fileName, error: `Qdrant: ${error}` });
      }
    } else {
      qdrantUpdated = true;
    }
    
    // Update Parquet
    if (!config.skipParquet) {
      try {
        const result = await updateParquetMetadata(filePath, metadataMap);
        parquetUpdated = true;
        keysUpdated = result.keysUpdated;
        progress.parquetUpdates += result.keysUpdated;
      } catch (error) {
        progress.errors.push({ file: fileName, error: `Parquet: ${error}` });
      }
    } else {
      parquetUpdated = true;
      keysUpdated = matchingKeys.length;
    }
    
    // Update state (atomic operation)
    const existingEntry = state.filesProcessed.find(f => f.file === fileName);
    if (existingEntry) {
      existingEntry.qdrantUpdated = existingEntry.qdrantUpdated || qdrantUpdated;
      existingEntry.parquetUpdated = existingEntry.parquetUpdated || parquetUpdated;
      existingEntry.keysUpdated = keysUpdated;
      existingEntry.updatedAt = new Date().toISOString();
    } else {
      state.filesProcessed.push({
        file: fileName,
        directory,
        qdrantUpdated,
        parquetUpdated,
        keysUpdated,
        updatedAt: new Date().toISOString()
      });
    }
    
    state.lastUpdatedAt = new Date().toISOString();
    
    progress.processedFiles++;
    progress.totalKeysUpdated += keysUpdated;
    
  } catch (error) {
    progress.errors.push({ file: fileName, error: `${error}` });
  }
}

async function processParquetFile(
  filePath: string,
  directory: string,
  metadataMap: Map<string, Record<string, any>>,
  config: ReturnType<typeof parseArgs>,
  state: UpdateState,
  progress: ProgressState,
  batchCtx: BatchContext
): Promise<void> {
  const fileName = path.basename(filePath);
  
  console.log(chalk.cyan(`\n📄 Processing: ${fileName}`));
  console.log(chalk.gray(`   Directory: ${directory}`));
  
  // Read parquet to get key count
  const buffer = await fs.readFile(filePath);
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength) as ArrayBuffer;
  
  let recordCount = 0;
  let matchingKeys: string[] = [];
  
  await parquetRead({
    file: arrayBuffer,
    rowFormat: 'object',
    onComplete: (rows: any[]) => {
      recordCount = rows.length;
      for (const row of rows) {
        if (row.key && metadataMap.has(String(row.key))) {
          matchingKeys.push(String(row.key));
        }
      }
    }
  });
  
  console.log(chalk.gray(`   Total records: ${recordCount.toLocaleString()}`));
  console.log(chalk.gray(`   Keys to update: ${matchingKeys.length.toLocaleString()}`));
  
  if (matchingKeys.length === 0) {
    console.log(chalk.yellow(`   ⏭️  Skipping - no matching keys`));
    progress.skippedFiles++;
    return;
  }
  
  // Prompt for confirmation (or auto-process if in batch mode)
  if (!config.dryRun) {
    if (batchCtx.autoProcessRemaining > 0) {
      // Auto-processing mode
      console.log(chalk.green(`   ▶️  Auto-processing (${batchCtx.autoProcessRemaining} remaining in batch)`));
      batchCtx.autoProcessRemaining--;
    } else {
      // Ask user
      const answer = await promptConfirmation(chalk.yellow(`   Continue?`), batchCtx.remainingFilesInQueue);
      
      if (answer === 'quit') {
        console.log(chalk.yellow('\n👋 Quitting...'));
        await saveState(state);
        process.exit(0);
      }
      
      if (answer === 'skip') {
        console.log(chalk.yellow(`   ⏭️  Skipped by user`));
        progress.skippedFiles++;
        return;
      }
      
      if (answer === 'batch5') {
        console.log(chalk.green(`   ▶️  Processing this file + next 4 files`));
        batchCtx.autoProcessRemaining = 4; // This one + 4 more
      } else if (answer === 'batch10') {
        console.log(chalk.green(`   ▶️  Processing this file + next 9 files`));
        batchCtx.autoProcessRemaining = 9; // This one + 9 more
      } else if (answer === 'all') {
        console.log(chalk.green(`   ▶️  Processing all remaining files`));
        batchCtx.autoProcessRemaining = Infinity;
      }
      // 'yes' just continues with this file
    }
  } else {
    console.log(chalk.yellow(`   [DRY RUN] Would update ${matchingKeys.length} keys`));
    progress.skippedFiles++;
    return;
  }
  
  // Prepare updates
  const updates = matchingKeys.map(key => ({
    key,
    metadata: metadataMap.get(key)!
  }));
  
  let qdrantUpdated = false;
  let parquetUpdated = false;
  let keysUpdated = 0;
  
  // Update Qdrant
  if (!config.skipQdrant) {
    console.log(chalk.blue(`   📤 Updating Qdrant payloads...`));
    try {
      const result = await updateQdrantMetadata(config.apiUrl, config.apiKey, updates, config.batchSize);
      qdrantUpdated = true;
      progress.qdrantUpdates += result.updated;
      console.log(chalk.green(`   ✓ Qdrant: ${result.updated} updated, ${result.failed} failed`));
    } catch (error) {
      console.error(chalk.red(`   ✗ Qdrant update failed: ${error}`));
      progress.errors.push({ file: fileName, error: `Qdrant: ${error}` });
    }
  } else {
    qdrantUpdated = true; // Mark as done since we're skipping
  }
  
  // Update Parquet
  if (!config.skipParquet) {
    console.log(chalk.blue(`   📝 Updating parquet file...`));
    try {
      const result = await updateParquetMetadata(filePath, metadataMap);
      parquetUpdated = true;
      keysUpdated = result.keysUpdated;
      progress.parquetUpdates += result.keysUpdated;
      console.log(chalk.green(`   ✓ Parquet: ${result.keysUpdated} keys updated`));
    } catch (error) {
      console.error(chalk.red(`   ✗ Parquet update failed: ${error}`));
      progress.errors.push({ file: fileName, error: `Parquet: ${error}` });
    }
  } else {
    parquetUpdated = true; // Mark as done since we're skipping
    keysUpdated = matchingKeys.length;
  }
  
  // Update state
  const existingEntry = state.filesProcessed.find(f => f.file === fileName);
  if (existingEntry) {
    existingEntry.qdrantUpdated = existingEntry.qdrantUpdated || qdrantUpdated;
    existingEntry.parquetUpdated = existingEntry.parquetUpdated || parquetUpdated;
    existingEntry.keysUpdated = keysUpdated;
    existingEntry.updatedAt = new Date().toISOString();
  } else {
    state.filesProcessed.push({
      file: fileName,
      directory,
      qdrantUpdated,
      parquetUpdated,
      keysUpdated,
      updatedAt: new Date().toISOString()
    });
  }
  
  state.lastUpdatedAt = new Date().toISOString();
  await saveState(state);
  
  progress.processedFiles++;
  progress.totalKeysUpdated += keysUpdated;
}

async function main(): Promise<void> {
  console.log(chalk.bold.cyan('\n╔════════════════════════════════════════════════════════════════╗'));
  console.log(chalk.bold.cyan('║') + chalk.bold.white('              METADATA UPDATE SCRIPT                         ') + chalk.bold.cyan('║'));
  console.log(chalk.bold.cyan('╚════════════════════════════════════════════════════════════════╝\n'));
  
  const config = parseArgs();
  
  console.log(chalk.bold('⚙️  Configuration:'));
  console.log(`   Batch Directory:    ${chalk.cyan(config.batchDir)}`);
  console.log(`   Archive Directories: ${chalk.cyan(config.archiveDirs.join(', '))}`);
  console.log(`   Batch Size:         ${chalk.cyan(config.batchSize)}`);
  console.log(`   API URL:            ${chalk.cyan(config.apiUrl)}`);
  
  // Show API key status (masked for security)
  const keyStatus = config.apiKey 
    ? chalk.green(`Set (${config.apiKey.substring(0, 4)}...${config.apiKey.substring(config.apiKey.length - 4)})`)
    : chalk.red('NOT SET');
  console.log(`   API Key:            ${keyStatus}`);
  
  console.log(`   Parallel Files:     ${chalk.cyan(config.parallel)}`);
  console.log(`   Skip Qdrant:        ${config.skipQdrant ? chalk.yellow('Yes') : chalk.green('No')}`);
  console.log(`   Skip Parquet:       ${config.skipParquet ? chalk.yellow('Yes') : chalk.green('No')}`);
  console.log(`   Dry Run:            ${config.dryRun ? chalk.yellow('Yes') : chalk.green('No')}`);
  
  // Check if API key is missing before trying to connect
  if (!config.skipQdrant && !config.dryRun && !config.apiKey) {
    console.error(chalk.red('\n❌ API Key is not set!\n'));
    console.error(chalk.yellow('   You need to provide an API key. Options:'));
    console.error(chalk.yellow('   1. Set EMBED_SERVICE_CLIENT_API environment variable'));
    console.error(chalk.yellow('   2. Use --api-key <your-key> flag'));
    console.error(chalk.yellow('   3. Use --skip-qdrant to skip Qdrant updates\n'));
    console.error(chalk.gray('   Note: The API key must match one configured in'));
    console.error(chalk.gray('         API_KEYS on your embedding server.'));
    process.exit(1);
  }
  
  // Pre-flight check: Verify API connection and authentication
  if (!config.skipQdrant && !config.dryRun) {
    console.log(chalk.cyan('\n🔍 Checking API connection...'));
    const healthCheck = await checkApiHealth(config.apiUrl, config.apiKey);
    
    if (!healthCheck.ok) {
      console.error(chalk.red('\n❌ API Health Check Failed:\n'));
      console.error(chalk.red(healthCheck.error));
      console.log(chalk.yellow('\n💡 Options:'));
      console.log(chalk.yellow('   - Fix the API configuration and try again'));
      console.log(chalk.yellow('   - Use --skip-qdrant to only update parquet files'));
      console.log(chalk.yellow('   - Use --dry-run to preview what would be updated'));
      process.exit(1);
    }
    
    console.log(chalk.green('   ✓ API connection verified'));
  }
  
  // Load metadata from batch files
  const metadataMap = await loadBatchMetadata(config.batchDir);
  
  if (metadataMap.size === 0) {
    console.log(chalk.yellow('\n⚠️  No metadata loaded. Exiting.'));
    return;
  }
  
  // Load state
  const state = await loadState();
  
  // Initialize progress
  const progress: ProgressState = {
    totalFiles: 0,
    processedFiles: 0,
    skippedFiles: 0,
    totalKeysUpdated: 0,
    qdrantUpdates: 0,
    parquetUpdates: 0,
    errors: [],
    startTime: Date.now()
  };
  
  // Batch processing context (shared across all files)
  const batchCtx: BatchContext = {
    autoProcessRemaining: 0,
    remainingFilesInQueue: 0
  };
  
  // First, collect all files to process to get accurate remaining count
  const allFilesToProcess: Array<{ filePath: string; archiveDir: string }> = [];
  
  for (const archiveDir of config.archiveDirs) {
    try {
      const files = await fs.readdir(archiveDir);
      const parquetFiles = files.filter(f => f.endsWith('.parquet')).sort();
      
      for (const parquetFile of parquetFiles) {
        const filePath = path.join(archiveDir, parquetFile);
        if (!isFileProcessed(state, filePath, config.skipQdrant, config.skipParquet)) {
          allFilesToProcess.push({ filePath, archiveDir });
        }
      }
    } catch (error) {
      console.error(chalk.red(`   Error scanning directory ${archiveDir}: ${error}`));
    }
  }
  
  progress.totalFiles = allFilesToProcess.length;
  console.log(chalk.cyan(`\n📊 Found ${allFilesToProcess.length} files to process across ${config.archiveDirs.length} directories`));
  console.log(chalk.cyan(`   Processing ${config.parallel} files in parallel`));
  
  // Process files in parallel batches
  for (let i = 0; i < allFilesToProcess.length; i += config.parallel) {
    const batch = allFilesToProcess.slice(i, i + config.parallel);
    batchCtx.remainingFilesInQueue = allFilesToProcess.length - i;
    
    // Show batch info
    console.log(chalk.cyan(`\n📦 Batch ${Math.floor(i / config.parallel) + 1}/${Math.ceil(allFilesToProcess.length / config.parallel)} (${batch.length} files)`));
    
    // Check for user confirmation if not auto-processing
    if (!config.dryRun && batchCtx.autoProcessRemaining <= 0) {
      const answer = await promptConfirmation(chalk.yellow(`   Process this batch?`), batchCtx.remainingFilesInQueue);
      
      if (answer === 'quit') {
        console.log(chalk.yellow('\n👋 Quitting...'));
        await saveState(state);
        process.exit(0);
      }
      
      if (answer === 'skip') {
        console.log(chalk.yellow(`   ⏭️  Batch skipped`));
        progress.skippedFiles += batch.length;
        continue;
      }
      
      if (answer === 'batch5') {
        batchCtx.autoProcessRemaining = 4; // This batch + 4 more
      } else if (answer === 'batch10') {
        batchCtx.autoProcessRemaining = 9; // This batch + 9 more
      } else if (answer === 'all') {
        batchCtx.autoProcessRemaining = Infinity;
      }
    } else if (batchCtx.autoProcessRemaining > 0 && batchCtx.autoProcessRemaining !== Infinity) {
      batchCtx.autoProcessRemaining--;
    }
    
    // Process batch in parallel
    const startTime = Date.now();
    await Promise.all(
      batch.map(fileInfo => 
        processParquetFileSilent(fileInfo.filePath, fileInfo.archiveDir, metadataMap, config, state, progress)
      )
    );
    const batchDuration = ((Date.now() - startTime) / 1000).toFixed(1);
    
    // Save state after each batch
    await saveState(state);
    
    console.log(chalk.green(`   ✓ Batch completed in ${batchDuration}s (${progress.processedFiles}/${progress.totalFiles} total)`));
  }
  
  // Final summary
  const duration = (Date.now() - progress.startTime) / 1000;
  
  console.log(chalk.bold.green('\n╔════════════════════════════════════════════════════════════════╗'));
  console.log(chalk.bold.green('║') + chalk.bold.white('                    UPDATE COMPLETE                          ') + chalk.bold.green('║'));
  console.log(chalk.bold.green('╚════════════════════════════════════════════════════════════════╝\n'));
  
  console.log(chalk.bold('📈 Final Statistics:'));
  console.log(`   Total files scanned:    ${chalk.cyan(progress.totalFiles)}`);
  console.log(`   Files processed:        ${chalk.green(progress.processedFiles)}`);
  console.log(`   Files skipped:          ${chalk.yellow(progress.skippedFiles)}`);
  console.log(`   Total keys updated:     ${chalk.cyan(progress.totalKeysUpdated.toLocaleString())}`);
  console.log(`   Qdrant updates:         ${chalk.cyan(progress.qdrantUpdates.toLocaleString())}`);
  console.log(`   Parquet updates:        ${chalk.cyan(progress.parquetUpdates.toLocaleString())}`);
  console.log(`   Duration:               ${chalk.magenta(duration.toFixed(1))}s`);
  
  if (progress.errors.length > 0) {
    console.log(chalk.bold.red(`\n❌ Errors (${progress.errors.length}):`));
    for (const err of progress.errors) {
      console.log(chalk.red(`   ${err.file}: ${err.error}`));
    }
  }
  
  console.log(chalk.gray(`\n💾 State saved to: ${STATE_FILE}`));
}

// Run the script
main().catch((error) => {
  console.error(chalk.red('\n❌ Fatal error:'), error);
  process.exit(1);
});

