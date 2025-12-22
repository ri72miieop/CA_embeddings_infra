#!/usr/bin/env bun
/**
 * Import embeddings from pending parquet queue files to Qdrant
 * 
 * This script reads all pending parquet files from the queue directory,
 * extracts the embeddings, and uploads them to a Qdrant collection.
 * 
 * Configuration:
 * - QDRANT_URL: Qdrant server URL (default: http://localhost:6333)
 * - QDRANT_API_KEY: Optional API key for Qdrant Cloud
 * - QDRANT_COLLECTION_NAME: Collection name (default: embeddings)
 * - QUEUE_DIR: Path to queue directory (default: ./data/embedding-calls/embedding-queue)
 * - BATCH_SIZE: Number of vectors to upload per batch (default: 100)
 * - VECTOR_DIMENSION: Vector dimension (default: 1024)
 * - MAX_PARALLEL_FILES: Number of files to process in parallel (default: 5)
 * - MAX_PARALLEL_BATCHES: Number of batches to upload in parallel per file (default: 3)
 */

import fs from 'fs/promises';
import path from 'path';
import { QdrantClient } from '@qdrant/qdrant-js';
import { parquetRead } from 'hyparquet';
import chalk from 'chalk';

// Configuration from environment or defaults
const QDRANT_URL = process.env.QDRANT_URL || 'http://localhost:6333';
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;
const QDRANT_PORT = parseInt(process.env.QDRANT_PORT || '6333');
const QDRANT_COLLECTION_NAME = process.env.QDRANT_COLLECTION_NAME || 'embeddings';
const QUEUE_DIR = process.env.QUEUE_DIR || './data/embedding-calls/embedding-queue';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '1000');
const VECTOR_DIMENSION = parseInt(process.env.VECTOR_DIMENSION || '1024');
const MAX_PARALLEL_FILES = parseInt(process.env.MAX_PARALLEL_FILES || '8');
const MAX_PARALLEL_BATCHES = parseInt(process.env.MAX_PARALLEL_BATCHES || '4');
const ERROR_LOG_FILE = process.env.ERROR_LOG_FILE || './qdrant-import-errors.log';
const SHARD_NUMBER = parseInt(process.env.SHARD_NUMBER || '4');
const OPTIMIZE_FOR_BULK = process.env.OPTIMIZE_FOR_BULK !== 'false'; // Default true
const ENABLE_INDEXING_AFTER = process.env.ENABLE_INDEXING_AFTER !== 'false'; // Default true
const MAX_FILES_TO_PROCESS = process.env.MAX_FILES ? parseInt(process.env.MAX_FILES) : undefined; // Optional limit

interface EmbeddingRecord {
  id: string;
  key: string;
  vector: number[];
  metadata: Record<string, any>;
  queueItemId: string;
  timestamp: string;
  correlationId?: string;
}

interface ProgressState {
  totalFiles: number;
  completedFiles: number;
  failedFiles: number;
  totalEmbeddings: number;
  uploadedEmbeddings: number;
  startTime: number;
  activeWorkers: Map<string, { fileName: string; status: string; progress: number; total: number }>;
  recentErrors: Array<{ fileName: string; error: string; timestamp: number }>;
}

const progressState: ProgressState = {
  totalFiles: 0,
  completedFiles: 0,
  failedFiles: 0,
  totalEmbeddings: 0,
  uploadedEmbeddings: 0,
  startTime: Date.now(),
  activeWorkers: new Map(),
  recentErrors: [],
};

interface ProfilingMetrics {
  fileReadTime: number;
  dataPreparationTime: number;
  uploadTime: number;
  totalFileTime: number;
  fileCount: number;
}

interface ProfilingState {
  perFileMetrics: Array<{
    fileName: string;
    readTime: number;
    prepTime: number;
    uploadTime: number;
    totalTime: number;
    embeddingCount: number;
  }>;
  aggregated: ProfilingMetrics;
}

const profilingState: ProfilingState = {
  perFileMetrics: [],
  aggregated: {
    fileReadTime: 0,
    dataPreparationTime: 0,
    uploadTime: 0,
    totalFileTime: 0,
    fileCount: 0,
  },
};

/**
 * Log error to file
 */
async function logErrorToFile(fileName: string, error: string): Promise<void> {
  try {
    const timestamp = new Date().toISOString();
    const logEntry = `[${timestamp}] ${fileName}: ${error}\n`;
    await fs.appendFile(ERROR_LOG_FILE, logEntry);
  } catch (e) {
    // Silently fail if we can't write to log file
  }
}

/**
 * Display progress with visual indicators
 */
function displayProgress(): void {
  const elapsed = (Date.now() - progressState.startTime) / 1000;
  const rate = progressState.uploadedEmbeddings / elapsed;
  const eta = progressState.totalEmbeddings > 0 
    ? (progressState.totalEmbeddings - progressState.uploadedEmbeddings) / rate 
    : 0;

  // Clear console and move cursor to top
  process.stdout.write('\x1Bc'); // Clear screen
  
  console.log(chalk.bold.cyan('╔════════════════════════════════════════════════════════════════╗'));
  console.log(chalk.bold.cyan('║') + chalk.bold.white('           QDRANT IMPORT - PARALLEL PROCESSING              ') + chalk.bold.cyan('║'));
  console.log(chalk.bold.cyan('╚════════════════════════════════════════════════════════════════╝'));
  console.log();

  // Overall progress
  const fileProgress = progressState.totalFiles > 0 
    ? (progressState.completedFiles / progressState.totalFiles * 100).toFixed(1)
    : '0.0';
  const embeddingProgress = progressState.totalEmbeddings > 0
    ? (progressState.uploadedEmbeddings / progressState.totalEmbeddings * 100).toFixed(1)
    : '0.0';

  console.log(chalk.bold('📊 Overall Progress:'));
  console.log(`   Files:      ${chalk.green(progressState.completedFiles)}/${chalk.blue(progressState.totalFiles)} completed ${chalk.yellow(`(${fileProgress}%)`)} ${progressState.failedFiles > 0 ? chalk.red(`[${progressState.failedFiles} failed]`) : ''}`);
  console.log(`   Embeddings: ${chalk.green(progressState.uploadedEmbeddings.toLocaleString())}/${chalk.blue(progressState.totalEmbeddings.toLocaleString())} uploaded ${chalk.yellow(`(${embeddingProgress}%)`)}`);;
  console.log(`   Rate:       ${chalk.cyan(rate.toFixed(0))} embeddings/sec`);
  console.log(`   Elapsed:    ${chalk.magenta(formatDuration(elapsed))}`);
  if (eta > 0 && eta < Infinity) {
    console.log(`   ETA:        ${chalk.magenta(formatDuration(eta))}`);
  }
  console.log();

  // Active workers
  if (progressState.activeWorkers.size > 0) {
    console.log(chalk.bold(`🔄 Active Workers (${progressState.activeWorkers.size}/${MAX_PARALLEL_FILES}):`));
    for (const [workerId, worker] of progressState.activeWorkers.entries()) {
      const percent = worker.total > 0 ? ((worker.progress / worker.total) * 100).toFixed(0) : '0';
      const bar = createProgressBar(worker.progress, worker.total, 30);
      const shortFileName = worker.fileName.length > 40 
        ? '...' + worker.fileName.slice(-37)
        : worker.fileName;
      console.log(`   ${chalk.dim(workerId.slice(0, 8))} ${bar} ${chalk.yellow(percent.padStart(3))}% ${chalk.dim(shortFileName)}`);
      console.log(`            ${chalk.dim(worker.status)}`);
    }
  } else {
    console.log(chalk.dim('⏸️  No active workers'));
  }
  
  console.log();

  // Recent errors (show last 5)
  if (progressState.recentErrors.length > 0) {
    console.log(chalk.bold.red(`❌ Recent Errors (last ${Math.min(5, progressState.recentErrors.length)}):`));
    const recentFive = progressState.recentErrors.slice(-5);
    for (const err of recentFive) {
      const shortFileName = err.fileName.length > 45 
        ? '...' + err.fileName.slice(-42)
        : err.fileName;
      const shortError = err.error.length > 80 
        ? err.error.slice(0, 77) + '...'
        : err.error;
      console.log(`   ${chalk.red('✗')} ${chalk.dim(shortFileName)}`);
      console.log(`      ${chalk.red(shortError)}`);
    }
    console.log();
  }
}

/**
 * Create a visual progress bar
 */
function createProgressBar(current: number, total: number, width: number): string {
  if (total === 0) return chalk.dim('░'.repeat(width));
  const filled = Math.round((current / total) * width);
  const empty = width - filled;
  return chalk.green('█'.repeat(filled)) + chalk.dim('░'.repeat(empty));
}

/**
 * Format duration in human-readable format
 */
function formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds.toFixed(0)}s`;
  if (seconds < 3600) {
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins}m ${secs}s`;
  }
  const hours = Math.floor(seconds / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  return `${hours}h ${mins}m`;
}

/**
 * Initialize Qdrant client and ensure collection exists
 */
async function initializeQdrant(): Promise<QdrantClient> {
  const client = new QdrantClient({
    url: QDRANT_URL,
    apiKey: QDRANT_API_KEY,
    port: QDRANT_PORT
  });

  console.log(chalk.cyan('⚡ Connecting to Qdrant at ') + chalk.bold(QDRANT_URL) + '...');
  
  // Check if collection exists
  const collections = await client.getCollections();
  const collectionExists = collections.collections.some(
    (c) => c.name === QDRANT_COLLECTION_NAME
  );

  if (!collectionExists) {
    console.log(chalk.yellow('📦 Creating collection: ') + chalk.bold(QDRANT_COLLECTION_NAME));
    
    if (OPTIMIZE_FOR_BULK) {
      // Optimized for bulk upload with minimal RAM usage
      // Based on: https://qdrant.tech/documentation/database-tutorials/bulk-upload/
      console.log(chalk.dim('   Optimizing for bulk upload (low RAM):'));
      console.log(chalk.dim('   - Deferring HNSW indexing (m: 0)'));
      console.log(chalk.dim('   - Using disk storage (on_disk: true)'));
      console.log(chalk.dim(`   - Using ${SHARD_NUMBER} shards for parallel upload`));
      
      await client.createCollection(QDRANT_COLLECTION_NAME, {
        vectors: {
          size: VECTOR_DIMENSION,
          distance: 'Cosine',
          on_disk: true, // Store vectors on disk to minimize RAM
        },
        hnsw_config: {
          m: 0, // Defer HNSW graph construction until after upload
        },
        shard_number: SHARD_NUMBER, // Multiple shards for parallel upload
      });
    } else {
      // Standard configuration
      await client.createCollection(QDRANT_COLLECTION_NAME, {
        vectors: {
          size: VECTOR_DIMENSION,
          distance: 'Cosine',
        },
        shard_number: SHARD_NUMBER,
      });
    }
    
    console.log(chalk.green('✓ Collection created successfully'));
  } else {
    console.log(chalk.green('✓ Collection ') + chalk.bold(QDRANT_COLLECTION_NAME) + chalk.green(' already exists'));
    
    // Check if collection is already optimized for bulk upload
    if (OPTIMIZE_FOR_BULK) {
      console.log(chalk.dim('   Note: Collection exists - bulk optimizations not applied'));
    }
  }

  return client;
}

/**
 * Re-enable indexing after bulk upload completes
 */
async function enableIndexing(client: QdrantClient): Promise<void> {
  if (!OPTIMIZE_FOR_BULK || !ENABLE_INDEXING_AFTER) {
    return;
  }

  console.log();
  console.log(chalk.cyan('🔧 Re-enabling indexing for fast search...'));
  console.log(chalk.dim('   This will build the HNSW index in the background'));
  
  try {
    // Update HNSW config to enable indexing (m: 16 is default)
    await client.updateCollection(QDRANT_COLLECTION_NAME, {
      hnsw_config: {
        m: 16, // Standard HNSW connections
      },
    });
    
    console.log(chalk.green('✓ Indexing enabled - HNSW will be built in background'));
    console.log(chalk.dim('   Monitor progress: Check collection status in Qdrant dashboard'));
  } catch (error) {
    console.log(chalk.yellow('⚠️  Failed to enable indexing:'), error instanceof Error ? error.message : error);
  }
}

/**
 * Read embeddings from a parquet file
 */
async function readEmbeddingsFromParquet(filePath: string): Promise<{ embeddings: EmbeddingRecord[]; readTime: number; prepTime: number }> {
  const readStart = performance.now();
  const buffer = await fs.readFile(filePath);
  const arrayBuffer = buffer.buffer.slice(
    buffer.byteOffset,
    buffer.byteOffset + buffer.byteLength
  ) as ArrayBuffer;
  const readEnd = performance.now();

  const prepStart = performance.now();
  const embeddings: EmbeddingRecord[] = [];

  await parquetRead({
    file: arrayBuffer,
    rowFormat: 'object',
    onComplete: (rows: any[]) => {
      if (rows.length === 0) return;
      
      // Cache vector dimension from first row (optimization: avoid repeated Object.keys calls)
      let vectorDim = VECTOR_DIMENSION;
      
      // If VECTOR_DIMENSION doesn't match actual data, detect from first row
      const firstRow = rows[0];
      if (firstRow && firstRow[`v${VECTOR_DIMENSION - 1}`] === undefined) {
        vectorDim = Object.keys(firstRow).filter((k) => k.startsWith('v')).length;
      }
      
      for (const row of rows) {
        // Fast vector extraction using cached dimension
        const vector = new Array(vectorDim);
        for (let i = 0; i < vectorDim; i++) {
          vector[i] = row[`v${i}`] as number;
        }

        // Optimized metadata parsing - check type first
        let metadata = row.metadata;
        if (metadata && typeof metadata === 'string') {
          try {
            metadata = JSON.parse(metadata);
          } catch (e) {
            metadata = {};
          }
        } else if (!metadata) {
          metadata = {};
        }

        embeddings.push({
          id: row.key as string, // Use the original key as the ID
          key: row.key as string,
          vector,
          metadata,
          queueItemId: row.queueItemId as string,
          timestamp: row.timestamp as string,
          correlationId: row.correlationId as string | undefined,
        });
      }
    },
  });
  const prepEnd = performance.now();

  return {
    embeddings,
    readTime: readEnd - readStart,
    prepTime: prepEnd - prepStart,
  };
}

/**
 * Upload embeddings to Qdrant in batches with parallel batch uploads
 */
async function uploadToQdrant(
  client: QdrantClient,
  embeddings: EmbeddingRecord[],
  workerId: string
): Promise<{ uploadTime: number }> {
  const uploadStart = performance.now();
  const totalBatches = Math.ceil(embeddings.length / BATCH_SIZE);
  let uploadedCount = 0;

  // Process batches with controlled parallelism
  for (let i = 0; i < embeddings.length; i += BATCH_SIZE * MAX_PARALLEL_BATCHES) {
    const batchPromises: Promise<void>[] = [];
    
    // Create parallel batch uploads
    for (let j = 0; j < MAX_PARALLEL_BATCHES; j++) {
      const startIdx = i + (j * BATCH_SIZE);
      if (startIdx >= embeddings.length) break;
      
      const batch = embeddings.slice(startIdx, startIdx + BATCH_SIZE);
      
      const uploadPromise = (async () => {
        // Prepare points outside try-catch for error reporting
        const points = batch.map((emb) => {
          // Use BigInt to preserve full 64-bit precision for tweet IDs.
          // The cast to `number` is compile-time only - the value remains a bigint at runtime.
          // Qdrant client serializes bigint correctly via JSON.rawJSON (requires Bun 1.2+).
          const pointId = BigInt(emb.key) as unknown as number;

          return {
            id: pointId,
            vector: emb.vector,
            payload: {
              key: emb.key, // Keep original string key in payload for reference
              metadata: emb.metadata,
              queueItemId: emb.queueItemId,
              timestamp: emb.timestamp,
              correlationId: emb.correlationId,
            },
          };
        });

        try {
          await client.upsert(QDRANT_COLLECTION_NAME, {
            wait: true,
            points,
          });

          uploadedCount += batch.length;
          progressState.uploadedEmbeddings += batch.length;
          
          // Update worker progress
          const worker = progressState.activeWorkers.get(workerId);
          if (worker) {
            worker.progress = uploadedCount;
            worker.status = `Uploading batches (${uploadedCount}/${embeddings.length})`;
          }
        } catch (error) {
          // Enhance error with batch details for debugging
          const errorMsg = error instanceof Error ? error.message : String(error);
          const firstKey = batch[0]?.key || 'unknown';
          const firstKeyType = typeof batch[0]?.key;
          
          let enhancedMessage = `Batch upload failed (${batch.length} points, first key: ${firstKey}, key type: ${firstKeyType}): ${errorMsg}`;
          
          // Try to extract more details from Qdrant error
          if (error && typeof error === 'object') {
            const qdrantError = error as any;
            
            // Check various error response formats
            if (qdrantError.data) {
              enhancedMessage += ` | Data: ${JSON.stringify(qdrantError.data)}`;
            }
            if (qdrantError.response?.data) {
              enhancedMessage += ` | Response: ${JSON.stringify(qdrantError.response.data)}`;
            }
            if (qdrantError.status) {
              enhancedMessage += ` | Status: ${qdrantError.status}`;
            }
            
            // Log the full error object to console for immediate debugging
            console.error('\n🔍 Full Qdrant Error Details:');
            if (points[0]) {
              console.error('First point ID:', points[0].id, `(${typeof points[0].id})`);
              console.error('Vector length:', points[0].vector?.length);
              console.error('Payload keys:', Object.keys(points[0].payload || {}));
            }
            console.error('Error status:', qdrantError.status);
            console.error('Error message:', qdrantError.message);
            if (qdrantError.data) {
              console.error('Error data:', JSON.stringify(qdrantError.data, null, 2));
            }
          }
          
          const enhancedError = new Error(enhancedMessage);
          throw enhancedError;
        }
      })();
      
      batchPromises.push(uploadPromise);
    }

    // Wait for this batch group to complete
    await Promise.all(batchPromises);
  }
  
  const uploadEnd = performance.now();
  return { uploadTime: uploadEnd - uploadStart };
}

/**
 * Process a single file
 */
async function processFile(
  client: QdrantClient,
  fileName: string,
  filePath: string
): Promise<{ success: boolean; embeddingCount: number }> {
  const fileStart = performance.now();
  const workerId = crypto.randomUUID();
  
  try {
    // Register worker
    progressState.activeWorkers.set(workerId, {
      fileName,
      status: 'Reading parquet file...',
      progress: 0,
      total: 0,
    });

    // Read embeddings
    const { embeddings, readTime, prepTime } = await readEmbeddingsFromParquet(filePath);
    
    if (embeddings.length === 0) {
      progressState.activeWorkers.delete(workerId);
      return { success: true, embeddingCount: 0 };
    }

    // Update worker with total
    const worker = progressState.activeWorkers.get(workerId);
    if (worker) {
      worker.total = embeddings.length;
      worker.status = `Starting upload of ${embeddings.length} embeddings...`;
    }

    // Upload to Qdrant
    const { uploadTime } = await uploadToQdrant(client, embeddings, workerId);

    // Complete
    const fileEnd = performance.now();
    const totalTime = fileEnd - fileStart;
    
    // Record profiling metrics
    profilingState.perFileMetrics.push({
      fileName,
      readTime,
      prepTime,
      uploadTime,
      totalTime,
      embeddingCount: embeddings.length,
    });
    
    profilingState.aggregated.fileReadTime += readTime;
    profilingState.aggregated.dataPreparationTime += prepTime;
    profilingState.aggregated.uploadTime += uploadTime;
    profilingState.aggregated.totalFileTime += totalTime;
    profilingState.aggregated.fileCount++;
    
    progressState.activeWorkers.delete(workerId);
    return { success: true, embeddingCount: embeddings.length };
    
  } catch (error) {
    progressState.activeWorkers.delete(workerId);
    
    // Capture error for display and logging
    const errorMessage = error instanceof Error ? error.message : String(error);
    const errorStack = error instanceof Error ? error.stack : undefined;
    const fullError = errorStack || errorMessage;
    
    progressState.recentErrors.push({
      fileName,
      error: errorMessage,
      timestamp: Date.now(),
    });
    
    // Keep only last 20 errors to prevent memory buildup
    if (progressState.recentErrors.length > 20) {
      progressState.recentErrors.shift();
    }
    
    // Log to file with full stack trace
    await logErrorToFile(fileName, fullError);
    
    throw error;
  }
}

/**
 * Display profiling report
 */
function displayProfilingReport(): void {
  console.log();
  console.log(chalk.bold.cyan('╔════════════════════════════════════════════════════════════════╗'));
  console.log(chalk.bold.cyan('║') + chalk.bold.white('              PERFORMANCE PROFILING REPORT                  ') + chalk.bold.cyan('║'));
  console.log(chalk.bold.cyan('╚════════════════════════════════════════════════════════════════╝'));
  console.log();
  
  const agg = profilingState.aggregated;
  const avgReadTime = agg.fileReadTime / agg.fileCount;
  const avgPrepTime = agg.dataPreparationTime / agg.fileCount;
  const avgUploadTime = agg.uploadTime / agg.fileCount;
  const avgFileTime = agg.totalFileTime / agg.fileCount;
  
  console.log(chalk.bold('⏱️  Average Time Per File:'));
  console.log(`   Read Parquet:      ${chalk.cyan((avgReadTime / 1000).toFixed(2))}s ${chalk.dim(`(${((avgReadTime / avgFileTime) * 100).toFixed(1)}%)`)}`);
  console.log(`   Data Preparation:  ${chalk.cyan((avgPrepTime / 1000).toFixed(2))}s ${chalk.dim(`(${((avgPrepTime / avgFileTime) * 100).toFixed(1)}%)`)}`);
  console.log(`   Upload to Qdrant:  ${chalk.cyan((avgUploadTime / 1000).toFixed(2))}s ${chalk.dim(`(${((avgUploadTime / avgFileTime) * 100).toFixed(1)}%)`)}`);
  console.log(`   Total:             ${chalk.cyan((avgFileTime / 1000).toFixed(2))}s`);
  console.log();
  
  console.log(chalk.bold('📊 Total Time Breakdown:'));
  console.log(`   Read Parquet:      ${chalk.cyan(formatDuration(agg.fileReadTime / 1000))}`);
  console.log(`   Data Preparation:  ${chalk.cyan(formatDuration(agg.dataPreparationTime / 1000))}`);
  console.log(`   Upload to Qdrant:  ${chalk.cyan(formatDuration(agg.uploadTime / 1000))}`);
  console.log(`   Overhead:          ${chalk.cyan(formatDuration((agg.totalFileTime - agg.fileReadTime - agg.dataPreparationTime - agg.uploadTime) / 1000))}`);
  console.log();
  
  // Show top 5 slowest files
  const slowest = [...profilingState.perFileMetrics]
    .sort((a, b) => b.totalTime - a.totalTime)
    .slice(0, 5);
  
  console.log(chalk.bold('🐌 Slowest Files:'));
  for (const file of slowest) {
    const shortName = file.fileName.length > 50 ? '...' + file.fileName.slice(-47) : file.fileName;
    console.log(`   ${chalk.dim(shortName)}`);
    console.log(`      Total: ${chalk.yellow((file.totalTime / 1000).toFixed(2))}s | Read: ${(file.readTime / 1000).toFixed(2)}s | Prep: ${(file.prepTime / 1000).toFixed(2)}s | Upload: ${(file.uploadTime / 1000).toFixed(2)}s`);
  }
  console.log();
}

/**
 * Main import function
 */
async function main() {
  console.log(chalk.bold.cyan('\n╔════════════════════════════════════════════════════════════════╗'));
  console.log(chalk.bold.cyan('║') + chalk.bold.white('              QDRANT IMPORT - INITIALIZATION                ') + chalk.bold.cyan('║'));
  console.log(chalk.bold.cyan('╚════════════════════════════════════════════════════════════════╝\n'));
  
  console.log(chalk.bold('⚙️  Configuration:'));
  console.log(`   Qdrant URL:          ${chalk.cyan(QDRANT_URL)}`);
  console.log(`   Collection:          ${chalk.cyan(QDRANT_COLLECTION_NAME)}`);
  console.log(`   Queue Dir:           ${chalk.cyan(QUEUE_DIR)}`);
  console.log(`   Batch Size:          ${chalk.cyan(BATCH_SIZE)}`);
  console.log(`   Vector Dimension:    ${chalk.cyan(VECTOR_DIMENSION)}`);
  console.log(`   Parallel Files:      ${chalk.cyan(MAX_PARALLEL_FILES)}`);
  console.log(`   Parallel Batches:    ${chalk.cyan(MAX_PARALLEL_BATCHES)}`);
  console.log(`   Shards:              ${chalk.cyan(SHARD_NUMBER)}`);
  console.log(`   Max Files to Process: ${MAX_FILES_TO_PROCESS ? chalk.cyan(MAX_FILES_TO_PROCESS) : chalk.dim('All')}`);
  console.log(`   Bulk Optimization:   ${OPTIMIZE_FOR_BULK ? chalk.green('Enabled') : chalk.yellow('Disabled')} ${OPTIMIZE_FOR_BULK ? chalk.dim('(low RAM)') : ''}`);
  console.log(`   Error Log:           ${chalk.cyan(ERROR_LOG_FILE)}`);
  console.log();

  // Initialize Qdrant
  const client = await initializeQdrant();

  // Find all pending parquet files
  console.log(chalk.cyan('🔍 Scanning for pending files...'));
  const files = await fs.readdir(QUEUE_DIR);
  const pendingFiles = files
    .filter((f) => f.endsWith('.pending.parquet'))
    .sort();

  if (pendingFiles.length === 0) {
    console.log(chalk.yellow('⚠️  No pending files found. Exiting.'));
    return;
  }

  console.log(chalk.green(`✓ Found ${pendingFiles.length} pending files`));
  
  // Limit files to process if MAX_FILES_TO_PROCESS is specified
  const filesToProcess = MAX_FILES_TO_PROCESS 
    ? pendingFiles.slice(0, MAX_FILES_TO_PROCESS)
    : pendingFiles;
  
  if (MAX_FILES_TO_PROCESS && filesToProcess.length < pendingFiles.length) {
    console.log(chalk.yellow(`⚠️  Limiting to ${filesToProcess.length} files (${pendingFiles.length - filesToProcess.length} files will be skipped)`));
  }
  
  // Estimate total embeddings (assume 1000 per file, check last file for accuracy)
  console.log(chalk.cyan('📊 Estimating total embeddings...'));
  const { parquetMetadata } = await import('hyparquet');
  let estimatedTotal = 0;
  
  if (filesToProcess.length > 0) {
    // Assume 1000 embeddings per file for all but the last file
    const assumedPerFile = 1000;
    estimatedTotal = (filesToProcess.length - 1) * assumedPerFile;
    
    // Read the last file's metadata to get actual count
    try {
      const lastFileName = filesToProcess[filesToProcess.length - 1]!;
      const buffer = await fs.readFile(path.join(QUEUE_DIR, lastFileName));
      const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength) as ArrayBuffer;
      const metadata = parquetMetadata(arrayBuffer);
      const lastFileCount = Number(metadata.num_rows);
      estimatedTotal += lastFileCount;
      console.log(chalk.dim(`   Assuming ${assumedPerFile} per file, last file has ${lastFileCount}`));
    } catch (e) {
      // If we can't read the last file, just assume 1000 for it too
      estimatedTotal += assumedPerFile;
      console.log(chalk.dim(`   Assuming ${assumedPerFile} per file (couldn't read last file)`));
    }
  }

  progressState.totalFiles = filesToProcess.length;
  progressState.totalEmbeddings = estimatedTotal;
  progressState.startTime = Date.now();

  console.log(chalk.green(`✓ Estimated ${estimatedTotal.toLocaleString()} total embeddings\n`));
  console.log(chalk.bold.green('🚀 Starting parallel import...\n'));

  // Start progress display
  const progressInterval = setInterval(() => {
    displayProgress();
  }, 500);

  // Process files with controlled parallelism
  const results: Array<{ fileName: string; success: boolean; embeddingCount: number }> = [];
  
  for (let i = 0; i < filesToProcess.length; i += MAX_PARALLEL_FILES) {
    const batch = filesToProcess.slice(i, i + MAX_PARALLEL_FILES);
    
    const batchResults = await Promise.allSettled(
      batch.map(async (fileName) => {
        const filePath = path.join(QUEUE_DIR, fileName);
        const result = await processFile(client, fileName, filePath);
        return { fileName, ...result };
      })
    );

    for (let j = 0; j < batchResults.length; j++) {
      const result = batchResults[j];
      if (result && result.status === 'fulfilled') {
        results.push(result.value);
        progressState.completedFiles++;
      } else {
        results.push({ fileName: batch[j] || 'unknown', success: false, embeddingCount: 0 });
        progressState.failedFiles++;
      }
    }
  }

  // Stop progress display
  clearInterval(progressInterval);
  displayProgress(); // One final update

  // Re-enable indexing if it was deferred for bulk upload
  await enableIndexing(client);

  // Display profiling report
  if (profilingState.aggregated.fileCount > 0) {
    displayProfilingReport();
  }

  // Final summary
  const successful = results.filter(r => r.success).length;
  const failed = results.filter(r => !r.success).length;
  const totalProcessed = results.reduce((sum, r) => sum + r.embeddingCount, 0);

  console.log();
  console.log(chalk.bold.green('╔════════════════════════════════════════════════════════════════╗'));
  console.log(chalk.bold.green('║') + chalk.bold.white('                    IMPORT COMPLETE                         ') + chalk.bold.green('║'));
  console.log(chalk.bold.green('╚════════════════════════════════════════════════════════════════╝'));
  console.log();
  console.log(chalk.bold('📈 Final Statistics:'));
  console.log(`   ${chalk.green('✓')} Successful:  ${chalk.green(successful)}/${filesToProcess.length} files`);
  if (failed > 0) {
    console.log(`   ${chalk.red('✗')} Failed:      ${chalk.red(failed)}/${filesToProcess.length} files`);
  }
  console.log(`   ${chalk.cyan('📦')} Embeddings:  ${chalk.cyan(totalProcessed.toLocaleString())} uploaded`);
  
  const duration = (Date.now() - progressState.startTime) / 1000;
  const avgRate = totalProcessed / duration;
  console.log(`   ${chalk.magenta('⏱️')}  Duration:    ${chalk.magenta(formatDuration(duration))}`);
  console.log(`   ${chalk.yellow('⚡')} Avg Rate:    ${chalk.yellow(avgRate.toFixed(0))} embeddings/sec`);
  console.log();
  
  // Get collection info
  try {
    const collectionInfo = await client.getCollection(QDRANT_COLLECTION_NAME);
    console.log(chalk.bold('📊 Qdrant Collection:'));
    console.log(`   Total vectors: ${chalk.cyan(collectionInfo.points_count?.toLocaleString() || 'N/A')}`);
    console.log(`   Status:        ${chalk.green(collectionInfo.status || 'N/A')}`);
  } catch (e) {
    console.log(chalk.yellow('⚠️  Could not fetch collection stats'));
  }
  
  console.log();

  if (failed > 0) {
    console.log(chalk.red('⚠️  Some files failed to process.'));
    console.log(chalk.yellow(`📄 Full error details have been logged to: ${ERROR_LOG_FILE}`));
    console.log();
    process.exit(1);
  }
}

// Run the script
main().catch((error) => {
  console.error(chalk.red('\n❌ Fatal error:'), error);
  process.exit(1);
});

