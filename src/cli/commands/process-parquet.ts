import fs from 'fs/promises';
import fsSync from 'fs';
import path from 'path';
import chalk from 'chalk';
import ora from 'ora';
import { asyncBufferFromFile, parquetRead, parquetReadObjects, parquetMetadataAsync } from 'hyparquet';
import { CA_EmbedClient } from '../utils/client.js';
import { StateManager } from '../utils/state-manager.js';
import { ProgressTracker } from '../utils/progress-tracker.js';
import { CircuitBreaker, DEFAULT_CIRCUIT_BREAKER_OPTIONS, CircuitState } from '../utils/circuit-breaker.js';
import { TweetProcessor, type TweetProcessorOptions } from '../utils/tweet-processor.js';
import { HTMLExporter, type TweetComparisonData } from '../utils/html-exporter.js';
import { EmbeddingExporter, type ExportFormat, type EmbeddingRecord } from '../utils/embedding-exporter.js';
import { FileHasher, type CacheManifest } from '../utils/file-hasher.js';
import { IntermediaryParquetWriter, type ProcessedTweetRecord } from '../utils/intermediary-parquet-writer.js';
import { BatchFileManager, type BatchManagerOptions, type BatchRecord } from '../utils/batch-file-manager.js';
import { confirmDeepInfraUsage, confirmProcessExistingBatchFiles, displayBatchInfo } from '../utils/user-confirmation.js';
import { BatchProgressDisplay } from '../utils/batch-progress-display.js';
import { MissingQuotedTweetsLogger } from '../utils/missing-quoted-tweets-logger.js';
import { QdrantVectorStore } from '../../stores/qdrant-vector-store.js';
import { appConfig } from '../../config/index.js';

interface ProcessParquetOptions {
  file: string;
  url: string;
  apiKey?: string;
  batchSize: string;
  parallel: string;
  textColumn: string;
  idColumn: string;
  reprocessExisting: boolean;
  resumeFile?: string;
  dryRun: boolean;
  maxRecords?: string;
  showHeaders: boolean;
  noContext?: boolean;
  maxTextLength?: string;
  includeDate?: boolean;
  exportHtml?: string | boolean;
  exportHtmlLimit?: string;
  exportEmbeddings?: string | boolean;
  exportFormat?: string;
  clearCache?: boolean;
  disableCache?: boolean;
  showCacheStats?: boolean;
  useStreaming?: boolean;
  batchFileDir?: string;
  skipConfirmation?: boolean;
  manifestSaveInterval?: string;
  disableProgressUi?: boolean;
  /** Skip checking Qdrant for existing records - useful when you know all records need processing */
  skipQdrantCheck?: boolean;
}

interface ParquetRecord {
  [key: string]: any;
}

interface ProcessingItem {
  key: string;
  content: string;
  rowIndex: number;
}

interface ColumnChunk {
  [columnName: string]: any[];
}

/**
 * Load all existing keys from Qdrant vector store
 * Uses efficient scrollPointIds() which only retrieves IDs without vectors
 */
async function loadExistingQdrantKeys(spinner: ReturnType<typeof ora>): Promise<Set<string>> {
  const existingKeys = new Set<string>();

  try {
    spinner.text = 'Connecting to Qdrant to check for existing records...';

    const vectorStore = new QdrantVectorStore(appConfig.database);
    await vectorStore.initialize();

    // Get total count first
    const totalCount = await vectorStore.getPointsCount();

    if (totalCount === 0) {
      spinner.text = 'Qdrant collection is empty, no existing records to skip';
      await vectorStore.close();
      return existingKeys;
    }

    spinner.text = `Loading ${totalCount.toLocaleString()} existing keys from Qdrant...`;

    let loadedCount = 0;
    const batchSize = 10000;

    for await (const batch of vectorStore.scrollPointIds(batchSize, (count) => {
      spinner.text = `Loading existing keys from Qdrant: ${count.toLocaleString()}/${totalCount.toLocaleString()}`;
    })) {
      for (const point of batch) {
        existingKeys.add(point.key);
      }
      loadedCount += batch.length;
    }

    await vectorStore.close();

    spinner.succeed(`Loaded ${existingKeys.size.toLocaleString()} existing keys from Qdrant`);
    return existingKeys;

  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    spinner.warn(`Could not load existing Qdrant keys: ${errorMessage}`);
    console.log(chalk.yellow('  ℹ️  Proceeding without Qdrant duplicate check - some records may already exist'));
    return existingKeys;
  }
}

async function processParquetWithBatchFiles(
  options: ProcessParquetOptions,
  client: CA_EmbedClient,
  embeddingExporter: EmbeddingExporter | null,
  batchSize: number,
  parallelCount: number,
  maxRecords: number | undefined,
  maxTextLength: number,
  existingQdrantKeys?: Set<string>
): Promise<void> {
  const spinner = ora('Initializing batch file manager...').start();

  // Create batch file manager
  const batchDir = options.batchFileDir || path.join(path.dirname(options.file), '.ca_embed-batches', path.basename(options.file, '.parquet'));
  console.log(chalk.gray(`Batch directory: ${batchDir}`));

  const batchManagerOptions: BatchManagerOptions = {
    sourceFile: options.file,
    outputDir: batchDir,
    textColumn: options.textColumn,
    idColumn: options.idColumn,
    maxTextLength,
    includeDate: options.includeDate ?? false,
    processConversationContext: !options.noContext,
    batchSize,
    reprocessExisting: options.reprocessExisting,
    maxRecords,
    existingQdrantKeys
  };

  const batchManager = new BatchFileManager(batchManagerOptions);
  await batchManager.initialize();

  // Configure manifest save interval if specified
  if (options.manifestSaveInterval) {
    const intervalMs = parseInt(options.manifestSaveInterval, 10);
    if (!isNaN(intervalMs) && intervalMs >= 5000) {
      batchManager.setManifestSaveInterval(intervalMs);
      console.log(chalk.gray(`⚙️  Manifest save interval set to ${intervalMs}ms`));
    } else {
      console.warn(chalk.yellow(`⚠️  Invalid manifest-save-interval: ${options.manifestSaveInterval} (minimum 5000ms)`));
    }
  }

  spinner.succeed('Batch file manager initialized');

  // Note: Server sync is disabled because the current checkExistingKeys()
  // implementation doesn't actually check the server and just returns empty results
  // This was causing the "Initializing..." spinner to hang while doing useless work
  console.log(chalk.gray('ℹ️  Server sync skipped - duplicate detection will be handled during processing'));

  // Get statistics
  const stats = batchManager.getStats();
  console.log(chalk.cyan('\n📊 Batch Processing Summary:'));
  console.log(`  📁 Total records: ${stats.totalRecords.toLocaleString()}`);
  console.log(`  📦 Total batches: ${stats.totalBatches}`);
  console.log(`  ✅ Records embedded: ${stats.recordsEmbedded.toLocaleString()}`);
  console.log(`  💾 Records stored: ${stats.recordsStored.toLocaleString()}`);
  console.log(`  🔄 Records pending: ${stats.recordsPending.toLocaleString()}`);

  if (options.dryRun) {
    console.log(chalk.yellow('\n🔍 Dry run mode - no API calls will be made'));
    return;
  }

  if (stats.recordsPending === 0) {
    console.log(chalk.green('\n✅ All records are already processed!'));
    return;
  }

  // Ask user for confirmation before sending data to DeepInfra (unless skipped)
  if (!options.skipConfirmation) {
    const shouldProceed = await confirmDeepInfraUsage(stats.recordsPending);
    if (!shouldProceed) {
      console.log(chalk.yellow('Processing cancelled by user.'));
      console.log(chalk.gray('💡 Batch files have been created and saved. You can resume anytime with the same command.'));
      return;
    }
  }

  // Get batches that need processing
  const batchesToProcess = await batchManager.getBatchesToProcess();
  console.log(chalk.cyan(`\n🚀 Processing ${batchesToProcess.length} batches with pending records...`));

  // Setup debug logging to file
  const debugLogPath = path.join(options.batchFileDir || path.join(path.dirname(options.file), '.ca_embed-batches', path.basename(options.file, '.parquet')), 'debug-concurrency.log');
  let debugLogStream: fsSync.WriteStream | null = fsSync.createWriteStream(debugLogPath, { flags: 'a' });
  const originalConsoleLog = console.log;
  console.log = (...args: any[]) => {
    const message = args.map(arg => typeof arg === 'string' ? arg : JSON.stringify(arg)).join(' ');
    if (debugLogStream) debugLogStream.write(message + '\n');
    originalConsoleLog(...args);
  };
  console.log(chalk.cyan(`📝 Debug logging enabled: ${debugLogPath}`));

  // Initialize progress tracker and visual display
  const progressTracker = new ProgressTracker();
  progressTracker.start(stats.recordsPending);

  const progressDisplay = new BatchProgressDisplay();
  if (!options.disableProgressUi) {
    progressDisplay.start();
  } else {
    console.log(chalk.gray('📊 Progress UI disabled - will show milestone logs only'));
  }

  let processed = 0;
  let errors = 0;

  // Initialize circuit breaker
  const circuitBreaker = new CircuitBreaker({
    ...DEFAULT_CIRCUIT_BREAKER_OPTIONS,
    resetTimeout: 60000,
    failureThreshold: 3,
    failureRate: 0.8
  });

  const processBatchFile = async (batchFile: any) => {
    const batchStartTime = Date.now();
    console.log(chalk.gray(`[${new Date().toISOString()}] 🔵 START processBatchFile ${batchFile.id}`));

    // Load and filter records from this batch file
    const loadStart = Date.now();
    const allRecords = await batchManager.loadBatch(batchFile);
    console.log(chalk.gray(`[${new Date().toISOString()}] 📁 LOADED ${batchFile.id} in ${Date.now() - loadStart}ms`));

    const unprocessedRecords = batchManager.filterUnprocessedRecords(allRecords);
    console.log(chalk.gray(`[${new Date().toISOString()}] 🔍 FILTERED ${batchFile.id} - ${unprocessedRecords.length} records`));

    if (unprocessedRecords.length === 0) {
      return; // Skip if no unprocessed records
    }

    // Respect maxRecords limit
    const recordsToProcess = maxRecords
      ? unprocessedRecords.slice(0, Math.max(0, maxRecords - processed))
      : unprocessedRecords;

    if (recordsToProcess.length === 0) {
      return; // Stop if we've hit the limit
    }

    // Split records into chunks to respect API limit using the user-specified batch size
    if (recordsToProcess.length > batchSize) {
      console.log(chalk.gray(`📦 Batch ${batchFile.id}: ${recordsToProcess.length} records → splitting into ${Math.ceil(recordsToProcess.length / batchSize)} chunks of ${batchSize}`));
    }

    // Process all chunks for this batch file
    console.log(chalk.blue(`[${new Date().toISOString()}] 📦 Creating chunks for ${batchFile.id}, recordsToProcess.length=${recordsToProcess.length}, batchSize=${batchSize}`));
    const chunkPromises: Promise<void>[] = [];
    for (let i = 0; i < recordsToProcess.length; i += batchSize) {
      const chunk = recordsToProcess.slice(i, i + batchSize);
      const chunkId = `${batchFile.id}_chunk_${Math.floor(i / batchSize)}`;
      console.log(chalk.blue(`[${new Date().toISOString()}] 🎯 Pushing processChunk for ${chunkId}`));
      chunkPromises.push(processChunk(chunkId, chunk));
      console.log(chalk.blue(`[${new Date().toISOString()}] ✓ Pushed processChunk for ${chunkId}`));
    }

    console.log(chalk.blue(`[${new Date().toISOString()}] ⏳ Awaiting Promise.all for ${batchFile.id} with ${chunkPromises.length} chunks`));
    await Promise.all(chunkPromises);
    console.log(chalk.blue(`[${new Date().toISOString()}] ✅ Promise.all completed for ${batchFile.id}`));
  };

  const processChunk = async (chunkId: string, batchRecords: BatchRecord[]) => {
    const chunkStartTime = Date.now();
    console.log(chalk.cyan(`[${new Date().toISOString()}] ⭐ START processChunk ${chunkId} with ${batchRecords.length} records`));

    const maxRetries = 2;
    let attempt = 0;

    // Initialize batch in progress display
    progressDisplay.updateBatch(chunkId, {
      status: 'preparing',
      recordCount: batchRecords.length,
      maxAttempts: maxRetries + 1
    });

    while (attempt <= maxRetries) {
      attempt++;

      progressDisplay.updateBatch(chunkId, {
        status: attempt > 1 ? 'retrying' : 'preparing',
        attempt
      });

      try {
        // Skip circuit breaker check for parallel processing
        // Circuit breaker was preventing true parallelism by serializing requests

        const embeddingItems = batchRecords.map(record => ({
          key: record.key,
          content: record.content,
          contentType: 'text' as const,
          metadata: {
            source: 'parquet_import',
            original_text: record.metadata.original_text,
            ...record.metadata.processing_metadata
          }
        }));

        // Update status to sending
        progressDisplay.updateBatch(chunkId, {
          status: 'sending'
        });

        // Execute API call directly without circuit breaker (for true parallelism)
        progressDisplay.updateBatch(chunkId, {
          status: 'processing'
        });

        console.log(chalk.yellow(`[${new Date().toISOString()}] 🚀 CALLING API for ${chunkId}`));
        const apiStartTime = Date.now();
        const response = await client.generateAndStoreEmbeddings(embeddingItems);
        console.log(chalk.green(`[${new Date().toISOString()}] ✅ API RESPONSE ${chunkId} in ${Date.now() - apiStartTime}ms`));

        if (response && response.success) {
          processed += batchRecords.length;

          // Mark records as fully processed (embedded and stored) - single call, debounced save
          const keys = batchRecords.map(r => r.key);
          batchManager.markRecordsAsProcessed(keys);

          // Update progress display to completed
          progressDisplay.updateBatch(chunkId, {
            status: 'completed'
          });

          // Export embeddings if requested
          if (embeddingExporter && response.results) {
            const embeddings: EmbeddingRecord[] = response.results.map(result => ({
              key: result.key,
              vector: result.vector,
              metadata: result.metadata,
              timestamp: new Date().toISOString()
            }));

            await embeddingExporter.addEmbeddings(embeddings);
          }

          progressTracker.update(processed);
          return; // Success - exit retry loop
        } else {
          throw new Error(`API returned error: ${response.error}`);
        }
      } catch (error) {
        const isLastAttempt = attempt > maxRetries;

        const errorMessage = error instanceof Error ? error.message : String(error);

        if (!isLastAttempt) {
          // Update status to retrying
          progressDisplay.updateBatch(chunkId, {
            status: 'retrying',
            attempt: attempt + 1,
            error: errorMessage
          });

          const delay = attempt === 1 ? 60000 : 300000;
          console.warn(chalk.yellow(`⚠️  Batch ${chunkId} failed (attempt ${attempt}/${maxRetries + 1}), retrying in ${delay/1000}s...`));
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }

        // Final failure
        progressDisplay.updateBatch(chunkId, {
          status: 'failed',
          error: errorMessage
        });

        errors += batchRecords.length;
        console.error(chalk.red(`❌ Failed to process batch ${chunkId} (${batchRecords.length} items) after ${maxRetries + 1} attempts:`));
        console.error(chalk.red(`   Error: ${error instanceof Error ? error.name : 'Unknown'} - ${error instanceof Error ? error.message : error}`));
        break;
      }
    }
  };

  // Process batch files with true parallelism using Promise.race for optimal concurrency
  const activePromises = new Set<Promise<void>>();
  let processedBatches = 0;

  for (const batchFile of batchesToProcess) {
    console.log(chalk.magenta(`[${new Date().toISOString()}] 🎬 LOOP ITERATION: Queueing ${batchFile.id}, activePromises.size=${activePromises.size}`));

    // IMPORTANT: Wrap in setImmediate to ensure promises start in parallel
    // Without this, the first await in processBatchFile blocks the loop
    const promise = new Promise<void>((resolve, reject) => {
      setImmediate(() => {
        console.log(chalk.blue(`[${new Date().toISOString()}] 🔷 setImmediate FIRED for ${batchFile.id}`));
        processBatchFile(batchFile).then(resolve).catch(reject);
      });
    }).then(() => {
      // Remove from active set when completed
      activePromises.delete(promise);
      console.log(chalk.gray(`[${new Date().toISOString()}] ✂️  REMOVED ${batchFile.id} from active set`));
    });

    activePromises.add(promise);
    console.log(chalk.magenta(`[${new Date().toISOString()}] ➕ ADDED ${batchFile.id} to active set, size=${activePromises.size}`));

    // Control parallelism - wait for one to complete when we reach the limit
    if (activePromises.size >= parallelCount) {
      console.log(chalk.red(`[${new Date().toISOString()}] ⏸️  WAITING: activePromises.size (${activePromises.size}) >= parallelCount (${parallelCount})`));
      await Promise.race(activePromises);
      console.log(chalk.red(`[${new Date().toISOString()}] ▶️  RESUMED: One promise completed, continuing loop`));
    }

    processedBatches++;
    if (processedBatches % 10 === 0) {
      console.log(chalk.gray(`Started processing ${processedBatches}/${batchesToProcess.length} batch files...`));
    }
  }

  // Wait for all remaining batches to complete
  if (activePromises.size > 0) {
    await Promise.all(activePromises);
  }

  progressTracker.finish();

  // Stop the progress display and show final summary
  progressDisplay.showFinalSummary();

  // Flush any pending manifest updates
  await batchManager.flushManifest();

  // Finalize embedding export
  if (embeddingExporter) {
    await embeddingExporter.finalize();
  }

  console.log(chalk.green('✨ Batch Processing Complete!'));
  console.log(`  📊 Total processed: ${processed}`);
  console.log(`  ❌ Total errors: ${errors}`);
  console.log(`  📈 Success rate: ${processed > 0 ? ((processed / (processed + errors)) * 100).toFixed(1) : 0}%`);
  console.log(`  🛡️  ${circuitBreaker.getStatusMessage()}`);

  // Show final stats
  const finalStats = batchManager.getStats();
  console.log(`\n📊 Final Statistics:`);
  console.log(`  ✅ Records complete: ${finalStats.recordsComplete.toLocaleString()}`);
  console.log(`  🔄 Records pending: ${finalStats.recordsPending.toLocaleString()}`);

  // Close debug log stream
  console.log = originalConsoleLog;
  if (debugLogStream) {
    debugLogStream.end();
    console.log(chalk.cyan(`📝 Debug log saved to: ${debugLogPath}`));
    debugLogStream = null;
  }
}

async function processParquetStreaming(
  file: any,
  totalRows: number,
  options: ProcessParquetOptions,
  client: CA_EmbedClient,
  stateManager: StateManager,
  progressTracker: ProgressTracker,
  tweetProcessor: TweetProcessor,
  embeddingExporter: EmbeddingExporter | null,
  batchSize: number,
  parallelCount: number,
  maxRecords: number | undefined,
  exportHtmlLimit: number,
  maxTextLength: number
): Promise<void> {
  const state = await stateManager.load();
  const processedKeys = new Set(state.processedKeys);

  let items: ProcessingItem[] = [];
  let tweetComparisons: TweetComparisonData[] = [];
  let skippedEmpty = 0;
  let skippedProcessed = 0;
  let processedCount = 0;
  let usedCache = false;

  const spinner = ora('Checking for cached processed data...').start();

  // Check cache if not disabled
  let cacheInfo: Awaited<ReturnType<typeof FileHasher.isCacheValid>> = { valid: false };

  if (!options.disableCache) {
    const processingOptions: CacheManifest['processingOptions'] = {
      textColumn: options.textColumn,
      idColumn: options.idColumn,
      maxTextLength,
      includeDate: options.includeDate ?? false,
      processConversationContext: !options.noContext
    };

    cacheInfo = await FileHasher.isCacheValid(options.file, processingOptions);

    if (cacheInfo.valid && cacheInfo.cacheDir) {
      spinner.text = 'Found valid cache, loading processed data...';

      try {
        const intermediaryPath = FileHasher.getIntermediaryFilePath(cacheInfo.cacheDir);
        const processedRecords = await IntermediaryParquetWriter.readProcessedTweets(intermediaryPath);

        // Convert cached records to the format needed
        items = processedRecords.map(record => IntermediaryParquetWriter.toProcessingItem(record));

        // Generate comparison data for HTML export (with limit)
        if (options.exportHtml) {
          tweetComparisons = processedRecords
            .slice(0, exportHtmlLimit)
            .map(record => IntermediaryParquetWriter.toTweetComparisonData(record));
        }

        // Filter out already processed items
        if (!options.reprocessExisting) {
          const originalCount = items.length;
          items = items.filter(item => !processedKeys.has(item.key));
          skippedProcessed = originalCount - items.length;
        }

        processedCount = items.length;
        usedCache = true;
        spinner.succeed(`Loaded ${processedRecords.length} processed records from cache (${skippedProcessed} already completed)`);

      } catch (error) {
        spinner.warn(`Cache read failed, processing from scratch: ${error}`);
        cacheInfo.valid = false;
      }
    }
  }

  // Initialize missing quoted tweets logger
  const missingQuotedLogger = new MissingQuotedTweetsLogger(path.dirname(options.file));
  await missingQuotedLogger.initialize();

  // If no valid cache, process from scratch
  if (!cacheInfo.valid || !usedCache) {
    spinner.text = 'Processing parquet file from source...';
    let conversationMapsBuilt = false;
    let rowIndex = 0;
    const processedRecords: ProcessedTweetRecord[] = [];

    // Get schema information first
    spinner.text = 'Reading parquet schema...';
    let schemaColumns: string[] = [];
    let schemaFirstRecord: any = null;

    const columnFirstValues: Record<string, any> = {};
    await parquetRead({
      file,
      onChunk: (chunk: any) => {
        if (chunk.columnName && chunk.columnData) {
          const columnName = chunk.columnName;
          if (!schemaColumns.includes(columnName)) {
            schemaColumns.push(columnName);
            if (chunk.columnData.length > 0) {
              columnFirstValues[columnName] = chunk.columnData[0];
            }
          }
        }
      }
    });

    if (schemaColumns.length > 0) {
      schemaFirstRecord = columnFirstValues;
    }

    // For conversation context, check if we have required fields
    if (!options.noContext) {
      spinner.text = 'Checking for conversation context fields...';
      let hasConversationFields = false;

      const requiredFields = ['conversation_id', 'reply_to_tweet_id', 'quoted_tweet_id', 'created_at', 'username'];
      const missingFields = requiredFields.filter(field => !schemaColumns.includes(field));

      if (missingFields.length === 0) {
        hasConversationFields = true;
      } else {
        console.log(chalk.yellow(`⚠️  Warning: Conversation context processing enabled but missing fields: ${missingFields.join(', ')}`));
        console.log(chalk.yellow('Falling back to simple text processing for all tweets.'));
      }

      if (hasConversationFields) {
        // Only build conversation maps for smaller datasets to preserve memory
        if (totalRows < 5000000) {
          conversationMapsBuilt = true;
        } else {
          console.log(chalk.yellow(`⚠️  Large dataset (${totalRows.toLocaleString()} rows) detected. Disabling conversation threading to preserve memory.`));
          console.log(chalk.gray(`💡 Text cleaning and quote-tweet inclusion will still be applied.`));
          conversationMapsBuilt = false;
        }
      }
    }

    // Validate columns exist
    if (!schemaFirstRecord) {
      console.log('DEBUG: schemaFirstRecord is null, available columns:', schemaColumns);
      console.log('DEBUG: columnFirstValues:', Object.keys(columnFirstValues || {}));
      throw new Error('Parquet file is empty');
    }

    if (!(options.textColumn in schemaFirstRecord)) {
      throw new Error(`Text column '${options.textColumn}' not found in parquet file. Available columns: ${Object.keys(schemaFirstRecord).join(', ')}`);
    }

    if (!(options.idColumn in schemaFirstRecord)) {
      throw new Error(`ID column '${options.idColumn}' not found in parquet file. Available columns: ${Object.keys(schemaFirstRecord).join(', ')}`);
    }

    spinner.text = 'Loading and processing tweet data...';

    // Build conversation maps if needed and enabled (memory-efficient approach)
    if (conversationMapsBuilt) {
      // Get current file hash for conversation cache validation
      const currentFileHash = await FileHasher.hashFile(options.file);
      const conversationCacheDir = cacheInfo.cacheDir || await FileHasher.createCacheDirectory(options.file);
      const conversationCacheFile = path.join(conversationCacheDir, 'conversation-maps.json');
      let conversationMapsLoaded = false;

      // Try to load cached conversation maps
      if (!options.disableCache && fsSync.existsSync(conversationCacheFile)) {
        try {
          spinner.text = 'Loading cached conversation maps...';
          const cachedMaps = JSON.parse(fsSync.readFileSync(conversationCacheFile, 'utf-8'));

          // Validate cache is for the same dataset
          if (cachedMaps.fileHash === currentFileHash && cachedMaps.totalRows === totalRows) {
            tweetProcessor.loadConversationMaps(cachedMaps.conversationMaps, cachedMaps.replyMaps, cachedMaps.quoteMaps);
            console.log(chalk.green(`✅ Loaded cached conversation maps: ${cachedMaps.stats.conversationsBuilt} conversations, ${cachedMaps.stats.totalTweets} tweets`));
            conversationMapsLoaded = true;
          } else {
            console.log(chalk.yellow('⚠️  Conversation map cache invalid (file changed), rebuilding...'));
          }
        } catch (error) {
          console.log(chalk.yellow(`⚠️  Failed to load conversation cache: ${error instanceof Error ? error.message : 'Unknown error'}`));
        }
      }

      // Build conversation maps if not loaded from cache
      if (!conversationMapsLoaded) {
        spinner.text = 'Building conversation maps (loading full dataset)...';

        // Load all records ONLY for building conversation maps
        const allRecordsForMaps = await parquetReadObjects({ file });

        tweetProcessor.buildConversationMaps(allRecordsForMaps as any[]);
        const stats = tweetProcessor.getStats();
        console.log(chalk.green(`Built conversation maps: ${stats.conversationsBuilt} conversations, ${stats.totalTweets} tweets`));

        // Cache the conversation maps (disabled for large datasets due to memory constraints)
        if (!options.disableCache && totalRows < 1000000) {
          try {
            spinner.text = 'Caching conversation maps...';
            const mapsToCache = {
              fileHash: currentFileHash,
              totalRows,
              timestamp: new Date().toISOString(),
              conversationMaps: tweetProcessor.getConversationMaps(),
              replyMaps: tweetProcessor.getReplyMaps(),
              quoteMaps: tweetProcessor.getQuoteMaps(),
              stats
            };
            fsSync.writeFileSync(conversationCacheFile, JSON.stringify(mapsToCache), 'utf-8');
            console.log(chalk.gray(`💾 Conversation maps cached for future runs`));
          } catch (error) {
            console.log(chalk.yellow(`⚠️  Failed to cache conversation maps: ${error instanceof Error ? error.message : 'Unknown error'}`));
          }
        } else if (totalRows >= 1000000) {
          console.log(chalk.yellow(`⚠️  Conversation map caching disabled for large datasets (${totalRows.toLocaleString()} rows) to preserve memory.`));
          console.log(chalk.gray(`💡 Conversation context will be rebuilt each run, but memory usage is optimized.`));
        }

        // 🚀 RELEASE MEMORY: Clear the full dataset, we'll stream process next
        console.log(chalk.gray(`🗑️  Releasing ${allRecordsForMaps.length.toLocaleString()} records from memory...`));
        // allRecordsForMaps will be garbage collected here
      }
    }

    spinner.text = 'Processing and caching tweet data (streaming)...';

    // 🔄 STREAM PROCESSING: Simplified processing with quoted tweets
    let localSkippedEmpty = 0;
    let localSkippedProcessed = 0;
    let localRowIndex = 0;

    // Load all records
    const streamRecords = await parquetReadObjects({ file });

    // Build simple tweet lookup map for quoted tweets
    spinner.text = 'Building tweet lookup map for quoted tweets...';
    const tweetMap = new Map<string, any>();
    for (const record of streamRecords) {
      if (record.tweet_id) {
        tweetMap.set(record.tweet_id.toString(), record);
      }
    }
    console.log(chalk.gray(`📊 Built lookup map with ${tweetMap.size.toLocaleString()} tweets`));

    spinner.text = 'Processing tweets with quoted tweet integration...';
    console.log(chalk.gray(`📊 Processing ${streamRecords.length.toLocaleString()} records...`));

    for (const record of streamRecords) {
      if (maxRecords && processedCount >= maxRecords) {
        break;
      }

      localRowIndex++;

      const id = record[options.idColumn];
      const text = record[options.textColumn];

      // Skip if no ID or text
      if (!id || !text || typeof text !== 'string' || text.trim().length === 0) {
        localSkippedEmpty++;
        continue;
      }

      const key = String(id);

      // Check if this tweet has a quoted tweet
      let quotedTweet = null;
      if (record.quoted_tweet_id) {
        const quotedId = record.quoted_tweet_id.toString();
        quotedTweet = tweetMap.get(quotedId);

        if (!quotedTweet) {
          // Log missing quoted tweet but continue processing without it
          missingQuotedLogger.logMissingQuotedTweet(key, quotedId);
          // quotedTweet remains null, so processTweetSimple will process without it
        }
      }

      // Process text using new simplified processor
      let processedText = text;
      let quotesIncluded = false;
      try {
        const processingResult = tweetProcessor.processTweetSimple(record as any, quotedTweet);
        processedText = processingResult.processedText;
        quotesIncluded = processingResult.quotesIncluded;
      } catch (error) {
        console.warn(chalk.yellow(`Warning: Failed to process tweet for ${key}: ${error}`));
        processedText = text; // Fallback to original text
      }

      // Create processed record for caching
      const processedRecord: ProcessedTweetRecord = {
        key,
        processed_text: processedText,
        original_text: text,
        row_index: localRowIndex,
        processing_metadata: {
          has_context: false,
          has_quotes: quotesIncluded,
          is_truncated: processedText.endsWith('...'),
          character_difference: processedText.length - text.length,
          // Tweet metadata fields from parquet (use != null to preserve zero values)
          account_id: record.account_id != null ? String(record.account_id) : undefined,
          username: record.username ?? undefined,
          account_display_name: record.account_display_name ?? undefined,
          created_at: record.created_at ?? undefined,
          retweet_count: record.retweet_count != null ? Number(record.retweet_count) : undefined,
          favorite_count: record.favorite_count != null ? Number(record.favorite_count) : undefined,
          reply_to_tweet_id: record.reply_to_tweet_id != null ? String(record.reply_to_tweet_id) : undefined,
          reply_to_user_id: record.reply_to_user_id != null ? String(record.reply_to_user_id) : undefined,
          reply_to_username: record.reply_to_username ?? undefined,
          quoted_tweet_id: record.quoted_tweet_id != null ? String(record.quoted_tweet_id) : undefined,
          conversation_id: record.conversation_id != null ? String(record.conversation_id) : undefined,
        }
      };

      processedRecords.push(processedRecord);

      // Skip if already processed (unless reprocessing is enabled)
      if (!options.reprocessExisting && processedKeys.has(key)) {
        localSkippedProcessed++;
      } else {
        // Only add items with non-empty processed text
        if (processedText && processedText.trim().length > 0) {
          items.push({
            key,
            content: processedText.trim(),
            rowIndex: localRowIndex
          });
        } else {
          localSkippedEmpty++;
        }
      }

      processedCount++;
    }

    skippedEmpty += localSkippedEmpty;
    skippedProcessed += localSkippedProcessed;


    // Cache the processed results if not disabled
    if (!options.disableCache && processedRecords.length > 0) {
      try {
        spinner.text = 'Saving processed data to cache...';

        const cacheDir = await FileHasher.createCacheDirectory(options.file);
        const intermediaryPath = FileHasher.getIntermediaryFilePath(cacheDir);

        await IntermediaryParquetWriter.writeProcessedTweets(processedRecords, intermediaryPath);

        const processingOptions: CacheManifest['processingOptions'] = {
          textColumn: options.textColumn,
          idColumn: options.idColumn,
          maxTextLength,
          includeDate: options.includeDate ?? false,
          processConversationContext: !options.noContext
        };

        const manifest = await FileHasher.createCacheManifest(
          options.file,
          intermediaryPath,
          processingOptions
        );

        await FileHasher.saveCacheManifest(manifest, cacheDir);

        console.log(chalk.green(`💾 Cached ${processedRecords.length} processed records for future use`));

      } catch (error) {
        console.warn(chalk.yellow(`Warning: Failed to save cache: ${error}`));
      }
    }

    spinner.succeed(`Processed ${items.length} new items from ${totalRows} total rows (${skippedProcessed} already completed)`);
  }

  // Flush missing quoted tweets log
  await missingQuotedLogger.flush();

  console.log(chalk.cyan('\\n📊 Processing Summary:'));
  console.log(`  📁 Total records in file: ${totalRows}`);
  console.log(`  ✅ Records to process: ${items.length}`);
  console.log(`  ⏭️  Skipped (empty/invalid): ${skippedEmpty}`);
  console.log(`  🔄 Skipped (already processed): ${skippedProcessed}`);

  // Report missing quoted tweets
  if (missingQuotedLogger.hasMessages()) {
    console.log(`  ⚠️  Tweets with missing quoted tweet data: ${missingQuotedLogger.getCount()}`);
    console.log(`      (processed without quoted content)`);
    console.log(`  📄 Missing quoted tweets log: ${missingQuotedLogger.getLogPath()}`);
  }

  if (usedCache) {
    console.log(chalk.green(`  💾 Used cached processed data (fast processing)`));
  } else if (!options.disableCache) {
    console.log(chalk.cyan(`  💾 Processed data cached for future runs`));
  } else {
    console.log(chalk.gray(`  💾 Cache disabled`));
  }

  if (options.dryRun) {
    console.log(chalk.yellow('\\n🔍 Dry run mode - no API calls will be made'));

    if (options.exportHtml) {
      const htmlExporter = new HTMLExporter();
      const htmlPath = typeof options.exportHtml === 'string'
        ? options.exportHtml
        : `./tweet-validation-${new Date().toISOString().slice(0, 19).replace(/[:.]/g, '-')}.html`;

      await HTMLExporter.exportToHTML(tweetComparisons, htmlPath);
      console.log(chalk.green(`📄 HTML validation report exported: ${htmlPath}`));
    }

    return;
  }

  if (items.length === 0) {
    console.log(chalk.yellow('No items to process.'));
    return;
  }

  // Process items in batches with parallel execution
  console.log(chalk.cyan(`\\n🚀 Processing ${items.length} records in batches of ${batchSize}...`));

  const semaphore = new Array(parallelCount).fill(0);
  const batches: ProcessingItem[][] = [];

  for (let i = 0; i < items.length; i += batchSize) {
    batches.push(items.slice(i, i + batchSize));
  }

  progressTracker.start(items.length);
  let processed = 0;
  let errors = 0;

  // Initialize circuit breaker for API resilience
  const circuitBreaker = new CircuitBreaker({
    ...DEFAULT_CIRCUIT_BREAKER_OPTIONS,
    resetTimeout: 60000, // Wait 1 minute before retry when API is down
    failureThreshold: 3,  // Open circuit after 3 consecutive failures
    failureRate: 0.8     // Open circuit if 80% of requests fail
  });

  console.log(chalk.gray(`🛡️  Circuit breaker initialized for API resilience`));

  const processBatch = async (batch: ProcessingItem[]) => {
    const maxRetries = 2; // 2 retries = 3 total attempts
    let attempt = 0;

    while (attempt <= maxRetries) {
      try {
        // Check circuit breaker before making API call
        if (!circuitBreaker.shouldAllowRequest()) {
          const stats = circuitBreaker.getStats();
          const timeLeft = Math.ceil((stats.timeUntilRetry || 0) / 1000);
          throw new Error(`Circuit breaker is OPEN - API appears to be down. Retrying in ${timeLeft}s`);
        }

        const embeddingItems = batch.map(item => ({
          key: item.key,
          content: item.content,
          contentType: 'text' as const,
          metadata: { source: 'parquet_import' }
        }));

        // Execute API call with circuit breaker protection
        const response = await circuitBreaker.execute(async () => {
          return await client.generateAndStoreEmbeddings(embeddingItems);
        });

        if (response.success) {
          processed += batch.length;

          // Export embeddings if requested
          if (embeddingExporter && response.results) {
            const embeddings: EmbeddingRecord[] = response.results.map(result => ({
              key: result.key,
              vector: result.vector,
              metadata: result.metadata,
              timestamp: new Date().toISOString()
            }));

            await embeddingExporter.addEmbeddings(embeddings);
          }

          // Update state
          const batchKeys = batch.map(item => item.key);
          await stateManager.update(batchKeys);

          progressTracker.update(processed);
          return; // Success - exit retry loop
        } else {
          throw new Error(`API returned error: ${response.error}`);
        }
      } catch (error) {
        attempt++;
        const isLastAttempt = attempt > maxRetries;

        if (!isLastAttempt) {
          // Custom backoff: 60s, 300s delays
          const delay = attempt === 1 ? 60000 : 300000; // 60s for first retry, 300s for second retry
          const batchNumber = batch.length > 0 && batch[0]?.rowIndex ? Math.floor(batch[0].rowIndex / parseInt(options.batchSize, 10)) + 1 : '?';

          console.warn(chalk.yellow(`⚠️  Batch ${batchNumber} failed (attempt ${attempt}/${maxRetries + 1}), retrying in ${delay/1000}s...`));
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }

        // Final failure after all retries
        errors += batch.length;
        const errorDetails = error instanceof Error ? {
          name: error.name,
          message: error.message,
          code: (error as any).code,
          cause: (error as any).cause
        } : String(error);

        const batchNumber = batch.length > 0 && batch[0]?.rowIndex ? Math.floor(batch[0].rowIndex / parseInt(options.batchSize, 10)) + 1 : '?';
        console.error(chalk.red(`❌ Failed to process batch (${batch.length} items, batch ${batchNumber}) after ${maxRetries + 1} attempts:`));
        console.error(chalk.red(`   Error: ${error instanceof Error ? error.name : 'Unknown'} - ${error instanceof Error ? error.message : error}`));

        if (error instanceof Error && (error as any).code) {
          console.error(chalk.red(`   Code: ${(error as any).code}`));
        }

        if (error instanceof Error && error.cause) {
          console.error(chalk.red(`   Cause: ${error.cause}`));
        }

        // Provide suggestions based on error type
        if (error instanceof Error) {
          if (error.name === 'AbortError' || error.message.includes('aborted')) {
            console.error(chalk.yellow(`   💡 Suggestion: Request was aborted. Try reducing --parallel or --batch-size, or check network connection.`));
          } else if (error.message.includes('timeout') || error.message.includes('ETIMEDOUT')) {
            console.error(chalk.yellow(`   💡 Suggestion: Request timed out. Try reducing --parallel, increasing timeout, or check server status.`));
          } else if (error.message.includes('ECONNREFUSED') || error.message.includes('Unable to connect')) {
            console.error(chalk.yellow(`   💡 Suggestion: Cannot connect to server. Check if the CA_Embed server is running on localhost:3000.`));
          } else if (error.message.includes('rate limit') || error.message.includes('429')) {
            console.error(chalk.yellow(`   💡 Suggestion: Rate limited. Reduce --parallel or add delays between requests.`));
          } else if (error.message.includes('502') || error.message.includes('503') || error.message.includes('504')) {
            console.error(chalk.yellow(`   💡 Suggestion: Server error. The API server might be overloaded. Try reducing --parallel.`));
          }
        }
        break; // Exit retry loop on final failure
      }
    }
  };

  // Process batches with parallelism control
  const promises: Promise<void>[] = [];
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    if (!batch) continue;
    const promise = processBatch(batch);
    promises.push(promise);

    if (promises.length >= parallelCount) {
      await Promise.all(promises.splice(0, parallelCount));
    }
  }

  // Wait for remaining batches
  if (promises.length > 0) {
    await Promise.all(promises);
  }

  progressTracker.finish();

  // Export HTML validation report if requested
  if (options.exportHtml && tweetComparisons.length > 0) {
    const htmlExporter = new HTMLExporter();
    const htmlPath = typeof options.exportHtml === 'string'
      ? options.exportHtml
      : `./tweet-validation-${new Date().toISOString().slice(0, 19).replace(/[:.]/g, '-')}.html`;

    await HTMLExporter.exportToHTML(tweetComparisons, htmlPath);
    console.log(chalk.green(`📄 HTML validation report exported: ${htmlPath} (${tweetComparisons.length} tweets)`));
  }

  // Finalize embedding export
  if (embeddingExporter) {
    await embeddingExporter.finalize();
  }

  console.log(chalk.green('\\n✨ Processing Complete!'));
  console.log(`  📊 Total processed: ${processed}`);
  console.log(`  ❌ Total errors: ${errors}`);
  console.log(`  📈 Success rate: ${processed > 0 ? ((processed / (processed + errors)) * 100).toFixed(1) : 0}%`);

  // Show circuit breaker final status
  console.log(`  🛡️  ${circuitBreaker.getStatusMessage()}`);

  if (state.processedKeys.length > 0) {
    console.log(`  💾 State saved to: processing state file`);
  }
}

async function showParquetSchema(file: any, metadata: any, totalRows: number, options: ProcessParquetOptions) {
  const spinner = ora('Reading sample data...').start();

  try {
    // Read first few rows for schema inspection
    let firstRecord: any = null;
    let columns: string[] = [];

    const columnFirstValues: Record<string, any> = {};

    await parquetRead({
      file,
      onChunk: (chunk: any) => {
        // Each chunk is for a single column: { columnName: "field_name", columnData: [values...], rowStart: n, rowEnd: n }
        if (chunk.columnName && chunk.columnData) {
          const columnName = chunk.columnName;

          // Only collect column names and first values for schema inspection
          if (!columns.includes(columnName)) {
            columns.push(columnName);

            // Get first value from this column
            if (chunk.columnData.length > 0) {
              // Handle typed arrays like BigInt64Array
              if (Array.isArray(chunk.columnData)) {
                columnFirstValues[columnName] = chunk.columnData[0];
              } else {
                columnFirstValues[columnName] = chunk.columnData[0];
              }
            }
          }
        }
      }
    });

    // Build first record from collected first values
    if (columns.length > 0) {
      firstRecord = columnFirstValues;
    }

    spinner.succeed('Loaded parquet file schema');

    console.log(chalk.cyan('\n📋 Parquet File Schema:'));
    console.log(chalk.gray(`📁 File: ${path.basename(options.file)}`));

    if (columns.length > 0) {
      console.log(`🔢 Total columns: ${columns.length}`);
      console.log(`📊 Total rows: ${totalRows}`);

      console.log('\n📝 Column Details:');
      columns.forEach(columnName => {
        const sampleValue = firstRecord?.[columnName];
        const type = typeof sampleValue;

        let icon = '  ';
        if (columnName === options.textColumn) {
          icon = chalk.green('📝');
        } else if (columnName === options.idColumn) {
          icon = chalk.blue('🔑');
        }

        console.log(`  ${icon} ${chalk.bold(columnName)} - ${type}`);
      });

      if (firstRecord) {
        console.log('\n🔍 Sample Data (first row):');
        Object.entries(firstRecord).forEach(([key, value]) => {
          let displayValue = value;

          if (typeof value === 'bigint') {
            displayValue = value.toString();
          } else if (typeof value === 'string' && value.length > 100) {
            displayValue = value.substring(0, 100) + '...';
          } else if (value === null || value === undefined) {
            displayValue = 'null';
          }

          let icon = '  ';
          if (key === options.textColumn) {
            icon = chalk.green('📝');
          } else if (key === options.idColumn) {
            icon = chalk.blue('🔑');
          }

          let jsonValue;
          try {
            if (typeof displayValue === 'bigint') {
              jsonValue = displayValue.toString();
            } else {
              jsonValue = JSON.stringify(displayValue);
            }
          } catch (error) {
            jsonValue = String(displayValue);
          }

          console.log(`  ${icon} ${chalk.bold(key)}: ${jsonValue}`);
        });
      }

      // Validation hints
      console.log('\n💡 Usage Hints:');
      const hasTextColumn = columns.includes(options.textColumn);
      const hasIdColumn = columns.includes(options.idColumn);

      if (!hasTextColumn) {
        console.log(chalk.yellow(`  ⚠️  Text column '${options.textColumn}' not found. Use --text-column to specify the correct column.`));
        const textLikeColumns = columns.filter(col =>
          col.toLowerCase().includes('text') ||
          col.toLowerCase().includes('content') ||
          col.toLowerCase().includes('message') ||
          col.toLowerCase().includes('body') ||
          col.toLowerCase().includes('tweet')
        );
        if (textLikeColumns.length > 0) {
          console.log(chalk.cyan(`      💡 Possible text columns: ${textLikeColumns.join(', ')}`));
        }
      } else {
        console.log(chalk.green(`  ✅ Text column '${options.textColumn}' found`));
      }

      if (!hasIdColumn) {
        console.log(chalk.yellow(`  ⚠️  ID column '${options.idColumn}' not found. Use --id-column to specify the correct column.`));
        const idLikeColumns = columns.filter(col =>
          col.toLowerCase().includes('id') ||
          col.toLowerCase().includes('key') ||
          col.toLowerCase().includes('identifier')
        );
        if (idLikeColumns.length > 0) {
          console.log(chalk.cyan(`      💡 Possible ID columns: ${idLikeColumns.join(', ')}`));
        }
      } else {
        console.log(chalk.green(`  ✅ ID column '${options.idColumn}' found`));
      }

      if ((!hasTextColumn || !hasIdColumn) && columns.length > 0) {
        console.log(chalk.cyan(`\n📋 All available columns: ${columns.join(', ')}`));
      }

      if (hasTextColumn && hasIdColumn) {
        console.log(chalk.green('\n🚀 Ready to process! Remove --show-headers to start processing.'));
      } else {
        console.log(chalk.yellow('\n🔧 Update your column names and try again.'));
      }
    } else {
      console.log(chalk.red('No data found in parquet file'));
    }
  } catch (error) {
    spinner.fail('Error reading schema');
    console.error(chalk.red('Error reading schema:'), error);
  }
}

export async function processParquetCommand(options: ProcessParquetOptions) {
  const spinner = ora('Initializing...').start();

  try {
    // Parse and validate options
    const batchSize = parseInt(options.batchSize, 10);
    const parallelCount = parseInt(options.parallel, 10);
    const maxRecords = options.maxRecords ? parseInt(options.maxRecords, 10) : undefined;
    const maxTextLength = options.maxTextLength ? parseInt(options.maxTextLength, 10) : 10240;
    const exportHtmlLimit = options.exportHtmlLimit ? parseInt(options.exportHtmlLimit, 10) : 1000;
    const exportFormat = (options.exportFormat as ExportFormat) || 'jsonl';

    if (isNaN(batchSize) || batchSize < 1 || batchSize > 1000) {
      throw new Error('Batch size must be between 1 and 1000');
    }

    if (isNaN(parallelCount) || parallelCount < 1 || parallelCount > 100) {
      throw new Error('Parallel count must be between 1 and 100');
    }

    if (maxRecords !== undefined && (isNaN(maxRecords) || maxRecords < 1)) {
      throw new Error('Max records must be a positive number');
    }

    if (isNaN(maxTextLength) || maxTextLength < 100 || maxTextLength > 10240) {
      throw new Error('Max text length must be between 100 and 10240 characters');
    }

    if (isNaN(exportHtmlLimit) || exportHtmlLimit < 10 || exportHtmlLimit > 10000) {
      throw new Error('Export HTML limit must be between 10 and 10000 records');
    }

    // Validate export format
    const validFormats: ExportFormat[] = ['jsonl', 'csv', 'npy'];
    if (!validFormats.includes(exportFormat)) {
      throw new Error(`Export format must be one of: ${validFormats.join(', ')}`);
    }

    // Initialize components
    const client = new CA_EmbedClient(options.url, options.apiKey);
    const stateManager = new StateManager(options.resumeFile);
    const progressTracker = new ProgressTracker();

    // Initialize tweet processor
    const processorOptions: TweetProcessorOptions = {
      maxChars: maxTextLength,
      includeDate: options.includeDate ?? false,
      processConversationContext: !options.noContext // Default true unless --no-context is specified
    };
    const tweetProcessor = new TweetProcessor(processorOptions);

    // Initialize embedding exporter if requested
    let embeddingExporter: EmbeddingExporter | null = null;
    if (options.exportEmbeddings) {
      const fileName = (typeof options.exportEmbeddings === 'boolean' || options.exportEmbeddings === 'true')
        ? path.join('./data/export-embeddings', EmbeddingExporter.generateFilename(exportFormat, path.basename(options.file, '.parquet')))
        : options.exportEmbeddings;

      embeddingExporter = new EmbeddingExporter({
        format: exportFormat,
        outputPath: fileName,
        append: false,
        includeMetadata: true,
        batchSize: 100
      });

      await embeddingExporter.initialize();
      console.log(chalk.cyan(`📁 Embedding export initialized: ${fileName}`));
    }

    // Check if file exists
    try {
      await fs.access(options.file);
    } catch {
      throw new Error(`Parquet file not found: ${options.file}`);
    }

    // Check if we should use batch files (default) or streaming mode
    const useBatchFiles = !options.useStreaming;

    // Check for existing batch files first (before reading parquet metadata)
    if (useBatchFiles) {
      // Create a temporary batch manager to properly validate existing files
      const batchDir = options.batchFileDir || path.join(path.dirname(options.file), '.ca_embed-batches', path.basename(options.file, '.parquet'));

      const tempBatchManagerOptions: BatchManagerOptions = {
        sourceFile: options.file,
        outputDir: batchDir,
        textColumn: options.textColumn,
        idColumn: options.idColumn,
        maxTextLength,
        includeDate: options.includeDate ?? false,
        processConversationContext: !options.noContext,
        batchSize,
        reprocessExisting: options.reprocessExisting
      };

      const tempBatchManager = new BatchFileManager(tempBatchManagerOptions);
      const manifestExists = await tempBatchManager.loadManifest();

      if (manifestExists && await tempBatchManager.isManifestValid()) {
        const stats = tempBatchManager.getStats();

        spinner.stop(); // Stop spinner before user interaction
        console.log(chalk.cyan('\n🔍 Found existing batch files!'));
        displayBatchInfo(
          stats.totalRecords,
          stats.totalBatches,
          stats.recordsPending,
          stats.recordsComplete
        );

        if (stats.recordsPending > 0) {
          const shouldProcess = options.skipConfirmation || await confirmProcessExistingBatchFiles(
            stats.totalBatches,
            stats.totalRecords,
            stats.recordsPending
          );

          if (shouldProcess) {
            // Set the batch directory to the existing one
            if (!options.batchFileDir) {
              options.batchFileDir = batchDir;
            }

            // Note: Existing batch files don't need Qdrant key checking - the manifest tracks progress
            await processParquetWithBatchFiles(
              options,
              client,
              embeddingExporter,
              batchSize,
              parallelCount,
              maxRecords,
              maxTextLength,
              undefined // No Qdrant keys needed for existing batches
            );
            return;
          } else {
            console.log(chalk.yellow('Processing cancelled by user.'));
            return;
          }
        } else {
          console.log(chalk.green('✅ All records in existing batch files have been processed!'));
          return;
        }
      }
    }

    // Only read parquet file if we need to (no existing batch files or using streaming mode)
    spinner.text = useBatchFiles ? 'Reading parquet file for new batch creation...' : 'Reading parquet file...';

    // Get parquet file metadata for schema inspection
    let file: any;
    let metadata: any;
    let totalRows = 0;

    try {
      console.log(chalk.gray(`📁 Loading parquet file: ${options.file}`));
      const startTime = Date.now();

      file = await asyncBufferFromFile(options.file);
      console.log(chalk.gray(`📁 File buffer loaded in ${Date.now() - startTime}ms`));

      metadata = await parquetMetadataAsync(file);
      totalRows = Number(metadata.num_rows) || 0;

      console.log(chalk.gray(`📊 Parquet metadata loaded: ${totalRows.toLocaleString()} rows, ${(file.length / 1024 / 1024).toFixed(1)}MB file size`));
    } catch (error) {
      throw new Error(`Failed to read parquet file metadata: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }

    // If just showing headers, display schema and exit
    if (options.showHeaders) {
      await showParquetSchema(file, metadata, totalRows, options);
      return;
    }

    // Handle cache management options
    if (options.showCacheStats) {
      spinner.text = 'Gathering cache statistics...';
      const stats = await FileHasher.getCacheStats();
      spinner.succeed('Cache statistics retrieved');

      console.log(chalk.cyan('\n📊 Cache Statistics:'));
      console.log(`  📁 Total cached files: ${stats.totalCaches}`);
      console.log(`  💾 Total cache size: ${(stats.totalSize / 1024 / 1024).toFixed(2)} MB`);

      if (stats.caches.length > 0) {
        console.log('\n📋 Cached Files:');
        stats.caches.forEach((cache, index) => {
          const sizeMB = (cache.size / 1024 / 1024).toFixed(2);
          const createdDate = new Date(cache.createdAt).toLocaleDateString();
          console.log(`  ${index + 1}. ${path.basename(cache.sourceFile)} (${sizeMB} MB, ${createdDate})`);
        });
      } else {
        console.log('\n📋 No cached files found');
      }
      return;
    }

    if (options.clearCache) {
      spinner.text = 'Clearing cache for this file...';
      await FileHasher.clearCache(options.file);
      spinner.succeed('Cache cleared');
      if (!options.dryRun) {
        console.log(chalk.green('💾 Processing cache cleared for this file'));
      }
    }

    if (useBatchFiles) {
      // No existing batch files, proceed with normal batch file processing
      // Load existing Qdrant keys to skip records that already exist (unless skip flag is set)
      let existingQdrantKeys: Set<string> | undefined;
      if (!options.skipQdrantCheck) {
        existingQdrantKeys = await loadExistingQdrantKeys(spinner);
      } else {
        console.log(chalk.gray('⏭️  Skipping Qdrant duplicate check (--skip-qdrant-check flag)'));
      }

      await processParquetWithBatchFiles(
        options,
        client,
        embeddingExporter,
        batchSize,
        parallelCount,
        maxRecords,
        maxTextLength,
        existingQdrantKeys
      );
    } else {
      // Use legacy streaming approach
      console.log(chalk.yellow('ℹ️  Using legacy streaming mode. Consider using batch files for better performance.'));
      await processParquetStreaming(
        file,
        totalRows,
        options,
        client,
        stateManager,
        progressTracker,
        tweetProcessor,
        embeddingExporter,
        batchSize,
        parallelCount,
        maxRecords,
        exportHtmlLimit,
        maxTextLength
      );
    }

  } catch (error) {
    spinner.fail('Processing failed');
    console.error(chalk.red('\\nError:'), error instanceof Error ? error.message : error);
    process.exit(1);
  }
}
