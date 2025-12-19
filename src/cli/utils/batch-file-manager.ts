import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import { parquetReadObjects, parquetRead } from 'hyparquet';
import { asyncBufferFromFile } from 'hyparquet';
import chalk from 'chalk';
import { TweetProcessor, type TweetProcessorOptions } from './tweet-processor.js';
import { CA_EmbedClient } from './client.js';
import { MissingQuotedTweetsLogger } from './missing-quoted-tweets-logger.js';
import { compressors } from 'hyparquet-compressors'

export interface BatchRecord {
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
      account_id?: string;
      username?: string;
      account_display_name?: string;
      created_at?: string;
      retweet_count?: number;
      favorite_count?: number;
      reply_to_tweet_id?: string;
      reply_to_user_id?: string;
      reply_to_username?: string;
      quoted_tweet_id?: string;
      conversation_id?: string;
    };
  };
}

export interface BatchFile {
  id: string;
  path: string;
  size: number;
  created_at: string;
}

export interface ProcessedIdIndex {
  [key: string]: {
    batch_file: string;
    embedded: boolean;
    embedding_stored: boolean;
    created_at: string;
  };
}

export interface BatchManagerManifest {
  source_file: string;
  source_hash: string;
  total_records: number;
  batch_files: BatchFile[];
  processed_ids: ProcessedIdIndex;
  created_at: string;
  updated_at: string;
  processing_options: {
    text_column: string;
    id_column: string;
    max_text_length: number;
    include_date: boolean;
    process_conversation_context: boolean;
    batch_size: number;
  };
}

export interface BatchManagerOptions {
  sourceFile: string;
  outputDir: string;
  textColumn: string;
  idColumn: string;
  maxTextLength: number;
  includeDate: boolean;
  processConversationContext: boolean;
  batchSize: number;
  reprocessExisting: boolean;
  maxRecords?: number;
  /** Set of keys that already exist in Qdrant - these will be skipped during batch creation */
  existingQdrantKeys?: Set<string>;
}

export class BatchFileManager {
  private options: BatchManagerOptions;
  private manifestPath: string;
  private manifest: BatchManagerManifest | null = null;
  private tweetProcessor: TweetProcessor;
  private missingQuotedLogger: MissingQuotedTweetsLogger;
  private manifestDirty: boolean = false;
  private saveTimer: NodeJS.Timeout | null = null;
  private saveInterval: number = 30000; // 30 seconds default
  private updatesSinceLastSave: number = 0;
  private maxUpdatesBeforeSave: number = 50;

  /**
   * Check if batch files exist for a given source file
   */
  static async checkExistingBatchFiles(
    sourceFile: string,
    batchFileDir?: string
  ): Promise<{
    exists: boolean;
    stats?: {
      totalRecords: number;
      totalBatches: number;
      recordsComplete: number;
      recordsPending: number;
    };
    batchDir?: string;
  }> {
    const batchDir = batchFileDir || path.join(path.dirname(sourceFile), '.ca_embed-batches', path.basename(sourceFile, '.parquet'));
    const manifestPath = path.join(batchDir, 'batch-manifest.json');

    try {
      await fs.access(manifestPath);
      const content = await fs.readFile(manifestPath, 'utf-8');
      const manifest: BatchManagerManifest = JSON.parse(content);

      // Calculate stats
      const processedIds = Object.values(manifest.processed_ids);
      const recordsComplete = processedIds.filter(p => p.embedded && p.embedding_stored).length;
      const recordsPending = processedIds.length - recordsComplete;

      return {
        exists: true,
        batchDir,
        stats: {
          totalRecords: manifest.total_records,
          totalBatches: manifest.batch_files.length,
          recordsComplete,
          recordsPending
        }
      };
    } catch {
      return { exists: false };
    }
  }

  constructor(options: BatchManagerOptions) {
    this.options = options;
    this.manifestPath = path.join(options.outputDir, 'batch-manifest.json');

    const processorOptions: TweetProcessorOptions = {
      maxChars: options.maxTextLength,
      includeDate: options.includeDate,
      processConversationContext: options.processConversationContext
    };
    this.tweetProcessor = new TweetProcessor(processorOptions);

    // Initialize missing quoted tweets logger
    this.missingQuotedLogger = new MissingQuotedTweetsLogger(
      path.dirname(options.sourceFile)
    );
  }

  /**
   * Initialize or load existing batch files
   */
  async initialize(): Promise<void> {
    // Create output directory
    await fs.mkdir(this.options.outputDir, { recursive: true });

    // Initialize missing quoted tweets logger
    await this.missingQuotedLogger.initialize();

    // Check if manifest exists and is valid
    const manifestExists = await this.loadManifest();

    console.log(chalk.gray(`Manifest exists: ${manifestExists}`));
    if (manifestExists) {
      console.log(chalk.gray(`Manifest path: ${this.manifestPath}`));
      console.log(chalk.gray(`Batch files in manifest: ${this.manifest!.batch_files.length}`));
    }

    if (!manifestExists || !(await this.isManifestValid())) {
      console.log(chalk.cyan('Building batch files from parquet data...'));
      await this.buildBatchFiles();
    } else {
      console.log(chalk.green(`Loaded existing batch files: ${this.manifest!.batch_files.length} batches, ${Object.keys(this.manifest!.processed_ids).length} records`));
    }
  }

  /**
   * Load existing manifest if it exists
   */
  async loadManifest(): Promise<boolean> {
    try {
      const content = await fs.readFile(this.manifestPath, 'utf-8');
      this.manifest = JSON.parse(content);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Check if the manifest is still valid for the current source file
   */
  async isManifestValid(): Promise<boolean> {
    if (!this.manifest) return false;

    try {
      // Check for corrupted manifest with 0 records
      if (this.manifest.total_records === 0 || this.manifest.batch_files.length === 0) {
        console.log(chalk.yellow('⚠️  Found corrupted manifest with 0 records, rebuilding batch files...'));
        return false;
      }

      // Check if source file hash matches
      const currentHash = await this.getFileHash(this.options.sourceFile);
      if (this.manifest.source_hash !== currentHash) {
        console.log(chalk.yellow('Source file has changed, rebuilding batch files...'));
        console.log(chalk.gray('Previous hash:'), this.manifest.source_hash);
        console.log(chalk.gray('Current hash:'), currentHash);
        return false;
      }

      // Check if processing options match
      const currentOptions = {
        text_column: this.options.textColumn,
        id_column: this.options.idColumn,
        max_text_length: this.options.maxTextLength,
        include_date: this.options.includeDate,
        process_conversation_context: this.options.processConversationContext,
        batch_size: this.options.batchSize
      };

      if (JSON.stringify(this.manifest.processing_options) !== JSON.stringify(currentOptions)) {
        console.log(chalk.yellow('Processing options have changed, rebuilding batch files...'));
        console.log(chalk.gray('Previous options:'), this.manifest.processing_options);
        console.log(chalk.gray('Current options:'), currentOptions);
        return false;
      }

      // Check if all batch files still exist
      for (const batchFile of this.manifest.batch_files) {
        try {
          await fs.access(batchFile.path);
        } catch {
          console.log(chalk.yellow(`Batch file missing: ${batchFile.path}, rebuilding...`));
          return false;
        }
      }

      return true;
    } catch {
      return false;
    }
  }

  /**
   * Build batch files from parquet source using memory-efficient single-pass approach
   */
  private async buildBatchFiles(): Promise<void> {
    // Show initial memory usage
    const initialMem = process.memoryUsage();
    console.log(chalk.gray(`Initial memory: ${Math.round(initialMem.heapUsed / 1024 / 1024)}MB heap, ${Math.round(initialMem.rss / 1024 / 1024)}MB total`));

    // Log Qdrant key skip info
    if (this.options.existingQdrantKeys && this.options.existingQdrantKeys.size > 0) {
      console.log(chalk.cyan(`🔍 Will skip ${this.options.existingQdrantKeys.size.toLocaleString()} keys already present in Qdrant`));
    }

    const file = await asyncBufferFromFile(this.options.sourceFile);

    console.log(chalk.cyan(`Loading and processing records from parquet file...`));

    // Variables that need to be accessible in finally block
    let batchFiles: BatchFile[] = [];
    let processedIds: ProcessedIdIndex = {};
    let totalProcessed = 0;
    let missingQuotedCount = 0;
    let skippedExistingInQdrant = 0;

    // Use efficient row-limited loading if maxRecords is specified
    let allRecords: any[] = [];

    if (this.options.maxRecords) {
      console.log(chalk.yellow(`📈 Loading first ${this.options.maxRecords.toLocaleString()} records using row limiting...`));

      allRecords = await new Promise<any[]>((resolve, reject) => {
        const records: any[] = [];
        parquetRead({
          file,
          compressors,
          rowStart: 0,
          rowEnd: this.options.maxRecords,
          rowFormat: 'object',
          onComplete: (data) => {
            records.push(...data);
            resolve(records);
          }
        }).catch(reject);
      });
    } else {
      console.log(chalk.yellow(`📈 Loading all records from parquet file...`));
      allRecords = await parquetReadObjects({ file, compressors });
    }

    // Show memory usage after loading parquet data
    const afterLoadMem = process.memoryUsage();
    console.log(chalk.gray(`Loaded ${allRecords.length.toLocaleString()} records into memory - Memory: ${Math.round(afterLoadMem.heapUsed / 1024 / 1024)}MB heap, ${Math.round(afterLoadMem.rss / 1024 / 1024)}MB total`));

    // Validate ID column type to detect precision loss early
    if (allRecords.length > 0) {
      const sampleRecord = allRecords[0];
      const sampleId = sampleRecord[this.options.idColumn];

      if (sampleId !== undefined && sampleId !== null) {
        if (typeof sampleId === 'number') {
          if (!Number.isSafeInteger(sampleId)) {
            throw new Error(
              `CRITICAL: ID column "${this.options.idColumn}" is a Number exceeding MAX_SAFE_INTEGER (${Number.MAX_SAFE_INTEGER}). ` +
              `Precision loss has already occurred. Value: ${sampleId}. ` +
              `Ensure your parquet library returns BigInt for int64 columns.`
            );
          }
          console.warn(chalk.yellow(
            `⚠️  ID column "${this.options.idColumn}" is Number type (value: ${sampleId}). ` +
            `Safe for now, but may lose precision for IDs > ${Number.MAX_SAFE_INTEGER.toLocaleString()}`
          ));
        } else if (typeof sampleId === 'bigint') {
          console.log(chalk.green(`✓ ID column "${this.options.idColumn}" is BigInt - precision preserved`));
        } else if (typeof sampleId === 'string') {
          console.log(chalk.green(`✓ ID column "${this.options.idColumn}" is String - precision preserved`));
        } else {
          console.warn(chalk.yellow(
            `⚠️  ID column "${this.options.idColumn}" has unexpected type: ${typeof sampleId}`
          ));
        }
      }
    }

    let currentBatch: BatchRecord[] = [];
    let batchIndex = 0;

    try {
      // Build simple tweet lookup map for quoted tweets
      console.log(chalk.cyan('Building tweet lookup map for quoted tweets...'));
      const tweetMap = new Map<string, any>();
      for (const record of allRecords) {
        if (record.tweet_id) {
          tweetMap.set(record.tweet_id.toString(), record);
        }
      }
      console.log(chalk.green(`✅ Built lookup map with ${tweetMap.size.toLocaleString()} tweets`));

      console.log(chalk.cyan('Processing records for batch file creation...'));

      // Determine how many records to process (respecting maxRecords limit)
      const recordsToProcess = this.options.maxRecords ?
        Math.min(allRecords.length, this.options.maxRecords) :
        allRecords.length;

      if (this.options.maxRecords && recordsToProcess < allRecords.length) {
        console.log(chalk.yellow(`⚠️  Processing limited to ${recordsToProcess.toLocaleString()} of ${allRecords.length.toLocaleString()} records due to --max-records limit`));
      }

      for (let rowIndex = 0; rowIndex < recordsToProcess; rowIndex++) {
        const record = allRecords[rowIndex];
        if (!record) continue;

        const id = record[this.options.idColumn];
        const text = record[this.options.textColumn];

        // Skip invalid records
        if (!id || !text || typeof text !== 'string' || text.trim().length === 0) {
          continue;
        }

        const key = String(id);

        // Skip if already processed (unless reprocessing)
        if (!this.options.reprocessExisting && processedIds[key]) {
          continue;
        }

        // Skip if already exists in Qdrant
        if (this.options.existingQdrantKeys?.has(key)) {
          skippedExistingInQdrant++;
          continue;
        }

        // Check if this tweet has a quoted tweet
        let quotedTweet = null;
        if (record.quoted_tweet_id) {
          const quotedId = record.quoted_tweet_id.toString();
          quotedTweet = tweetMap.get(quotedId);

          if (!quotedTweet) {
            // Log missing quoted tweet but continue processing without it
            this.missingQuotedLogger.logMissingQuotedTweet(key, quotedId);
            missingQuotedCount++;
            // quotedTweet remains null, so processTweetSimple will process without it
          }
        }

        // Process text using new simplified processor
        let processedText = text;
        let processingMetadata = undefined;

        try {
          const processingResult = this.tweetProcessor.processTweetSimple(record as any, quotedTweet);
          processedText = processingResult.processedText;

          processingMetadata = {
            has_context: false,
            has_quotes: processingResult.quotesIncluded,
            is_truncated: processingResult.truncated,
            character_difference: processedText.length - text.length,
            text: processedText.trim(),
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
          };
        } catch (error) {
          console.warn(chalk.yellow(`Warning: Failed to process record ${key}: ${error}`));
          processedText = text; // Fallback to original
        }

        // Skip empty processed text
        if (!processedText || processedText.trim().length === 0) {
          continue;
        }
        const finalText = processedText.trim();
        // Create batch record with minimal data duplication
        const batchRecord: BatchRecord = {
          key,
          content: finalText,
          metadata: {
            original_text: text,
            row_index: rowIndex,
            batch_file: '', // Will be set when batch is saved
            processing_metadata: processingMetadata
          }
        };

        currentBatch.push(batchRecord);

        // Save batch if it reaches the size limit
        if (currentBatch.length >= this.options.batchSize) {
          const batchFile = await this.saveBatch(currentBatch, batchIndex);
          batchFiles.push(batchFile);

          // Update processed IDs index
          for (const batchRecord of currentBatch) {
            batchRecord.metadata.batch_file = batchFile.id;
            processedIds[batchRecord.key] = {
              batch_file: batchFile.id,
              embedded: false,
              embedding_stored: false,
              created_at: new Date().toISOString()
            };
          }

          totalProcessed += currentBatch.length;
          currentBatch = []; // Clear the batch to release memory
          batchIndex++;

          if (batchIndex % 10 === 0) {
            const memUsage = process.memoryUsage();
            const memUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
            const memTotalMB = Math.round(memUsage.rss / 1024 / 1024);
            console.log(chalk.gray(`Created ${batchIndex} batches (${totalProcessed.toLocaleString()} records processed) - Memory: ${memUsedMB}MB heap, ${memTotalMB}MB total`));
          }

          // Periodically help garbage collection by nulling out processed records
          if (batchIndex % 100 === 0) {
            const memUsage = process.memoryUsage();
            const memUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
            const memTotalMB = Math.round(memUsage.rss / 1024 / 1024);
            console.log(chalk.gray(`🗑️  GC hint: processed ${batchIndex} batches (${totalProcessed.toLocaleString()} records) - Memory: ${memUsedMB}MB heap, ${memTotalMB}MB total`));

            // Force garbage collection if available (node --expose-gc)
            if (global.gc) {
              global.gc();
              const afterGcMem = process.memoryUsage();
              const afterGcMB = Math.round(afterGcMem.heapUsed / 1024 / 1024);
              console.log(chalk.gray(`🗑️  After GC: ${afterGcMB}MB heap`));
            }
          }

          // Save manifest checkpoint every 1000 batches to prevent data loss on OOM
          if (batchIndex % 1000 === 0) {
            this.manifest = {
              source_file: this.options.sourceFile,
              source_hash: await this.getFileHash(this.options.sourceFile),
              total_records: totalProcessed,
              batch_files: batchFiles,
              processed_ids: processedIds,
              created_at: this.manifest?.created_at || new Date().toISOString(),
              updated_at: new Date().toISOString(),
              processing_options: {
                text_column: this.options.textColumn,
                id_column: this.options.idColumn,
                max_text_length: this.options.maxTextLength,
                include_date: this.options.includeDate,
                process_conversation_context: this.options.processConversationContext,
                batch_size: this.options.batchSize
              }
            };
            await this.saveManifest();
            console.log(chalk.cyan(`💾 Checkpoint: Saved manifest at batch ${batchIndex} (${totalProcessed.toLocaleString()} records)`));
          }
        }
      }

      // Release the allRecords array from memory
      (allRecords as any).length = 0;

      // Save remaining batch if any
      if (currentBatch.length > 0) {
        const batchFile = await this.saveBatch(currentBatch, batchIndex);
        batchFiles.push(batchFile);

        for (const batchRecord of currentBatch) {
          batchRecord.metadata.batch_file = batchFile.id;
          processedIds[batchRecord.key] = {
            batch_file: batchFile.id,
            embedded: false,
            embedding_stored: false,
            created_at: new Date().toISOString()
          };
        }
        totalProcessed += currentBatch.length;
      }

      console.log(chalk.green(`✅ Created ${batchFiles.length} batch files with ${totalProcessed.toLocaleString()} records`));

      // Report skipped records (already in Qdrant)
      if (skippedExistingInQdrant > 0) {
        console.log(chalk.cyan(`⏭️  Skipped ${skippedExistingInQdrant.toLocaleString()} records already present in Qdrant`));
      }

      // Report missing quoted tweets
      if (this.missingQuotedLogger.hasMessages()) {
        console.log(chalk.yellow(`⚠️  ${missingQuotedCount} tweets had missing quoted tweet data (processed without quoted content)`));
        console.log(chalk.gray(`   Log file: ${this.missingQuotedLogger.getLogPath()}`));
      }

    } finally {
      // Always save manifest and flush logs, even on crash/OOM
      console.log(chalk.cyan(`💾 Saving final manifest with ${batchFiles.length} batches and ${totalProcessed.toLocaleString()} records...`));

      this.manifest = {
        source_file: this.options.sourceFile,
        source_hash: await this.getFileHash(this.options.sourceFile),
        total_records: totalProcessed,
        batch_files: batchFiles,
        processed_ids: processedIds,
        created_at: this.manifest?.created_at || new Date().toISOString(),
        updated_at: new Date().toISOString(),
        processing_options: {
          text_column: this.options.textColumn,
          id_column: this.options.idColumn,
          max_text_length: this.options.maxTextLength,
          include_date: this.options.includeDate,
          process_conversation_context: this.options.processConversationContext,
          batch_size: this.options.batchSize
        }
      };

      await this.saveManifest();

      // Flush missing quoted tweets log
      await this.missingQuotedLogger.flush();

      console.log(chalk.green(`✅ Manifest saved successfully`));
    }
  }


  /**
   * Save a batch of records to a file
   */
  private async saveBatch(records: BatchRecord[], batchIndex: number): Promise<BatchFile> {
    const batchId = `batch_${batchIndex.toString().padStart(4, '0')}`;
    const batchPath = path.join(this.options.outputDir, `${batchId}.jsonl`);

    const jsonlContent = records.map(record => JSON.stringify(record)).join('\n');
    await fs.writeFile(batchPath, jsonlContent, 'utf-8');

    return {
      id: batchId,
      path: batchPath,
      size: records.length,
      created_at: new Date().toISOString()
    };
  }

  /**
   * Save manifest to disk immediately
   */
  private async saveManifest(): Promise<void> {
    await fs.writeFile(this.manifestPath, JSON.stringify(this.manifest, null, 2));
    this.manifestDirty = false;
    this.updatesSinceLastSave = 0;
  }

  /**
   * Schedule a debounced manifest save
   * Saves are batched to reduce I/O: either after saveInterval ms or maxUpdatesBeforeSave updates
   */
  private scheduleSave(): void {
    this.manifestDirty = true;
    this.updatesSinceLastSave++;

    // Immediate save if we've hit the update threshold
    if (this.updatesSinceLastSave >= this.maxUpdatesBeforeSave) {
      this.flushManifest().catch(err => {
        console.error('Error flushing manifest:', err);
      });
      return;
    }

    // Otherwise debounce the save
    if (this.saveTimer) {
      return; // Save already scheduled
    }

    this.saveTimer = setTimeout(() => {
      this.flushManifest().catch(err => {
        console.error('Error saving manifest:', err);
      });
    }, this.saveInterval);
  }

  /**
   * Flush pending manifest changes to disk immediately
   */
  async flushManifest(): Promise<void> {
    if (this.saveTimer) {
      clearTimeout(this.saveTimer);
      this.saveTimer = null;
    }

    if (this.manifestDirty && this.manifest) {
      await this.saveManifest();
    }
  }

  /**
   * Set the manifest save interval (for tuning performance)
   */
  setManifestSaveInterval(intervalMs: number): void {
    this.saveInterval = intervalMs;
  }

  /**
   * Get file hash for change detection
   */
  private async getFileHash(filePath: string): Promise<string> {
    const content = await fs.readFile(filePath);
    return crypto.createHash('sha256').update(content).digest('hex');
  }

  /**
   * Get batches that need processing (not yet embedded)
   */
  async getBatchesToProcess(): Promise<BatchFile[]> {
    if (!this.manifest) {
      throw new Error('BatchFileManager not initialized');
    }

    // Build a set of batch IDs that have pending records (O(n) instead of O(n²))
    const batchIdsWithPending = new Set<string>();

    for (const processedInfo of Object.values(this.manifest.processed_ids)) {
      if (!processedInfo.embedded || !processedInfo.embedding_stored) {
        batchIdsWithPending.add(processedInfo.batch_file);
      }
    }

    // Filter batch files using the set (O(1) lookup)
    return this.manifest.batch_files.filter(batchFile =>
      batchIdsWithPending.has(batchFile.id)
    );
  }

  /**
   * Load a specific batch file
   */
  async loadBatch(batchFile: BatchFile): Promise<BatchRecord[]> {
    const content = await fs.readFile(batchFile.path, 'utf-8');
    return content.trim().split('\n').map(line => JSON.parse(line));
  }

  /**
   * Filter batch records to only include unprocessed ones
   */
  filterUnprocessedRecords(records: BatchRecord[]): BatchRecord[] {
    if (!this.manifest) {
      return records;
    }

    return records.filter(record => {
      const processedInfo = this.manifest!.processed_ids[record.key];
      return !processedInfo?.embedded || !processedInfo?.embedding_stored;
    });
  }

  /**
   * Mark records as fully processed (embedded and stored)
   * This combines the previous markRecordsAsEmbedded + markRecordsAsStored to reduce I/O
   */
  markRecordsAsProcessed(keys: string[]): void {
    if (!this.manifest) {
      throw new Error('BatchFileManager not initialized');
    }

    for (const key of keys) {
      if (this.manifest.processed_ids[key]) {
        this.manifest.processed_ids[key].embedded = true;
        this.manifest.processed_ids[key].embedding_stored = true;
      }
    }

    this.manifest.updated_at = new Date().toISOString();
    this.scheduleSave(); // Debounced save instead of immediate
  }

  /**
   * Mark records as embedded (legacy method - prefer markRecordsAsProcessed)
   * @deprecated Use markRecordsAsProcessed instead
   */
  async markRecordsAsEmbedded(keys: string[]): Promise<void> {
    if (!this.manifest) {
      throw new Error('BatchFileManager not initialized');
    }

    for (const key of keys) {
      if (this.manifest.processed_ids[key]) {
        this.manifest.processed_ids[key].embedded = true;
      }
    }

    this.manifest.updated_at = new Date().toISOString();
    this.scheduleSave(); // Debounced save
  }

  /**
   * Mark records as having embeddings stored (legacy method - prefer markRecordsAsProcessed)
   * @deprecated Use markRecordsAsProcessed instead
   */
  async markRecordsAsStored(keys: string[]): Promise<void> {
    if (!this.manifest) {
      throw new Error('BatchFileManager not initialized');
    }

    for (const key of keys) {
      if (this.manifest.processed_ids[key]) {
        this.manifest.processed_ids[key].embedding_stored = true;
      }
    }

    this.manifest.updated_at = new Date().toISOString();
    this.scheduleSave(); // Debounced save
  }

  /**
   * Check if a key is already processed
   */
  isProcessed(key: string): boolean {
    if (!this.manifest) {
      return false;
    }

    const processedInfo = this.manifest.processed_ids[key];
    return !!(processedInfo?.embedded && processedInfo?.embedding_stored);
  }

  /**
   * Get processing statistics
   */
  getStats(): {
    totalRecords: number;
    totalBatches: number;
    recordsEmbedded: number;
    recordsStored: number;
    recordsComplete: number;
    recordsPending: number;
  } {
    if (!this.manifest) {
      return {
        totalRecords: 0,
        totalBatches: 0,
        recordsEmbedded: 0,
        recordsStored: 0,
        recordsComplete: 0,
        recordsPending: 0
      };
    }

    const processedIds = Object.values(this.manifest.processed_ids);
    const recordsEmbedded = processedIds.filter(p => p.embedded).length;
    const recordsStored = processedIds.filter(p => p.embedding_stored).length;
    const recordsComplete = processedIds.filter(p => p.embedded && p.embedding_stored).length;

    // If reprocessExisting is enabled, treat all records as pending
    const recordsPending = this.options.reprocessExisting
      ? processedIds.length
      : processedIds.length - recordsComplete;

    return {
      totalRecords: this.manifest.total_records,
      totalBatches: this.manifest.batch_files.length,
      recordsEmbedded,
      recordsStored,
      recordsComplete,
      recordsPending
    };
  }

  /**
   * Get text for a specific key (for storage alongside embeddings)
   * Note: This reads from the batch file, not the manifest
   */
  async getTextForKey(key: string): Promise<string | null> {
    if (!this.manifest) {
      return null;
    }

    const processedInfo = this.manifest.processed_ids[key];
    if (!processedInfo) {
      return null;
    }

    // Find the batch file containing this key
    const batchFile = this.manifest.batch_files.find(bf => bf.id === processedInfo.batch_file);
    if (!batchFile) {
      return null;
    }

    // Read the batch file and find the record
    try {
      const content = await fs.readFile(batchFile.path, 'utf-8');
      const lines = content.split('\n');

      for (const line of lines) {
        if (!line.trim()) continue;
        const record = JSON.parse(line) as BatchRecord;
        if (record.key === key) {
          return record.content;
        }
      }
    } catch (error) {
      console.error(`Error reading batch file ${batchFile.path}:`, error);
    }

    return null;
  }

  /**
   * Clean up batch files and manifest
   */
  async cleanup(): Promise<void> {
    if (!this.manifest) {
      return;
    }

    // Delete all batch files
    for (const batchFile of this.manifest.batch_files) {
      try {
        await fs.unlink(batchFile.path);
      } catch {
        // Ignore if file doesn't exist
      }
    }

    // Delete manifest
    try {
      await fs.unlink(this.manifestPath);
    } catch {
      // Ignore if file doesn't exist
    }

    console.log(chalk.green('✅ Cleaned up batch files and manifest'));
  }

  /**
   * Sync processed status with the server to check for existing embeddings
   */
  async syncWithServer(client: CA_EmbedClient): Promise<void> {
    if (!this.manifest) {
      throw new Error('BatchFileManager not initialized');
    }

    console.log(chalk.cyan('Checking for existing embeddings on server...'));

    // Get all keys that are not marked as embedded
    const unembeddedKeys = Object.entries(this.manifest.processed_ids)
      .filter(([_, info]) => !info.embedded)
      .map(([key, _]) => key);

    if (unembeddedKeys.length === 0) {
      console.log(chalk.green('All records are already marked as embedded'));
      return;
    }

    // Check server for existing embeddings in batches
    const batchSize = 1000; // Check in smaller batches to avoid overwhelming the server
    let existingCount = 0;

    for (let i = 0; i < unembeddedKeys.length; i += batchSize) {
      const keyBatch = unembeddedKeys.slice(i, i + batchSize);

      try {
        // Use the text storage check to see if embeddings exist
        // Since embeddings and text are stored together, we can infer embedding existence
        const existingKeys = await client.checkExistingKeys(keyBatch);

        // Mark existing keys as embedded and stored
        for (const key of existingKeys) {
          if (this.manifest.processed_ids[key]) {
            this.manifest.processed_ids[key].embedded = true;
            this.manifest.processed_ids[key].embedding_stored = true;
            existingCount++;
          }
        }

        if (i % (batchSize * 10) === 0) {
          console.log(chalk.gray(`Checked ${i + keyBatch.length}/${unembeddedKeys.length} keys...`));
        }
      } catch (error) {
        console.warn(chalk.yellow(`Warning: Failed to check batch of ${keyBatch.length} keys: ${error}`));
      }
    }

    if (existingCount > 0) {
      this.manifest.updated_at = new Date().toISOString();
      await this.saveManifest();
      console.log(chalk.green(`✅ Found ${existingCount.toLocaleString()} existing embeddings on server`));
    } else {
      console.log(chalk.gray('No existing embeddings found on server'));
    }
  }
}