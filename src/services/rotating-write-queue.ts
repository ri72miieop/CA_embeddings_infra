import fs from 'fs/promises';
import path from 'path';
import { createContextLogger } from '../observability/logger.js';
import type { EmbeddingVector } from '../types/index.js';
import { ByteWriter, parquetWrite, type BasicType } from 'hyparquet-writer';
import { parquetRead, parquetMetadata } from 'hyparquet';

interface QueuedEmbedding {
  id: string;
  timestamp: string;
  embeddings: EmbeddingVector[];
  retryCount: number;
  correlationId?: string;
}

interface QueueManifest {
  processingFiles: string[];
  completedFiles: string[];
  failedFiles: string[];
  maxFilesRetained: number;
}

export class RotatingEmbeddingWriteQueue {
  private queueDir: string;
  private completedDir: string;
  private manifestFile: string;
  private manifest: QueueManifest;
  private isProcessing = false;
  private isShuttingDown = false;
  private activeFileProcesses = new Set<Promise<void>>();
  private processInterval: NodeJS.Timeout | null = null;
  private logger = createContextLogger({ service: 'rotating-write-queue' });

  constructor(
    private dataDir: string,
    private embeddingService: any,
    private batchSize = 500,
    private processIntervalMs = 1000,
    private maxRetries = 3,
    private maxFilesRetained = 50, // Keep max 50 completed files before cleanup
    private maxParallelFiles = 5, // Process up to 5 files in parallel
    private insertChunkSize = 1000 // Insert 1000 embeddings at a time to CoreNN
  ) {
    this.queueDir = path.join(dataDir, 'embedding-queue');
    this.completedDir = path.join(this.queueDir, 'completed');
    this.manifestFile = path.join(this.queueDir, 'manifest.json');
    this.manifest = {
      processingFiles: [],
      completedFiles: [],
      failedFiles: [],
      maxFilesRetained: this.maxFilesRetained
    };
  }

  async initialize(): Promise<void> {
    // Ensure queue directories exist
    await fs.mkdir(this.queueDir, { recursive: true });
    await fs.mkdir(this.completedDir, { recursive: true });

    // Load or create manifest
    await this.loadManifest();

    // Recover any processing files from previous shutdown
    await this.recoverProcessingFiles();

    // Start the background processor
    this.startProcessor();

    this.logger.info({
      queueDir: this.queueDir,
      completedDir: this.completedDir,
      processingFiles: this.manifest.processingFiles.length,
      maxFilesRetained: this.maxFilesRetained,
      maxParallelFiles: this.maxParallelFiles,
      insertChunkSize: this.insertChunkSize
    }, 'Rotating write queue initialized');
  }

  private async loadManifest(): Promise<void> {
    try {
      const data = await fs.readFile(this.manifestFile, 'utf-8');
      this.manifest = { ...this.manifest, ...JSON.parse(data) };
    } catch (error) {
      if ((error as any).code !== 'ENOENT') {
        this.logger.warn({ error }, 'Failed to load manifest, using defaults');
      }
      // Use default manifest
      await this.saveManifest();
    }
  }

  private async saveManifest(): Promise<void> {
    await fs.writeFile(this.manifestFile, JSON.stringify(this.manifest, null, 2));
  }

  private async recoverProcessingFiles(): Promise<void> {
    // Move any processing files back to pending
    for (const processingFile of this.manifest.processingFiles) {
      const processingPath = path.join(this.queueDir, processingFile);
      const pendingFile = processingFile.replace('.processing', '.pending');
      const pendingPath = path.join(this.queueDir, pendingFile);
      
      try {
        await fs.rename(processingPath, pendingPath);
        this.logger.info({ file: pendingFile }, 'Recovered processing file from previous shutdown');
      } catch (error) {
        if ((error as any).code !== 'ENOENT') {
          this.logger.warn({ error, file: processingFile }, 'Failed to recover processing file');
        }
      }
    }
    
    this.manifest.processingFiles = [];
    await this.saveManifest();
  }

  private generateFileName(suffix: string): string {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const random = Math.random().toString(36).substring(2, 8);
    return `queue-${timestamp}-${random}.${suffix}.parquet`;
  }

  async enqueue(embeddings: EmbeddingVector[], correlationId?: string): Promise<void> {
    const queueItem: QueuedEmbedding = {
      id: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      embeddings,
      retryCount: 0,
      correlationId
    };

    // Generate a unique file for this enqueue request
    // No synchronization needed - each request gets its own file
    const fileName = this.generateFileName('pending');
    const filePath = path.join(this.queueDir, fileName);
    
    // OPTIMIZED: Flatten embeddings into rows for better columnar compression
    // Each embedding becomes a row with vector dimensions as separate FLOAT columns
    if (embeddings.length === 0) {
      this.logger.warn('No embeddings to enqueue');
      return;
    }

    const vectorDim = embeddings[0]!.vector.length;
    const numRows = embeddings.length;

    // Build flattened column data
    const columnData: any[] = [
      { name: 'queueItemId', data: Array(numRows).fill(queueItem.id), type: 'STRING' as BasicType },
      { name: 'timestamp', data: Array(numRows).fill(queueItem.timestamp), type: 'STRING' as BasicType },
      { name: 'retryCount', data: Array(numRows).fill(queueItem.retryCount), type: 'INT32' as BasicType },
      { name: 'correlationId', data: Array(numRows).fill(queueItem.correlationId || ''), type: 'STRING' as BasicType },
      { name: 'key', data: embeddings.map(emb => emb.key), type: 'STRING' as BasicType },
      { name: 'metadata', data: embeddings.map(emb => emb.metadata || {}), type: 'JSON' as BasicType },
    ];

    // Add vector dimensions as separate FLOAT columns
    for (let dim = 0; dim < vectorDim; dim++) {
      columnData.push({
        name: `v${dim}`,
        data: embeddings.map(emb => emb.vector[dim]),
        type: 'FLOAT' as BasicType
      });
    }

    // Write parquet file with compression (Snappy)
    const writer = new ByteWriter();
    parquetWrite({
      writer,
      columnData,
      compressed: true, // Uses Snappy compression
      rowGroupSize: 10000, // Larger row groups for better compression
    });
    const arrayBuffer = writer.getBuffer();
    
    // Write to file
    await fs.writeFile(filePath, Buffer.from(arrayBuffer));

    this.logger.debug({
      queueItemId: queueItem.id,
      embeddingCount: embeddings.length,
      vectorDimension: vectorDim,
      correlationId,
      writeFile: fileName
    }, 'Embeddings queued for persistent storage (flattened format)');
  }


  private startProcessor(): void {
    //this.processInterval = setInterval(async () => {
    //  if (!this.isProcessing) {
    //    await this.processQueue();
    //  }
    //}, this.processIntervalMs);
  }

  private async processQueue(): Promise<void> {
    if (this.isProcessing) return;
    this.isProcessing = true;

    try {
      // Find pending files to process
      const files = await fs.readdir(this.queueDir);
      const pendingFiles = files
        .filter(f => f.endsWith('.pending.parquet'))
        .sort(); // Process oldest first

      if (pendingFiles.length === 0) {
        return;
      }

      // OPTIMIZATION: Process multiple files in parallel for better throughput
      const activePromises = new Set<Promise<void>>();

      for (const fileToProcess of pendingFiles) {
        // GRACEFUL SHUTDOWN: Stop accepting new files if shutdown is requested
        if (this.isShuttingDown) {
          this.logger.info({
            remainingFiles: pendingFiles.length - activePromises.size,
            activeFiles: activePromises.size
          }, 'Shutdown requested, stopping new file processing');
          break;
        }

        const promise = this.processFile(fileToProcess)
          .then(() => {
            activePromises.delete(promise);
            this.activeFileProcesses.delete(promise);
          })
          .catch(error => {
            this.logger.error({ error, file: fileToProcess }, 'Error processing file in parallel batch');
            activePromises.delete(promise);
            this.activeFileProcesses.delete(promise);
          });

        activePromises.add(promise);
        this.activeFileProcesses.add(promise);

        // Control parallelism - wait for one to complete when we reach the limit
        if (activePromises.size >= this.maxParallelFiles) {
          await Promise.race(activePromises);
        }
      }

      // Wait for all remaining files to complete
      if (activePromises.size > 0) {
        await Promise.all(activePromises);
      }

    } catch (error) {
      this.logger.error({ error }, 'Error processing queue');
    } finally {
      this.isProcessing = false;
    }
  }

  private async processFile(fileName: string): Promise<void> {
    const pendingPath = path.join(this.queueDir, fileName);
    const processingFileName = fileName.replace('.pending.', '.processing.');
    const processingPath = path.join(this.queueDir, processingFileName);

    try {
      // Move to processing
      await fs.rename(pendingPath, processingPath);
      this.manifest.processingFiles.push(processingFileName);
      await this.saveManifest();

      // Read and process items from parquet
      const buffer = await fs.readFile(processingPath);
      const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength) as ArrayBuffer;
      
      // Parse parquet file - flattened format with FLOAT columns
      const items: QueuedEmbedding[] = [];
      await parquetRead({
        file: arrayBuffer,
        rowFormat: 'object',
        onComplete: (rows: any[]) => {
          if (rows.length === 0) return;
          
          // Group rows by queueItemId since each row is now a single embedding
          const itemsMap = new Map<string, QueuedEmbedding>();
          
          for (const row of rows) {
            const queueItemId = row.queueItemId as string;
            
            // Extract vector from v0, v1, v2... columns
            const vectorDim = Object.keys(row).filter(k => k.startsWith('v')).length;
            const vector = new Float32Array(vectorDim);
            for (let i = 0; i < vectorDim; i++) {
              vector[i] = row[`v${i}`] as number;
            }
            
            const embedding: EmbeddingVector = {
              key: row.key as string,
              vector,
              metadata: typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata
            };
            
            // Get or create QueuedEmbedding item
            if (!itemsMap.has(queueItemId)) {
              itemsMap.set(queueItemId, {
                id: queueItemId,
                timestamp: row.timestamp as string,
                retryCount: row.retryCount as number,
                correlationId: (row.correlationId as string) || undefined,
                embeddings: []
              });
            }
            
            itemsMap.get(queueItemId)!.embeddings.push(embedding);
          }
          
          // Convert map to array
          items.push(...itemsMap.values());
        }
      });
      
      if (items.length === 0) {
        await this.completeFile(processingFileName);
        return;
      }

      const failed: QueuedEmbedding[] = [];
      let processed = 0;

      // Process in batches
      for (let i = 0; i < items.length; i += this.batchSize) {
        const batch = items.slice(i, i + this.batchSize);
        
        for (const item of batch) {
          try {
            // OPTIMIZATION: Chunk embeddings into smaller groups for faster CoreNN inserts
            // Insert embeddings in smaller chunks instead of all at once
            // This reduces RocksDB write amplification and improves throughput
            for (let j = 0; j < item.embeddings.length; j += this.insertChunkSize) {
              const embeddingChunk = item.embeddings.slice(j, j + this.insertChunkSize);
              await this.embeddingService.insert(embeddingChunk);
            }
            
            processed++;
            
            this.logger.debug({
              queueItemId: item.id,
              embeddingCount: item.embeddings.length,
              correlationId: item.correlationId
            }, 'Successfully processed queued embeddings');
            
          } catch (error) {
            item.retryCount++;
            if (item.retryCount <= this.maxRetries) {
              failed.push(item);
              this.logger.warn({
                queueItemId: item.id,
                retryCount: item.retryCount,
                maxRetries: this.maxRetries,
                error: error instanceof Error ? error.message : 'Unknown error'
              }, 'Failed to process queued embeddings, will retry');
            } else {
              this.logger.error({
                queueItemId: item.id,
                retryCount: item.retryCount,
                error: error instanceof Error ? error.message : 'Unknown error'
              }, 'Failed to process queued embeddings after max retries, dropping item');
            }
          }
        }
      }

      // Handle failed items
      if (failed.length > 0) {
        const failedFileName = processingFileName.replace('.processing.', '.failed.');
        const failedPath = path.join(this.queueDir, failedFileName);
        
        // Flatten failed items using the same optimized format
        const failedEmbeddings: Array<{
          queueItemId: string;
          timestamp: string;
          retryCount: number;
          correlationId: string;
          embedding: EmbeddingVector;
        }> = [];
        
        for (const failedItem of failed) {
          for (const emb of failedItem.embeddings) {
            failedEmbeddings.push({
              queueItemId: failedItem.id,
              timestamp: failedItem.timestamp,
              retryCount: failedItem.retryCount,
              correlationId: failedItem.correlationId || '',
              embedding: emb
            });
          }
        }
        
        if (failedEmbeddings.length > 0) {
          const vectorDim = failedEmbeddings[0]!.embedding.vector.length;
          const numRows = failedEmbeddings.length;
          
          // Build flattened column data
          const failedColumnData: any[] = [
            { name: 'queueItemId', data: failedEmbeddings.map(fe => fe.queueItemId), type: 'STRING' as BasicType },
            { name: 'timestamp', data: failedEmbeddings.map(fe => fe.timestamp), type: 'STRING' as BasicType },
            { name: 'retryCount', data: failedEmbeddings.map(fe => fe.retryCount), type: 'INT32' as BasicType },
            { name: 'correlationId', data: failedEmbeddings.map(fe => fe.correlationId), type: 'STRING' as BasicType },
            { name: 'key', data: failedEmbeddings.map(fe => fe.embedding.key), type: 'STRING' as BasicType },
            { name: 'metadata', data: failedEmbeddings.map(fe => fe.embedding.metadata || {}), type: 'JSON' as BasicType },
          ];
          
          // Add vector dimensions as separate FLOAT columns
          for (let dim = 0; dim < vectorDim; dim++) {
            failedColumnData.push({
              name: `v${dim}`,
              data: failedEmbeddings.map(fe => fe.embedding.vector[dim]),
              type: 'FLOAT' as BasicType
            });
          }

          // Write failed parquet file with compression (Snappy)
          const failedWriter = new ByteWriter();
          parquetWrite({
            writer: failedWriter,
            columnData: failedColumnData,
            compressed: true, // Uses Snappy compression
            rowGroupSize: 10000,
          });
          const failedArrayBuffer = failedWriter.getBuffer();
          await fs.writeFile(failedPath, Buffer.from(failedArrayBuffer));
          
          this.manifest.failedFiles.push(failedFileName);
          this.logger.warn({
            fileName: failedFileName,
            failedCount: failed.length,
            failedEmbeddingsCount: failedEmbeddings.length,
            processedCount: processed
          }, 'Created failed items file for retry');
        }
      }

      await this.completeFile(processingFileName);
      
      this.logger.info({
        fileName,
        processedCount: processed,
        failedCount: failed.length,
        totalItems: items.length
      }, 'Completed processing queue file');

    } catch (error) {
      this.logger.error({
        fileName,
        error: error instanceof Error ? error.message : 'Unknown error'
      }, 'Failed to process queue file');
      
      // Move back to pending for retry
      try {
        await fs.rename(processingPath, pendingPath);
        this.manifest.processingFiles = this.manifest.processingFiles.filter(f => f !== processingFileName);
        await this.saveManifest();
      } catch (renameError) {
        this.logger.error({ renameError }, 'Failed to move file back to pending');
      }
    }
  }

  private async completeFile(processingFileName: string): Promise<void> {
    const processingPath = path.join(this.queueDir, processingFileName);
    const completedFileName = processingFileName.replace('.processing.', '.completed.');
    const completedPath = path.join(this.completedDir, completedFileName);

    // Move to completed directory
    await fs.rename(processingPath, completedPath);
    
    // Update manifest
    this.manifest.processingFiles = this.manifest.processingFiles.filter(f => f !== processingFileName);
    this.manifest.completedFiles.push(completedFileName);
    
    // Clean up old completed files if we have too many
    await this.cleanupCompletedFiles();
    
    await this.saveManifest();

    this.logger.info({
      fileName: completedFileName,
      location: 'completed/'
    }, 'Moved processed file to completed directory');
  }

  private async cleanupCompletedFiles(): Promise<void> {
    if (this.manifest.completedFiles.length <= this.maxFilesRetained) {
      return;
    }

    // Get actual files in completed directory and sort by modification time
    const completedFiles = await fs.readdir(this.completedDir);
    const fileStats = await Promise.all(
      completedFiles.map(async (fileName) => {
        const filePath = path.join(this.completedDir, fileName);
        const stats = await fs.stat(filePath);
        return { fileName, mtime: stats.mtime };
      })
    );

    // Sort by modification time (oldest first)
    fileStats.sort((a, b) => a.mtime.getTime() - b.mtime.getTime());

    // Remove oldest files if we exceed the limit
    const filesToRemove = fileStats.slice(0, fileStats.length - this.maxFilesRetained);

    for (const { fileName } of filesToRemove) {
      try {
        const filePath = path.join(this.completedDir, fileName);
        await fs.unlink(filePath);
        this.manifest.completedFiles = this.manifest.completedFiles.filter(f => f !== fileName);
        this.logger.info({ 
          fileName, 
          reason: 'exceeded_retention_limit',
          maxRetained: this.maxFilesRetained 
        }, 'Cleaned up old completed file');
      } catch (error) {
        this.logger.warn({ fileName, error }, 'Failed to cleanup completed file');
      }
    }
  }

  async retryFailedFiles(): Promise<void> {
    for (const failedFile of this.manifest.failedFiles) {
      const failedPath = path.join(this.queueDir, failedFile);
      const pendingFile = failedFile.replace('.failed.', '.pending.');
      const pendingPath = path.join(this.queueDir, pendingFile);
      
      try {
        await fs.rename(failedPath, pendingPath);
        this.manifest.failedFiles = this.manifest.failedFiles.filter(f => f !== failedFile);
        this.logger.info({ failedFile, pendingFile }, 'Moved failed file back to pending for retry');
      } catch (error) {
        this.logger.warn({ failedFile, error }, 'Failed to retry failed file');
      }
    }
    
    await this.saveManifest();
  }

  async shutdown(): Promise<void> {
    this.logger.info('Shutting down rotating write queue...');
    
    // Signal shutdown to stop accepting new work
    this.isShuttingDown = true;
    
    // Stop the background processor interval
    if (this.processInterval) {
      clearInterval(this.processInterval);
      this.processInterval = null;
    }

    // Wait for current processing cycle to complete
    while (this.isProcessing) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    // Wait for all active file processes to complete
    if (this.activeFileProcesses.size > 0) {
      this.logger.info({
        activeFiles: this.activeFileProcesses.size
      }, 'Waiting for active file processing to complete...');
      
      await Promise.all(Array.from(this.activeFileProcesses));
      
      this.logger.info('All active file processing completed');
    }

    // Get final stats
    const files = await fs.readdir(this.queueDir);
    const pendingFiles = files.filter(f => f.endsWith('.pending.parquet'));
    
    if (pendingFiles.length > 0) {
      this.logger.info({
        pendingFiles: pendingFiles.length
      }, 'Shutdown complete - pending files will be processed on next startup');
    } else {
      this.logger.info('Shutdown complete - all files processed');
    }
  }

  async getQueueStats(): Promise<{
    pendingFiles: number;
    processingFiles: number;
    completedFiles: number;
    failedFiles: number;
    isProcessing: boolean;
    totalPendingItems: number;
    completedDirectory: string;
  }> {
    const files = await fs.readdir(this.queueDir);
    const pendingFiles = files.filter(f => f.endsWith('.pending.parquet')).length;

    // Count total pending items (approximate)
    let totalPendingItems = 0;
    const pendingFileNames = files.filter(f => f.endsWith('.pending.parquet'));
    for (const fileName of pendingFileNames) {
      try {
        const buffer = await fs.readFile(path.join(this.queueDir, fileName));
        const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength) as ArrayBuffer;
        const metadata = parquetMetadata(arrayBuffer);
        totalPendingItems += Number(metadata.num_rows);
      } catch (error) {
        // Skip files we can't read
      }
    }

    return {
      pendingFiles,
      processingFiles: this.manifest.processingFiles.length,
      completedFiles: this.manifest.completedFiles.length,
      failedFiles: this.manifest.failedFiles.length,
      isProcessing: this.isProcessing,
      totalPendingItems,
      completedDirectory: this.completedDir
    };
  }

  async getFailedItemsCount(): Promise<number> {
    let count = 0;
    for (const failedFile of this.manifest.failedFiles) {
      try {
        const buffer = await fs.readFile(path.join(this.queueDir, failedFile));
        const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength) as ArrayBuffer;
        const metadata = parquetMetadata(arrayBuffer);
        count += Number(metadata.num_rows);
      } catch (error) {
        // Skip files we can't read
      }
    }
    return count;
  }

  async getCompletedFilesInfo(): Promise<Array<{
    fileName: string;
    filePath: string;
    size: number;
    modifiedAt: Date;
    itemCount: number;
  }>> {
    const completedFiles = await fs.readdir(this.completedDir);
    const fileInfos = [];

    for (const fileName of completedFiles) {
      try {
        const filePath = path.join(this.completedDir, fileName);
        const stats = await fs.stat(filePath);
        
        // Count items in file
        const buffer = await fs.readFile(filePath);
        const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength) as ArrayBuffer;
        const metadata = parquetMetadata(arrayBuffer);
        const itemCount = Number(metadata.num_rows);

        fileInfos.push({
          fileName,
          filePath,
          size: stats.size,
          modifiedAt: stats.mtime,
          itemCount
        });
      } catch (error) {
        this.logger.warn({ fileName, error }, 'Failed to get info for completed file');
      }
    }

    return fileInfos.sort((a, b) => b.modifiedAt.getTime() - a.modifiedAt.getTime());
  }

  /**
   * Get a summary of completed files for monitoring
   */
  async getCompletedFilesSummary(): Promise<{
    totalFiles: number;
    totalItems: number;
    totalSizeBytes: number;
    oldestFile?: string;
    newestFile?: string;
  }> {
    const fileInfos = await this.getCompletedFilesInfo();
    
    if (fileInfos.length === 0) {
      return {
        totalFiles: 0,
        totalItems: 0,
        totalSizeBytes: 0
      };
    }

    const totalItems = fileInfos.reduce((sum, info) => sum + info.itemCount, 0);
    const totalSizeBytes = fileInfos.reduce((sum, info) => sum + info.size, 0);
    const oldestFile = fileInfos[fileInfos.length - 1]?.fileName;
    const newestFile = fileInfos[0]?.fileName;

    return {
      totalFiles: fileInfos.length,
      totalItems,
      totalSizeBytes,
      oldestFile,
      newestFile
    };
  }
}
