import { RotatingEmbeddingWriteQueue } from './rotating-write-queue.js';
import type { IVectorStore } from '../interfaces/vector-store.interface.js';
import { createContextLogger } from '../observability/logger.js';
import type { EmbeddingVector } from '../types/index.js';

/**
 * Integration wrapper for the rotating write queue
 * This provides a clean interface for the API to use
 */
export class EmbeddingWriteQueueManager {
  private queue: RotatingEmbeddingWriteQueue;
  private logger = createContextLogger({ service: 'embedding-write-queue-manager' });

  constructor(
    dataDir: string,
    embeddingService: IVectorStore,
    options: {
      batchSize?: number;
      processIntervalMs?: number;
      maxRetries?: number;
      maxFilesRetained?: number;
      maxParallelFiles?: number;
      insertChunkSize?: number;
    } = {}
  ) {
    this.queue = new RotatingEmbeddingWriteQueue(
      dataDir,
      embeddingService,
      options.batchSize || 500,
      options.processIntervalMs || 1000,
      options.maxRetries || 3,
      options.maxFilesRetained || 50,
      options.maxParallelFiles || 5,
      options.insertChunkSize || 1000
    );
  }

  async initialize(): Promise<void> {
    await this.queue.initialize();
    this.logger.info('Embedding write queue manager initialized');
  }

  /**
   * Queue embeddings for asynchronous storage
   * This returns immediately, embeddings will be stored in the background
   */
  async queueEmbeddings(embeddings: EmbeddingVector[], correlationId?: string): Promise<void> {
    await this.queue.enqueue(embeddings, correlationId);
  }

  /**
   * Get comprehensive queue statistics for monitoring
   */
  async getStats(): Promise<{
    queue: {
      pendingFiles: number;
      processingFiles: number;
      completedFiles: number;
      failedFiles: number;
      isProcessing: boolean;
      totalPendingItems: number;
      completedDirectory: string;
    };
    completed: {
      totalFiles: number;
      totalItems: number;
      totalSizeBytes: number;
      totalSizeMB: number;
      oldestFile?: string;
      newestFile?: string;
    };
    failed: {
      totalItems: number;
    };
  }> {
    const [queueStats, completedSummary, failedCount] = await Promise.all([
      this.queue.getQueueStats(),
      this.queue.getCompletedFilesSummary(),
      this.queue.getFailedItemsCount()
    ]);

    return {
      queue: queueStats,
      completed: {
        ...completedSummary,
        totalSizeMB: Math.round(completedSummary.totalSizeBytes / (1024 * 1024) * 100) / 100
      },
      failed: {
        totalItems: failedCount
      }
    };
  }

  /**
   * Get detailed information about completed files
   */
  async getCompletedFilesInfo() {
    return await this.queue.getCompletedFilesInfo();
  }

  /**
   * Retry all failed items
   */
  async retryFailedItems(): Promise<void> {
    await this.queue.retryFailedFiles();
    this.logger.info('Initiated retry of all failed items');
  }

  /**
   * Graceful shutdown - waits for current processing to complete
   */
  async shutdown(): Promise<void> {
    await this.queue.shutdown();
    this.logger.info('Embedding write queue manager shutdown complete');
  }

  /**
   * Health check - returns true if queue is healthy
   */
  async isHealthy(): Promise<{
    healthy: boolean;
    issues: string[];
  }> {
    const stats = await this.getStats();
    const issues: string[] = [];

    // Check for excessive failed items
    if (stats.failed.totalItems > 1000) {
      issues.push(`High number of failed items: ${stats.failed.totalItems}`);
    }

    // Check for excessive pending items (might indicate processing issues)
    if (stats.queue.totalPendingItems > 10000) {
      issues.push(`High number of pending items: ${stats.queue.totalPendingItems}`);
    }

    // Check if processing is stuck (many processing files)
    if (stats.queue.processingFiles > 5) {
      issues.push(`Many files in processing state: ${stats.queue.processingFiles}`);
    }

    return {
      healthy: issues.length === 0,
      issues
    };
  }
}
