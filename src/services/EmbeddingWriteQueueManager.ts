import { SqliteEmbeddingQueue } from './sqlite-embedding-queue.js';
import type { IVectorStore } from '../interfaces/vector-store.interface.js';
import { createContextLogger } from '../observability/logger.js';
import type { EmbeddingVector } from '../types/index.js';
import { queueDepth, queueFileCount } from '../observability/metrics.js';
import path from 'path';

/**
 * Integration wrapper for the SQLite embedding queue
 * This provides a clean interface for the API to use
 */
export class EmbeddingWriteQueueManager {
  private queue: SqliteEmbeddingQueue;
  private logger = createContextLogger({ service: 'embedding-write-queue-manager' });
  private metricsInterval: NodeJS.Timeout | null = null;

  constructor(
    dbPathOrDataDir: string,
    embeddingService: IVectorStore,
    options: {
      processIntervalMs?: number;
      maxRetries?: number;
      insertChunkSize?: number;
    } = {}
  ) {
    // If path doesn't end with .db, assume it's a data directory and append the db file name
    const dbPath = dbPathOrDataDir.endsWith('.db')
      ? dbPathOrDataDir
      : path.join(dbPathOrDataDir, 'embedding-queue.db');

    this.queue = new SqliteEmbeddingQueue(
      dbPath,
      embeddingService,
      options.processIntervalMs || 1000,
      options.insertChunkSize || 1000,
      options.maxRetries || 3
    );
  }

  async initialize(): Promise<void> {
    await this.queue.initialize();

    // Set up periodic metrics refresh (every 30 seconds)
    this.metricsInterval = setInterval(async () => {
      await this.updateQueueMetrics();
    }, 30000);

    // Initial metrics update
    await this.updateQueueMetrics();

    this.logger.info('Embedding write queue manager initialized');
  }

  /**
   * Queue embeddings for asynchronous storage
   * This returns immediately, embeddings will be stored in the background
   */
  async queueEmbeddings(embeddings: EmbeddingVector[], correlationId?: string): Promise<void> {
    await this.queue.enqueue(embeddings, correlationId);

    // Update metrics after enqueueing
    await this.updateQueueMetrics();
  }

  /**
   * Get comprehensive queue statistics for monitoring
   */
  async getStats(): Promise<{
    queue: {
      pending: number;
      processing: number;
      failed: number;
      completed: number;
    };
  }> {
    const stats = this.queue.getStats();

    // Update metrics when stats are requested
    await this.updateQueueMetrics();

    return {
      queue: {
        pending: stats.pending,
        processing: stats.processing,
        failed: stats.failed,
        completed: stats.completed
      }
    };
  }

  /**
   * Retry all failed items
   */
  async retryFailedItems(): Promise<void> {
    const retriedCount = await this.queue.retryFailed();
    this.logger.info({ retriedCount }, 'Initiated retry of all failed items');
  }

  /**
   * Graceful shutdown - waits for current processing to complete
   */
  async shutdown(): Promise<void> {
    // Clear metrics interval
    if (this.metricsInterval) {
      clearInterval(this.metricsInterval);
      this.metricsInterval = null;
    }

    await this.queue.shutdown();
    this.queue.close();
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
    if (stats.queue.failed > 1000) {
      issues.push(`High number of failed items: ${stats.queue.failed}`);
    }

    // Check for excessive pending items (might indicate processing issues)
    if (stats.queue.pending > 10000) {
      issues.push(`High number of pending items: ${stats.queue.pending}`);
    }

    // Check if processing is stuck (many processing items)
    if (stats.queue.processing > 100) {
      issues.push(`Many items in processing state: ${stats.queue.processing}`);
    }

    return {
      healthy: issues.length === 0,
      issues
    };
  }

  /**
   * Update queue metrics proactively
   * This ensures metrics are always up to date
   */
  private async updateQueueMetrics(): Promise<void> {
    try {
      const stats = this.queue.getStats();

      // Update queue depth (total pending items)
      queueDepth.set(stats.pending);

      // Update item counts by status
      queueFileCount.set({ status: 'pending' }, stats.pending);
      queueFileCount.set({ status: 'processing' }, stats.processing);
      queueFileCount.set({ status: 'failed' }, stats.failed);
      queueFileCount.set({ status: 'completed' }, stats.completed);
    } catch (error) {
      // Log but don't throw - metrics update failures shouldn't break operations
      this.logger.debug({ error }, 'Failed to update queue metrics');
    }
  }
}
