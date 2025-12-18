import { Database } from 'bun:sqlite';
import type { EmbeddingVector } from '../types/index.js';
import { createContextLogger } from '../observability/logger.js';
import { queueProcessingRate, queueRetryCount } from '../observability/metrics.js';

interface QueueStats {
  pending: number;
  processing: number;
  failed: number;
  completed: number;
}

export class SqliteEmbeddingQueue {
  private db: Database;
  private logger = createContextLogger({ service: 'sqlite-embedding-queue' });
  private isProcessing = false;
  private isShuttingDown = false;
  private processInterval: NodeJS.Timeout | null = null;

  constructor(
    private dbPath: string,
    private embeddingService: any,
    private processIntervalMs: number = 1000,
    private insertChunkSize: number = 1000,
    private maxRetries: number = 3
  ) {
    // Initialize database with WAL mode for better concurrency
    this.db = new Database(dbPath, { create: true });
    this.db.run('PRAGMA journal_mode = WAL');
    this.db.run('PRAGMA synchronous = NORMAL');
    this.db.run('PRAGMA cache_size = -64000'); // 64MB cache

    this.initializeSchema();
  }

  private initializeSchema(): void {
    // Create tables
    this.db.run(`
      CREATE TABLE IF NOT EXISTS embedding_queue (
        key TEXT PRIMARY KEY,
        vector BLOB NOT NULL,
        metadata TEXT,
        correlation_id TEXT,
        status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'processing', 'failed')),
        retry_count INTEGER DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        error_message TEXT
      )
    `);

    this.db.run(`
      CREATE TABLE IF NOT EXISTS embedding_completed (
        key TEXT PRIMARY KEY,
        vector BLOB NOT NULL,
        metadata TEXT,
        correlation_id TEXT,
        completed_at TEXT NOT NULL
      )
    `);

    // Create indexes
    this.db.run(`
      CREATE INDEX IF NOT EXISTS idx_queue_status_created
      ON embedding_queue(status, created_at)
    `);

    this.logger.info('SQLite schema initialized');
  }

  async initialize(): Promise<void> {
    // Recover any processing items from previous shutdown
    await this.recoverProcessingItems();

    // Start background processor
    this.startProcessor();

    this.logger.info({
      dbPath: this.dbPath,
      insertChunkSize: this.insertChunkSize
    }, 'SQLite embedding queue initialized');
  }

  private async recoverProcessingItems(): Promise<void> {
    const result = this.db.run(`
      UPDATE embedding_queue
      SET status = 'pending', updated_at = datetime('now')
      WHERE status = 'processing'
    `);

    if (result.changes > 0) {
      this.logger.info({ recoveredItems: result.changes }, 'Recovered processing items from previous shutdown');
    }
  }

  async enqueue(embeddings: EmbeddingVector[], correlationId?: string): Promise<void> {
    if (embeddings.length === 0) {
      this.logger.warn('No embeddings to enqueue');
      return;
    }

    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO embedding_queue
        (key, vector, metadata, correlation_id, status, retry_count, created_at, updated_at)
      VALUES (?, ?, ?, ?, 'pending', 0, datetime('now'), datetime('now'))
    `);

    const transaction = this.db.transaction((embeddings: EmbeddingVector[]) => {
      for (const emb of embeddings) {
        // Compress vector using Bun native gzip
        const uint8View = new Uint8Array(emb.vector.buffer);
        // @ts-expect-error - Bun's gzipSync returns Uint8Array<ArrayBufferLike> but Buffer.from can handle it
        const compressed: Uint8Array = Bun.gzipSync(uint8View);
        // Convert to Buffer for SQLite compatibility
        const compressedBuffer = Buffer.from(compressed);

        stmt.run(
          emb.key,
          compressedBuffer,
          JSON.stringify(emb.metadata || {}),
          correlationId || null
        );
      }
    });

    transaction(embeddings);

    this.logger.debug({
      embeddingCount: embeddings.length,
      correlationId
    }, 'Embeddings enqueued');
  }

  private startProcessor(): void {
    this.processInterval = setInterval(async () => {
      if (!this.isProcessing && !this.isShuttingDown) {
        await this.processQueue();
      }
    }, this.processIntervalMs);
  }

  private async processQueue(): Promise<void> {
    if (this.isProcessing || this.isShuttingDown) return;

    this.isProcessing = true;

    try {
      const result = await this.processPendingBatch();

      if (result.processed > 0) {
        this.logger.debug({
          processed: result.processed,
          failed: result.failed
        }, 'Processed batch');
      }
    } catch (error) {
      this.logger.error({ error }, 'Error processing queue');
    } finally {
      this.isProcessing = false;
    }
  }

  private async processPendingBatch(): Promise<{ processed: number; failed: number }> {
    const batchSize = 1000;

    // Get pending batch
    const rows = this.db.query(`
      SELECT key, vector, metadata, correlation_id, retry_count
      FROM embedding_queue
      WHERE status = 'pending'
      ORDER BY created_at ASC
      LIMIT ?
    `).all(batchSize) as Array<{
      key: string;
      vector: Uint8Array;
      metadata: string;
      correlation_id: string | null;
      retry_count: number;
    }>;

    if (rows.length === 0) {
      return { processed: 0, failed: 0 };
    }

    let processed = 0;
    let failed = 0;

    const updateStmt = this.db.prepare(`
      UPDATE embedding_queue
      SET status = ?, updated_at = datetime('now'), error_message = ?
      WHERE key = ?
    `);

    const moveToCompletedStmt = this.db.prepare(`
      INSERT INTO embedding_completed (key, vector, metadata, correlation_id, completed_at)
      VALUES (?, ?, ?, ?, datetime('now'))
    `);

    const deleteFromQueueStmt = this.db.prepare(`
      DELETE FROM embedding_queue WHERE key = ?
    `);

    const updateRetryStmt = this.db.prepare(`
      UPDATE embedding_queue
      SET status = 'pending', retry_count = ?, updated_at = datetime('now'), error_message = ?
      WHERE key = ?
    `);

    for (const row of rows) {
      try {
        // Mark as processing
        updateStmt.run('processing', null, row.key);

        // Decompress vector using Bun native gunzip
        const compressedBuffer = Buffer.from(row.vector);
        const decompressed = Bun.gunzipSync(compressedBuffer);
        const vector = new Float32Array(
          decompressed.buffer,
          decompressed.byteOffset,
          decompressed.byteLength / 4
        );

        const embedding: EmbeddingVector = {
          key: row.key,
          vector,
          metadata: JSON.parse(row.metadata)
        };

        // Insert to vector store in chunks
        await this.embeddingService.insert([embedding]);

        // Move to completed table (transaction)
        const transaction = this.db.transaction(() => {
          moveToCompletedStmt.run(
            row.key,
            row.vector,
            row.metadata,
            row.correlation_id
          );
          deleteFromQueueStmt.run(row.key);
        });
        transaction();

        processed++;

        // Record successful processing metric
        queueProcessingRate.inc({ status: 'success' }, 1);

        this.logger.debug({
          key: row.key,
          correlationId: row.correlation_id
        }, 'Successfully processed queued embedding');

      } catch (error) {
        const retryCount = row.retry_count + 1;
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';

        // Record failure metric
        queueProcessingRate.inc({ status: 'failure' }, 1);

        if (retryCount <= this.maxRetries) {
          // Retry - back to pending with incremented count
          updateRetryStmt.run(retryCount, errorMessage, row.key);

          // Record retry metric
          queueRetryCount.inc();

          this.logger.warn({
            key: row.key,
            retryCount,
            maxRetries: this.maxRetries,
            error: errorMessage
          }, 'Failed to process queued embedding, will retry');
        } else {
          // Max retries exceeded - keep in failed state
          updateStmt.run('failed', errorMessage, row.key);
          failed++;

          this.logger.error({
            key: row.key,
            retryCount,
            error: errorMessage
          }, 'Failed to process queued embedding after max retries');
        }
      }
    }

    return { processed, failed };
  }

  getStats(): QueueStats {
    const pending = this.db.query(`SELECT COUNT(*) as count FROM embedding_queue WHERE status = 'pending'`).get() as { count: number };
    const processing = this.db.query(`SELECT COUNT(*) as count FROM embedding_queue WHERE status = 'processing'`).get() as { count: number };
    const failed = this.db.query(`SELECT COUNT(*) as count FROM embedding_queue WHERE status = 'failed'`).get() as { count: number };
    const completed = this.db.query(`SELECT COUNT(*) as count FROM embedding_completed`).get() as { count: number };

    return {
      pending: pending.count,
      processing: processing.count,
      failed: failed.count,
      completed: completed.count
    };
  }

  async retryFailed(): Promise<number> {
    const result = this.db.run(`
      UPDATE embedding_queue
      SET status = 'pending', retry_count = 0, updated_at = datetime('now'), error_message = NULL
      WHERE status = 'failed'
    `);

    this.logger.info({ retriedCount: result.changes }, 'Retried failed embeddings');
    return result.changes;
  }

  async shutdown(): Promise<void> {
    this.logger.info('Shutting down SQLite embedding queue...');

    // Signal shutdown
    this.isShuttingDown = true;

    // Stop the background processor interval
    if (this.processInterval) {
      clearInterval(this.processInterval);
      this.processInterval = null;
    }

    // Wait for current processing to complete
    while (this.isProcessing) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    const stats = this.getStats();

    this.logger.info({
      pending: stats.pending,
      processing: stats.processing,
      failed: stats.failed,
      completed: stats.completed
    }, 'SQLite queue shutdown complete');
  }

  close(): void {
    this.db.close();
  }
}
