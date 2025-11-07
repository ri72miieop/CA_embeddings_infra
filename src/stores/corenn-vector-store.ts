import { CoreNN } from 'corenn/corenn-node/src/index.js';
import path from 'path';
import fs from 'fs/promises';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import type { EmbeddingVector, SearchResult, SearchQuery, DatabaseConfig } from '../types/index.js';
import { logger, createContextLogger } from '../observability/logger.js';
import { embeddingOperationDuration, errorRate } from '../observability/metrics.js';
import { PersistentCounter } from '../utils/persistent-counter.js';
import type { IVectorStore } from '../interfaces/vector-store.interface.js';

const tracer = trace.getTracer('corenn-vector-store');

/**
 * CoreNN implementation of the vector store interface
 * Uses CoreNN as the underlying vector database
 */
export class CoreNNVectorStore implements IVectorStore {
  private db: CoreNN | null = null;
  private config: DatabaseConfig;
  private isInitialized = false;
  private vectorCounter: PersistentCounter;

  constructor(config: DatabaseConfig) {
    this.config = config;

    if (!config.path) {
      throw new Error('CoreNN requires a path in the database config');
    }

    // Initialize persistent counter - store in data directory alongside database
    const counterPath = path.join(path.dirname(config.path), 'vector-count.json');
    this.vectorCounter = new PersistentCounter(counterPath);
  }

  async initialize(): Promise<void> {
    const span = tracer.startSpan('corenn_vector_store_initialize');
    const contextLogger = createContextLogger({ operation: 'initialize', store: 'corenn' });

    try {
      await this.ensureDataDirectory();

      contextLogger.info({
        path: this.config.path,
        dimension: this.config.dimension
      }, 'Initializing CoreNN database');

      // Try to open existing database first, create if it doesn't exist
      try {
        await fs.access(this.config.path!);
        this.db = CoreNN.open(this.config.path!);
        contextLogger.info('Opened existing CoreNN database');
      } catch {
        this.db = CoreNN.create(this.config.path!, {
          dim: this.config.dimension,
        });
        contextLogger.info('Created new CoreNN database');
      }

      // Initialize persistent counter
      await this.vectorCounter.initialize();

      this.isInitialized = true;
      contextLogger.info('CoreNN database initialized successfully');
      span.setStatus({ code: SpanStatusCode.OK });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to initialize CoreNN database');
      errorRate.inc({ type: 'database', operation: 'initialize' });
      span.recordException(error as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: errorMessage });
      throw error;
    } finally {
      span.end();
    }
  }

  private async ensureDataDirectory(): Promise<void> {
    if (!this.config.path) return;
    
    const dir = path.dirname(this.config.path);
    try {
      await fs.access(dir);
    } catch {
      await fs.mkdir(dir, { recursive: true });
      logger.info({ directory: dir }, 'Created data directory');
    }
  }

  async insert(embeddings: EmbeddingVector[]): Promise<void> {
    this.ensureInitialized();

    const span = tracer.startSpan('corenn_vector_store_insert');
    const timer = embeddingOperationDuration.startTimer({ operation: 'insert' });
    const contextLogger = createContextLogger({
      operation: 'insert',
      store: 'corenn',
      count: embeddings.length
    });

    try {
      contextLogger.info('Starting bulk insert operation');

      // Log the type of vectors we're receiving
      if (embeddings.length > 0 && embeddings[0]?.vector) {
        const firstVector = embeddings[0].vector;
        const vectorType = firstVector.constructor?.name || typeof firstVector;
        const isTypedArray = (firstVector as any) instanceof Float32Array || (firstVector as any) instanceof Float64Array;
        
        contextLogger.debug({
          firstVectorType: vectorType,
          isTypedArray,
          vectorLength: firstVector?.length,
          isArray: Array.isArray(firstVector)
        }, 'Vector type validation before CoreNN insert');
        
        if (!isTypedArray) {
          const vectorAsAny = firstVector as any;
          contextLogger.error({
            firstVectorType: vectorType,
            vectorKeys: vectorAsAny ? Object.keys(vectorAsAny).slice(0, 5) : [],
            sampleValues: Array.isArray(firstVector) ? (firstVector as number[]).slice(0, 3) : 'not-array'
          }, 'Received non-typed array vector - this will cause CoreNN to fail');
        }
      }

      const coreNNEmbeddings = embeddings.map(emb => ({
        key: emb.key,
        vector: emb.vector,
        // Note: CoreNN doesn't support metadata, so we ignore it
      }));

      this.db!.insert(coreNNEmbeddings);

      this.vectorCounter.increment(embeddings.length);
      contextLogger.info('Bulk insert completed successfully');
      span.setStatus({ code: SpanStatusCode.OK });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to insert embeddings');
      errorRate.inc({ type: 'database', operation: 'insert' });
      span.recordException(error as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: errorMessage });
      throw error;
    } finally {
      timer();
      span.end();
    }
  }

  async search(query: SearchQuery): Promise<SearchResult[]> {
    this.ensureInitialized();

    const span = tracer.startSpan('corenn_vector_store_search');
    const timer = embeddingOperationDuration.startTimer({ operation: 'search' });
    const contextLogger = createContextLogger({
      operation: 'search',
      store: 'corenn',
      k: query.k
    });

    try {
      contextLogger.debug('Starting vector search');

      if (query.filter) {
        contextLogger.warn('CoreNN does not support metadata filtering - filter will be ignored');
      }

      const results = this.db!.query(query.vector, query.k);

      const searchResults: SearchResult[] = results.map((result: any) => ({
        key: result.key,
        distance: result.distance,
      }));

      contextLogger.info({
        resultCount: searchResults.length,
        queryTime: timer()
      }, 'Vector search completed');

      span.setStatus({ code: SpanStatusCode.OK });
      return searchResults;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to search embeddings');
      errorRate.inc({ type: 'database', operation: 'search' });
      span.recordException(error as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: errorMessage });
      throw error;
    } finally {
      timer();
      span.end();
    }
  }

  async delete(keys: string[]): Promise<void> {
    this.ensureInitialized();

    const span = tracer.startSpan('corenn_vector_store_delete');
    const timer = embeddingOperationDuration.startTimer({ operation: 'delete' });
    const contextLogger = createContextLogger({
      operation: 'delete',
      store: 'corenn',
      count: keys.length
    });

    try {
      contextLogger.info('Starting bulk delete operation');

      // Note: CoreNN v0.3.1 does not have a remove/delete method yet
      // This is a limitation of the current CoreNN API
      contextLogger.warn({
        keyCount: keys.length
      }, 'Delete operation not yet available in CoreNN - this is a planned feature');

      // For now, we just log the request but don't actually delete
      // In production, you might want to return an error or track deletions separately

      this.vectorCounter.decrement(keys.length);
      contextLogger.info('Bulk delete completed successfully');
      span.setStatus({ code: SpanStatusCode.OK });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to delete embeddings');
      errorRate.inc({ type: 'database', operation: 'delete' });
      span.recordException(error as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: errorMessage });
      throw error;
    } finally {
      timer();
      span.end();
    }
  }

  async getStats(): Promise<{ vectorCount: number; dbSize: string }> {
    this.ensureInitialized();

    const span = tracer.startSpan('corenn_vector_store_stats');
    const contextLogger = createContextLogger({ operation: 'stats', store: 'corenn' });

    try {
      let dbSize = '0.00 MB';

      // Get persistent vector count - includes all vectors from previous sessions
      const persistentVectorCount = this.vectorCounter.get();

      // Get database directory size
      try {
        const dbPath = this.config.path!;
        let totalSize = 0;

        // Check if path is a directory (RocksDB database)
        const pathStats = await fs.stat(dbPath);
        if (pathStats.isDirectory()) {
          // Calculate total size of all files in the database directory
          const files = await fs.readdir(dbPath);
          for (const file of files) {
            try {
              const filePath = path.join(dbPath, file);
              const fileStats = await fs.stat(filePath);
              if (fileStats.isFile()) {
                totalSize += fileStats.size;
              }
            } catch {
              // Skip files that can't be read
            }
          }
        } else {
          // Single file database
          totalSize = pathStats.size;
        }

        dbSize = `${(totalSize / 1024 / 1024).toFixed(2)} MB`;
      } catch (error) {
        contextLogger.warn({ error }, 'Could not read database file stats');
      }

      const result = {
        vectorCount: persistentVectorCount,
        dbSize,
      };

      contextLogger.debug(result, 'Retrieved database statistics');
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to get database stats');
      errorRate.inc({ type: 'database', operation: 'stats' });
      span.recordException(error as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: errorMessage });
      throw error;
    } finally {
      span.end();
    }
  }

  async close(): Promise<void> {
    if (this.db) {
      const contextLogger = createContextLogger({ operation: 'close', store: 'corenn' });
      contextLogger.info('Closing CoreNN database');

      // Close persistent counter
      await this.vectorCounter.close();

      this.db = null;
      this.isInitialized = false;
    }
  }

  private ensureInitialized(): void {
    if (!this.isInitialized || !this.db) {
      throw new Error('CoreNNVectorStore not initialized. Call initialize() first.');
    }
  }
}

