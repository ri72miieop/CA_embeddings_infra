import { QdrantClient } from '@qdrant/js-client-rest';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import type { EmbeddingVector, SearchResult, SearchQuery, DatabaseConfig } from '../types/index.js';
import { createContextLogger } from '../observability/logger.js';
import { embeddingOperationDuration, errorRate } from '../observability/metrics.js';
import type { IVectorStore } from '../interfaces/vector-store.interface.js';

const tracer = trace.getTracer('qdrant-vector-store');

/**
 * Qdrant implementation of the vector store interface
 * Uses Qdrant as the underlying vector database with advanced features
 */
export class QdrantVectorStore implements IVectorStore {
  private client: QdrantClient | null = null;
  private config: DatabaseConfig;
  private collectionName: string;
  private isInitialized = false;

  constructor(config: DatabaseConfig) {
    this.config = config;

    if (!config.qdrant) {
      throw new Error('Qdrant configuration is required for QdrantVectorStore');
    }

    this.collectionName = config.qdrant.collectionName;
  }

  async initialize(): Promise<void> {
    const span = tracer.startSpan('qdrant_vector_store_initialize');
    const contextLogger = createContextLogger({ operation: 'initialize', store: 'qdrant' });

    try {
      const qdrantConfig = this.config.qdrant!;

      contextLogger.info({
        url: qdrantConfig.url,
        port: qdrantConfig.port,
        collection: this.collectionName,
        dimension: this.config.dimension

      }, 'Initializing Qdrant client');

      // Initialize Qdrant client
      this.client = new QdrantClient({
        url: qdrantConfig.url,
        apiKey: qdrantConfig.apiKey,
        port: qdrantConfig.port,
        timeout: qdrantConfig.timeout || 30000,
      });

      // Check if collection exists
      const collections = await this.client.getCollections();
      const collectionExists = collections.collections.some(
        col => col.name === this.collectionName
      );

      if (!collectionExists) {
        contextLogger.info('Collection does not exist, creating new collection');
        
        // Create collection with proper vector configuration
        await this.client.createCollection(this.collectionName, {
          vectors: {
            size: this.config.dimension,
            distance: 'Cosine', // Default to Cosine similarity
          },
          // Enable payload indexing for metadata filtering
          optimizers_config: {
            indexing_threshold: 10000,
          },
        });

        contextLogger.info('Created new Qdrant collection');
      } else {
        contextLogger.info('Using existing Qdrant collection');
        
        // Verify collection configuration
        const collectionInfo = await this.client.getCollection(this.collectionName);
        const vectorsConfig = collectionInfo.config?.params?.vectors;
        const vectorSize = typeof vectorsConfig === 'object' && 'size' in vectorsConfig 
          ? vectorsConfig.size 
          : 0;
        
        if (vectorSize !== this.config.dimension) {
          throw new Error(
            `Collection dimension mismatch: expected ${this.config.dimension}, got ${vectorSize}`
          );
        }
      }

      this.isInitialized = true;
      contextLogger.info('Qdrant client initialized successfully');
      span.setStatus({ code: SpanStatusCode.OK });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to initialize Qdrant client');
      errorRate.inc({ type: 'database', operation: 'initialize' });
      span.recordException(error as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: errorMessage });
      throw error;
    } finally {
      span.end();
    }
  }

  async insert(embeddings: EmbeddingVector[]): Promise<void> {
    this.ensureInitialized();

    const span = tracer.startSpan('qdrant_vector_store_insert');
    const timer = embeddingOperationDuration.startTimer({ operation: 'insert' });
    const contextLogger = createContextLogger({
      operation: 'insert',
      store: 'qdrant',
      count: embeddings.length
    });

    try {
      contextLogger.info('Starting bulk insert operation');

      // Convert embeddings to Qdrant points format
      const points = embeddings.map((emb, index) => {
        // Convert string key to numeric ID for Qdrant (same as exists method)
        let numericId: number;
        try {
          numericId = Number(BigInt(emb.key));
        } catch (e) {
          // Fallback for non-numeric keys
          numericId = Number(emb.key);
        }
        
        return {
          id: numericId, // Use numeric ID for Qdrant
          vector: Array.from(emb.vector), // Qdrant expects array, not Float32Array
          payload: {
            key: emb.key, // Keep original string key in payload
            metadata: emb.metadata || {}, // Keep metadata nested
          },
        };
      });

      // Batch upsert for performance
      await this.client!.upsert(this.collectionName, {
        wait: true,
        points,
      });

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

  async exists(key: string): Promise<boolean> {
    this.ensureInitialized();

    const span = tracer.startSpan('qdrant_vector_store_exists');
    const contextLogger = createContextLogger({
      operation: 'exists',
      store: 'qdrant',
      key
    });

    try {
      // Convert key to numeric ID (same format used for insertion)
      const numericId = Number(BigInt(key));

      contextLogger.debug({ key, numericId }, 'Checking if vector exists');

      // Retrieve the point without payload or vector data for efficiency
      const result = await this.client!.retrieve(this.collectionName, {
        ids: [numericId],
        with_payload: false,
        with_vector: false
      });

      const exists = result.length > 0;
      contextLogger.debug({ exists }, 'Vector existence check completed');
      
      span.setStatus({ code: SpanStatusCode.OK });
      return exists;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to check vector existence');
      span.recordException(error as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: errorMessage });
      // Return false on error rather than throwing
      return false;
    } finally {
      span.end();
    }
  }

  async search(query: SearchQuery): Promise<SearchResult[]> {
    this.ensureInitialized();

    const span = tracer.startSpan('qdrant_vector_store_search');
    const timer = embeddingOperationDuration.startTimer({ operation: 'search' });
    const contextLogger = createContextLogger({
      operation: 'search',
      store: 'qdrant',
      k: query.k,
      hasFilter: !!query.filter
    });

    try {
      contextLogger.debug('Starting vector search');

      // Build Qdrant filter if provided
      const filter = query.filter ? this.buildQdrantFilter(query.filter) : undefined;

      if (filter) {
        contextLogger.debug({ filter }, 'Applying metadata filter to search');
      }
      

      const searchResults = await this.client!.search(this.collectionName, {
        vector: Array.from(query.vector),
        limit: query.k,
        filter,
        with_payload: true,
        score_threshold: query.threshold,
      });

      const results: SearchResult[] = searchResults.map(result => {
        // Extract and flatten metadata from payload
        // Payload structure: { key: string, metadata: {...}, ...otherFields }
        let key: string;
        let metadata: Record<string, any> | undefined;
        
        if (result.payload) {
          const sanitizedPayload = this.sanitizeMetadata(result.payload);
          const { key: payloadKey, metadata: nestedMetadata } = sanitizedPayload;
          
          // Use the original string key from payload (not the numeric ID)
          // This preserves the full precision of large numeric keys
          key = payloadKey as string;
          
          metadata = {
            ...nestedMetadata,
          };
        } else {
          // Fallback to ID if no payload (shouldn't happen in normal operation)
          const id = result.id;
          key = typeof id === 'bigint' ? (id as bigint).toString() : String(id);
        }
        
        return {
          key,
          distance: result.score,
          metadata,
        };
      });

      contextLogger.info({
        resultCount: results.length,
        queryTime: timer()
      }, 'Vector search completed');

      span.setStatus({ code: SpanStatusCode.OK });
      return results;
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

  /**
   * Build Qdrant filter from simple key-value metadata
   * Supports basic equality filtering
   */
  private buildQdrantFilter(metadata: Record<string, any>): any {
    const must = Object.entries(metadata).map(([key, value]) => ({
      key,
      match: { value },
    }));

    return {
      must,
    };
  }

  /**
   * Sanitize metadata by converting BigInt values to strings
   * This is needed because JSON.stringify cannot serialize BigInt
   */
  private sanitizeMetadata(metadata: Record<string, any>): Record<string, any> {
    const sanitized: Record<string, any> = {};
    
    for (const [key, value] of Object.entries(metadata)) {
      if (typeof value === 'bigint') {
        sanitized[key] = value.toString();
      } else if (value && typeof value === 'object' && !Array.isArray(value)) {
        sanitized[key] = this.sanitizeMetadata(value);
      } else if (Array.isArray(value)) {
        sanitized[key] = value.map(item => 
          typeof item === 'bigint' ? item.toString() :
          item && typeof item === 'object' ? this.sanitizeMetadata(item) :
          item
        );
      } else {
        sanitized[key] = value;
      }
    }
    
    return sanitized;
  }

  async delete(keys: string[]): Promise<void> {
    this.ensureInitialized();

    const span = tracer.startSpan('qdrant_vector_store_delete');
    const timer = embeddingOperationDuration.startTimer({ operation: 'delete' });
    const contextLogger = createContextLogger({
      operation: 'delete',
      store: 'qdrant',
      count: keys.length
    });

    try {
      contextLogger.info('Starting bulk delete operation');

      // Delete points by IDs
      await this.client!.delete(this.collectionName, {
        wait: true,
        points: keys,
      });

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

    const span = tracer.startSpan('qdrant_vector_store_stats');
    const contextLogger = createContextLogger({ operation: 'stats', store: 'qdrant' });

    try {
      // Get collection info from Qdrant
      const collectionInfo = await this.client!.getCollection(this.collectionName);

      const vectorCount = collectionInfo.points_count || 0;
      
      // Qdrant provides segment info, we can estimate size
      const segments = collectionInfo.segments_count || 0;
      const dbSize = 'N/A'; // Qdrant doesn't expose exact size via API

      const result = {
        vectorCount,
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
    if (this.client) {
      const contextLogger = createContextLogger({ operation: 'close', store: 'qdrant' });
      contextLogger.info('Closing Qdrant client');

      // Qdrant client doesn't require explicit close
      this.client = null;
      this.isInitialized = false;
    }
  }

  private ensureInitialized(): void {
    if (!this.isInitialized || !this.client) {
      throw new Error('QdrantVectorStore not initialized. Call initialize() first.');
    }
  }
}

