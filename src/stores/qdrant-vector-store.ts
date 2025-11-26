import { QdrantClient } from '@qdrant/js-client-rest';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import type { 
  EmbeddingVector, 
  SearchResult, 
  SearchQuery, 
  DatabaseConfig,
  Filter,
  FilterItem,
  FieldCondition,
  RangeCondition
} from '../types/index.js';
import { createContextLogger } from '../observability/logger.js';
import { embeddingOperationDuration, errorRate } from '../observability/metrics.js';
import type { IVectorStore } from '../interfaces/vector-store.interface.js';
import { cleanTweetText } from '../utils/tweet-text-processor.js';

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

      // Ensure payload indexes exist for text fields (works for both new and existing collections)
      await this.ensurePayloadIndexes(contextLogger);

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

      // Build Qdrant filter if provided (supports both new and legacy formats)
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
   * Ensure payload indexes exist for common fields
   * Checks if indexes already exist before creating them
   */
  private async ensurePayloadIndexes(contextLogger: any): Promise<void> {
    try {
      // Get existing collection info to check for existing indexes
      const collectionInfo = await this.client!.getCollection(this.collectionName);
      const existingIndexes = collectionInfo.payload_schema || {};

      // Define indexes to create
      const textIndexes = [
        { field: 'metadata.text', schema: 'text' },
        { field: 'metadata.original_text', schema: 'text' },
      ];

      const keywordIndexes = [
        { field: 'metadata.source', schema: 'keyword' },
        { field: 'metadata.provider', schema: 'keyword' },
        { field: 'metadata.model', schema: 'keyword' },
      ];

      const allIndexes = [...textIndexes, ...keywordIndexes];

      // Create missing indexes
      for (const { field, schema } of allIndexes) {
        // Check if index already exists
        if (existingIndexes[field]) {
          contextLogger.debug(`Payload index for ${field} already exists`);
          continue;
        }

        try {
          await this.client!.createPayloadIndex(this.collectionName, {
            field_name: field,
            field_schema: schema as any,
          });
          contextLogger.info(`Created payload index for ${field}`);
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : 'Unknown error';
          contextLogger.warn(`Failed to create ${field} field index: ${errorMessage}`);
        }
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.warn(`Failed to ensure payload indexes: ${errorMessage}`);
    }
  }

  // ============================================================================
  // Filter Building Methods (Qdrant-style with must/should/must_not)
  // ============================================================================

  /**
   * Parse datetime value from ISO 8601 string or Unix timestamp
   * Returns Unix timestamp in seconds for Qdrant datetime range queries
   */
  private parseDatetimeValue(value: string | number): number {
    if (typeof value === 'number') {
      // If value is already a number, assume it's a Unix timestamp
      // If it looks like milliseconds (> year 2100 in seconds), convert to seconds
      if (value > 4102444800) {
        return Math.floor(value / 1000);
      }
      return value;
    }

    // Try to parse as ISO 8601 datetime string
    const date = new Date(value);
    if (!isNaN(date.getTime())) {
      return Math.floor(date.getTime() / 1000);
    }

    // If not a valid datetime, try parsing as a number string
    const numValue = parseFloat(value);
    if (!isNaN(numValue)) {
      if (numValue > 4102444800) {
        return Math.floor(numValue / 1000);
      }
      return numValue;
    }

    throw new Error(`Invalid datetime value: ${value}`);
  }

  /**
   * Check if a value looks like a datetime (ISO 8601 string or large timestamp)
   */
  private isDatetimeValue(value: string | number): boolean {
    if (typeof value === 'string') {
      // Check for ISO 8601 format patterns
      const isoPattern = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?/;
      if (isoPattern.test(value)) {
        const date = new Date(value);
        return !isNaN(date.getTime());
      }
      return false;
    }
    // Large numbers (> year 2000 in seconds) are likely timestamps
    return value > 946684800;
  }

  /**
   * Build a range condition for Qdrant
   * Handles both numeric and datetime values
   */
  private buildRangeCondition(range: RangeCondition): any {
    const qdrantRange: any = {};

    // Check if any value looks like a datetime
    const values = [range.gt, range.gte, range.lt, range.lte].filter(v => v !== undefined);
    const isDatetime = values.some(v => this.isDatetimeValue(v!));

    if (range.gt !== undefined) {
      qdrantRange.gt = isDatetime ? this.parseDatetimeValue(range.gt) : 
        (typeof range.gt === 'string' ? parseFloat(range.gt) : range.gt);
    }
    if (range.gte !== undefined) {
      qdrantRange.gte = isDatetime ? this.parseDatetimeValue(range.gte) : 
        (typeof range.gte === 'string' ? parseFloat(range.gte) : range.gte);
    }
    if (range.lt !== undefined) {
      qdrantRange.lt = isDatetime ? this.parseDatetimeValue(range.lt) : 
        (typeof range.lt === 'string' ? parseFloat(range.lt) : range.lt);
    }
    if (range.lte !== undefined) {
      qdrantRange.lte = isDatetime ? this.parseDatetimeValue(range.lte) : 
        (typeof range.lte === 'string' ? parseFloat(range.lte) : range.lte);
    }

    return qdrantRange;
  }

  /**
   * Check if an object is a FieldCondition (has 'key' property)
   */
  private isFieldCondition(item: FilterItem): item is FieldCondition {
    return 'key' in item;
  }

  /**
   * Build a single Qdrant condition from a FieldCondition
   */
  private buildCondition(condition: FieldCondition): any {
    // Prefix with 'metadata.' to match the payload structure
    const filterKey = `metadata.${condition.key}`;

    if (condition.match) {
      if (condition.match.text !== undefined) {
        // Full-text substring matching
        // Apply the same text cleaning as used when storing tweets
        // This ensures filters match against the processed/cleaned text
        const cleanedText = cleanTweetText(condition.match.text);
        return {
          key: filterKey,
          match: { text: cleanedText },
        };
      } else if (condition.match.value !== undefined) {
        // Exact value matching
        return {
          key: filterKey,
          match: { value: condition.match.value },
        };
      }
    }

    if (condition.range) {
      return {
        key: filterKey,
        range: this.buildRangeCondition(condition.range),
      };
    }

    throw new Error(`Invalid condition: must have either 'match' or 'range'`);
  }

  /**
   * Build Qdrant filter from Filter object with must/should/must_not clauses
   * Supports recursive nesting of filter clauses
   */
  private buildFilter(filter: Filter): any {
    const qdrantFilter: any = {};

    if (filter.must && filter.must.length > 0) {
      qdrantFilter.must = filter.must.map(item => {
        if (this.isFieldCondition(item)) {
          return this.buildCondition(item);
        } else {
          // Nested filter clause
          return this.buildFilter(item as Filter);
        }
      });
    }

    if (filter.should && filter.should.length > 0) {
      qdrantFilter.should = filter.should.map(item => {
        if (this.isFieldCondition(item)) {
          return this.buildCondition(item);
        } else {
          // Nested filter clause
          return this.buildFilter(item as Filter);
        }
      });
    }

    if (filter.must_not && filter.must_not.length > 0) {
      qdrantFilter.must_not = filter.must_not.map(item => {
        if (this.isFieldCondition(item)) {
          return this.buildCondition(item);
        } else {
          // Nested filter clause
          return this.buildFilter(item as Filter);
        }
      });
    }

    return qdrantFilter;
  }

  // ============================================================================
  // Legacy Filter Support (Backward Compatibility)
  // ============================================================================

  /**
   * Check if a filter object is using the new clause-based format
   */
  private isClauseBasedFilter(filter: any): boolean {
    if (!filter || typeof filter !== 'object') return false;
    const keys = Object.keys(filter);
    // New format has must, should, or must_not at top level
    return keys.some(key => ['must', 'should', 'must_not'].includes(key));
  }

  /**
   * Parse numeric range expression from string (legacy format)
   * Supports: ">10", "<50", ">=10", "<=50", ">10 & <50", ">=10 & <=50"
   */
  private parseNumericRange(value: string): { range?: any; isRange: boolean } {
    // Try to parse range expression
    const rangePattern = /^\s*(>=?|<=?)\s*(-?\d+(?:\.\d+)?)\s*(?:&\s*(>=?|<=?)\s*(-?\d+(?:\.\d+)?))?\s*$/;
    const match = value.match(rangePattern);

    if (!match) {
      return { isRange: false };
    }

    const op1 = match[1];
    const val1 = match[2];
    const op2 = match[3];
    const val2 = match[4];
    const range: any = {};

    // Parse first operator (always present if regex matched)
    if (op1 && val1) {
      const numVal = parseFloat(val1);
      if (op1 === '>') {
        range.gt = numVal;
      } else if (op1 === '>=') {
        range.gte = numVal;
      } else if (op1 === '<') {
        range.lt = numVal;
      } else if (op1 === '<=') {
        range.lte = numVal;
      }
    }

    // Parse second operator if exists (for range expressions like ">10 & <50")
    if (op2 && val2) {
      const numVal = parseFloat(val2);
      if (op2 === '>') {
        range.gt = numVal;
      } else if (op2 === '>=') {
        range.gte = numVal;
      } else if (op2 === '<') {
        range.lt = numVal;
      } else if (op2 === '<=') {
        range.lte = numVal;
      }
    }

    return { range, isRange: true };
  }

  /**
   * Build Qdrant filter from legacy flat key-value metadata format
   * Supports:
   * - Exact matching for non-string values (numbers, booleans)
   * - Substring matching for string values using full-text search
   * - Range queries for numeric fields (">10", "<50", ">10 & <50")
   */
  private buildLegacyFilter(metadata: Record<string, any>): any {
    const must = Object.entries(metadata).map(([key, value]) => {
      // Prefix with 'metadata.' to match the payload structure
      const filterKey = `metadata.${key}`;

      // Handle string values
      if (typeof value === 'string') {
        // Check if it's a numeric range expression
        const { range, isRange } = this.parseNumericRange(value);

        if (isRange) {
          // Use range filter for numeric ranges
          return {
            key: filterKey,
            range,
          };
        } else {
          // Use text matching for regular strings (enables substring matching)
          // Apply the same text cleaning as used when storing tweets
          // This ensures filters match against the processed/cleaned text
          const cleanedText = cleanTweetText(value);
          return {
            key: filterKey,
            match: { text: cleanedText },
          };
        }
      } else {
        // Use value matching for other types (exact matching)
        return {
          key: filterKey,
          match: { value },
        };
      }
    });

    return {
      must,
    };
  }

  /**
   * Build Qdrant filter from either new clause-based or legacy flat format
   * Automatically detects the format and routes to the appropriate handler
   */
  private buildQdrantFilter(filter: any): any {
    if (this.isClauseBasedFilter(filter)) {
      // New clause-based format (must/should/must_not)
      return this.buildFilter(filter as Filter);
    } else {
      // Legacy flat format (key-value pairs)
      return this.buildLegacyFilter(filter);
    }
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

