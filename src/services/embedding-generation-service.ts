import fs from 'fs/promises';
import path from 'path';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import type {
  EmbeddingGenerationConfig,
  EmbeddingGenerationItem,
  GeneratedEmbedding,
  StoredEmbeddingCall
} from '../types/index.js';
import { EmbeddingProvider } from './embedding-provider.js';
import { DeepInfraProvider } from './deepinfra-provider.js';
import { OpenAIProvider } from './openai-provider.js';
import { createContextLogger } from '../observability/logger.js';
import { TextStorage, type TextRecord } from '../utils/text-storage.js';

const tracer = trace.getTracer('embedding-generation-service');

export class EmbeddingGenerationService {
  private provider: EmbeddingProvider;
  private config: EmbeddingGenerationConfig;
  private storageFile?: string;
  private textStorage?: TextStorage;

  constructor(config: EmbeddingGenerationConfig) {
    this.config = config;
    this.provider = this.createProvider();

    if (config.storage.enabled && config.storage.path) {
      this.storageFile = path.join(config.storage.path, 'embedding-calls.jsonl');

      // Initialize text storage
      this.textStorage = new TextStorage({
        path: path.join(config.storage.path, 'texts.jsonl'),
        append: true
      });
    }
  }

  private createProvider(): EmbeddingProvider {
    switch (this.config.provider.provider) {
      case 'deepinfra':
        return new DeepInfraProvider(this.config.provider);
      case 'openai':
        return new OpenAIProvider(this.config.provider);
      case 'local':
        throw new Error('Local provider not yet implemented');
      default:
        throw new Error(`Unsupported embedding provider: ${this.config.provider.provider}`);
    }
  }

  async initialize(): Promise<void> {
    const contextLogger = createContextLogger({ operation: 'initialize' });

    try {
      if (this.storageFile) {
        const dir = path.dirname(this.storageFile);
        await fs.mkdir(dir, { recursive: true });
        contextLogger.info({ storageFile: this.storageFile }, 'Initialized embedding call storage');
      }

      // Initialize text storage if enabled
      if (this.textStorage) {
        await this.textStorage.initialize();
        contextLogger.info('Text storage initialized');
      }

      contextLogger.info({
        provider: this.config.provider.provider,
        model: this.config.provider.model,
        storageEnabled: this.config.storage.enabled,
        textStorageEnabled: !!this.textStorage
      }, 'Embedding generation service initialized');

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to initialize embedding generation service');
      throw error;
    }
  }

  async generateEmbeddings(items: EmbeddingGenerationItem[]): Promise<GeneratedEmbedding[]> {
    const span = tracer.startSpan('embedding_generation_service_generate');
    const contextLogger = createContextLogger({
      operation: 'generate_embeddings',
      count: items.length
    });

    const startTime = Date.now();

    try {
      contextLogger.info('Starting embedding generation process');

      // Validate items
      this.validateItems(items);

      // Generate embeddings using the provider
      const results = await this.provider.generateEmbeddings(items);

      const latencyMs = Date.now() - startTime;

      // Store text records alongside embeddings if enabled
      // IMPORTANT: Run in background without blocking to maintain parallelism
      if (this.textStorage) {
        const textRecords: Omit<TextRecord, 'created_at'>[] = items.map(item => ({
          key: item.key,
          text: typeof item.content === 'string' ? item.content : '',
          metadata: {
            ...item.metadata,
            original_text: item.metadata?.original_text
          }
        }));

        // Fire-and-forget: don't await to avoid blocking parallel requests
        this.textStorage.storeTextRecords(textRecords).catch(err => {
          contextLogger.error({ error: err.message }, 'Failed to store text records (non-blocking)');
        });
        contextLogger.info({ textRecords: textRecords.length }, 'Text records queued for storage (non-blocking)');
      }

      // Store the call record if enabled (non-blocking to maintain parallelism)
      if (this.config.storage.enabled) {
        const callRecord = this.createCallRecord(items, latencyMs, true);
        this.storeCallRecord(callRecord).catch(err => {
          contextLogger.error({ error: err.message }, 'Failed to store call record (non-blocking)');
        });
      }

      contextLogger.info({
        generated: results.length,
        latencyMs,
        textStored: !!this.textStorage
      }, 'Embedding generation completed successfully');

      span.setStatus({ code: SpanStatusCode.OK });
      return results;

    } catch (error) {
      const latencyMs = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';

      // Store the failed call record if enabled (non-blocking)
      if (this.config.storage.enabled) {
        const callRecord = this.createCallRecord(items, latencyMs, false, errorMessage);
        this.storeCallRecord(callRecord).catch(err => {
          // Ignore errors in error case storage
        });
      }

      contextLogger.error({
        error: errorMessage,
        latencyMs
      }, 'Failed to generate embeddings');

      span.recordException(error as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: errorMessage });
      throw error;
    } finally {
      span.end();
    }
  }

  async getCallHistory(limit: number = 100): Promise<StoredEmbeddingCall[]> {
    if (!this.storageFile) {
      return [];
    }

    try {
      const data = await fs.readFile(this.storageFile, 'utf-8');
      const lines = data.trim().split('\n').filter(line => line);
      const calls = lines
        .slice(-limit)
        .map(line => JSON.parse(line))
        .map(call => ({
          ...call,
          timestamp: new Date(call.timestamp)
        }));

      return calls.reverse(); // Most recent first
    } catch (error) {
      if ((error as any).code === 'ENOENT') {
        return [];
      }
      throw error;
    }
  }

  async getCallStats(): Promise<{
    totalCalls: number;
    successRate: number;
    averageLatency: number;
    totalTokens: number;
  }> {
    const calls = await this.getCallHistory(1000); // Get last 1000 calls for stats

    if (calls.length === 0) {
      return {
        totalCalls: 0,
        successRate: 0,
        averageLatency: 0,
        totalTokens: 0
      };
    }

    const successfulCalls = calls.filter(call => call.success);
    const averageLatency = calls.reduce((sum, call) => sum + call.latencyMs, 0) / calls.length;
    const totalTokens = calls.reduce((sum, call) => sum + call.inputTokens, 0);

    return {
      totalCalls: calls.length,
      successRate: successfulCalls.length / calls.length,
      averageLatency,
      totalTokens
    };
  }

  private validateItems(items: EmbeddingGenerationItem[]): void {
    if (!Array.isArray(items) || items.length === 0) {
      throw new Error('Items must be a non-empty array');
    }

    if (items.length > 1000) {
      throw new Error('Cannot process more than 1000 items at once');
    }

    for (const item of items) {
      if (!item.key || typeof item.key !== 'string') {
        throw new Error('Each item must have a valid key');
      }

      if (!item.content) {
        throw new Error(`Item with key "${item.key}" has no content`);
      }

      if (item.contentType === 'text' && typeof item.content !== 'string') {
        throw new Error(`Item with key "${item.key}" has invalid text content`);
      }

      if (item.contentType === 'image' && !this.isImageContent(item.content)) {
        throw new Error(`Item with key "${item.key}" has invalid image content`);
      }
    }
  }

  private isImageContent(content: any): boolean {
    return typeof content === 'object' &&
           content.data instanceof Buffer &&
           typeof content.mimeType === 'string';
  }

  private createCallRecord(
    items: EmbeddingGenerationItem[],
    latencyMs: number,
    success: boolean,
    error?: string
  ): StoredEmbeddingCall {
    const totalTokens = items.reduce((sum, item) => {
      return sum + (typeof item.content === 'string' ? Math.ceil(item.content.length / 4) : 0);
    }, 0);

    return {
      id: crypto.randomUUID(),
      timestamp: new Date(),
      provider: this.config.provider.provider,
      model: this.config.provider.model,
      inputTokens: totalTokens,
      outputDimension: 1024, // Default dimension for most models
      latencyMs,
      success,
      error,
      metadata: {
        itemCount: items.length,
        textItems: items.filter(item => item.contentType === 'text').length,
        imageItems: items.filter(item => item.contentType === 'image').length
      }
    };
  }

  private async storeCallRecord(record: StoredEmbeddingCall): Promise<void> {
    if (!this.storageFile) {
      return;
    }

    try {
      const line = JSON.stringify(record) + '\n';
      await fs.appendFile(this.storageFile, line);
    } catch (error) {
      const contextLogger = createContextLogger({ operation: 'store_call_record' });
      contextLogger.warn({ error }, 'Failed to store embedding call record');
    }
  }

  /**
   * Get texts for specific keys
   */
  async getTexts(keys: string[]): Promise<Map<string, TextRecord>> {
    if (!this.textStorage) {
      return new Map();
    }

    return await this.textStorage.getTexts(keys);
  }

  /**
   * Check if texts exist for given keys
   */
  async checkTextsExist(keys: string[]): Promise<Set<string>> {
    if (!this.textStorage) {
      return new Set();
    }

    return await this.textStorage.checkTextsExist(keys);
  }

  /**
   * Get text storage statistics
   */
  async getTextStorageStats(): Promise<{
    totalRecords: number;
    fileSizeBytes: number;
    fileSizeMB: number;
    oldestRecord?: string;
    newestRecord?: string;
  } | null> {
    if (!this.textStorage) {
      return null;
    }

    return await this.textStorage.getStats();
  }

  /**
   * Clear text storage
   */
  async clearTextStorage(): Promise<void> {
    if (!this.textStorage) {
      return;
    }

    await this.textStorage.clear();
  }
}