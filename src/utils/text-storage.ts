import fs from 'fs/promises';
import path from 'path';
import { createContextLogger } from '../observability/logger.js';

export interface TextRecord {
  key: string;
  text: string;
  metadata?: {
    original_text?: string;
    processing_metadata?: {
      has_context: boolean;
      has_quotes: boolean;
      is_truncated: boolean;
      character_difference: number;
    };
    [key: string]: any;
  };
  created_at: string;
}

export interface TextStorageConfig {
  path: string;
  append: boolean;
}

/**
 * Utility class for storing text content alongside embeddings
 * Uses JSONL format for efficient storage and retrieval
 */
export class TextStorage {
  private config: TextStorageConfig;
  private storageFile: string;

  constructor(config: TextStorageConfig) {
    this.config = config;
    this.storageFile = config.path;
  }

  async initialize(): Promise<void> {
    const contextLogger = createContextLogger({ operation: 'text_storage_init' });

    try {
      // Ensure directory exists
      const dir = path.dirname(this.storageFile);
      await fs.mkdir(dir, { recursive: true });

      // Create file if it doesn't exist or if not appending
      if (!this.config.append) {
        await fs.writeFile(this.storageFile, '', 'utf-8');
        contextLogger.info({ file: this.storageFile }, 'Initialized text storage file');
      } else {
        contextLogger.info({ file: this.storageFile }, 'Text storage initialized in append mode');
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to initialize text storage');
      throw error;
    }
  }

  /**
   * Store multiple text records efficiently
   */
  async storeTextRecords(records: Omit<TextRecord, 'created_at'>[]): Promise<void> {
   //const contextLogger = createContextLogger({
   //  operation: 'store_text_records',
   //  count: records.length
   //});

   //try {
   //  const timestamp = new Date().toISOString();
   //  const lines = records.map(record => {
   //    const textRecord: TextRecord = {
   //      ...record,
   //      created_at: timestamp
   //    };
   //    return JSON.stringify(textRecord);
   //  });

   //  const content = lines.join('\n') + '\n';
   //  await fs.appendFile(this.storageFile, content);

   //  contextLogger.info({
   //    stored: records.length,
   //    file: this.storageFile
   //  }, 'Text records stored successfully');

   //} catch (error) {
   //  const errorMessage = error instanceof Error ? error.message : 'Unknown error';
   //  contextLogger.error({ error: errorMessage }, 'Failed to store text records');
   //  throw error;
   //}
  }

  /**
   * Retrieve text for specific keys
   */
  async getTexts(keys: string[]): Promise<Map<string, TextRecord>> {
    const contextLogger = createContextLogger({
      operation: 'get_texts',
      keys: keys.length
    });

    const result = new Map<string, TextRecord>();

    try {
      const content = await fs.readFile(this.storageFile, 'utf-8');
      const lines = content.trim().split('\n').filter(line => line);

      const keySet = new Set(keys);

      for (const line of lines) {
        try {
          const record: TextRecord = JSON.parse(line);
          if (keySet.has(record.key)) {
            result.set(record.key, record);
          }
        } catch {
          // Skip malformed lines
          continue;
        }
      }

      contextLogger.info({
        requested: keys.length,
        found: result.size
      }, 'Text retrieval completed');

      return result;

    } catch (error) {
      if ((error as any).code === 'ENOENT') {
        contextLogger.warn('Text storage file not found, returning empty results');
        return result;
      }

      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to retrieve texts');
      throw error;
    }
  }

  /**
   * Check if texts exist for given keys
   */
  async checkTextsExist(keys: string[]): Promise<Set<string>> {
    const contextLogger = createContextLogger({
      operation: 'check_texts_exist',
      keys: keys.length
    });

    const existingKeys = new Set<string>();

    try {
      const content = await fs.readFile(this.storageFile, 'utf-8');
      const lines = content.trim().split('\n').filter(line => line);

      const keySet = new Set(keys);

      for (const line of lines) {
        try {
          const record: TextRecord = JSON.parse(line);
          if (keySet.has(record.key)) {
            existingKeys.add(record.key);
          }
        } catch {
          // Skip malformed lines
          continue;
        }
      }

      contextLogger.info({
        requested: keys.length,
        existing: existingKeys.size
      }, 'Text existence check completed');

      return existingKeys;

    } catch (error) {
      if ((error as any).code === 'ENOENT') {
        contextLogger.warn('Text storage file not found, no existing texts');
        return existingKeys;
      }

      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to check text existence');
      throw error;
    }
  }

  /**
   * Stream all text records (for large datasets)
   */
  async *streamTextRecords(batchSize: number = 1000): AsyncGenerator<TextRecord[], void, unknown> {
    const contextLogger = createContextLogger({
      operation: 'stream_text_records',
      batchSize
    });

    try {
      const content = await fs.readFile(this.storageFile, 'utf-8');
      const lines = content.trim().split('\n').filter(line => line);

      let batch: TextRecord[] = [];

      for (const line of lines) {
        try {
          const record: TextRecord = JSON.parse(line);
          batch.push(record);

          if (batch.length >= batchSize) {
            yield batch;
            batch = [];
          }
        } catch {
          // Skip malformed lines
          continue;
        }
      }

      // Yield remaining batch
      if (batch.length > 0) {
        yield batch;
      }

      contextLogger.info({ totalLines: lines.length }, 'Text streaming completed');

    } catch (error) {
      if ((error as any).code === 'ENOENT') {
        contextLogger.warn('Text storage file not found, no records to stream');
        return;
      }

      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to stream text records');
      throw error;
    }
  }

  /**
   * Get storage statistics
   */
  async getStats(): Promise<{
    totalRecords: number;
    fileSizeBytes: number;
    fileSizeMB: number;
    oldestRecord?: string;
    newestRecord?: string;
  }> {
    const contextLogger = createContextLogger({ operation: 'text_storage_stats' });

    try {
      const stats = await fs.stat(this.storageFile);
      const content = await fs.readFile(this.storageFile, 'utf-8');
      const lines = content.trim().split('\n').filter(line => line);

      let oldestRecord: string | undefined;
      let newestRecord: string | undefined;
      let validRecords = 0;

      for (const line of lines) {
        try {
          const record: TextRecord = JSON.parse(line);
          validRecords++;

          if (!oldestRecord || record.created_at < oldestRecord) {
            oldestRecord = record.created_at;
          }
          if (!newestRecord || record.created_at > newestRecord) {
            newestRecord = record.created_at;
          }
        } catch {
          // Skip malformed lines
          continue;
        }
      }

      const result = {
        totalRecords: validRecords,
        fileSizeBytes: stats.size,
        fileSizeMB: Math.round((stats.size / 1024 / 1024) * 100) / 100,
        oldestRecord,
        newestRecord
      };

      contextLogger.info(result, 'Text storage statistics retrieved');
      return result;

    } catch (error) {
      if ((error as any).code === 'ENOENT') {
        return {
          totalRecords: 0,
          fileSizeBytes: 0,
          fileSizeMB: 0
        };
      }

      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to get text storage statistics');
      throw error;
    }
  }

  /**
   * Delete the text storage file
   */
  async clear(): Promise<void> {
    const contextLogger = createContextLogger({ operation: 'clear_text_storage' });

    try {
      await fs.unlink(this.storageFile);
      contextLogger.info({ file: this.storageFile }, 'Text storage cleared');
    } catch (error) {
      if ((error as any).code !== 'ENOENT') {
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        contextLogger.error({ error: errorMessage }, 'Failed to clear text storage');
        throw error;
      }
    }
  }

  getStorageFile(): string {
    return this.storageFile;
  }
}