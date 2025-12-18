import fs from 'fs/promises';
import path from 'path';
import { Worker } from 'worker_threads';

export interface ProcessedTweetRecord {
  key: string;
  processed_text: string;
  original_text: string;
  row_index: number;
  processing_metadata?: {
    has_context: boolean;
    has_quotes: boolean;
    is_truncated: boolean;
    character_difference: number;
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
}

export class IntermediaryParquetWriter {
  /**
   * Write processed tweet records to a simple JSON file (parquet-like structure)
   * Note: Using JSON for simplicity since creating proper parquet files requires
   * complex dependencies. This provides the same caching benefits.
   */
  static async writeProcessedTweets(
    records: ProcessedTweetRecord[],
    outputPath: string
  ): Promise<void> {
    try {
      // Ensure output directory exists
      const dir = path.dirname(outputPath);
      await fs.mkdir(dir, { recursive: true });

      // Write as JSONL (JSON Lines) for efficient streaming
      const jsonlPath = outputPath.replace('.parquet', '.jsonl');
      const lines = records.map(record => JSON.stringify(record)).join('\\n');

      await fs.writeFile(jsonlPath, lines, 'utf-8');

      // Also create a metadata file with summary info
      const metadataPath = outputPath.replace('.parquet', '.metadata.json');
      const metadata = {
        totalRecords: records.length,
        createdAt: new Date().toISOString(),
        schema: {
          key: 'string',
          processed_text: 'string',
          original_text: 'string',
          row_index: 'number',
          processing_metadata: 'object'
        },
        statistics: {
          avgProcessedLength: records.reduce((sum, r) => sum + r.processed_text.length, 0) / records.length,
          avgOriginalLength: records.reduce((sum, r) => sum + r.original_text.length, 0) / records.length,
          recordsWithContext: records.filter(r => r.processing_metadata?.has_context).length,
          recordsWithQuotes: records.filter(r => r.processing_metadata?.has_quotes).length,
          recordsTruncated: records.filter(r => r.processing_metadata?.is_truncated).length
        }
      };

      await fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2));

      console.log(`📝 Wrote ${records.length} processed records to cache`);
      console.log(`📊 Cache statistics: ${metadata.statistics.recordsWithContext} with context, ${metadata.statistics.recordsWithQuotes} with quotes`);

    } catch (error) {
      throw new Error(`Failed to write intermediary file: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  /**
   * Read processed tweet records from intermediary file
   */
  static async readProcessedTweets(inputPath: string): Promise<ProcessedTweetRecord[]> {
    try {
      const jsonlPath = inputPath.replace('.parquet', '.jsonl');
      const content = await fs.readFile(jsonlPath, 'utf-8');

      if (!content.trim()) {
        return [];
      }

      const lines = content.trim().split('\\n');
      const records: ProcessedTweetRecord[] = lines.map(line => JSON.parse(line));

      console.log(`📖 Read ${records.length} processed records from cache`);
      return records;

    } catch (error) {
      throw new Error(`Failed to read intermediary file: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  /**
   * Check if intermediary file exists and is readable
   */
  static async exists(filePath: string): Promise<boolean> {
    try {
      const jsonlPath = filePath.replace('.parquet', '.jsonl');
      await fs.access(jsonlPath);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Get file size and record count from intermediary file
   */
  static async getFileInfo(filePath: string): Promise<{
    exists: boolean;
    size: number;
    recordCount: number;
    createdAt?: string;
  }> {
    try {
      const jsonlPath = filePath.replace('.parquet', '.jsonl');
      const metadataPath = filePath.replace('.parquet', '.metadata.json');

      const [jsonlStats, metadataContent] = await Promise.all([
        fs.stat(jsonlPath),
        fs.readFile(metadataPath, 'utf-8').catch(() => '{}')
      ]);

      const metadata = JSON.parse(metadataContent);

      return {
        exists: true,
        size: jsonlStats.size,
        recordCount: metadata.totalRecords || 0,
        createdAt: metadata.createdAt
      };
    } catch {
      return {
        exists: false,
        size: 0,
        recordCount: 0
      };
    }
  }

  /**
   * Stream processed tweets in batches for memory efficiency
   */
  static async *streamProcessedTweets(
    inputPath: string,
    batchSize: number = 1000
  ): AsyncGenerator<ProcessedTweetRecord[], void, unknown> {
    try {
      const jsonlPath = inputPath.replace('.parquet', '.jsonl');
      const content = await fs.readFile(jsonlPath, 'utf-8');

      if (!content.trim()) {
        return;
      }

      const lines = content.trim().split('\\n');

      for (let i = 0; i < lines.length; i += batchSize) {
        const batchLines = lines.slice(i, i + batchSize);
        const batch: ProcessedTweetRecord[] = batchLines.map(line => JSON.parse(line));
        yield batch;
      }

    } catch (error) {
      throw new Error(`Failed to stream intermediary file: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  /**
   * Convert ProcessedTweetRecord to ProcessingItem format used by CLI
   */
  static toProcessingItem(record: ProcessedTweetRecord): {
    key: string;
    content: string;
    rowIndex: number;
  } {
    return {
      key: record.key,
      content: record.processed_text,
      rowIndex: record.row_index
    };
  }

  /**
   * Convert ProcessedTweetRecord to TweetComparisonData for HTML export
   */
  static toTweetComparisonData(record: ProcessedTweetRecord): {
    tweet_id: string;
    full_text: string;
    processed_text: string;
    contextIncluded: boolean;
    quotesIncluded: boolean;
    truncated: boolean;
    charactersUsed: number;
    originalLength: number;
  } {
    return {
      tweet_id: record.key,
      full_text: record.original_text,
      processed_text: record.processed_text,
      contextIncluded: record.processing_metadata?.has_context || false,
      quotesIncluded: record.processing_metadata?.has_quotes || false,
      truncated: record.processing_metadata?.is_truncated || false,
      charactersUsed: record.processed_text.length,
      originalLength: record.original_text.length
    };
  }

  /**
   * Delete intermediary file and metadata
   */
  static async delete(filePath: string): Promise<void> {
    try {
      const jsonlPath = filePath.replace('.parquet', '.jsonl');
      const metadataPath = filePath.replace('.parquet', '.metadata.json');

      await Promise.all([
        fs.unlink(jsonlPath).catch(() => {}),
        fs.unlink(metadataPath).catch(() => {})
      ]);
    } catch (error) {
      console.warn(`Failed to delete intermediary file: ${error}`);
    }
  }
}