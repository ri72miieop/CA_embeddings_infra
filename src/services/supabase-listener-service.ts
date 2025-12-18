import { createClient, RealtimeChannel, SupabaseClient } from '@supabase/supabase-js';
import type { IVectorStore } from '../interfaces/vector-store.interface.js';
import type { EmbeddingGenerationService } from './embedding-generation-service.js';
import type { EmbeddingWriteQueueManager } from './EmbeddingWriteQueueManager.js';
import { processTweetText } from '../utils/tweet-text-processor.js';
import { createContextLogger } from '../observability/logger.js';
import { logger } from '../observability/logger.js';

/**
 * Interface for quoted tweet data from Supabase
 */
interface QuotedTweetData {
  tweet_id: string;
  full_text: string;
}

/**
 * Configuration for the Supabase listener service
 */
export interface SupabaseListenerConfig {
  supabaseUrl: string;
  supabaseKey: string;
  maxTextLength?: number;
  batchSize?: number;        // Default: 1000
  flushTimeoutMs?: number;   // Default: 15000 (15 seconds)
}

/**
 * Buffered tweet data waiting to be processed
 */
interface BufferedTweet {
  tweetId: string;
  fullText: string;
  tweetData: any;
}

/**
 * Statistics for the listener service
 */
interface ListenerStats {
  processed: number;
  skipped: number;
  failed: number;
  startTime: number;
}

/**
 * Service that listens to Supabase tweet insertions and generates embeddings
 * Integrates with the server's embedding services directly (no HTTP calls)
 * Batches tweets for efficient embedding generation.
 */
export class SupabaseListenerService {
  private supabase: SupabaseClient;
  private channel: RealtimeChannel | null = null;
  private stats: ListenerStats;
  private isShuttingDown = false;

  // Batching properties
  private tweetBuffer: BufferedTweet[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly BATCH_SIZE: number;
  private readonly FLUSH_TIMEOUT_MS: number;
  private readonly MAX_RETRIES = 2;
  private readonly RETRY_BACKOFF_MS = [1000, 2000];
  
  constructor(
    private config: SupabaseListenerConfig,
    private embeddingService: IVectorStore,
    private embeddingGenerationService: EmbeddingGenerationService,
    private embeddingWriteQueue: EmbeddingWriteQueueManager
  ) {
    this.supabase = createClient(config.supabaseUrl, config.supabaseKey);
    this.stats = {
      processed: 0,
      skipped: 0,
      failed: 0,
      startTime: Date.now()
    };
    this.BATCH_SIZE = config.batchSize ?? 1000;
    this.FLUSH_TIMEOUT_MS = config.flushTimeoutMs ?? 15000;
  }

  /**
   * Initialize the listener and subscribe to Supabase events
   */
  async initialize(): Promise<void> {
    logger.info({
      supabaseUrl: this.config.supabaseUrl,
      maxTextLength: this.config.maxTextLength || 1024,
      batchSize: this.BATCH_SIZE,
      flushTimeoutMs: this.FLUSH_TIMEOUT_MS
    }, 'Initializing Supabase listener service with batching');

    this.channel = this.supabase
      .channel('tweets')
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'tweets'
        },
        (payload) => {
          if (payload.new && !this.isShuttingDown) {
            this.bufferTweet(payload.new);
          }
        }
      )
      .subscribe((status) => {
        if (status === 'SUBSCRIBED') {
          logger.info('Successfully subscribed to Supabase tweets table');
        } else if (status === 'CHANNEL_ERROR') {
          logger.error('Error subscribing to Supabase channel');
        } else if (status === 'TIMED_OUT') {
          logger.error('Supabase subscription timed out');
        } else if (status === 'CLOSED') {
          logger.info('Supabase channel closed');
        }
      });
  }

  /**
   * Check if a tweet embedding already exists in the vector store
   */
  private async checkTweetExists(tweetId: string): Promise<boolean> {
    try {
      // Call the vector store's exists method directly (no HTTP request)
      if ('exists' in this.embeddingService && typeof (this.embeddingService as any).exists === 'function') {
        return await (this.embeddingService as any).exists(tweetId);
      }
      return false;
    } catch (error) {
      logger.warn({ error, tweetId }, 'Failed to check tweet existence');
      return false;
    }
  }

  /**
   * Fetch quoted tweets from Supabase with JOIN to get full tweet data
   */
  private async fetchQuotedTweets(tweetId: string): Promise<QuotedTweetData[]> {
    try {
      const { data, error } = await this.supabase
        .from('quote_tweets')
        .select(`
          quoted_tweet_id,
          tweets!inner (
            tweet_id,
            full_text
          )
        `)
        .eq('tweet_id', tweetId);
      
      if (error) {
        logger.error({ error: error.message, tweetId }, 'Error fetching quoted tweets');
        return [];
      }
      
      // Extract and flatten the joined tweet data
      const quotedTweets: QuotedTweetData[] = ((data as any[]) || [])
        .map((item: any) => item.tweets)
        .filter((tweet: any) => tweet && tweet.full_text)
        .map((tweet: any) => ({
          tweet_id: String(tweet.tweet_id),
          full_text: tweet.full_text
        }));
      
      return quotedTweets;
    } catch (error) {
      logger.error({ error, tweetId }, 'Error fetching quoted tweets');
      return [];
    }
  }

  /**
   * Buffer a tweet for batch processing
   */
  private bufferTweet(tweetData: any): void {
    const tweetId = String(tweetData.tweet_id || tweetData.id);
    const fullText = tweetData.full_text || tweetData.text || '';

    if (!tweetId || !fullText) {
      logger.warn({ tweetData }, 'Tweet missing ID or text, skipping');
      this.stats.skipped++;
      return;
    }

    logger.debug({
      tweetId,
      textPreview: fullText.substring(0, 50) + (fullText.length > 50 ? '...' : ''),
      bufferSize: this.tweetBuffer.length + 1
    }, 'Buffering tweet');

    this.tweetBuffer.push({ tweetId, fullText, tweetData });

    // Reset flush timer on each new tweet
    this.resetFlushTimer();

    // If buffer is full, flush immediately
    if (this.tweetBuffer.length >= this.BATCH_SIZE) {
      this.flushBatchWithRetry();
    }
  }

  /**
   * Reset the flush timer
   */
  private resetFlushTimer(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
    }
    this.flushTimer = setTimeout(() => {
      if (this.tweetBuffer.length > 0 && !this.isShuttingDown) {
        logger.debug({ bufferSize: this.tweetBuffer.length }, 'Flush timeout reached, processing batch');
        this.flushBatchWithRetry();
      }
    }, this.FLUSH_TIMEOUT_MS);
  }

  /**
   * Sleep utility for retry backoff
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Flush batch with retry logic
   */
  private async flushBatchWithRetry(): Promise<void> {
    if (this.tweetBuffer.length === 0) return;

    for (let attempt = 0; attempt <= this.MAX_RETRIES; attempt++) {
      try {
        await this.flushBatch();
        return; // Success
      } catch (error) {
        if (attempt < this.MAX_RETRIES) {
          const backoffMs = this.RETRY_BACKOFF_MS[attempt] ?? 1000;
          logger.warn({
            attempt: attempt + 1,
            maxRetries: this.MAX_RETRIES,
            backoffMs,
            error: error instanceof Error ? error.message : 'Unknown error'
          }, 'Batch processing failed, retrying');
          await this.sleep(backoffMs);
        } else {
          logger.error({
            error: error instanceof Error ? error.message : 'Unknown error',
            batchSize: this.tweetBuffer.length
          }, `Batch failed after ${this.MAX_RETRIES} retries, marking all as failed`);
          // Mark all tweets in current batch as failed
          this.stats.failed += this.tweetBuffer.length;
          this.tweetBuffer = [];
        }
      }
    }
  }

  /**
   * Process all buffered tweets as a batch
   */
  private async flushBatch(): Promise<void> {
    // Clear the flush timer
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    // Take all tweets from buffer (swap with empty array)
    const batch = this.tweetBuffer;
    this.tweetBuffer = [];

    if (batch.length === 0) return;

    const contextLogger = createContextLogger({
      operation: 'supabase_batch_processing',
      batchSize: batch.length
    });

    contextLogger.info('Processing tweet batch');

    // Step 1: Prepare embedding items (check existence, fetch quotes, process text)
    const embeddingItems: Array<{
      key: string;
      content: string;
      contentType: 'text';
      metadata: Record<string, any>;
    }> = [];

    for (const { tweetId, fullText, tweetData } of batch) {
      try {
        // Check if tweet already exists
        const exists = await this.checkTweetExists(tweetId);
        if (exists) {
          contextLogger.debug({ tweetId }, 'Tweet already exists, skipping');
          this.stats.skipped++;
          continue;
        }

        // Fetch quoted tweets
        const quotedTweets = await this.fetchQuotedTweets(tweetId);
        const quotedText = quotedTweets.length > 0 && quotedTweets[0] ? quotedTweets[0].full_text : null;
        const maxChars = this.config.maxTextLength || 1024;

        // Process text
        const processed = processTweetText(fullText, quotedText, maxChars);

        // Skip if processed text is empty
        if (!processed.processedText || processed.processedText.trim().length === 0) {
          contextLogger.debug({ tweetId }, 'Tweet has empty text after processing, skipping');
          this.stats.skipped++;
          continue;
        }

        // Prepare metadata (matching schema from batch-file-manager.ts)
        const metadata: Record<string, any> = {
          source: 'supabase_realtime',
          original_text: fullText,
          has_context: false,
          has_quotes: processed.quotesIncluded,
          is_truncated: processed.truncated,
          character_difference: processed.charactersUsed - fullText.length,
          text: processed.processedText,
          // Tweet metadata fields from Supabase (use != null to preserve zero values)
          account_id: tweetData.account_id != null ? Number(tweetData.account_id) : undefined,
          username: tweetData.username ?? undefined,
          account_display_name: tweetData.account_display_name ?? undefined,
          created_at: tweetData.created_at ?? undefined,
          retweet_count: tweetData.retweet_count != null ? Number(tweetData.retweet_count) : undefined,
          favorite_count: tweetData.favorite_count != null ? Number(tweetData.favorite_count) : undefined,
          reply_to_tweet_id: tweetData.reply_to_tweet_id != null ? Number(tweetData.reply_to_tweet_id) : undefined,
          reply_to_user_id: tweetData.reply_to_user_id != null ? Number(tweetData.reply_to_user_id) : undefined,
          reply_to_username: tweetData.reply_to_username ?? undefined,
          quoted_tweet_id: tweetData.quoted_tweet_id != null ? Number(tweetData.quoted_tweet_id) : undefined,
          conversation_id: tweetData.conversation_id != null ? Number(tweetData.conversation_id) : undefined,
        };

        embeddingItems.push({
          key: tweetId,
          content: processed.processedText,
          contentType: 'text' as const,
          metadata
        });
      } catch (error) {
        contextLogger.warn({
          tweetId,
          error: error instanceof Error ? error.message : 'Unknown error'
        }, 'Error preparing tweet for batch, skipping');
        this.stats.failed++;
      }
    }

    if (embeddingItems.length === 0) {
      contextLogger.info('No tweets to process after filtering');
      return;
    }

    contextLogger.info({ itemCount: embeddingItems.length }, 'Generating embeddings for batch');

    // Step 2: Generate embeddings in batch
    const results = await this.embeddingGenerationService.generateEmbeddings(embeddingItems);

    if (results.length === 0) {
      throw new Error('Failed to generate embeddings for batch');
    }

    // Step 3: Queue all embeddings for storage
    const embeddings = results.map(result => ({
      key: result.key,
      vector: new Float32Array(result.vector),
      metadata: result.metadata
    }));

    contextLogger.debug({ embeddingCount: embeddings.length }, 'Queueing embeddings for storage');

    // Queue embeddings with batch ID
    const batchId = `batch_${Date.now()}`;
    await this.embeddingWriteQueue.queueEmbeddings(embeddings, batchId);

    this.stats.processed += results.length;

    // Log statistics
    const elapsed = ((Date.now() - this.stats.startTime) / 1000).toFixed(1);
    const rate = (this.stats.processed / (Date.now() - this.stats.startTime) * 1000 * 60).toFixed(2);

    contextLogger.info({
      batchProcessed: results.length,
      totalProcessed: this.stats.processed,
      totalSkipped: this.stats.skipped,
      totalFailed: this.stats.failed,
      elapsedSeconds: elapsed,
      tweetsPerMinute: rate
    }, 'Successfully processed batch');
  }

  /**
   * Get current statistics
   */
  getStats(): ListenerStats & { uptime: number; rate: number; bufferedTweets: number } {
    const uptime = Date.now() - this.stats.startTime;
    const rate = this.stats.processed / (uptime / 1000 / 60); // tweets per minute

    return {
      ...this.stats,
      uptime,
      rate,
      bufferedTweets: this.tweetBuffer.length
    };
  }

  /**
   * Gracefully shutdown the listener
   */
  async shutdown(): Promise<void> {
    this.isShuttingDown = true;

    // Clear flush timer
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    logger.info({
      processed: this.stats.processed,
      skipped: this.stats.skipped,
      failed: this.stats.failed,
      bufferedTweets: this.tweetBuffer.length,
      uptime: ((Date.now() - this.stats.startTime) / 1000).toFixed(1)
    }, 'Shutting down Supabase listener service');

    // Process remaining buffered tweets before shutdown
    if (this.tweetBuffer.length > 0) {
      logger.info({ count: this.tweetBuffer.length }, 'Processing remaining buffered tweets before shutdown');
      try {
        await this.flushBatchWithRetry();
      } catch (error) {
        logger.error({
          error: error instanceof Error ? error.message : 'Unknown error',
          lostTweets: this.tweetBuffer.length
        }, 'Failed to process remaining tweets during shutdown');
      }
    }

    if (this.channel) {
      await this.supabase.removeChannel(this.channel);
      this.channel = null;
      logger.info('Unsubscribed from Supabase channel');
    }

    logger.info({
      finalProcessed: this.stats.processed,
      finalSkipped: this.stats.skipped,
      finalFailed: this.stats.failed
    }, 'Supabase listener shutdown complete');
  }
}

