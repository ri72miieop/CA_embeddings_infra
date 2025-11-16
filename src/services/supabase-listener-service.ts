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
 */
export class SupabaseListenerService {
  private supabase: SupabaseClient;
  private channel: RealtimeChannel | null = null;
  private stats: ListenerStats;
  private isShuttingDown = false;
  
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
  }

  /**
   * Initialize the listener and subscribe to Supabase events
   */
  async initialize(): Promise<void> {
    logger.info({
      supabaseUrl: this.config.supabaseUrl,
      maxTextLength: this.config.maxTextLength || 1024
    }, 'Initializing Supabase listener service');

    this.channel = this.supabase
      .channel('tweets')
      .on(
        'postgres_changes',
        {
          event: 'INSERT',
          schema: 'public',
          table: 'tweets'
        },
        async (payload) => {
          if (payload.new && !this.isShuttingDown) {
            await this.processTweet(payload.new);
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
   * Process a single tweet: check existence, fetch quotes, generate embedding
   */
  private async processTweet(tweetData: any): Promise<void> {
    const tweetId = String(tweetData.tweet_id || tweetData.id);
    const fullText = tweetData.full_text || tweetData.text || '';
    
    if (!tweetId || !fullText) {
      logger.warn({ tweetData }, 'Tweet missing ID or text, skipping');
      this.stats.skipped++;
      return;
    }
    
    const contextLogger = createContextLogger({
      operation: 'supabase_tweet_processing',
      tweetId
    });
    
    contextLogger.info({
      textPreview: fullText.substring(0, 100) + (fullText.length > 100 ? '...' : '')
    }, 'New tweet inserted from Supabase');
    
    try {
      // Step 1: Check if tweet already exists in vector store
      contextLogger.debug('Checking if tweet already exists');
      const exists = await this.checkTweetExists(tweetId);
      
      if (exists) {
        contextLogger.info('Tweet already exists in vector store, skipping');
        this.stats.skipped++;
        return;
      }
      
      contextLogger.debug('Tweet is new, processing');
      
      // Step 2: Fetch quoted tweets if any
      contextLogger.debug('Fetching quoted tweets');
      const quotedTweets = await this.fetchQuotedTweets(tweetId);
      
      if (quotedTweets.length > 0) {
        contextLogger.debug({ quotedCount: quotedTweets.length }, 'Found quoted tweets');
      }
      
      // Step 3: Process text using shared utility
      const quotedText = quotedTweets.length > 0 && quotedTweets[0] ? quotedTweets[0].full_text : null;
      const maxChars = this.config.maxTextLength || 1024;
      
      contextLogger.debug({ maxChars }, 'Processing tweet text');
      const processed = processTweetText(fullText, quotedText, maxChars);
      
      contextLogger.debug({
        quotesIncluded: processed.quotesIncluded,
        truncated: processed.truncated,
        charactersUsed: processed.charactersUsed
      }, 'Tweet text processed');
      
      // Skip if processed text is empty
      if (!processed.processedText || processed.processedText.trim().length === 0) {
        contextLogger.warn('Tweet has empty text after processing, skipping');
        this.stats.skipped++;
        return;
      }
      
      // Step 4: Prepare metadata
      const metadata: Record<string, any> = {
        source: 'supabase_realtime',
        original_text: fullText,
        has_quotes: processed.quotesIncluded,
        is_truncated: processed.truncated,
        character_difference: processed.charactersUsed - fullText.length,
        text: processed.processedText
      };
      
      // Step 5: Generate embedding (direct service call, no HTTP)
      contextLogger.debug('Generating embedding');
      const results = await this.embeddingGenerationService.generateEmbeddings([{
        key: tweetId,
        content: processed.processedText,
        contentType: 'text' as const,
        metadata
      }]);
      
      if (results.length === 0) {
        throw new Error('Failed to generate embedding');
      }
      
      // Step 6: Queue for storage (direct service call, no HTTP, no rate limits)
      const embeddings = results.map(result => ({
        key: result.key,
        vector: new Float32Array(result.vector),
        metadata: result.metadata
      }));
      
      contextLogger.debug('Queueing embedding for storage');
      await this.embeddingWriteQueue.queueEmbeddings(embeddings, tweetId);
      
      this.stats.processed++;
      
      // Log statistics periodically
      const elapsed = ((Date.now() - this.stats.startTime) / 1000).toFixed(1);
      const rate = (this.stats.processed / (Date.now() - this.stats.startTime) * 1000 * 60).toFixed(2);
      
      contextLogger.info({
        processed: this.stats.processed,
        skipped: this.stats.skipped,
        failed: this.stats.failed,
        elapsedSeconds: elapsed,
        tweetsPerMinute: rate
      }, 'Successfully processed tweet');
      
    } catch (error) {
      this.stats.failed++;
      contextLogger.error({
        error: error instanceof Error ? error.message : 'Unknown error',
        errorStack: error instanceof Error ? error.stack : undefined
      }, 'Failed to process tweet');
    }
  }

  /**
   * Get current statistics
   */
  getStats(): ListenerStats & { uptime: number; rate: number } {
    const uptime = Date.now() - this.stats.startTime;
    const rate = this.stats.processed / (uptime / 1000 / 60); // tweets per minute
    
    return {
      ...this.stats,
      uptime,
      rate
    };
  }

  /**
   * Gracefully shutdown the listener
   */
  async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    
    logger.info({
      processed: this.stats.processed,
      skipped: this.stats.skipped,
      failed: this.stats.failed,
      uptime: ((Date.now() - this.stats.startTime) / 1000).toFixed(1)
    }, 'Shutting down Supabase listener service');
    
    if (this.channel) {
      await this.supabase.removeChannel(this.channel);
      this.channel = null;
      logger.info('Unsubscribed from Supabase channel');
    }
  }
}

