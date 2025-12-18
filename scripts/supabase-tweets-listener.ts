import { createClient } from '@supabase/supabase-js';
import { config } from 'dotenv';
import { appendFile, mkdir } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { processTweetText } from '../src/utils/tweet-text-processor.js';

// Load environment variables
config();

// Get configuration from environment variables
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;
const embeddingServiceUrl = process.env.EMBEDDING_SERVICE_URL || 'http://localhost:3000';
const embeddingServiceApiKey = process.env.EMBED_SERVICE_CLIENT_API;

if (!supabaseUrl || !supabaseKey) {
  console.error('Error: SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment variables');
  process.exit(1);
}

if (!embeddingServiceApiKey) {
  console.error('Error: EMBED_SERVICE_CLIENT_API must be set in environment variables');
  process.exit(1);
}

// Create Supabase client
const supabase = createClient(supabaseUrl, supabaseKey);

// File to store tweet IDs
const TWEET_IDS_FILE = join(process.cwd(), 'data', 'tweet-ids-from-supabase.txt');

// Ensure data directory exists
await mkdir(dirname(TWEET_IDS_FILE), { recursive: true });

// Batching configuration
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '1000', 10);
const FLUSH_TIMEOUT_MS = parseInt(process.env.FLUSH_TIMEOUT_MS || '30000', 10);
const MAX_RETRIES = 2;
const RETRY_BACKOFF_MS = [1000, 2000];

// Buffered tweet interface
interface BufferedTweet {
  tweetId: string;
  fullText: string;
  tweetData: any;
}

// Tweet buffer and timer
let tweetBuffer: BufferedTweet[] = [];
let flushTimer: ReturnType<typeof setTimeout> | null = null;
let isShuttingDown = false;

// Statistics tracking
let stats = {
  processed: 0,
  skipped: 0,
  failed: 0,
  startTime: Date.now()
};

console.log('🚀 Starting Supabase tweets listener with batching...');
console.log(`📡 Connected to: ${supabaseUrl}`);
console.log(`🔗 Embedding service: ${embeddingServiceUrl}`);
console.log(`📝 Saving tweet IDs to: ${TWEET_IDS_FILE}`);
console.log(`📦 Batch size: ${BATCH_SIZE}, Flush timeout: ${FLUSH_TIMEOUT_MS}ms`);
console.log('👂 Listening for INSERT events on tweets table...\n');

/**
 * Interface for quoted tweet data from Supabase
 */
interface QuotedTweetData {
  tweet_id: string;
  full_text: string;
}

/**
 * Check if a tweet embedding already exists in the vector store
 */
async function checkTweetExists(tweetId: string): Promise<boolean> {
  try {
    const response = await fetch(`${embeddingServiceUrl}/embeddings/exists/${tweetId}`);
    
    if (!response.ok) {
      console.warn(`⚠️  Failed to check existence for tweet ${tweetId}: ${response.statusText}`);
      return false;
    }
    
    const data = await response.json() as { exists?: boolean };
    return data.exists === true;
  } catch (error) {
    console.error(`❌ Error checking tweet existence:`, error);
    return false;
  }
}

/**
 * Fetch quoted tweets from Supabase with JOIN to get full tweet data
 */
async function fetchQuotedTweets(tweetId: string): Promise<QuotedTweetData[]> {
  try {
    // Query quote_tweets and JOIN with tweets table to get the actual tweet content
    const { data, error } = await supabase
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
      console.error(`❌ Error fetching quoted tweets for ${tweetId}:`, error.message);
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
    console.error(`❌ Error fetching quoted tweets for ${tweetId}:`, error);
    return [];
  }
}

/**
 * Generate and store embeddings for a batch of items
 */
async function generateAndStoreEmbeddingsBatch(
  items: Array<{ key: string; content: string; metadata: Record<string, any> }>
): Promise<any> {
  const requestBody = {
    items: items.map(item => ({
      key: item.key,
      content: item.content,
      contentType: 'text',
      metadata: item.metadata
    }))
  };

  const response = await fetch(`${embeddingServiceUrl}/embeddings/generate-and-store`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${embeddingServiceApiKey}`
    },
    body: JSON.stringify(requestBody)
  });

  if (!response.ok) {
    const errorText = await response.text();
    console.error(`❌ Response body:`, errorText);
    throw new Error(`Embedding API error (${response.status}): ${errorText}`);
  }

  return await response.json();
}

/**
 * Sleep utility for retry backoff
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Buffer a tweet for batch processing
 */
function bufferTweet(tweetData: any): void {
  const tweetId = String(tweetData.tweet_id || tweetData.id);
  const fullText = tweetData.full_text || tweetData.text || '';

  if (!tweetId || !fullText) {
    console.warn('⚠️  Tweet missing ID or text, skipping');
    stats.skipped++;
    return;
  }

  console.log(`📥 Buffering tweet ${tweetId} (buffer: ${tweetBuffer.length + 1}/${BATCH_SIZE})`);
  tweetBuffer.push({ tweetId, fullText, tweetData });

  // Reset flush timer on each new tweet
  resetFlushTimer();

  // If buffer is full, flush immediately
  if (tweetBuffer.length >= BATCH_SIZE) {
    flushBatchWithRetry();
  }
}

/**
 * Reset the flush timer
 */
function resetFlushTimer(): void {
  if (flushTimer) {
    clearTimeout(flushTimer);
  }
  flushTimer = setTimeout(() => {
    if (tweetBuffer.length > 0 && !isShuttingDown) {
      console.log(`⏰ Flush timeout reached, processing ${tweetBuffer.length} buffered tweets`);
      flushBatchWithRetry();
    }
  }, FLUSH_TIMEOUT_MS);
}

/**
 * Flush batch with retry logic
 */
async function flushBatchWithRetry(): Promise<void> {
  if (tweetBuffer.length === 0) return;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      await flushBatch();
      return; // Success
    } catch (error) {
      if (attempt < MAX_RETRIES) {
        const backoffMs = RETRY_BACKOFF_MS[attempt] ?? 1000;
        console.warn(`⚠️  Batch failed (attempt ${attempt + 1}/${MAX_RETRIES + 1}), retrying in ${backoffMs}ms...`);
        await sleep(backoffMs);
      } else {
        console.error(`❌ Batch failed after ${MAX_RETRIES + 1} attempts:`, error instanceof Error ? error.message : error);
        // Mark all tweets in current batch as failed
        for (const { tweetId } of tweetBuffer) {
          await appendFile(TWEET_IDS_FILE, `${tweetId} (failed: batch error after retries)\n`, 'utf-8');
        }
        stats.failed += tweetBuffer.length;
        tweetBuffer = [];
      }
    }
  }
}

/**
 * Process all buffered tweets as a batch
 */
async function flushBatch(): Promise<void> {
  // Clear the flush timer
  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }

  // Take all tweets from buffer (swap with empty array)
  const batch = tweetBuffer;
  tweetBuffer = [];

  if (batch.length === 0) return;

  console.log('─'.repeat(80));
  console.log(`📦 Processing batch of ${batch.length} tweets`);
  console.log('─'.repeat(80));

  // Step 1: Prepare embedding items (check existence, fetch quotes, process text)
  const embeddingItems: Array<{ key: string; content: string; metadata: Record<string, any> }> = [];
  const processedTweetIds: string[] = [];

  for (const { tweetId, fullText, tweetData } of batch) {
    try {
      // Check if tweet already exists
      const exists = await checkTweetExists(tweetId);
      if (exists) {
        console.log(`⏭️  Tweet ${tweetId} already exists, skipping`);
        stats.skipped++;
        await appendFile(TWEET_IDS_FILE, `${tweetId} (skipped: already exists)\n`, 'utf-8');
        continue;
      }

      // Fetch quoted tweets
      const quotedTweets = await fetchQuotedTweets(tweetId);
      const quotedText = quotedTweets.length > 0 && quotedTweets[0] ? quotedTweets[0].full_text : null;

      // Process text
      const processed = processTweetText(fullText, quotedText, 1024);

      // Skip if processed text is empty
      if (!processed.processedText || processed.processedText.trim().length === 0) {
        console.log(`⏭️  Tweet ${tweetId} has empty text after processing, skipping`);
        stats.skipped++;
        await appendFile(TWEET_IDS_FILE, `${tweetId} (skipped: empty after processing)\n`, 'utf-8');
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
        metadata
      });
      processedTweetIds.push(tweetId);
    } catch (error) {
      console.warn(`⚠️  Error preparing tweet ${tweetId}:`, error instanceof Error ? error.message : error);
      stats.failed++;
      await appendFile(TWEET_IDS_FILE, `${tweetId} (failed: ${error instanceof Error ? error.message : 'unknown error'})\n`, 'utf-8');
    }
  }

  if (embeddingItems.length === 0) {
    console.log('📭 No tweets to process after filtering');
    return;
  }

  // Step 2: Generate and store embeddings in batch
  console.log(`🧠 Generating embeddings for ${embeddingItems.length} tweets...`);
  const result = await generateAndStoreEmbeddingsBatch(embeddingItems);
  console.log(`✅ Batch API response: ${JSON.stringify(result)}`);

  // Step 3: Save processed tweet IDs to file
  for (const tweetId of processedTweetIds) {
    await appendFile(TWEET_IDS_FILE, `${tweetId}\n`, 'utf-8');
  }

  stats.processed += embeddingItems.length;

  // Print statistics
  const elapsed = ((Date.now() - stats.startTime) / 1000).toFixed(1);
  const rate = (stats.processed / (Date.now() - stats.startTime) * 1000 * 60).toFixed(2);
  console.log(`📊 Batch complete: +${embeddingItems.length} | Total: ${stats.processed} processed, ${stats.skipped} skipped, ${stats.failed} failed | ${elapsed}s | ${rate} tweets/min\n`);
}

// Subscribe to INSERT events on the tweets table
const channel = supabase
  .channel('tweets')
  .on(
    'postgres_changes',
    {
      event: 'INSERT',
      schema: 'public',
      table: 'tweets'
    },
    (payload) => {
      if (payload.new && !isShuttingDown) {
        bufferTweet(payload.new);
      }
    }
  )
  .subscribe((status) => {
    if (status === 'SUBSCRIBED') {
      console.log('✅ Successfully subscribed to tweets table');
      console.log('💡 Waiting for new inserts...\n');
    } else if (status === 'CHANNEL_ERROR') {
      console.error('❌ Error subscribing to channel');
    } else if (status === 'TIMED_OUT') {
      console.error('⏱️  Subscription timed out');
    } else if (status === 'CLOSED') {
      console.log('🔌 Channel closed');
    }
  });

// Handle graceful shutdown
async function shutdown() {
  isShuttingDown = true;
  console.log('\n\n🛑 Shutting down...');

  // Clear flush timer
  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }

  // Process remaining buffered tweets before shutdown
  if (tweetBuffer.length > 0) {
    console.log(`📦 Processing ${tweetBuffer.length} remaining buffered tweets...`);
    try {
      await flushBatchWithRetry();
    } catch (error) {
      console.error('❌ Failed to process remaining tweets:', error instanceof Error ? error.message : error);
    }
  }

  // Print final statistics
  const elapsed = ((Date.now() - stats.startTime) / 1000).toFixed(1);
  console.log('📊 Final Statistics:');
  console.log(`   Processed: ${stats.processed}`);
  console.log(`   Skipped: ${stats.skipped}`);
  console.log(`   Failed: ${stats.failed}`);
  console.log(`   Total time: ${elapsed}s`);

  await supabase.removeChannel(channel);
  console.log('👋 Disconnected from Supabase');
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
