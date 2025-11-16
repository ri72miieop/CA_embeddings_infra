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

// Statistics tracking
let stats = {
  processed: 0,
  skipped: 0,
  failed: 0,
  startTime: Date.now()
};

console.log('🚀 Starting Supabase tweets listener with embedding generation...');
console.log(`📡 Connected to: ${supabaseUrl}`);
console.log(`🔗 Embedding service: ${embeddingServiceUrl}`);
console.log(`📝 Saving tweet IDs to: ${TWEET_IDS_FILE}`);
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
 * Generate and store embedding for a tweet
 */
async function generateAndStoreEmbedding(
  key: string,
  content: string,
  metadata: Record<string, any>
): Promise<void> {
  const requestBody = {
    items: [{
      key,
      content,
      contentType: 'text',
      metadata: metadata
    }]
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
    console.error(`❌ Response headers:`, JSON.stringify(Object.fromEntries(response.headers.entries()), null, 2));
    throw new Error(`Embedding API error (${response.status}): ${errorText}`);
  }
  
  const result = await response.json();
  console.log(`✅ API response:`, JSON.stringify(result, null, 2));
}

/**
 * Process a single tweet: check existence, fetch quotes, generate embedding
 */
async function processTweet(tweetData: any): Promise<void> {
  const tweetId = String(tweetData.tweet_id || tweetData.id);
  const fullText = tweetData.full_text || tweetData.text || '';
  
  if (!tweetId || !fullText) {
    console.warn('⚠️  Tweet missing ID or text, skipping\n');
    stats.skipped++;
    return;
  }
  
  console.log('✨ New tweet inserted:');
  console.log('─'.repeat(80));
  console.log(`Tweet ID: ${tweetId}`);
  console.log(`Text: ${fullText.substring(0, 100)}${fullText.length > 100 ? '...' : ''}`);
  console.log('─'.repeat(80));
  
  try {
    // Step 1: Check if tweet already exists in vector store
    console.log(`🔍 Checking if tweet ${tweetId} already exists...`);
    const exists = await checkTweetExists(tweetId);
    
    if (exists) {
      console.log(`⏭️  Tweet ${tweetId} already exists in vector store, skipping\n`);
      stats.skipped++;
      await appendFile(TWEET_IDS_FILE, `${tweetId} (skipped: already exists)\n`, 'utf-8');
      return;
    }
    
    console.log(`✅ Tweet ${tweetId} is new, processing...`);
    
    // Step 2: Fetch quoted tweets if any
    console.log(`📥 Fetching quoted tweets for ${tweetId}...`);
    const quotedTweets = await fetchQuotedTweets(tweetId);
    
    if (quotedTweets.length > 0) {
      console.log(`📑 Found ${quotedTweets.length} quoted tweet(s)`);
    } else {
      console.log(`📑 No quoted tweets found`);
    }
    
    // Step 3: Process text using shared utility
    // Note: Currently only handles first quoted tweet, could be extended for multiple
    const quotedText = quotedTweets.length > 0 && quotedTweets[0] ? quotedTweets[0].full_text : null;
    
    console.log(`🔧 Processing tweet text...`);
    const processed = processTweetText(fullText, quotedText, 1024);
    
    console.log(`📝 Processed text: ${processed.processedText.substring(0, 100)}${processed.processedText.length > 100 ? '...' : ''}`);
    console.log(`   Quotes included: ${processed.quotesIncluded}`);
    console.log(`   Truncated: ${processed.truncated}`);
    console.log(`   Character count: ${processed.charactersUsed}`);
    
    // Skip if processed text is empty (after cleaning)
    if (!processed.processedText || processed.processedText.trim().length === 0) {
      console.warn(`⚠️  Tweet ${tweetId} has empty text after processing, skipping\n`);
      stats.skipped++;
      await appendFile(TWEET_IDS_FILE, `${tweetId} (skipped: empty after processing)\n`, 'utf-8');
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
    
    //if (tweetData?.username) {
    //  metadata.username = tweetData.username;
    //}
    //if (tweetData?.account_display_name) {
    //  metadata.account_display_name = tweetData.account_display_name;
    //}
    
    // Step 5: Generate and store embedding
    console.log(`🧠 Generating embedding for tweet ${tweetId}...`);
    await generateAndStoreEmbedding(tweetId, processed.processedText, metadata);
    
    console.log(`✅ Successfully generated and stored embedding for tweet ${tweetId}`);
    
    // Step 6: Save to file
    await appendFile(TWEET_IDS_FILE, `${tweetId}\n`, 'utf-8');
    console.log(`📝 Saved tweet ID to file`);
    
    stats.processed++;
    
    // Print statistics
    const elapsed = ((Date.now() - stats.startTime) / 1000).toFixed(1);
    const rate = (stats.processed / (Date.now() - stats.startTime) * 1000 * 60).toFixed(2);
    console.log(`📊 Stats: ${stats.processed} processed, ${stats.skipped} skipped, ${stats.failed} failed | ${elapsed}s | ${rate} tweets/min\n`);
    
  } catch (error) {
    console.error(`❌ Error processing tweet ${tweetId}:`, error);
    stats.failed++;
    await appendFile(TWEET_IDS_FILE, `${tweetId} (failed: ${error instanceof Error ? error.message : 'unknown error'})\n`, 'utf-8');
    console.log('');
  }
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
    async (payload) => {
      if (payload.new) {
        await processTweet(payload.new);
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
  console.log('\n\n🛑 Shutting down...');
  
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
