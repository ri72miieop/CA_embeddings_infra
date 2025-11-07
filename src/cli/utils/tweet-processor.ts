import { processTweetText } from '../../utils/tweet-text-processor.js';

interface TweetRecord {
  tweet_id: bigint;
  full_text: string;
  conversation_id: bigint;
  reply_to_tweet_id: bigint | null;
  quoted_tweet_id: bigint | null;
  created_at: string;
  username: string;
  account_display_name: string;
  [key: string]: any;
}

export interface TweetProcessorOptions {
  maxChars?: number;
  includeDate?: boolean;
  processConversationContext?: boolean;
}

interface ConversationContext {
  tweets: Map<string, TweetRecord>;
  parentMap: Map<string, string>; // child_id -> parent_id
  quotedTweets: Map<string, TweetRecord>;
}

interface ProcessedTweetText {
  originalText: string;
  processedText: string;
  contextIncluded: boolean;
  quotesIncluded: boolean;
  truncated: boolean;
  charactersUsed: number;
}

export class TweetProcessor {
  private conversations: Map<string, ConversationContext> = new Map();
  private processConversationContext: boolean;

  constructor(private options: TweetProcessorOptions = {}) {
    this.processConversationContext = options.processConversationContext ?? true;
  }

  get maxChars(): number {
    return this.options.maxChars ?? 1024;
  }

  get includeDate(): boolean {
    return this.options.includeDate ?? false;
  }

  /**
   * Build conversation maps from all tweet records
   */
  buildConversationMaps(records: TweetRecord[]): void {
    console.log(`Building conversation maps for ${records.length} tweets...`);

    // Group by conversation_id
    const conversationGroups = new Map<string, TweetRecord[]>();

    for (const record of records) {
      // Skip records with null/undefined conversation_id
      if (!record.conversation_id) {
        continue;
      }

      const convId = record.conversation_id.toString();
      if (!conversationGroups.has(convId)) {
        conversationGroups.set(convId, []);
      }
      conversationGroups.get(convId)!.push(record);
    }

    // Build conversation contexts
    for (const [convId, tweets] of conversationGroups) {
      const context: ConversationContext = {
        tweets: new Map(),
        parentMap: new Map(),
        quotedTweets: new Map()
      };

      // Add tweets to maps
      for (const tweet of tweets) {
        // Skip tweets with null/undefined tweet_id
        if (!tweet.tweet_id) {
          continue;
        }

        const tweetId = tweet.tweet_id.toString();
        context.tweets.set(tweetId, tweet);

        // Build parent relationships
        if (tweet.reply_to_tweet_id) {
          const parentId = tweet.reply_to_tweet_id.toString();
          context.parentMap.set(tweetId, parentId);
        }

        // Collect quoted tweets
        if (tweet.quoted_tweet_id) {
          const quotedId = tweet.quoted_tweet_id.toString();
          // Find the quoted tweet in our dataset
          const quotedTweet = tweets.find(t => t.tweet_id && t.tweet_id.toString() === quotedId);
          if (quotedTweet) {
            context.quotedTweets.set(quotedId, quotedTweet);
          }
        }
      }

      this.conversations.set(convId, context);
    }

    console.log(`Built ${this.conversations.size} conversation contexts`);
  }

  /**
   * Process a single tweet to create rich embedding text
   */
  processTweet(tweet: TweetRecord): ProcessedTweetText {
    // Handle null/undefined tweet_id
    if (!tweet.tweet_id) {
      const cleaned = this.cleanTweetText(tweet.full_text || '');
      return {
        originalText: tweet.full_text || '',
        processedText: cleaned,
        contextIncluded: false,
        quotesIncluded: false,
        truncated: false,
        charactersUsed: cleaned.length
      };
    }

    const tweetId = tweet.tweet_id.toString();

    // If conversation context processing is disabled, return simple cleaned text
    if (!this.processConversationContext) {
      const cleaned = this.cleanTweetText(tweet.full_text);
      return {
        originalText: tweet.full_text,
        processedText: cleaned,
        contextIncluded: false,
        quotesIncluded: false,
        truncated: false,
        charactersUsed: cleaned.length
      };
    }

    // Handle null/undefined conversation_id
    if (!tweet.conversation_id) {
      const cleaned = this.cleanTweetText(tweet.full_text);
      return {
        originalText: tweet.full_text,
        processedText: cleaned,
        contextIncluded: false,
        quotesIncluded: false,
        truncated: false,
        charactersUsed: cleaned.length
      };
    }

    const convId = tweet.conversation_id.toString();
    const conversation = this.conversations.get(convId);

    if (!conversation) {
      // Fallback to simple cleaned text
      const cleaned = this.cleanTweetText(tweet.full_text);
      return {
        originalText: tweet.full_text,
        processedText: cleaned,
        contextIncluded: false,
        quotesIncluded: false,
        truncated: false,
        charactersUsed: cleaned.length
      };
    }

    const parts: string[] = [];

    // Helper function to format tweet parts
    const formatTweet = (type: string, tweetData: TweetRecord): string => {
      let text = this.cleanTweetText(tweetData.full_text);

      // Add quoted content if available
      if (tweetData.quoted_tweet_id) {
        const quotedId = tweetData.quoted_tweet_id.toString();
        const quotedTweet = conversation.quotedTweets.get(quotedId);
        if (quotedTweet) {
          const quotedText = this.cleanTweetText(quotedTweet.full_text);
          text = `${text}\n[quoted] ${quotedText}`;
        }
      }

      if (this.includeDate) {
        const date = this.formatDate(tweetData.created_at);
        return `[${type} ${date}] ${text}`;
      }

      return `[${type}] ${text}`;
    };

    // Collect conversation thread in chronological order
    const thread = this.assembleThread(tweetId, conversation);

    // Format each part
    for (const { type, tweet: threadTweet } of thread) {
      parts.push(formatTweet(type, threadTweet));
    }

    // Smart truncation with priority
    const selected = this.smartTruncate(parts, this.maxChars);
    const finalText = selected.join('\n');

    const hasQuotes = conversation.quotedTweets.size > 0;
    const hasContext = parts.length > 1;

    return {
      originalText: tweet.full_text,
      processedText: finalText,
      contextIncluded: hasContext,
      quotesIncluded: hasQuotes,
      truncated: selected.length < parts.length,
      charactersUsed: finalText.length
    };
  }

  /**
   * Assemble conversation thread in chronological order
   */
  private assembleThread(tweetId: string, conversation: ConversationContext): Array<{type: string, tweet: TweetRecord}> {
    const thread: Array<{type: string, tweet: TweetRecord}> = [];
    const tweet = conversation.tweets.get(tweetId);

    if (!tweet) return thread;

    // Find root of conversation
    let rootId = tweetId;
    let current = tweetId;
    while (conversation.parentMap.has(current)) {
      const parent = conversation.parentMap.get(current)!;
      if (conversation.tweets.has(parent)) {
        rootId = parent;
        current = parent;
      } else {
        break;
      }
    }

    // Add root if different from current tweet
    if (rootId !== tweetId) {
      const rootTweet = conversation.tweets.get(rootId);
      if (rootTweet) {
        thread.push({ type: 'root', tweet: rootTweet });
      }
    }

    // Collect path from root to current tweet
    const pathToTweet = this.getPathToTweet(rootId, tweetId, conversation);

    // Add context tweets (excluding root and current)
    for (let i = 1; i < pathToTweet.length - 1; i++) {
      const pathId = pathToTweet[i];
      if (pathId) {
        const contextTweet = conversation.tweets.get(pathId);
        if (contextTweet) {
          thread.push({ type: 'context', tweet: contextTweet });
        }
      }
    }

    // Add current tweet
    thread.push({ type: 'current', tweet });

    return thread;
  }

  /**
   * Find path from root to target tweet
   */
  private getPathToTweet(rootId: string, targetId: string, conversation: ConversationContext): string[] {
    if (rootId === targetId) return [rootId];

    const visited = new Set<string>();
    const queue: Array<{id: string, path: string[]}> = [{id: rootId, path: [rootId]}];

    while (queue.length > 0) {
      const item = queue.shift();
      if (!item) continue;
      const {id, path} = item;

      if (id === targetId) {
        return path;
      }

      if (visited.has(id)) continue;
      visited.add(id);

      // Find children of current tweet
      for (const [childId, parentId] of conversation.parentMap) {
        if (parentId === id && !visited.has(childId)) {
          queue.push({
            id: childId,
            path: [...path, childId]
          });
        }
      }
    }

    return [targetId]; // Fallback if path not found
  }

  /**
   * Smart truncation prioritizing current tweet and immediate context
   */
  private smartTruncate(parts: string[], maxChars: number): string[] {
    if (parts.length === 0) return parts;

    // Priority order: current tweet, immediate context, then root
    const checkOrder: number[] = [];

    if (parts.length === 1) {
      checkOrder.push(0);
    } else if (parts.length === 2) {
      checkOrder.push(1, 0); // current, then root
    } else if (parts.length === 3) {
      checkOrder.push(2, 1, 0); // current, context, root
    } else {
      // current, immediate context, root, then remaining context
      checkOrder.push(parts.length - 1); // current
      checkOrder.push(parts.length - 2); // immediate context
      checkOrder.push(0); // root

      // Add remaining context in reverse order
      for (let i = parts.length - 3; i > 0; i--) {
        checkOrder.push(i);
      }
    }

    const selected = new Set<number>();
    let totalChars = 0;

    // Select parts without exceeding max_chars
    for (const index of checkOrder) {
      const part = parts[index];
      if (!part) continue;
      const partLength = part.length + 1; // +1 for newline
      if (totalChars + partLength <= maxChars) {
        selected.add(index);
        totalChars += partLength;
      }
    }

    // Assemble selected parts in original order
    const result: string[] = [];
    let hasGap = false;

    for (let i = 0; i < parts.length; i++) {
      if (selected.has(i)) {
        const part = parts[i];
        if (part) {
          result.push(part);
          hasGap = false;
        }
      } else if (!hasGap) {
        result.push('<...>');
        hasGap = true;
      }
    }

    return result;
  }

  /**
   * Clean tweet text for embedding
   */
  private cleanTweetText(text: string): string {
    if (!text) return '';

    return text
      // Remove URLs
      .replace(/https?:\/\/\S+/g, '')
      // Remove extra whitespace
      .replace(/\s+/g, ' ')
      // Remove leading/trailing whitespace
      .trim()
      // Remove all mentions at start
      .replace(/^(@\w+\s*)+/, '')
      // Normalize unicode quotes
      .replace(/[""]/g, '"')
      .replace(/['']/g, "'");
  }

  /**
   * Format date for display
   */
  private formatDate(dateStr: string): string {
    try {
      const date = new Date(dateStr);
      return date.toLocaleDateString('en-US', {
        day: '2-digit',
        month: 'short',
        year: 'numeric'
      });
    } catch {
      return dateStr.slice(0, 10); // Fallback to first 10 chars
    }
  }

  /**
   * Get processing statistics
   */
  getStats(): {
    conversationsBuilt: number;
    totalTweets: number;
    avgTweetsPerConversation: number;
  } {
    const totalTweets = Array.from(this.conversations.values())
      .reduce((sum, conv) => sum + conv.tweets.size, 0);

    return {
      conversationsBuilt: this.conversations.size,
      totalTweets,
      avgTweetsPerConversation: this.conversations.size > 0
        ? totalTweets / this.conversations.size
        : 0
    };
  }

  /**
   * Get conversation maps for caching
   */
  getConversationMaps(): any {
    // Convert Map to plain object for JSON serialization
    const conversationsObj: Record<string, any> = {};
    for (const [key, value] of this.conversations.entries()) {
      conversationsObj[key] = {
        tweets: Array.from(value.tweets.entries()).map(([id, tweet]) => [
          String(id), // Convert BigInt keys to strings
          this.serializeRecord(tweet) // Convert BigInt values to strings
        ]),
        parentMap: Array.from(value.parentMap.entries()).map(([childId, parentId]) => [
          String(childId),
          String(parentId)
        ]),
        quotedTweets: Array.from(value.quotedTweets.entries()).map(([id, tweet]) => [
          String(id),
          this.serializeRecord(tweet)
        ])
      };
    }
    return conversationsObj;
  }

  /**
   * Helper to convert BigInt values to strings for JSON serialization
   */
  private serializeRecord(record: any): any {
    if (record === null || record === undefined) return record;
    if (typeof record === 'bigint') return record.toString();
    if (Array.isArray(record)) return record.map(item => this.serializeRecord(item));
    if (typeof record === 'object') {
      const serialized: any = {};
      for (const [key, value] of Object.entries(record)) {
        serialized[key] = this.serializeRecord(value);
      }
      return serialized;
    }
    return record;
  }

  /**
   * Load conversation maps from cache
   */
  loadConversationMaps(conversationMaps: any, replyMaps?: any, quoteMaps?: any): void {
    this.conversations.clear();

    for (const [convId, conv] of Object.entries(conversationMaps)) {
      const convData = conv as any;
      this.conversations.set(convId, {
        tweets: new Map(convData.tweets || []),
        parentMap: new Map(convData.parentMap || []),
        quotedTweets: new Map(convData.quotedTweets || [])
      });
    }
  }

  /**
   * Get reply maps (for backward compatibility)
   */
  getReplyMaps(): any {
    return {}; // Not used in current implementation
  }

  /**
   * Get quote maps (for backward compatibility)
   */
  getQuoteMaps(): any {
    return {}; // Not used in current implementation
  }

  /**
   * Process tweet with simplified logic (no conversation threading)
   * Combines tweet's full_text with quoted tweet's full_text if available
   * Both texts are cleaned using cleanTweetText()
   *
   * Smart truncation strategy:
   * - If combined text fits within maxChars, use it all
   * - If too long, prioritize main tweet but give quoted tweet some space
   * - Allocate 70% to main tweet, 30% to quoted tweet (proportional)
   * - Ensure main tweet always gets at least 512 chars (or maxChars if less)
   */
  processTweetSimple(tweet: TweetRecord, quotedTweet?: TweetRecord | null): ProcessedTweetText {
    // Use the shared utility function for text processing
    const result = processTweetText(
      tweet.full_text || '',
      quotedTweet?.full_text || null,
      this.maxChars
    );

    return {
      originalText: result.originalText,
      processedText: result.processedText,
      contextIncluded: false,
      quotesIncluded: result.quotesIncluded,
      truncated: result.truncated,
      charactersUsed: result.charactersUsed
    };
  }
}