import fs from 'fs/promises';
import path from 'path';

/**
 * Logger for tracking tweets that have quoted_tweet_id but the quoted tweet data is missing
 * These tweets are skipped during processing
 */
export class MissingQuotedTweetsLogger {
  private logPath: string;
  private missingTweets: Set<string> = new Set();
  private batchBuffer: string[] = [];
  private readonly batchSize = 100; // Flush every 100 entries

  constructor(outputDir: string) {
    this.logPath = path.join(outputDir, 'missing-quoted-tweets.log');
  }

  async initialize(): Promise<void> {
    // Create directory if it doesn't exist
    await fs.mkdir(path.dirname(this.logPath), { recursive: true });

    // Clear the file or create new
    await fs.writeFile(this.logPath, '', 'utf-8');
  }

  /**
   * Log a tweet with missing quoted tweet data
   * @param tweetId The ID of the tweet that has a quoted_tweet_id
   * @param quotedTweetId The ID of the quoted tweet that is missing
   */
  logMissingQuotedTweet(tweetId: string, quotedTweetId: string): void {
    const entry = `tweet_id=${tweetId}, quoted_tweet_id=${quotedTweetId}`;

    if (!this.missingTweets.has(entry)) {
      this.missingTweets.add(entry);
      this.batchBuffer.push(entry);

      // Flush if batch is full
      if (this.batchBuffer.length >= this.batchSize) {
        this.flushBatch();
      }
    }
  }

  /**
   * Flush the batch buffer to disk asynchronously
   */
  private async flushBatch(): Promise<void> {
    if (this.batchBuffer.length === 0) return;

    const entries = this.batchBuffer.join('\n') + '\n';
    this.batchBuffer = [];

    try {
      await fs.appendFile(this.logPath, entries, 'utf-8');
    } catch (error) {
      console.error('Error writing to missing quoted tweets log:', error);
    }
  }

  /**
   * Flush any remaining entries to disk
   */
  async flush(): Promise<void> {
    await this.flushBatch();
  }

  /**
   * Get the count of missing quoted tweets
   */
  getCount(): number {
    return this.missingTweets.size;
  }

  /**
   * Get the path to the log file
   */
  getLogPath(): string {
    return this.logPath;
  }

  /**
   * Check if any tweets with missing quoted tweets were logged
   */
  hasMessages(): boolean {
    return this.missingTweets.size > 0;
  }
}
