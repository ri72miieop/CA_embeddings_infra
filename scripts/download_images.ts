#!/usr/bin/env bun

/**
 * Download Images CLI (High Performance & Memory Fixed)
 * 
 * Features:
 * - Solved 7GB RAM leak by replacing Promise.race with signal-based waiting
 * - O(1) deduplication using memory caching
 * - Streaming CSV parsing
 * - Streaming disk writes (zero-copy where possible)
 * - Dynamic concurrency up to 2000+
 * - Filesystem sharding
 * 
 * Usage:
 *   bun run download_images.ts --csv-folder <path> [--output-folder <path>] [--concurrency <n>]
 */

import { Command } from 'commander';
import { Glob } from 'bun';
import path from 'path';
import chalk from 'chalk';
import { mkdir, appendFile } from 'fs/promises';
import { createReadStream, existsSync } from 'fs';
import { createInterface } from 'readline';

// ============================================================================
// Constants
// ============================================================================

const CONSTANTS = {
  // Retry configuration
  INITIAL_RETRY_DELAY_MS: 30000,
  MAX_RETRIES: 0,
  
  // Concurrency configuration
  MIN_CONCURRENCY: 1,
  MAX_CONCURRENCY: 1000,
  CONSECUTIVE_SUCCESSES_TO_INCREASE: 100,
  CONCURRENCY_INCREMENT_FACTOR: 1.25, 
  
  // Buffer configuration
  FAILED_BUFFER_FLUSH_INTERVAL_MS: 30000,
  FAILED_BUFFER_MAX_SIZE: 100,
  
  // Network configuration
  FETCH_TIMEOUT_MS: 30000, // Reduced from 60s - detect hung connections faster
  RETRY_CHECK_INTERVAL_MS: 1000,
  
  // Stall detection
  STALL_CHECK_INTERVAL_MS: 10000, // Check every 10s if progress is stalling
  MIN_COMPLETIONS_PER_INTERVAL: 5, // Reduce concurrency if fewer than this complete
  
  // Progress update interval
  PROGRESS_UPDATE_INTERVAL_MS: 5000,
  
  // Rate limit status codes
  RATE_LIMIT_STATUS_CODES: new Set([429, 420, 503]),
  
  // Valid protocols for URLs
  VALID_PROTOCOLS: new Set(['http:', 'https:']),
  
  // Sharding configuration
  SHARD_LENGTH: 3, // '12345' -> folder '123'
} as const;

// Precompiled regex for file extension validation
const FILE_EXTENSION_REGEX = /^\.[a-zA-Z0-9]+$/;

// Content-Type to extension mapping
const CONTENT_TYPE_TO_EXT: Record<string, string> = {
  'image/jpeg': '.jpg',
  'image/jpg': '.jpg',
  'image/png': '.png',
  'image/gif': '.gif',
  'image/webp': '.webp',
  'image/svg+xml': '.svg',
  'image/bmp': '.bmp',
  'image/tiff': '.tiff',
  'image/x-icon': '.ico',
  'image/avif': '.avif',
  'image/heic': '.heic',
  'image/heif': '.heif',
};

// ============================================================================
// Types & Interfaces
// ============================================================================

interface CSVRow {
  media_id: string;
  tweet_id: string;
  media_url: string;
  media_type: string;
  width: string;
  height: string;
  archive_upload_id: string;
  updated_at: string;
}

interface DownloadItem {
  tweetId: string;
  mediaId: string;
  mediaUrl: string;
  uniqueKey: string; 
  retryCount: number;
  retryAt?: number;
  abortController?: AbortController;
}

interface FailedItem {
  tweet_id: string;
  media_id: string;
}

interface Stats {
  total: number;
  skipped: number;
  downloaded: number;
  failed: number;
  retrying: number;
  inProgress: number;
  statusCodes: Map<number, number>;
  timeouts: number; // Track timeouts separately
}

// ============================================================================
// Logger
// ============================================================================

const logger = {
  error: (message: string) => console.error(chalk.red(`✗ ${message}`)),
  warn: (message: string) => console.log(chalk.yellow(`⚠ ${message}`)),
  info: (message: string) => console.log(chalk.cyan(`ℹ ${message}`)),
  success: (message: string) => console.log(chalk.green(`✓ ${message}`)),
  debug: (message: string) => console.log(chalk.gray(`  ${message}`)),
};

// ============================================================================
// Memory-Based File Library
// ============================================================================

async function loadExistingLibrary(outputFolder: string): Promise<Set<string>> {
  console.log(chalk.yellow(`\n🔍 Scanning output directory to build memory cache...`));
  const startTime = Date.now();
  const existingIds = new Set<string>();
  
  const glob = new Glob('**/*');
  
  try {
    for await (const file of glob.scan(outputFolder)) {
      const basename = path.basename(file);
      const lastDotIndex = basename.lastIndexOf('.');
      if (lastDotIndex > 0) {
        const id = basename.substring(0, lastDotIndex);
        existingIds.add(id);
      }
    }
  } catch (error) {
    logger.debug(`Output folder scan: ${error}`);
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(chalk.green(`✓ Indexing complete in ${duration}s. Found ${existingIds.size} existing items.\n`));
  
  // Manual GC hint after large allocation
  if (global.gc) global.gc();

  return existingIds;
}

async function loadFailedLibrary(outputFolder: string): Promise<Set<string>> {
  const failedCsvPath = path.join(outputFolder, 'failed_download_image.csv');
  const failedIds = new Set<string>();
  
  if (!existsSync(failedCsvPath)) {
    return failedIds;
  }
  
  console.log(chalk.yellow(`🔍 Loading previously failed items...`));
  const startTime = Date.now();
  
  const fileStream = createReadStream(failedCsvPath, { encoding: 'utf-8' });
  const rl = createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });
  
  try {
    let isHeader = true;
    for await (const line of rl) {
      if (isHeader) {
        isHeader = false;
        continue;
      }
      const trimmed = line.trim();
      if (!trimmed) continue;
      const parts = trimmed.split(',');
      if (parts.length >= 2) {
        const tweetId = parts[0]?.trim();
        const mediaId = parts[1]?.trim();
        if (tweetId && mediaId) {
          failedIds.add(`${tweetId}_${mediaId}`);
        }
      }
    }
  } catch (error) {
    logger.warn(`Could not load failed items: ${error}`);
  } finally {
    rl.close();
    fileStream.destroy();
  }
  
  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(chalk.green(`✓ Loaded ${failedIds.size} previously failed items in ${duration}s.\n`));
  
  return failedIds;
}

// ============================================================================
// Sharding - Directory Management
// ============================================================================

const directoryCache = new Set<string>();

async function ensureShardDirectory(root: string, tweetId: string): Promise<string> {
  const prefix = tweetId.length >= CONSTANTS.SHARD_LENGTH 
    ? tweetId.slice(0, CONSTANTS.SHARD_LENGTH) 
    : 'misc';
    
  const dirPath = path.join(root, prefix);
  
  if (!directoryCache.has(dirPath)) {
    await mkdir(dirPath, { recursive: true });
    directoryCache.add(dirPath);
  }
  
  return dirPath;
}

// ============================================================================
// URL Validation & Extension Extraction
// ============================================================================

function isValidUrl(urlString: string): boolean {
  try {
    const url = new URL(urlString);
    return CONSTANTS.VALID_PROTOCOLS.has(url.protocol);
  } catch {
    return false;
  }
}

function getExtensionFromContentType(contentType: string | null): string | null {
  if (!contentType) return null;
  const mainType = contentType.split(';')[0]?.trim().toLowerCase();
  if (!mainType) return null;
  return CONTENT_TYPE_TO_EXT[mainType] ?? null;
}

function getExtensionFromUrl(url: string): string {
  try {
    const urlObj = new URL(url);
    const pathname = urlObj.pathname;
    const ext = path.extname(pathname.split('?')[0] || '');
    if (ext && FILE_EXTENSION_REGEX.test(ext)) {
      return ext.toLowerCase();
    }
    return '.jpg'; 
  } catch {
    return '.jpg';
  }
}

// ============================================================================
// Streaming CSV Parser
// ============================================================================

interface CSVParserState {
  headers: string[] | null;
  currentField: string;
  currentRow: string[];
  inQuotes: boolean;
}

class StreamingCSVParser {
  private state: CSVParserState = {
    headers: null,
    currentField: '',
    currentRow: [],
    inQuotes: false,
  };
  private readonly requiredHeaders = ['media_id', 'tweet_id', 'media_url'];
  private headerIndices: Map<string, number> = new Map();
  private lineBuffer = '';

  async *parse(filePath: string): AsyncGenerator<CSVRow> {
    const fileStream = createReadStream(filePath, { encoding: 'utf-8' });
    const rl = createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    });

    try {
      for await (const line of rl) {
        this.lineBuffer += (this.lineBuffer ? '\n' : '') + line;
        
        if (this.countQuotes(this.lineBuffer) % 2 !== 0) {
          continue;
        }

        const rows = this.processLine(this.lineBuffer);
        this.lineBuffer = '';

        for (const row of rows) {
          if (row) yield row;
        }
      }

      if (this.lineBuffer) {
        const rows = this.processLine(this.lineBuffer);
        for (const row of rows) {
          if (row) yield row;
        }
      }
    } finally {
      rl.close();
      fileStream.destroy();
    }
  }

  private countQuotes(str: string): number {
    let count = 0;
    for (let i = 0; i < str.length; i++) {
      if (str[i] === '"') count++;
    }
    return count;
  }

  private processLine(line: string): (CSVRow | null)[] {
    const values = this.parseCSVLine(line);
    
    if (!this.state.headers) {
      this.state.headers = values;
      values.forEach((header, index) => {
        this.headerIndices.set(header.trim(), index);
      });

      const missingHeaders = this.requiredHeaders.filter(h => !this.headerIndices.has(h));
      if (missingHeaders.length > 0) {
        throw new Error(`CSV missing required headers: ${missingHeaders.join(', ')}`);
      }
      return [null];
    }

    const row: Partial<CSVRow> = {};
    for (const [header, index] of this.headerIndices) {
      (row as any)[header] = values[index]?.trim() ?? '';
    }

    if (!row.media_id || !row.tweet_id || !row.media_url) {
      return [null];
    }

    return [row as CSVRow];
  }

  private parseCSVLine(line: string): string[] {
    const values: string[] = [];
    let current = '';
    let inQuotes = false;
    let i = 0;

    while (i < line.length) {
      const char = line[i];
      const nextChar = line[i + 1];

      if (inQuotes) {
        if (char === '"') {
          if (nextChar === '"') {
            current += '"';
            i += 2;
            continue;
          } else {
            inQuotes = false;
            i++;
            continue;
          }
        } else {
          current += char;
        }
      } else {
        if (char === '"') {
          inQuotes = true;
        } else if (char === ',') {
          values.push(current);
          current = '';
        } else if (char !== '\r') {
          current += char;
        }
      }
      i++;
    }
    values.push(current);
    return values;
  }

  reset(): void {
    this.state = {
      headers: null,
      currentField: '',
      currentRow: [],
      inQuotes: false,
    };
    this.headerIndices.clear();
    this.lineBuffer = '';
  }
}

// ============================================================================
// Priority Queue for Retry Management
// ============================================================================

class MinHeap<T> {
  private heap: T[] = [];
  private readonly compareFn: (a: T, b: T) => number;

  constructor(compareFn: (a: T, b: T) => number) {
    this.compareFn = compareFn;
  }

  push(item: T): void {
    this.heap.push(item);
    this.bubbleUp(this.heap.length - 1);
  }

  pop(): T | undefined {
    if (this.heap.length === 0) return undefined;
    if (this.heap.length === 1) return this.heap.pop();

    const root = this.heap[0];
    const last = this.heap.pop();
    if (last !== undefined && this.heap.length > 0) {
      this.heap[0] = last;
      this.bubbleDown(0);
    }
    return root;
  }

  peek(): T | undefined {
    return this.heap[0];
  }

  get size(): number {
    return this.heap.length;
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2);
      const current = this.heap[index];
      const parent = this.heap[parentIndex];
      if (current === undefined || parent === undefined) break;
      if (this.compareFn(current, parent) >= 0) break;
      this.heap[index] = parent;
      this.heap[parentIndex] = current;
      index = parentIndex;
    }
  }

  private bubbleDown(index: number): void {
    const length = this.heap.length;
    while (true) {
      let smallest = index;
      const leftChild = 2 * index + 1;
      const rightChild = 2 * index + 2;

      const current = this.heap[smallest];
      const left = this.heap[leftChild];
      const right = this.heap[rightChild];

      if (leftChild < length && left !== undefined && current !== undefined && this.compareFn(left, current) < 0) {
        smallest = leftChild;
      }

      const smallestItem = this.heap[smallest];
      if (rightChild < length && right !== undefined && smallestItem !== undefined && this.compareFn(right, smallestItem) < 0) {
        smallest = rightChild;
      }

      if (smallest === index) break;

      const temp = this.heap[index];
      const smallestVal = this.heap[smallest];
      if (temp !== undefined && smallestVal !== undefined) {
        this.heap[index] = smallestVal;
        this.heap[smallest] = temp;
      }
      index = smallest;
    }
  }
}

// ============================================================================
// Semaphore for Concurrency Control
// ============================================================================

class Semaphore {
  private current = 0;
  private max: number;

  constructor(max: number) {
    this.max = Math.max(CONSTANTS.MIN_CONCURRENCY, max);
  }

  // Simplified semaphore - we check explicitly in the main loop
  canAcquire(): boolean {
    return this.current < this.max;
  }

  acquireSync(): void {
    this.current++;
  }

  release(): void {
    this.current--;
  }

  setMax(newMax: number): void {
    const clampedMax = Math.max(CONSTANTS.MIN_CONCURRENCY, Math.min(CONSTANTS.MAX_CONCURRENCY, newMax));
    this.max = clampedMax;
  }

  getMax(): number {
    return this.max;
  }

  getActive(): number {
    return this.current;
  }
}

// ============================================================================
// Download Manager
// ============================================================================

class DownloadManager {
  private readonly retryQueue: MinHeap<DownloadItem>;
  private failedBuffer: FailedItem[] = [];
  private readonly semaphore: Semaphore;
  private readonly stats: Stats = {
    total: 0,
    skipped: 0,
    downloaded: 0,
    failed: 0,
    retrying: 0,
    inProgress: 0,
    statusCodes: new Map<number, number>(),
    timeouts: 0,
  };
  private readonly outputFolder: string;
  private readonly existingIds: Set<string>; 
  private readonly failedIds: Set<string>; 
  private consecutiveSuccesses = 0;
  private flushInterval: Timer | null = null;
  private progressInterval: Timer | null = null;
  private shuttingDown = false;
  private readonly failedCsvPath: string;
  private failedCsvInitialized = false;
  private readonly activeAbortControllers = new Set<AbortController>();
  
  // Signal handlers
  private readonly sigintHandler: () => void;
  private readonly sigtermHandler: () => void;

  private itemGenerator: AsyncGenerator<DownloadItem> | null = null;
  private generatorExhausted = false;
  
  // Stall detection
  private lastCompletedCount = 0;
  private stallCheckInterval: Timer | null = null;

  constructor(
    outputFolder: string, 
    initialConcurrency: number,
    existingIds: Set<string>,
    failedIds: Set<string>
  ) {
    this.outputFolder = outputFolder;
    this.existingIds = existingIds;
    this.failedIds = failedIds;
    this.semaphore = new Semaphore(initialConcurrency);
    this.retryQueue = new MinHeap<DownloadItem>(
      (a, b) => (a.retryAt ?? 0) - (b.retryAt ?? 0)
    );
    this.failedCsvPath = path.join(outputFolder, 'failed_download_image.csv');
    this.failedCsvInitialized = existsSync(this.failedCsvPath);

    this.flushInterval = setInterval(() => {
      this.flushFailedBuffer().catch(err => logger.error(`Flush error: ${err}`));
    }, CONSTANTS.FAILED_BUFFER_FLUSH_INTERVAL_MS);

    this.progressInterval = setInterval(() => {
      this.printProgress();
    }, CONSTANTS.PROGRESS_UPDATE_INTERVAL_MS);

    // Stall detection - reduce concurrency if progress is too slow
    this.stallCheckInterval = setInterval(() => {
      this.checkForStall();
    }, CONSTANTS.STALL_CHECK_INTERVAL_MS);

    this.sigintHandler = () => this.handleShutdown();
    this.sigtermHandler = () => this.handleShutdown();
    process.on('SIGINT', this.sigintHandler);
    process.on('SIGTERM', this.sigtermHandler);
  }

  setItemGenerator(generator: AsyncGenerator<DownloadItem>): void {
    this.itemGenerator = generator;
    this.generatorExhausted = false;
  }

  private async handleShutdown(): Promise<void> {
    if (this.shuttingDown) return;
    this.shuttingDown = true;

    console.log(chalk.yellow('\n\n🛑 Shutting down gracefully...'));
    
    for (const controller of this.activeAbortControllers) {
      controller.abort();
    }
    this.activeAbortControllers.clear();
    
    this.cleanup();
    await this.flushFailedBuffer();
    this.printStats();
    
    setImmediate(() => process.exit(0));
  }

  private cleanup(): void {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
      this.flushInterval = null;
    }
    if (this.progressInterval) {
      clearInterval(this.progressInterval);
      this.progressInterval = null;
    }
    if (this.stallCheckInterval) {
      clearInterval(this.stallCheckInterval);
      this.stallCheckInterval = null;
    }
    process.off('SIGINT', this.sigintHandler);
    process.off('SIGTERM', this.sigtermHandler);
  }

  private checkForStall(): void {
    const currentCompleted = this.stats.downloaded + this.stats.skipped + this.stats.failed;
    const completedSinceLastCheck = currentCompleted - this.lastCompletedCount;
    this.lastCompletedCount = currentCompleted;
    
    const currentMax = this.semaphore.getMax();
    
    // If we have high concurrency but very few completions, reduce it
    if (
      currentMax > CONSTANTS.MIN_CONCURRENCY * 2 &&
      this.stats.inProgress > currentMax / 2 &&
      completedSinceLastCheck < CONSTANTS.MIN_COMPLETIONS_PER_INTERVAL
    ) {
      const newMax = Math.max(CONSTANTS.MIN_CONCURRENCY, Math.floor(currentMax * 0.75));
      this.semaphore.setMax(newMax);
      this.consecutiveSuccesses = 0;
      logger.warn(`Slow progress detected (${completedSinceLastCheck} completions). Reducing concurrency: ${currentMax} → ${newMax}`);
    }
  }

  private async flushFailedBuffer(): Promise<void> {
    if (this.failedBuffer.length === 0) return;
    const itemsToFlush = [...this.failedBuffer];
    this.failedBuffer = [];

    try {
      let content = '';
      if (!this.failedCsvInitialized) {
        content = 'tweet_id,media_id\n';
        this.failedCsvInitialized = true;
      }
      content += itemsToFlush.map(item => `${item.tweet_id},${item.media_id}`).join('\n') + '\n';
      await appendFile(this.failedCsvPath, content);
    } catch (error) {
      this.failedBuffer.unshift(...itemsToFlush);
      logger.error(`Failed to flush failed buffer: ${error}`);
    }
  }

  private adjustConcurrency(isRateLimit: boolean, retryAfterSeconds?: number): void {
    const currentMax = this.semaphore.getMax();

    if (isRateLimit) {
      const newMax = Math.max(CONSTANTS.MIN_CONCURRENCY, Math.floor(currentMax / 2));
      this.semaphore.setMax(newMax);
      this.consecutiveSuccesses = 0;
      const retryMsg = retryAfterSeconds ? ` (Retry-After: ${retryAfterSeconds}s)` : '';
      logger.warn(`Rate limit detected! Reducing concurrency: ${currentMax} → ${newMax}${retryMsg}`);
    } else {
      this.consecutiveSuccesses++;
      if (this.consecutiveSuccesses >= CONSTANTS.CONSECUTIVE_SUCCESSES_TO_INCREASE && currentMax < CONSTANTS.MAX_CONCURRENCY) {
        const increment = Math.max(5, Math.floor(currentMax * (CONSTANTS.CONCURRENCY_INCREMENT_FACTOR - 1)));
        const newMax = Math.min(CONSTANTS.MAX_CONCURRENCY, currentMax + increment);
        this.semaphore.setMax(newMax);
        this.consecutiveSuccesses = 0;
        logger.success(`Increasing concurrency: ${currentMax} → ${newMax} (+${increment})`);
      }
    }
  }

  private isRateLimitError(status: number): boolean {
    return CONSTANTS.RATE_LIMIT_STATUS_CODES.has(status);
  }

  private isPermanentError(status: number): boolean {
    return status >= 400 && status < 500 && !this.isRateLimitError(status);
  }

  private trackStatusCode(status: number): void {
    const current = this.stats.statusCodes.get(status) || 0;
    this.stats.statusCodes.set(status, current + 1);
  }

  private async downloadImage(item: DownloadItem, shardDir: string): Promise<{ success: boolean; permanent?: boolean; retryAfter?: number }> {
    const controller = new AbortController();
    item.abortController = controller;
    this.activeAbortControllers.add(controller);
    
    const timeoutId = setTimeout(() => controller.abort(), CONSTANTS.FETCH_TIMEOUT_MS);

    try {
      const response = await fetch(item.mediaUrl, {
        signal: controller.signal,
      });

      clearTimeout(timeoutId);
      this.trackStatusCode(response.status);

      if (this.isRateLimitError(response.status)) {
        await response.body?.cancel().catch(() => {});
        const retryAfter = response.headers.get('Retry-After');
        const retryAfterSeconds = retryAfter ? parseInt(retryAfter, 10) : undefined;
        this.adjustConcurrency(true, retryAfterSeconds);
        return { success: false, retryAfter: retryAfterSeconds };
      }

      if (this.isPermanentError(response.status)) {
        await response.body?.cancel().catch(() => {});
        return { success: false, permanent: true };
      }

      if (!response.ok) {
        await response.body?.cancel().catch(() => {});
        return { success: false };
      }

      const contentType = response.headers.get('Content-Type');
      let ext = getExtensionFromContentType(contentType);
      if (!ext) ext = getExtensionFromUrl(item.mediaUrl);
      
      const filename = `${item.uniqueKey}${ext}`;
      const outputPath = path.join(shardDir, filename);

      await Bun.write(outputPath, response);

      this.stats.downloaded++;
      this.adjustConcurrency(false);
      return { success: true };
    } catch (error) {
      clearTimeout(timeoutId);
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      if (errorMessage.includes('abort') || errorMessage.includes('AbortError')) {
        if (!this.shuttingDown) {
          this.trackStatusCode(-4);
          this.stats.timeouts++;
        }
        return { success: false };
      }
      
      if (errorMessage.includes('ENOTFOUND')) {
        this.trackStatusCode(-1);
        return { success: false, permanent: true };
      }
      
      if (errorMessage.includes('Invalid URL')) {
        this.trackStatusCode(-2);
        return { success: false, permanent: true };
      }
      
      this.trackStatusCode(-3);
      return { success: false };
    } finally {
      this.activeAbortControllers.delete(controller);
      item.abortController = undefined;
    }
  }

  private calculateRetryDelay(retryCount: number, serverRetryAfter?: number): number {
    if (serverRetryAfter && serverRetryAfter > 0) {
      return Math.max(serverRetryAfter * 1000, CONSTANTS.INITIAL_RETRY_DELAY_MS);
    }
    return CONSTANTS.INITIAL_RETRY_DELAY_MS * Math.pow(2, retryCount);
  }

  private async processItem(item: DownloadItem): Promise<void> {
    if (this.existingIds.has(item.uniqueKey) || this.failedIds.has(item.uniqueKey)) {
      this.stats.skipped++;
      return;
    }

    // Explicitly acquire sync here, control is in processQueues
    this.semaphore.acquireSync();
    this.stats.inProgress++;

    try {
      if (!isValidUrl(item.mediaUrl)) {
        logger.error(`Invalid URL for ${item.uniqueKey}: ${item.mediaUrl}`);
        this.addToFailedBuffer(item);
        return;
      }

      const shardDir = await ensureShardDirectory(this.outputFolder, item.tweetId);
      const result = await this.downloadImage(item, shardDir);

      if (result.success) {
        this.existingIds.add(item.uniqueKey);
      } else {
        if (result.permanent) {
          this.addToFailedBuffer(item);
          return;
        }

        if (item.retryCount < CONSTANTS.MAX_RETRIES) {
          item.retryCount++;
          item.retryAt = Date.now() + this.calculateRetryDelay(item.retryCount - 1, result.retryAfter);
          this.retryQueue.push(item);
          this.stats.retrying++;
        } else {
          this.addToFailedBuffer(item);
        }
      }
    } finally {
      this.stats.inProgress--;
      this.semaphore.release();
    }
  }

  private addToFailedBuffer(item: DownloadItem): void {
    this.failedBuffer.push({
      tweet_id: item.tweetId,
      media_id: item.mediaId,
    });
    this.stats.failed++;

    if (this.failedBuffer.length >= CONSTANTS.FAILED_BUFFER_MAX_SIZE) {
      this.flushFailedBuffer().catch(err => logger.error(`Flush error: ${err}`));
    }
  }

  private printProgress(): void {
    const total = this.stats.total;
    const completed = this.stats.downloaded + this.stats.skipped + this.stats.failed;
    const percent = total > 0 ? ((completed / total) * 100).toFixed(1) : '0.0';
    
    const errorCodes: string[] = [];
    for (const [code, count] of this.stats.statusCodes) {
      if (code < 200 || code >= 300) {
        const label = code < 0 ? this.formatStatusCode(code).split(' ')[0] : `${code}`;
        errorCodes.push(`${label}:${count}`);
      }
    }
    const errorSummary = errorCodes.length > 0 ? ` | Errors: ${errorCodes.join(', ')}` : '';
    
    const timeoutInfo = this.stats.timeouts > 0 ? ` | ⏱️ ${this.stats.timeouts} timeouts` : '';
    
    console.log(chalk.blue(
      `📊 Progress: ${completed}/${total} (${percent}%) | ` +
      `⬇️ ${this.stats.downloaded} | ⊘ ${this.stats.skipped} | ` +
      `↻ ${this.stats.retrying} | ✗ ${this.stats.failed} | ` +
      `🔄 ${this.stats.inProgress} active | ` +
      `⚡ ${this.semaphore.getMax()}${timeoutInfo}${errorSummary}`
    ));
  }

  // ========================================================================
  // MEMORY LEAK FIX v2: Clear timeouts properly to prevent accumulation
  // ========================================================================
  async processQueues(): Promise<void> {
    const activePromises = new Set<Promise<void>>();
    
    // Signal to wake up the loop when a slot frees up
    let wakeUpResolve: (() => void) | null = null;
    let wakeUpTimeout: Timer | null = null;
    
    const triggerWakeUp = () => {
      if (wakeUpTimeout) {
        clearTimeout(wakeUpTimeout);
        wakeUpTimeout = null;
      }
      if (wakeUpResolve) {
        const resolve = wakeUpResolve;
        wakeUpResolve = null;
        resolve();
      }
    };

    const createTrackedPromise = (item: DownloadItem): void => {
      const promise = this.processItem(item).finally(() => {
        activePromises.delete(promise);
        triggerWakeUp();
      });
      activePromises.add(promise);
    };

    while (!this.shuttingDown) {
      // 1. Process Retries (Highest Priority)
      while (this.retryQueue.size > 0 && this.semaphore.canAcquire()) {
        const nextRetry = this.retryQueue.peek();
        if (!nextRetry) break;
        
        if (nextRetry.retryAt && nextRetry.retryAt > Date.now()) {
          break;
        }

        const item = this.retryQueue.pop();
        if (item) {
          this.stats.retrying--;
          createTrackedPromise(item);
        }
      }

      // 2. Process New Items (only when no retries pending)
      while (
        this.itemGenerator && 
        !this.generatorExhausted && 
        this.semaphore.canAcquire() && 
        this.retryQueue.size === 0
      ) {
        try {
          const result = await this.itemGenerator.next();
          if (result.done) {
            this.generatorExhausted = true;
            break;
          }
          
          const item = result.value;
          this.stats.total++;
          
          if (this.existingIds.has(item.uniqueKey) || this.failedIds.has(item.uniqueKey)) {
            this.stats.skipped++;
            continue;
          }
          
          createTrackedPromise(item);
        } catch (error) {
          logger.error(`Generator error: ${error}`);
          this.generatorExhausted = true;
          break;
        }
      }

      // 3. Exit condition
      const hasGenerator = this.itemGenerator && !this.generatorExhausted;
      const hasRetries = this.retryQueue.size > 0;
      const hasActiveWork = activePromises.size > 0;
      
      if (!hasGenerator && !hasRetries && !hasActiveWork) {
        break;
      }

      // 4. Wait for a slot to open OR a timeout (for retry checks)
      const needToWait = activePromises.size >= this.semaphore.getMax() || hasRetries;
      
      if (needToWait) {
        await new Promise<void>(resolve => {
          wakeUpResolve = resolve;
          wakeUpTimeout = setTimeout(() => {
            wakeUpTimeout = null;
            if (wakeUpResolve === resolve) {
              wakeUpResolve = null;
              resolve();
            }
          }, CONSTANTS.RETRY_CHECK_INTERVAL_MS);
        });
      }
    }

    // Cleanup any pending timeout
    if (wakeUpTimeout) {
      clearTimeout(wakeUpTimeout);
    }

    if (activePromises.size > 0) {
      await Promise.all(Array.from(activePromises));
    }
  }

  async finalize(): Promise<void> {
    this.cleanup();
    await this.flushFailedBuffer();
  }

  private formatStatusCode(code: number): string {
    const specialCodes: Record<number, string> = {
      [-1]: 'DNS Error',
      [-2]: 'Invalid URL',
      [-3]: 'Network Error',
      [-4]: 'Timeout',
    };
    return specialCodes[code] || `HTTP ${code}`;
  }

  printStats(): void {
    console.log(chalk.blue('\n' + '='.repeat(70)));
    console.log(chalk.blue.bold('📊 DOWNLOAD STATISTICS'));
    console.log(chalk.blue('='.repeat(70)));
    console.log(chalk.gray(`Total items processed: ${this.stats.total}`));
    console.log(chalk.green(`✓ Downloaded: ${this.stats.downloaded}`));
    console.log(chalk.yellow(`⊘ Skipped (already exists): ${this.stats.skipped}`));
    console.log(chalk.cyan(`↻ Still retrying: ${this.stats.retrying}`));
    console.log(chalk.red(`✗ Failed: ${this.stats.failed}`));
    if (this.stats.failed > 0) {
      console.log(chalk.gray(`  Failed items saved to: ${this.failedCsvPath}`));
    }
    
    if (this.stats.statusCodes.size > 0) {
      console.log(chalk.blue('\n📈 Response Status Codes:'));
      const sorted = [...this.stats.statusCodes.entries()].sort((a, b) => b[1] - a[1]);
      for (const [code, count] of sorted) {
        const label = this.formatStatusCode(code);
        console.log(chalk.gray(`  ${label}: ${count.toLocaleString()}`));
      }
    }
    console.log(chalk.blue('='.repeat(70) + '\n'));
  }
}

// ============================================================================
// Item Generator (Continuous Feeding from Multiple CSVs)
// ============================================================================

async function* createItemGenerator(csvFiles: string[]): AsyncGenerator<DownloadItem> {
  const csvParser = new StreamingCSVParser();
  for (const csvFile of csvFiles) {
    logger.info(`Processing: ${path.basename(csvFile)}`);
    csvParser.reset();
    try {
      for await (const row of csvParser.parse(csvFile)) {
        if (!row.media_url) continue;
        const uniqueKey = `${row.tweet_id}_${row.media_id}`;
        yield {
          tweetId: row.tweet_id,
          mediaId: row.media_id,
          mediaUrl: row.media_url,
          uniqueKey,
          retryCount: 0,
        };
      }
    } catch (error) {
      logger.error(`Error parsing ${path.basename(csvFile)}: ${error}`);
    }
  }
}

// ============================================================================
// Main Processing Function
// ============================================================================

async function processCSVFiles(
  csvFolder: string,
  outputFolder: string,
  initialConcurrency: number
): Promise<void> {
  await mkdir(outputFolder, { recursive: true });

  console.log(chalk.blue.bold('\n📥 Image Download Manager (Memory Fixed)\n'));

  // Hints to the GC before starting heavy work
  if (global.gc) global.gc();

  // 1. Load memory caches
  const existingLibrary = await loadExistingLibrary(outputFolder);
  const failedLibrary = await loadFailedLibrary(outputFolder);

  // 2. Find CSVs
  const glob = new Glob('*.csv');
  const csvFiles: string[] = [];
  try {
    for await (const file of glob.scan(csvFolder)) {
      csvFiles.push(path.join(csvFolder, file));
    }
  } catch (error) {
    throw new Error(`CSV folder error: ${error}`);
  }

  if (csvFiles.length === 0) {
    logger.warn('No CSV files found in the specified folder.');
    return;
  }

  logger.info(`Found ${csvFiles.length} CSV file(s)\n`);

  // 3. Initialize Manager
  const manager = new DownloadManager(outputFolder, initialConcurrency, existingLibrary, failedLibrary);
  const itemGenerator = createItemGenerator(csvFiles);
  manager.setItemGenerator(itemGenerator);
  
  // 4. Run
  await manager.processQueues();
  await manager.finalize();
  manager.printStats();
}

// ============================================================================
// CLI Setup
// ============================================================================

const program = new Command();
program
  .name('download-images')
  .description('Download images from CSV files')
  .requiredOption('-c, --csv-folder <path>', 'Path to CSV folder')
  .option('-o, --output-folder <path>', 'Path to output folder', 'data/images')
  .option('--concurrency <number>', 'Initial concurrency', '30')
  .action(async (options) => {
    try {
      await processCSVFiles(
        path.resolve(options.csvFolder),
        path.resolve(options.outputFolder),
        parseInt(options.concurrency, 10)
      );
    } catch (error) {
      console.error(error);
      process.exit(1);
    }
  });

program.parse();