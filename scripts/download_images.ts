#!/usr/bin/env bun

/**
 * Download Images CLI (High Performance & Memory Optimized)
 *
 * Features:
 * - Fixed 4GB native memory issue by replacing Bun's fetch() with undici
 * - Memory now stable at ~1GB instead of 4GB+ (70% reduction)
 * - GC can properly release native HTTP buffers
 * - O(1) deduplication using memory caching
 * - Streaming CSV parsing and disk writes
 * - Dynamic concurrency up to 500
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
import { createReadStream, createWriteStream, existsSync } from 'fs';
import { createInterface } from 'readline';
import { pipeline } from 'stream/promises';
import { request } from 'undici';
import type { Dispatcher } from 'undici';

// ============================================================================
// Constants
// ============================================================================

const CONSTANTS = {
  // Retry configuration
  INITIAL_RETRY_DELAY_MS: 30000,
  MAX_RETRIES: 0,

  // Concurrency configuration
  MIN_CONCURRENCY: 30,
  MAX_CONCURRENCY: 1000, // Higher max since undici handles memory better
  CONSECUTIVE_SUCCESSES_TO_INCREASE: 1000,
  CONCURRENCY_INCREMENT_FACTOR: 1.20,

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

  // Memory monitoring
  MEMORY_CHECK_INTERVAL_MS: 3000, // Check memory every 3s
  MEMORY_GC_INTERVAL_MS: 15000, // Force GC every 15s (more frequent)
  MEMORY_THRESHOLD_MB: 2048, // Start throttling at 2GB (earlier)
  MEMORY_CRITICAL_MB: 3072, // Critical threshold at 3GB (lower)
  MEMORY_COOLDOWN_MS: 30000, // Shorter cooldown for faster recovery

  // Rate limit status codes
  RATE_LIMIT_STATUS_CODES: new Set([429, 420, 503]),

  // Rate limit backoff
  RATE_LIMIT_BACKOFF_MS: 15 * 60 * 1000, // 15 minutes default backoff
  RATE_LIMIT_BACKOFF_MIN_MS: 60 * 1000, // Minimum 1 minute

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
  memory: (message: string) => console.log(chalk.magenta(`🧠 ${message}`)),
};

// ============================================================================
// Memory Monitor
// ============================================================================

class MemoryMonitor {
  private lastHeapUsed = 0;
  private lastRSS = 0;
  private peakHeapUsed = 0;
  private peakRSS = 0;
  private monitorInterval: Timer | null = null;
  private gcInterval: Timer | null = null;
  private lastGCTime = Date.now();

  start(): void {
    // Monitor memory usage
    this.monitorInterval = setInterval(() => {
      this.checkMemory();
    }, CONSTANTS.MEMORY_CHECK_INTERVAL_MS);

    // Periodic GC
    this.gcInterval = setInterval(() => {
      this.triggerGC();
    }, CONSTANTS.MEMORY_GC_INTERVAL_MS);
  }

  stop(): void {
    if (this.monitorInterval) {
      clearInterval(this.monitorInterval);
      this.monitorInterval = null;
    }
    if (this.gcInterval) {
      clearInterval(this.gcInterval);
      this.gcInterval = null;
    }
  }

  private formatMB(bytes: number): string {
    return (bytes / 1024 / 1024).toFixed(2);
  }

  checkMemory(): { heapUsedMB: number; rssMB: number } {
    const usage = process.memoryUsage();
    const heapUsedMB = parseFloat(this.formatMB(usage.heapUsed));
    const rssMB = parseFloat(this.formatMB(usage.rss));

    // Track peaks
    if (usage.heapUsed > this.peakHeapUsed) this.peakHeapUsed = usage.heapUsed;
    if (usage.rss > this.peakRSS) this.peakRSS = usage.rss;

    const heapDelta = usage.heapUsed - this.lastHeapUsed;
    const rssDelta = usage.rss - this.lastRSS;

    // Log if significant change or threshold exceeded
    if (Math.abs(heapDelta) > 50 * 1024 * 1024 || heapUsedMB > CONSTANTS.MEMORY_THRESHOLD_MB || rssMB > CONSTANTS.MEMORY_THRESHOLD_MB) {
      const heapDeltaStr = heapDelta > 0 ? `+${this.formatMB(heapDelta)}` : this.formatMB(heapDelta);
      const rssDeltaStr = rssDelta > 0 ? `+${this.formatMB(rssDelta)}` : this.formatMB(rssDelta);

      logger.memory(
        `Heap: ${this.formatMB(usage.heapUsed)} MB (${heapDeltaStr}) | ` +
        `RSS: ${this.formatMB(usage.rss)} MB (${rssDeltaStr}) | ` +
        `Ext: ${this.formatMB(usage.external)} MB`
      );
    }

    // Critical memory warning
    if (heapUsedMB > CONSTANTS.MEMORY_CRITICAL_MB || rssMB > CONSTANTS.MEMORY_CRITICAL_MB) {
      logger.warn(`CRITICAL: Memory usage high (Heap: ${heapUsedMB} MB, RSS: ${rssMB} MB)! Triggering emergency GC...`);
      this.triggerGC();
    }

    this.lastHeapUsed = usage.heapUsed;
    this.lastRSS = usage.rss;

    return { heapUsedMB, rssMB };
  }

  private triggerGC(): void {
    if (global.gc) {
      const timeSinceLastGC = Date.now() - this.lastGCTime;
      if (timeSinceLastGC > 5000) { // Don't GC more than once per 5s
        const before = process.memoryUsage().heapUsed;
        global.gc();
        const after = process.memoryUsage().heapUsed;
        const freed = before - after;
        if (freed > 10 * 1024 * 1024) { // Only log if freed > 10MB
          logger.memory(`GC freed ${this.formatMB(freed)} MB`);
        }
        this.lastGCTime = Date.now();
      }
    }
  }

  printSummary(): void {
    const current = process.memoryUsage();
    console.log(chalk.magenta('\n📊 Memory Summary:'));
    console.log(chalk.gray(`  Peak Heap: ${this.formatMB(this.peakHeapUsed)} MB`));
    console.log(chalk.gray(`  Peak RSS:  ${this.formatMB(this.peakRSS)} MB`));
    console.log(chalk.gray(`  Final Heap: ${this.formatMB(current.heapUsed)} MB`));
    console.log(chalk.gray(`  Final RSS:  ${this.formatMB(current.rss)} MB`));
  }
}

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

  // Memory monitoring
  private readonly memoryMonitor = new MemoryMonitor();
  private lastMemoryThrottleTime = 0;

  // Rate limit backoff
  private rateLimitUntil = 0;
  private rateLimitCount = 0;

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

    // Start memory monitoring
    this.memoryMonitor.start();
  }

  setItemGenerator(generator: AsyncGenerator<DownloadItem>): void {
    this.itemGenerator = generator;
    this.generatorExhausted = false;
  }

  private async handleShutdown(): Promise<void> {
    if (this.shuttingDown) return;
    this.shuttingDown = true;

    console.log(chalk.yellow('\n\n🛑 Shutting down gracefully...'));
    console.log(chalk.gray('Aborting active downloads...'));

    // Abort all active downloads
    for (const controller of this.activeAbortControllers) {
      controller.abort();
    }
    this.activeAbortControllers.clear();

    // Print final progress before cleanup
    console.log(chalk.gray('Saving final state...'));
    this.printProgress();

    // Cleanup intervals
    this.cleanup();

    // Flush any remaining failed items
    await this.flushFailedBuffer();

    // Print final statistics
    console.log(chalk.green('\n✓ Shutdown complete. Final statistics:\n'));
    this.printStats();

    // Give console time to flush before exiting
    await new Promise(resolve => setTimeout(resolve, 100));
    process.exit(0);
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
    this.memoryMonitor.stop();
    process.off('SIGINT', this.sigintHandler);
    process.off('SIGTERM', this.sigtermHandler);
  }

  private checkForStall(): void {
    const currentCompleted = this.stats.downloaded + this.stats.skipped + this.stats.failed;
    const completedSinceLastCheck = currentCompleted - this.lastCompletedCount;
    this.lastCompletedCount = currentCompleted;

    const currentMax = this.semaphore.getMax();

    // MEMORY-BASED THROTTLING: Check memory and reduce concurrency if too high
    const { heapUsedMB, rssMB } = this.memoryMonitor.checkMemory();

    if (rssMB > CONSTANTS.MEMORY_THRESHOLD_MB && currentMax > CONSTANTS.MIN_CONCURRENCY * 2) {
      const newMax = Math.max(CONSTANTS.MIN_CONCURRENCY * 2, Math.floor(currentMax * 0.5));
      this.semaphore.setMax(newMax);
      this.consecutiveSuccesses = 0;
      this.lastMemoryThrottleTime = Date.now();
      logger.warn(`High memory usage (RSS: ${rssMB.toFixed(0)} MB). Reducing concurrency: ${currentMax} → ${newMax}`);
      // Trigger GC to help
      if (global.gc) {
        global.gc();
      }
      return; // Don't check stall if we just reduced due to memory
    }

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

      // Check if we're in memory throttle cooldown
      const timeSinceMemoryThrottle = Date.now() - this.lastMemoryThrottleTime;
      const inCooldown = timeSinceMemoryThrottle < CONSTANTS.MEMORY_COOLDOWN_MS;

      if (this.consecutiveSuccesses >= CONSTANTS.CONSECUTIVE_SUCCESSES_TO_INCREASE && currentMax < CONSTANTS.MAX_CONCURRENCY && !inCooldown) {
        const increment = Math.max(3, Math.floor(currentMax * (CONSTANTS.CONCURRENCY_INCREMENT_FACTOR - 1)));
        const newMax = Math.min(CONSTANTS.MAX_CONCURRENCY, currentMax + increment);
        this.semaphore.setMax(newMax);
        this.consecutiveSuccesses = 0;
        logger.success(`Increasing concurrency: ${currentMax} → ${newMax} (+${increment})`);
      } else if (inCooldown && this.consecutiveSuccesses >= CONSTANTS.CONSECUTIVE_SUCCESSES_TO_INCREASE) {
        // Reset counter during cooldown to avoid rapid increase when cooldown ends
        this.consecutiveSuccesses = CONSTANTS.CONSECUTIVE_SUCCESSES_TO_INCREASE / 2;
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
    let response: Dispatcher.ResponseData | null = null;

    try {
      // Using undici for better memory performance (1GB vs 4GB with Bun's fetch)
      response = await request(item.mediaUrl, {
        signal: controller.signal,
      });

      clearTimeout(timeoutId);
      this.trackStatusCode(response.statusCode);

      if (this.isRateLimitError(response.statusCode)) {
        // Extract rate limit information from headers and body
        const retryAfterHeader = response.headers['retry-after'] as string | undefined;
        const rateLimitReset = response.headers['x-ratelimit-reset'] as string | undefined;
        const rateLimitRemaining = response.headers['x-ratelimit-remaining'] as string | undefined;
        const rateLimitLimit = response.headers['x-ratelimit-limit'] as string | undefined;

        // Try to extract additional info from response body
        let bodyText = '';
        let bodyInfo: any = null;
        try {
          bodyText = await response.body.text();
          // Try parsing as JSON
          if (bodyText.trim().startsWith('{') || bodyText.trim().startsWith('[')) {
            try {
              bodyInfo = JSON.parse(bodyText);
            } catch {
              // Not JSON, that's ok
            }
          }
        } catch {
          try {
            response.body.destroy();
          } catch {}
        }

        // Log detailed rate limit info
        logger.warn(`⏸️  Rate Limit Hit (${response.statusCode}):`);
        if (retryAfterHeader) logger.warn(`   Retry-After: ${retryAfterHeader}s`);
        if (rateLimitReset) logger.warn(`   X-RateLimit-Reset: ${new Date(parseInt(rateLimitReset) * 1000).toISOString()}`);
        if (rateLimitRemaining) logger.warn(`   X-RateLimit-Remaining: ${rateLimitRemaining}`);
        if (rateLimitLimit) logger.warn(`   X-RateLimit-Limit: ${rateLimitLimit}`);
        if (bodyInfo) {
          logger.warn(`   Response Body: ${JSON.stringify(bodyInfo, null, 2).substring(0, 500)}`);
        } else if (bodyText && bodyText.length < 200) {
          logger.warn(`   Response Body: ${bodyText}`);
        }

        // Calculate backoff time (prefer server-provided, otherwise use default)
        let backoffMs = CONSTANTS.RATE_LIMIT_BACKOFF_MS;
        if (retryAfterHeader) {
          const retryAfterSeconds = parseInt(retryAfterHeader, 10);
          if (!isNaN(retryAfterSeconds)) {
            backoffMs = Math.max(retryAfterSeconds * 1000, CONSTANTS.RATE_LIMIT_BACKOFF_MIN_MS);
          }
        } else if (rateLimitReset) {
          const resetTime = parseInt(rateLimitReset, 10) * 1000;
          const now = Date.now();
          if (resetTime > now) {
            backoffMs = Math.max(resetTime - now, CONSTANTS.RATE_LIMIT_BACKOFF_MIN_MS);
          }
        }

        // Set global rate limit pause
        this.rateLimitUntil = Date.now() + backoffMs;
        this.rateLimitCount++;

        const backoffMinutes = Math.ceil(backoffMs / 60000);
        logger.warn(`   🛑 PAUSING ALL DOWNLOADS for ${backoffMinutes} minutes (until ${new Date(this.rateLimitUntil).toISOString()})`);

        this.adjustConcurrency(true, Math.ceil(backoffMs / 1000));
        return { success: false, retryAfter: Math.ceil(backoffMs / 1000) };
      }

      if (this.isPermanentError(response.statusCode)) {
        // CRITICAL: Properly drain/cancel the response body to free memory
        try {
          await response.body.text(); // Consume body completely
        } catch {
          response.body.destroy();
        }
        return { success: false, permanent: true };
      }

      if (response.statusCode < 200 || response.statusCode >= 300) {
        // CRITICAL: Properly drain/cancel the response body to free memory
        try {
          await response.body.text(); // Consume body completely
        } catch {
          response.body.destroy();
        }
        return { success: false };
      }

      const contentType = response.headers['content-type'] as string | undefined;
      let ext = getExtensionFromContentType(contentType ?? null);
      if (!ext) ext = getExtensionFromUrl(item.mediaUrl);

      const filename = `${item.uniqueKey}${ext}`;
      const outputPath = path.join(shardDir, filename);

      // Stream undici response directly to disk without buffering in memory
      const fileStream = createWriteStream(outputPath);
      await pipeline(response.body, fileStream);

      this.stats.downloaded++;
      this.adjustConcurrency(false);
      return { success: true };
    } catch (error) {
      clearTimeout(timeoutId);

      // CRITICAL: Clean up response body if it exists
      if (response?.body) {
        try {
          response.body.destroy();
        } catch {}
      }

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
      response = null; // Clear reference to help GC
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

    // Rate limit status
    let rateLimitInfo = '';
    if (this.rateLimitUntil > Date.now()) {
      const remainingMinutes = Math.ceil((this.rateLimitUntil - Date.now()) / 60000);
      rateLimitInfo = ` | ⏸️  PAUSED (${remainingMinutes}m)`;
    } else if (this.rateLimitCount > 0) {
      rateLimitInfo = ` | 🚦 ${this.rateLimitCount} rate limits`;
    }

    console.log(chalk.blue(
      `📊 Progress: ${completed}/${total} (${percent}%) | ` +
      `⬇️ ${this.stats.downloaded} | ⊘ ${this.stats.skipped} | ` +
      `↻ ${this.stats.retrying} | ✗ ${this.stats.failed} | ` +
      `🔄 ${this.stats.inProgress} active | ` +
      `⚡ ${this.semaphore.getMax()}${timeoutInfo}${rateLimitInfo}${errorSummary}`
    ));
  }

  // ========================================================================
  // MEMORY LEAK FIX v3: Improved cleanup and GC hints
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
      // 0. Check if we're in rate limit backoff
      if (this.rateLimitUntil > 0 && this.rateLimitUntil > Date.now()) {
        const remainingMs = this.rateLimitUntil - Date.now();

        // Log remaining time
        logger.warn(`⏸️  Rate limit backoff active. Resuming in ${Math.ceil(remainingMs / 60000)} minutes...`);

        // Wait and skip processing
        await new Promise<void>(resolve => {
          wakeUpResolve = resolve;
          wakeUpTimeout = setTimeout(() => {
            wakeUpTimeout = null;
            if (wakeUpResolve === resolve) {
              wakeUpResolve = null;
              resolve();
            }
          }, Math.min(remainingMs, 10000)); // Check every 10s or when backoff ends
        });

        // Check again after waiting
        if (this.rateLimitUntil > Date.now()) {
          continue; // Still in backoff, loop again
        } else {
          logger.success(`✓ Rate limit backoff ended. Resuming downloads...`);
        }
      }

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

      // 4. Wait for a slot to open OR a timeout (for retry checks) OR active work to finish
      const needToWait =
        activePromises.size >= this.semaphore.getMax() || // At max concurrency
        hasRetries || // Have retries pending
        (!hasGenerator && hasActiveWork); // Generator done but promises still active

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

    // Rate limit summary
    if (this.rateLimitCount > 0) {
      console.log(chalk.yellow(`\n🚦 Rate Limits Encountered: ${this.rateLimitCount}`));
      console.log(chalk.gray(`  Downloads were paused when rate limits were hit`));
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

    // Print memory summary
    this.memoryMonitor.printSummary();
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
    let rowCount = 0;
    try {
      for await (const row of csvParser.parse(csvFile)) {
        if (!row.media_url) continue;
        rowCount++;
        const uniqueKey = `${row.tweet_id}_${row.media_id}`;
        yield {
          tweetId: row.tweet_id,
          mediaId: row.media_id,
          mediaUrl: row.media_url,
          uniqueKey,
          retryCount: 0,
        };
      }
      logger.info(`✓ Completed ${path.basename(csvFile)} (${rowCount} rows)`);
    } catch (error) {
      logger.error(`Error parsing ${path.basename(csvFile)} at row ${rowCount}: ${error}`);
    }
  }
  logger.success('All CSV files processed');
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

  console.log(chalk.blue.bold('\n📥 Image Download Manager (Memory Optimized v3)\n'));

  // Check if GC is exposed
  if (!global.gc) {
    logger.warn('Manual GC not available. For best memory performance, run with: bun --expose-gc run ...');
  } else {
    logger.success('Manual GC enabled - memory will be actively managed');
  }

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