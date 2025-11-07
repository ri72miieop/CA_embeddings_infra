import fs from 'fs/promises';
import path from 'path';
import { logger } from '../observability/logger.js';

export interface CounterData {
  count: number;
  lastUpdated: string;
}

export class PersistentCounter {
  private filePath: string;
  private currentCount: number = 0;
  private pendingCount: number = 0;
  private saveInterval: NodeJS.Timeout | null = null;
  private readonly saveIntervalMs: number;

  constructor(filePath: string, saveIntervalMs: number = 60000) { // Default 1 minute
    this.filePath = filePath;
    this.saveIntervalMs = saveIntervalMs;
  }

  async initialize(): Promise<void> {
    try {
      // Ensure directory exists
      const dir = path.dirname(this.filePath);
      try {
        await fs.access(dir);
      } catch {
        await fs.mkdir(dir, { recursive: true });
        logger.info({ directory: dir }, 'Created counter directory');
      }

      // Load existing count
      await this.loadFromFile();

      // Start periodic save
      this.startPeriodicSave();

      logger.info({
        filePath: this.filePath,
        initialCount: this.currentCount,
        saveIntervalMs: this.saveIntervalMs
      }, 'Persistent counter initialized');

    } catch (error) {
      logger.error({ error }, 'Failed to initialize persistent counter');
      throw error;
    }
  }

  private async loadFromFile(): Promise<void> {
    try {
      const data = await fs.readFile(this.filePath, 'utf-8');
      const counterData: CounterData = JSON.parse(data);

      this.currentCount = counterData.count || 0;

      logger.info({
        count: this.currentCount,
        lastUpdated: counterData.lastUpdated
      }, 'Loaded vector count from file');

    } catch (error) {
      if ((error as any).code === 'ENOENT') {
        // File doesn't exist, start with 0
        this.currentCount = 0;
        logger.info('Counter file not found, starting with count 0');
        await this.saveToFile();
      } else {
        logger.warn({ error }, 'Failed to load counter file, starting with 0');
        this.currentCount = 0;
      }
    }
  }

  private async saveToFile(): Promise<void> {
    try {
      const counterData: CounterData = {
        count: this.currentCount + this.pendingCount,
        lastUpdated: new Date().toISOString()
      };

      await fs.writeFile(this.filePath, JSON.stringify(counterData, null, 2));

      // Update current count and reset pending
      this.currentCount += this.pendingCount;
      this.pendingCount = 0;

      logger.debug({
        count: this.currentCount,
        filePath: this.filePath
      }, 'Saved vector count to file');

    } catch (error) {
      logger.error({ error }, 'Failed to save counter to file');
    }
  }

  private startPeriodicSave(): void {
    this.saveInterval = setInterval(async () => {
      if (this.pendingCount !== 0) {
        await this.saveToFile();
      }
    }, this.saveIntervalMs);
  }

  increment(amount: number = 1): void {
    this.pendingCount += amount;
  }

  decrement(amount: number = 1): void {
    this.pendingCount -= amount;
  }

  get(): number {
    return this.currentCount + this.pendingCount;
  }

  async forceSave(): Promise<void> {
    await this.saveToFile();
  }

  async close(): Promise<void> {
    // Stop periodic saves
    if (this.saveInterval) {
      clearInterval(this.saveInterval);
      this.saveInterval = null;
    }

    // Save any pending changes
    if (this.pendingCount !== 0) {
      await this.saveToFile();
    }

    logger.info({ finalCount: this.currentCount }, 'Persistent counter closed');
  }
}