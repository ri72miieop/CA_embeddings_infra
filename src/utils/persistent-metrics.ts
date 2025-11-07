import fs from 'fs/promises';
import path from 'path';
import { logger } from '../observability/logger.js';

export interface CounterMetric {
  name: string;
  help: string;
  labels: Record<string, string>;
  value: number;
}

export interface GaugeMetric {
  name: string;
  help: string;
  labels: Record<string, string>;
  value: number;
}

export interface HistogramBucket {
  le: number;
  count: number;
}

export interface HistogramMetric {
  name: string;
  help: string;
  labels: Record<string, string>;
  buckets: HistogramBucket[];
  count: number;
  sum: number;
}

export interface MetricsData {
  counters: CounterMetric[];
  gauges: GaugeMetric[];
  histograms: HistogramMetric[];
  lastUpdated: string;
}

export class PersistentMetrics {
  private filePath: string;
  private metricsData: MetricsData;
  private saveInterval: NodeJS.Timeout | null = null;
  private readonly saveIntervalMs: number;
  private isDirty: boolean = false;

  constructor(filePath: string, saveIntervalMs: number = 60000) { // Default 1 minute
    this.filePath = filePath;
    this.saveIntervalMs = saveIntervalMs;
    this.metricsData = {
      counters: [],
      gauges: [],
      histograms: [],
      lastUpdated: new Date().toISOString()
    };
  }

  async initialize(): Promise<void> {
    try {
      // Ensure directory exists
      const dir = path.dirname(this.filePath);
      try {
        await fs.access(dir);
      } catch {
        await fs.mkdir(dir, { recursive: true });
        logger.info({ directory: dir }, 'Created metrics directory');
      }

      // Load existing metrics
      await this.loadFromFile();

      // Start periodic save
      this.startPeriodicSave();

      logger.info({
        filePath: this.filePath,
        counters: this.metricsData.counters.length,
        gauges: this.metricsData.gauges.length,
        histograms: this.metricsData.histograms.length,
        saveIntervalMs: this.saveIntervalMs
      }, 'Persistent metrics initialized');

    } catch (error) {
      logger.error({ error }, 'Failed to initialize persistent metrics');
      throw error;
    }
  }

  private async loadFromFile(): Promise<void> {
    try {
      const data = await fs.readFile(this.filePath, 'utf-8');
      this.metricsData = JSON.parse(data);

      logger.info({
        counters: this.metricsData.counters.length,
        gauges: this.metricsData.gauges.length,
        histograms: this.metricsData.histograms.length,
        lastUpdated: this.metricsData.lastUpdated
      }, 'Loaded metrics from file');

    } catch (error) {
      if ((error as any).code === 'ENOENT') {
        // File doesn't exist, start with empty metrics
        logger.info('Metrics file not found, starting with empty metrics');
        await this.saveToFile();
      } else {
        logger.warn({ error }, 'Failed to load metrics file, starting with empty metrics');
      }
    }
  }

  private async saveToFile(): Promise<void> {
    if (!this.isDirty) return;

    try {
      this.metricsData.lastUpdated = new Date().toISOString();
      await fs.writeFile(this.filePath, JSON.stringify(this.metricsData, null, 2));

      this.isDirty = false;

      logger.debug({
        counters: this.metricsData.counters.length,
        gauges: this.metricsData.gauges.length,
        histograms: this.metricsData.histograms.length,
        filePath: this.filePath
      }, 'Saved metrics to file');

    } catch (error) {
      logger.error({ error }, 'Failed to save metrics to file');
    }
  }

  private startPeriodicSave(): void {
    this.saveInterval = setInterval(async () => {
      await this.saveToFile();
    }, this.saveIntervalMs);
  }

  // Counter methods
  incrementCounter(name: string, labels: Record<string, string> = {}, amount: number = 1): void {
    const key = this.getMetricKey(name, labels);
    let counter = this.metricsData.counters.find(c => this.getMetricKey(c.name, c.labels) === key);

    if (!counter) {
      counter = {
        name,
        help: `Counter metric: ${name}`,
        labels: { ...labels },
        value: 0
      };
      this.metricsData.counters.push(counter);
    }

    counter.value += amount;
    this.isDirty = true;
  }

  getCounterValue(name: string, labels: Record<string, string> = {}): number {
    const key = this.getMetricKey(name, labels);
    const counter = this.metricsData.counters.find(c => this.getMetricKey(c.name, c.labels) === key);
    return counter?.value || 0;
  }

  // Gauge methods
  setGauge(name: string, labels: Record<string, string> = {}, value: number): void {
    const key = this.getMetricKey(name, labels);
    let gauge = this.metricsData.gauges.find(g => this.getMetricKey(g.name, g.labels) === key);

    if (!gauge) {
      gauge = {
        name,
        help: `Gauge metric: ${name}`,
        labels: { ...labels },
        value: 0
      };
      this.metricsData.gauges.push(gauge);
    }

    gauge.value = value;
    this.isDirty = true;
  }

  incrementGauge(name: string, labels: Record<string, string> = {}, amount: number = 1): void {
    const currentValue = this.getGaugeValue(name, labels);
    this.setGauge(name, labels, currentValue + amount);
  }

  decrementGauge(name: string, labels: Record<string, string> = {}, amount: number = 1): void {
    const currentValue = this.getGaugeValue(name, labels);
    this.setGauge(name, labels, currentValue - amount);
  }

  getGaugeValue(name: string, labels: Record<string, string> = {}): number {
    const key = this.getMetricKey(name, labels);
    const gauge = this.metricsData.gauges.find(g => this.getMetricKey(g.name, g.labels) === key);
    return gauge?.value || 0;
  }

  // Histogram methods
  observeHistogram(name: string, labels: Record<string, string> = {}, value: number, buckets: number[]): void {
    const key = this.getMetricKey(name, labels);
    let histogram = this.metricsData.histograms.find(h => this.getMetricKey(h.name, h.labels) === key);

    if (!histogram) {
      histogram = {
        name,
        help: `Histogram metric: ${name}`,
        labels: { ...labels },
        buckets: buckets.map(le => ({ le, count: 0 })),
        count: 0,
        sum: 0
      };
      this.metricsData.histograms.push(histogram);
    }

    // Update histogram
    histogram.count += 1;
    histogram.sum += value;

    // Update buckets
    for (const bucket of histogram.buckets) {
      if (value <= bucket.le) {
        bucket.count += 1;
      }
    }

    this.isDirty = true;
  }

  getHistogramData(name: string, labels: Record<string, string> = {}): HistogramMetric | null {
    const key = this.getMetricKey(name, labels);
    return this.metricsData.histograms.find(h => this.getMetricKey(h.name, h.labels) === key) || null;
  }

  // Utility methods
  private getMetricKey(name: string, labels: Record<string, string>): string {
    const sortedLabels = Object.keys(labels)
      .sort()
      .map(key => `${key}=${labels[key]}`)
      .join(',');
    return `${name}{${sortedLabels}}`;
  }

  // Get all metrics for export
  getAllMetrics(): MetricsData {
    return JSON.parse(JSON.stringify(this.metricsData));
  }

  // Get metrics summary
  getMetricsSummary(): { counters: number; gauges: number; histograms: number; lastUpdated: string } {
    return {
      counters: this.metricsData.counters.length,
      gauges: this.metricsData.gauges.length,
      histograms: this.metricsData.histograms.length,
      lastUpdated: this.metricsData.lastUpdated
    };
  }

  async forceSave(): Promise<void> {
    this.isDirty = true;
    await this.saveToFile();
  }

  async close(): Promise<void> {
    // Stop periodic saves
    if (this.saveInterval) {
      clearInterval(this.saveInterval);
      this.saveInterval = null;
    }

    // Save any pending changes
    if (this.isDirty) {
      await this.saveToFile();
    }

    logger.info({
      summary: this.getMetricsSummary()
    }, 'Persistent metrics closed');
  }
}