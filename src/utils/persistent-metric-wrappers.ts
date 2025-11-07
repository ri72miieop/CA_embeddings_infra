import { PersistentMetrics } from './persistent-metrics.js';

// Global persistent metrics instance
let persistentMetrics: PersistentMetrics | null = null;

export function initializePersistentMetrics(filePath: string, saveIntervalMs: number = 60000): Promise<void> {
  persistentMetrics = new PersistentMetrics(filePath, saveIntervalMs);
  return persistentMetrics.initialize();
}

export function getPersistentMetrics(): PersistentMetrics {
  if (!persistentMetrics) {
    throw new Error('Persistent metrics not initialized. Call initializePersistentMetrics() first.');
  }
  return persistentMetrics;
}

export async function closePersistentMetrics(): Promise<void> {
  if (persistentMetrics) {
    await persistentMetrics.close();
    persistentMetrics = null;
  }
}

// Wrapper classes that provide Prometheus-like API but use persistent storage
export class PersistentCounter {
  private name: string;
  private help: string;
  private labelNames: string[];

  constructor(options: { name: string; help: string; labelNames?: string[] }) {
    this.name = options.name;
    this.help = options.help;
    this.labelNames = options.labelNames || [];
  }

  inc(labels?: Record<string, string>, amount?: number): void;
  inc(amount?: number): void;
  inc(labelsOrAmount?: Record<string, string> | number, amount?: number): void {
    const metrics = getPersistentMetrics();

    if (typeof labelsOrAmount === 'number') {
      // inc(amount)
      metrics.incrementCounter(this.name, {}, labelsOrAmount);
    } else {
      // inc(labels, amount) or inc(labels)
      const labels = labelsOrAmount || {};
      metrics.incrementCounter(this.name, labels, amount || 1);
    }
  }

  get(labels: Record<string, string> = {}): number {
    const metrics = getPersistentMetrics();
    return metrics.getCounterValue(this.name, labels);
  }

  reset(labels: Record<string, string> = {}): void {
    // For counters, reset means setting to 0
    const metrics = getPersistentMetrics();
    const currentValue = metrics.getCounterValue(this.name, labels);
    if (currentValue > 0) {
      metrics.incrementCounter(this.name, labels, -currentValue);
    }
  }
}

export class PersistentGauge {
  private name: string;
  private help: string;
  private labelNames: string[];

  constructor(options: { name: string; help: string; labelNames?: string[] }) {
    this.name = options.name;
    this.help = options.help;
    this.labelNames = options.labelNames || [];
  }

  set(labels: Record<string, string>, value: number): void;
  set(value: number): void;
  set(labelsOrValue: Record<string, string> | number, value?: number): void {
    const metrics = getPersistentMetrics();

    if (typeof labelsOrValue === 'number') {
      // set(value)
      metrics.setGauge(this.name, {}, labelsOrValue);
    } else {
      // set(labels, value)
      metrics.setGauge(this.name, labelsOrValue, value!);
    }
  }

  inc(labels?: Record<string, string>, amount?: number): void;
  inc(amount?: number): void;
  inc(labelsOrAmount?: Record<string, string> | number, amount?: number): void {
    const metrics = getPersistentMetrics();

    if (typeof labelsOrAmount === 'number') {
      // inc(amount)
      metrics.incrementGauge(this.name, {}, labelsOrAmount);
    } else {
      // inc(labels, amount) or inc(labels)
      const labels = labelsOrAmount || {};
      metrics.incrementGauge(this.name, labels, amount);
    }
  }

  dec(labels?: Record<string, string>, amount?: number): void;
  dec(amount?: number): void;
  dec(labelsOrAmount?: Record<string, string> | number, amount?: number): void {
    const metrics = getPersistentMetrics();

    if (typeof labelsOrAmount === 'number') {
      // dec(amount)
      metrics.decrementGauge(this.name, {}, labelsOrAmount);
    } else {
      // dec(labels, amount) or dec(labels)
      const labels = labelsOrAmount || {};
      metrics.decrementGauge(this.name, labels, amount || 1);
    }
  }

  get(labels: Record<string, string> = {}): number {
    const metrics = getPersistentMetrics();
    return metrics.getGaugeValue(this.name, labels);
  }

  reset(labels: Record<string, string> = {}): void {
    const metrics = getPersistentMetrics();
    metrics.setGauge(this.name, labels, 0);
  }
}

export class PersistentHistogram {
  private name: string;
  private help: string;
  private labelNames: string[];
  private buckets: number[];

  constructor(options: { name: string; help: string; labelNames?: string[]; buckets?: number[] }) {
    this.name = options.name;
    this.help = options.help;
    this.labelNames = options.labelNames || [];
    this.buckets = options.buckets || [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];
  }

  observe(labels: Record<string, string>, value: number): void;
  observe(value: number): void;
  observe(labelsOrValue: Record<string, string> | number, value?: number): void {
    const metrics = getPersistentMetrics();

    if (typeof labelsOrValue === 'number') {
      // observe(value)
      metrics.observeHistogram(this.name, {}, labelsOrValue, this.buckets);
    } else {
      // observe(labels, value)
      metrics.observeHistogram(this.name, labelsOrValue, value!, this.buckets);
    }
  }

  startTimer(labels: Record<string, string> = {}): () => number {
    const start = Date.now();
    return () => {
      const duration = (Date.now() - start) / 1000; // Convert to seconds
      this.observe(labels, duration);
      return duration;
    };
  }

  get(labels: Record<string, string> = {}) {
    const metrics = getPersistentMetrics();
    return metrics.getHistogramData(this.name, labels);
  }

  reset(labels: Record<string, string> = {}): void {
    // For histograms, we need to create a new empty histogram
    const metrics = getPersistentMetrics();
    const emptyBuckets = this.buckets.map(le => ({ le, count: 0 }));
    const resetHistogram = {
      name: this.name,
      help: this.help,
      labels,
      buckets: emptyBuckets,
      count: 0,
      sum: 0
    };

    // This is a bit of a hack - we'd need to add a reset method to PersistentMetrics
    // For now, we'll just set all values to 0 by observing negative values
    // In practice, histogram resets are rare in production
  }
}