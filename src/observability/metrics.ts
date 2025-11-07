import { register, collectDefaultMetrics } from 'prom-client';
import { appConfig } from '../config/index.js';
import {
  PersistentCounter,
  PersistentGauge,
  PersistentHistogram,
  initializePersistentMetrics,
  closePersistentMetrics,
  getPersistentMetrics
} from '../utils/persistent-metric-wrappers.js';

if (appConfig.observability.enableMetrics) {
  collectDefaultMetrics();
}

// Initialize persistent metrics
export async function initializeMetrics(): Promise<void> {
  await initializePersistentMetrics('./data/metrics.json');
}

export async function closeMetrics(): Promise<void> {
  await closePersistentMetrics();
}

// Persistent metrics with same API as Prometheus metrics
export const httpRequestsTotal = new PersistentCounter({
  name: 'http_requests_total',
  help: 'Total number of HTTP requests',
  labelNames: ['method', 'route', 'status_code'],
});

export const httpRequestDuration = new PersistentHistogram({
  name: 'http_request_duration_seconds',
  help: 'Duration of HTTP requests in seconds',
  labelNames: ['method', 'route'],
  buckets: [0.001, 0.005, 0.015, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1],
});

export const embeddingOperationDuration = new PersistentHistogram({
  name: 'embedding_operation_duration_seconds',
  help: 'Duration of embedding operations in seconds',
  labelNames: ['operation'],
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
});

// Note: vectorCount is handled separately by PersistentCounter in embedding-service.ts
// Keeping this for backward compatibility but it won't be used
export const vectorCount = new PersistentGauge({
  name: 'vector_count_total',
  help: 'Total number of vectors stored',
});

export const searchAccuracy = new PersistentHistogram({
  name: 'search_accuracy_score',
  help: 'Search accuracy scores',
  buckets: [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0],
});

export const activeConnections = new PersistentGauge({
  name: 'active_connections',
  help: 'Number of active connections',
});

export const errorRate = new PersistentCounter({
  name: 'errors_total',
  help: 'Total number of errors',
  labelNames: ['type', 'operation'],
});

export const metricsRegister = register;

export { register, getPersistentMetrics };