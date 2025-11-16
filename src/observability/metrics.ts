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

// Vector store operation counters
export const vectorStoreOperationsTotal = new PersistentCounter({
  name: 'vector_store_operations_total',
  help: 'Total vector store operations by type and status',
  labelNames: ['operation', 'status'], // operation: insert/search/delete, status: success/failure
});

export const vectorStoreItemsProcessed = new PersistentCounter({
  name: 'vector_store_items_processed_total',
  help: 'Total items processed by operation type',
  labelNames: ['operation'], // operation: insert/search/delete
});

export const vectorStoreBatchSize = new PersistentHistogram({
  name: 'vector_store_batch_size',
  help: 'Size of batches processed',
  labelNames: ['operation'],
  buckets: [1, 10, 50, 100, 500, 1000, 5000, 10000],
});

// Search quality metrics
export const searchResultsCount = new PersistentHistogram({
  name: 'search_results_count',
  help: 'Number of results returned per search',
  buckets: [0, 1, 5, 10, 25, 50, 100, 500, 1000],
});

export const embeddingDimension = new PersistentGauge({
  name: 'embedding_dimension_size',
  help: 'Configured embedding dimension size',
});

// Collection statistics
export const collectionVectorCount = new PersistentGauge({
  name: 'collection_vector_count_total',
  help: 'Total vectors in collection',
  labelNames: ['collection'],
});

export const collectionIndexedPercentage = new PersistentGauge({
  name: 'collection_indexed_percentage',
  help: 'Percentage of vectors indexed',
  labelNames: ['collection'],
});

// Queue metrics
export const queueDepth = new PersistentGauge({
  name: 'embedding_queue_depth',
  help: 'Current number of items in embedding queue',
});

export const queueProcessingRate = new PersistentCounter({
  name: 'embedding_queue_processed_total',
  help: 'Total embeddings processed from queue',
  labelNames: ['status'], // success/failure
});

export const queueFileCount = new PersistentGauge({
  name: 'embedding_queue_items',
  help: 'Number of queue items by status',
  labelNames: ['status'], // pending/processing/completed/failed/exported
});

export const queueRetryCount = new PersistentCounter({
  name: 'embedding_queue_retries_total',
  help: 'Total number of queue item retries',
});

// Parquet export metrics
export const parquetBatchCount = new PersistentGauge({
  name: 'embedding_queue_parquet_batches_total',
  help: 'Total number of Parquet batches created',
});

export const parquetExportSize = new PersistentCounter({
  name: 'embedding_queue_parquet_export_bytes_total',
  help: 'Total size of exported Parquet files in bytes',
});

export const metricsRegister = register;

export { register, getPersistentMetrics };