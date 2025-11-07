import { config } from 'dotenv';
import { z } from 'zod';
import type { AppConfig } from '../types/index.js';

config();

const envSchema = z.object({
  PORT: z.string().default('3000').transform(Number),
  HOST: z.string().default('0.0.0.0'),
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),

  // Vector store configuration
  VECTOR_STORE: z.enum(['corenn', 'qdrant']).default('corenn'),
  VECTOR_DIMENSION: z.string().default('1024').transform(Number),

  // CoreNN-specific configuration
  CORENN_DB_PATH: z.string().default('./data/embeddings.db'),
  INDEX_TYPE: z.string().default('hnsw'),

  // Qdrant-specific configuration
  QDRANT_URL: z.string().default('http://localhost:6333'),
  QDRANT_API_KEY: z.string().optional(),
  QDRANT_PORT: z.string().default('6333').transform(Number),
  QDRANT_COLLECTION_NAME: z.string().default('embeddings'),
  QDRANT_TIMEOUT: z.string().default('30000').transform(Number),

  ENABLE_METRICS: z.string().default('true').transform(val => val === 'true'),
  METRICS_PORT: z.string().default('9090').transform(Number),
  ENABLE_TRACING: z.string().default('true').transform(val => val === 'true'),
  JAEGER_ENDPOINT: z.string().optional(),
  LOG_LEVEL: z.enum(['debug', 'info', 'warn', 'error']).default('info'),

  RATE_LIMIT_MAX: z.string().default('100').transform(Number),
  RATE_LIMIT_WINDOW: z.string().default('60000').transform(Number),

  CORS_ORIGIN: z.string().default('*'),
  HELMET_ENABLED: z.string().default('true').transform(val => val === 'true'),

  EMBEDDING_GENERATION_ENABLED: z.string().default('false').transform(val => val === 'true'),
  EMBEDDING_PROVIDER: z.enum(['deepinfra', 'openai', 'local']).default('deepinfra'),
  EMBEDDING_MODEL: z.string().default('Qwen/Qwen3-Embedding-4B'),
  EMBEDDING_API_KEY: z.string().optional(),
  EMBEDDING_ENDPOINT: z.string().optional(),
  EMBEDDING_TIMEOUT: z.string().default('30000').transform(Number),
  EMBEDDING_RETRIES: z.string().default('2').transform(Number),
  EMBEDDING_STORAGE_ENABLED: z.string().default('true').transform(val => val === 'true'),
  EMBEDDING_STORAGE_PATH: z.string().default('./data/embedding-calls'),
  
  // Queue performance settings
  QUEUE_MAX_PARALLEL_FILES: z.string().default('5').transform(Number),
  QUEUE_INSERT_CHUNK_SIZE: z.string().default('1000').transform(Number),
  QUEUE_MAX_FILES_RETAINED: z.string().default('100').transform(Number),
});

const env = envSchema.parse(process.env);

export const appConfig: AppConfig = {
  server: {
    port: env.PORT,
    host: env.HOST,
    environment: env.NODE_ENV,
  },
  database: {
    type: env.VECTOR_STORE,
    dimension: env.VECTOR_DIMENSION,
    // CoreNN-specific
    path: env.CORENN_DB_PATH,
    indexType: env.INDEX_TYPE,
    // Qdrant-specific
    qdrant: env.VECTOR_STORE === 'qdrant' ? {
      url: env.QDRANT_URL,
      apiKey: env.QDRANT_API_KEY,
      port: env.QDRANT_PORT,
      collectionName: env.QDRANT_COLLECTION_NAME,
      timeout: env.QDRANT_TIMEOUT,
    } : undefined,
  },
  observability: {
    enableMetrics: env.ENABLE_METRICS,
    metricsPort: env.METRICS_PORT,
    enableTracing: env.ENABLE_TRACING,
    jaegerEndpoint: env.JAEGER_ENDPOINT,
    logLevel: env.LOG_LEVEL,
  },
  rateLimit: {
    max: env.RATE_LIMIT_MAX,
    windowMs: env.RATE_LIMIT_WINDOW,
  },
  security: {
    corsOrigin: env.CORS_ORIGIN,
    helmetEnabled: env.HELMET_ENABLED,
  },
  embeddingGeneration: {
    enabled: env.EMBEDDING_GENERATION_ENABLED,
    storage: {
      enabled: env.EMBEDDING_STORAGE_ENABLED,
      path: env.EMBEDDING_STORAGE_PATH,
    },
    provider: {
      provider: env.EMBEDDING_PROVIDER,
      model: env.EMBEDDING_MODEL,
      apiKey: env.EMBEDDING_API_KEY,
      endpoint: env.EMBEDDING_ENDPOINT,
      timeout: env.EMBEDDING_TIMEOUT,
      retries: env.EMBEDDING_RETRIES,
    },
    queue: {
      maxParallelFiles: env.QUEUE_MAX_PARALLEL_FILES,
      insertChunkSize: env.QUEUE_INSERT_CHUNK_SIZE,
      maxFilesRetained: env.QUEUE_MAX_FILES_RETAINED,
    },
  },
};

export default appConfig;