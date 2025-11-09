export interface EmbeddingVector {
  key: string;
  vector: Float32Array;
  metadata?: Record<string, any>;
}

export interface SearchResult {
  key: string;
  distance: number;
  metadata?: Record<string, any>;
}

export interface SearchQuery {
  vector: Float32Array;
  k: number;
  threshold?: number;
  filter?: Record<string, any>; // Metadata filter for advanced search (Qdrant)
}

export type VectorStoreType = 'qdrant';

export interface QdrantConfig {
  url: string;
  apiKey?: string;
  port: number;
  collectionName: string;
  timeout?: number;
}

export interface DatabaseConfig {
  type: VectorStoreType;
  dimension: number;
  // Qdrant-specific config
  qdrant?: QdrantConfig;
}

export interface ServerConfig {
  port: number;
  host: string;
  environment: 'development' | 'production' | 'test';
}

export interface ObservabilityConfig {
  enableMetrics: boolean;
  metricsPort: number;
  enableTracing: boolean;
  jaegerEndpoint?: string;
  logLevel: 'debug' | 'info' | 'warn' | 'error';
}

export interface RateLimitConfig {
  max: number;
  windowMs: number;
}

export interface SecurityConfig {
  corsOrigin: string;
  helmetEnabled: boolean;
  apiKeys: string[];
}

export interface EmbeddingProviderConfig {
  provider: 'deepinfra' | 'openai' | 'local';
  apiKey?: string;
  model: string;
  endpoint?: string;
  timeout?: number;
  retries?: number;
}

export interface EmbeddingGenerationConfig {
  enabled: boolean;
  storage: {
    enabled: boolean;
    path?: string;
  };
  provider: EmbeddingProviderConfig;
  queue?: {
    maxParallelFiles?: number;
    insertChunkSize?: number;
    maxFilesRetained?: number;
  };
}

export interface GenerateEmbeddingsRequest {
  items: EmbeddingGenerationItem[];
}

export interface EmbeddingGenerationItem {
  key: string;
  content: string | ImageContent;
  contentType: 'text' | 'image';
  metadata?: Record<string, any>;
}

export interface ImageContent {
  data: Buffer;
  mimeType: string;
}

export interface GenerateEmbeddingsResponse {
  success: boolean;
  results?: GeneratedEmbedding[];
  error?: string;
  generated: number;
}

export interface GeneratedEmbedding {
  key: string;
  vector: number[];
  metadata?: Record<string, any>;
}

export interface EmbeddingProviderResponse {
  embeddings: Array<{
    embedding: number[];
    index: number;
  }>;
  usage: {
    total_tokens: number;
    prompt_tokens: number;
  };
}

export interface StoredEmbeddingCall {
  id: string;
  timestamp: Date;
  provider: string;
  model: string;
  inputTokens: number;
  outputDimension: number;
  latencyMs: number;
  success: boolean;
  error?: string;
  metadata?: Record<string, any>;
}

export interface AppConfig {
  server: ServerConfig;
  database: DatabaseConfig;
  observability: ObservabilityConfig;
  rateLimit: RateLimitConfig;
  security: SecurityConfig;
  embeddingGeneration?: EmbeddingGenerationConfig;
}