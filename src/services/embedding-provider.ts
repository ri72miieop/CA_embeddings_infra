import type {
  EmbeddingGenerationItem,
  GeneratedEmbedding,
  EmbeddingProviderConfig,
  StoredEmbeddingCall
} from '../types/index.js';

export abstract class EmbeddingProvider {
  protected config: EmbeddingProviderConfig;

  constructor(config: EmbeddingProviderConfig) {
    this.config = config;
  }

  abstract generateEmbeddings(items: EmbeddingGenerationItem[]): Promise<GeneratedEmbedding[]>;

  abstract validateConfiguration(): void;

  protected createStorageRecord(
    items: EmbeddingGenerationItem[],
    latencyMs: number,
    success: boolean,
    error?: string
  ): StoredEmbeddingCall {
    const totalTokens = items.reduce((sum, item) => {
      return sum + (typeof item.content === 'string' ? item.content.length : 0);
    }, 0);

    return {
      id: crypto.randomUUID(),
      timestamp: new Date(),
      provider: this.config.provider,
      model: this.config.model,
      inputTokens: Math.ceil(totalTokens / 4), // Rough token estimation
      outputDimension: 1024, // Will be updated by actual response
      latencyMs,
      success,
      error,
      metadata: {
        itemCount: items.length,
        endpoint: this.config.endpoint
      }
    };
  }
}