import { EmbeddingProvider } from './embedding-provider.js';
import type {
  EmbeddingGenerationItem,
  GeneratedEmbedding,
  EmbeddingProviderConfig
} from '../types/index.js';
import { createContextLogger } from '../observability/logger.js';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import { createDeepInfra } from '@ai-sdk/deepinfra';
import { embedMany } from 'ai';

const tracer = trace.getTracer('deepinfra-provider');

export class DeepInfraProvider extends EmbeddingProvider {
  constructor(config: EmbeddingProviderConfig) {
    super(config);
    this.validateConfiguration();
  }

  validateConfiguration(): void {
    if (!this.config.apiKey) {
      throw new Error('DeepInfra API key is required');
    }
    if (!this.config.model) {
      throw new Error('Model name is required');
    }
  }

  async generateEmbeddings(items: EmbeddingGenerationItem[]): Promise<GeneratedEmbedding[]> {
    const span = tracer.startSpan('deepinfra_generate_embeddings');
    const contextLogger = createContextLogger({
      operation: 'generate_embeddings',
      provider: 'deepinfra',
      count: items.length
    });

    const startTime = Date.now();

    try {
      contextLogger.info('Starting embedding generation with DeepInfra');

      // Filter text items (images not supported yet)
      const textItems = items.filter(item => item.contentType === 'text');
      if (textItems.length !== items.length) {
        contextLogger.warn({
          totalItems: items.length,
          textItems: textItems.length
        }, 'Some items were skipped (only text content supported)');
      }

      if (textItems.length === 0) {
        throw new Error('No valid text items to process');
      }

      const textInputs = textItems.map(item => item.content as string);

      // Use ai-sdk with DeepInfra provider with custom fetch to add missing parameters
      const customFetch: any = async (url: any, options: any) => {
        // Modify the request to include dimensions and normalize parameters
        if (options?.body) {
          const body = JSON.parse(options.body as string);

          // Add the missing parameters that ai-sdk doesn't pass through
          body.dimensions = 1024;
          body.normalize = true;

          // Update the options with modified body
          options.body = JSON.stringify(body);

          contextLogger.info({
            url: url.toString(),
            method: options?.method,
            bodyPreview: JSON.stringify(body).substring(0, 200)
          }, 'Making DeepInfra API request with corrected parameters');
        }

        // Make the actual request
        return fetch(url, options);
      };

      const provider = createDeepInfra({
        apiKey: this.config.apiKey!,
        baseURL: this.config.endpoint || undefined,
        fetch: customFetch
      });

      const { embeddings, usage } = await embedMany({
        model: provider.textEmbeddingModel(this.config.model),
        values: textInputs
      });

      const results: GeneratedEmbedding[] = textItems.map((item, index) => {
        const embedding = embeddings[index];
        if (!embedding) {
          throw new Error(`Missing embedding for item at index ${index}`);
        }

        return {
          key: item.key,
          vector: embedding,
          metadata: {
            ...item.metadata,
            provider: 'deepinfra',
            model: this.config.model,
            tokens: Math.ceil((item.content as string).length / 4)
          }
        };
      });

      const latencyMs = Date.now() - startTime;
      contextLogger.info({
        generated: results.length,
        latencyMs,
        totalTokens: usage?.tokens || textInputs.reduce((sum, text) => sum + Math.ceil(text.length / 4), 0)
      }, 'Embedding generation completed successfully');

      span.setStatus({ code: SpanStatusCode.OK });
      return results;

    } catch (error) {
      const latencyMs = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';

      contextLogger.error({
        error: errorMessage,
        latencyMs
      }, 'Failed to generate embeddings');

      span.recordException(error as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: errorMessage });
      throw error;
    } finally {
      span.end();
    }
  }

}