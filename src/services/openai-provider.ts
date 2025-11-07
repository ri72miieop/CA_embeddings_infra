import { EmbeddingProvider } from './embedding-provider.js';
import type {
  EmbeddingGenerationItem,
  GeneratedEmbedding,
  EmbeddingProviderConfig
} from '../types/index.js';
import { createContextLogger } from '../observability/logger.js';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import { createOpenAI } from '@ai-sdk/openai';
import { embedMany } from 'ai';

const tracer = trace.getTracer('openai-provider');

// AI Gateway endpoint - always routes through Vercel's AI Gateway
const AI_GATEWAY_ENDPOINT = 'https://ai-gateway.vercel.sh/v1';

export class OpenAIProvider extends EmbeddingProvider {
  constructor(config: EmbeddingProviderConfig) {
    super(config);
    this.validateConfiguration();
  }

  validateConfiguration(): void {
    if (!this.config.apiKey) {
      throw new Error('AI_GATEWAY_API_KEY is required');
    }
    if (!this.config.model) {
      throw new Error('Model name is required (e.g., openai/text-embedding-3-small)');
    }
  }

  async generateEmbeddings(items: EmbeddingGenerationItem[]): Promise<GeneratedEmbedding[]> {
    const span = tracer.startSpan('openai_generate_embeddings');
    const contextLogger = createContextLogger({
      operation: 'generate_embeddings',
      provider: 'openai',
      count: items.length
    });

    const startTime = Date.now();

    try {
      contextLogger.info('Starting embedding generation with OpenAI via AI Gateway');

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

      // Custom fetch to add dimensions parameter for text-embedding-3-* models
      const customFetch: any = async (url: any, options: any) => {
        if (options?.body) {
          const body = JSON.parse(options.body as string);

          // Add dimensions parameter to get 1024-dimensional embeddings
          // text-embedding-3-small: supports 512 or 1536 dimensions
          // text-embedding-3-large: supports 256, 1024, or 3072 dimensions
          body.dimensions = 1024;

          options.body = JSON.stringify(body);

          contextLogger.info({
            url: url.toString(),
            method: options?.method,
            model: body.model,
            dimensions: body.dimensions
          }, 'Making OpenAI API request via AI Gateway');
        }

        return fetch(url, options);
      };

      // Configure OpenAI provider to always use AI Gateway
      const provider = createOpenAI({
        apiKey: this.config.apiKey!,
        baseURL: AI_GATEWAY_ENDPOINT,
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
          metadata: item.metadata
        };
      });

      const latencyMs = Date.now() - startTime;

      contextLogger.info({
        generated: results.length,
        latencyMs,
        tokensUsed: usage?.tokens || 0,
        vectorDimension: embeddings[0]?.length || 0
      }, 'OpenAI embedding generation completed via AI Gateway');

      span.setStatus({ code: SpanStatusCode.OK });
      return results;

    } catch (error) {
      const latencyMs = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';

      contextLogger.error({
        error: errorMessage,
        latencyMs
      }, 'OpenAI embedding generation failed');

      span.recordException(error as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: errorMessage });

      throw error;
    } finally {
      span.end();
    }
  }
}
