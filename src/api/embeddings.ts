import type { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import type { IVectorStore } from '../interfaces/vector-store.interface.js';
import {
  BulkInsertSchema,
  BulkDeleteSchema,
  SearchQuerySchema,
  validateVector,
  normalizeVector,
  type BulkInsertInput,
  type BulkDeleteInput,
  type SearchQueryInput
} from '../utils/validation.js';
import { createContextLogger } from '../observability/logger.js';
import { appConfig } from '../config/index.js';
import { apiKeyAuthMiddleware } from '../middleware/index.js';

interface EmbeddingRoutes {
  embeddingService: IVectorStore;
}

const embeddingRoutes: FastifyPluginAsync<EmbeddingRoutes> = async (fastify, { embeddingService }) => {

  fastify.post<{
    Body: BulkInsertInput;
  }>('/embeddings', {
    preHandler: apiKeyAuthMiddleware
  }, async (request: FastifyRequest<{ Body: BulkInsertInput }>, reply: FastifyReply) => {
    const correlationId = (request as any).correlationId;
    const contextLogger = createContextLogger({
      correlationId,
      operation: 'bulk_insert',
      count: request.body.embeddings.length
    });

    try {
      contextLogger.info('Processing bulk insert request');

      const embeddings = request.body.embeddings.map(emb => {
        validateVector(emb.vector, appConfig.database.dimension);

        return {
          key: emb.key,
          vector: new Float32Array(emb.vector),
          metadata: emb.metadata,
        };
      });

      await embeddingService.insert(embeddings);

      reply.status(201).send({
        success: true,
        message: 'Embeddings inserted successfully',
        inserted: embeddings.length,
      });

      contextLogger.info('Bulk insert completed successfully');
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Bulk insert failed');

      reply.status(400).send({
        success: false,
        error: errorMessage,
      });
    }
  });

  fastify.post<{
    Body: SearchQueryInput;
  }>('/embeddings/search', async (request: FastifyRequest<{ Body: SearchQueryInput }>, reply: FastifyReply) => {
    const correlationId = (request as any).correlationId;
    const contextLogger = createContextLogger({
      correlationId,
      operation: 'search',
      k: request.body.k,
      hasVector: !!request.body.vector,
      hasSearchTerm: !!request.body.searchTerm
    });

    try {
      contextLogger.info('Processing search request');

      let queryVector: Float32Array;

      if (request.body.vector) {
        // Direct vector search
        validateVector(request.body.vector, appConfig.database.dimension);
        queryVector = new Float32Array(request.body.vector);
        contextLogger.info('Using provided vector for search');
      } else if (request.body.searchTerm) {
        // Text-based search - generate embedding first
        contextLogger.info('Generating embedding for search term');

        // Get embedding generation service from the context
        const embeddingGenerationService = (fastify as any).embeddingGenerationService;
        if (!embeddingGenerationService) {
          throw new Error('Embedding generation service not available. Please ensure it is enabled in the server configuration.');
        }

        // Generate embedding for the search term
        const generationResults = await embeddingGenerationService.generateEmbeddings([{
          key: `search_query_${Date.now()}`,
          content: request.body.searchTerm,
          contentType: 'text' as const,
          metadata: { type: 'search_query' }
        }]);

        if (generationResults.length === 0) {
          throw new Error('Failed to generate embedding for search term');
        }

        queryVector = new Float32Array(generationResults[0].vector);
        contextLogger.info('Embedding generated successfully for search term');
      } else {
        throw new Error('Either vector or searchTerm must be provided');
      }

      const results = await embeddingService.search({
        vector: queryVector,
        k: request.body.k || 10,
        threshold: request.body.threshold || 0.65,
        filter: request.body.filter,
      });

      reply.send({
        success: true,
        results,
        count: results.length,
      });

      contextLogger.info({ resultCount: results.length }, 'Search completed successfully');
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Search failed');

      reply.status(400).send({
        success: false,
        error: errorMessage,
      });
    }
  });

  fastify.delete<{
    Body: BulkDeleteInput;
  }>('/embeddings', {
    preHandler: apiKeyAuthMiddleware
  }, async (request: FastifyRequest<{ Body: BulkDeleteInput }>, reply: FastifyReply) => {
    const correlationId = (request as any).correlationId;
    const contextLogger = createContextLogger({
      correlationId,
      operation: 'bulk_delete',
      count: request.body.keys.length
    });

    try {
      contextLogger.info('Processing bulk delete request');

      await embeddingService.delete(request.body.keys);

      reply.send({
        success: true,
        message: 'Embeddings deleted successfully',
        deleted: request.body.keys.length,
      });

      contextLogger.info('Bulk delete completed successfully');
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Bulk delete failed');

      reply.status(400).send({
        success: false,
        error: errorMessage,
      });
    }
  });

  fastify.get<{
    Params: { key: string };
  }>('/embeddings/exists/:key', async (request: FastifyRequest<{ Params: { key: string } }>, reply: FastifyReply) => {
    const correlationId = (request as any).correlationId;
    const { key } = request.params;
    const contextLogger = createContextLogger({
      correlationId,
      operation: 'exists',
      key
    });

    try {
      contextLogger.debug('Checking if embedding exists');

      // Check if the vector store has an exists method (Qdrant does)
      if ('exists' in embeddingService && typeof (embeddingService as any).exists === 'function') {
        const exists = await (embeddingService as any).exists(key);
        
        reply.send({
          success: true,
          exists,
          key,
        });

        contextLogger.debug({ exists }, 'Existence check completed');
      } else {
        throw new Error('Exists operation not supported by current vector store');
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to check existence');

      reply.status(500).send({
        success: false,
        error: errorMessage,
      });
    }
  });

  fastify.get('/embeddings/stats', async (request: FastifyRequest, reply: FastifyReply) => {
    const correlationId = (request as any).correlationId;
    const contextLogger = createContextLogger({
      correlationId,
      operation: 'stats'
    });

    try {
      contextLogger.info('Getting database statistics');

      const stats = await embeddingService.getStats();

      reply.send({
        ...stats,
        dimension: appConfig.database.dimension,
      });

      contextLogger.info('Statistics retrieved successfully');
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Failed to get statistics');

      reply.status(500).send({
        success: false,
        error: errorMessage,
      });
    }
  });
};

export default embeddingRoutes;