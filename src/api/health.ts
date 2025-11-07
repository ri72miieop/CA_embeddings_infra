import type { FastifyPluginAsync, FastifyRequest, FastifyReply } from 'fastify';
import type { IVectorStore } from '../interfaces/vector-store.interface.js';
import { createContextLogger } from '../observability/logger.js';
import { register } from '../observability/metrics.js';

interface HealthRoutes {
  embeddingService: IVectorStore;
}

const healthRoutes: FastifyPluginAsync<HealthRoutes> = async (fastify, { embeddingService }) => {

  fastify.get('/health', async (request: FastifyRequest, reply: FastifyReply) => {
    const correlationId = (request as any).correlationId;
    const contextLogger = createContextLogger({
      correlationId,
      operation: 'health_check'
    });

    try {
      let databaseStatus = 'healthy';

      try {
        await embeddingService.getStats();
      } catch (error) {
        databaseStatus = 'unhealthy';
        contextLogger.warn({ error }, 'Database health check failed');
      }

      const health = {
        status: databaseStatus === 'healthy' ? 'healthy' : 'degraded',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        checks: {
          database: databaseStatus,
        },
      };

      const statusCode = health.status === 'healthy' ? 200 : 503;
      reply.status(statusCode).send(health);

      contextLogger.info({ status: health.status }, 'Health check completed');
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      contextLogger.error({ error: errorMessage }, 'Health check failed');

      reply.status(503).send({
        status: 'unhealthy',
        timestamp: new Date().toISOString(),
        error: errorMessage,
      });
    }
  });

  fastify.get('/health/readiness', async (request: FastifyRequest, reply: FastifyReply) => {
    try {
      await embeddingService.getStats();

      reply.send({
        ready: true,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      reply.status(503).send({
        ready: false,
        timestamp: new Date().toISOString(),
      });
    }
  });

  fastify.get('/health/liveness', async (request: FastifyRequest, reply: FastifyReply) => {
    reply.send({
      alive: true,
      timestamp: new Date().toISOString(),
    });
  });

  fastify.get('/metrics', async (request: FastifyRequest, reply: FastifyReply) => {
    const metrics = await register.metrics();
    reply.type('text/plain').send(metrics);
  });
};

export default healthRoutes;