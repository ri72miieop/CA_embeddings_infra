import type { FastifyRequest, FastifyReply } from 'fastify';
import { extractCorrelationId } from '../utils/correlation.js';

export const correlationMiddleware = async (request: FastifyRequest, reply: FastifyReply): Promise<void> => {
  const correlationId = extractCorrelationId(request.headers);

  request.headers['x-correlation-id'] = correlationId;
  reply.header('x-correlation-id', correlationId);

  (request as any).correlationId = correlationId;
};