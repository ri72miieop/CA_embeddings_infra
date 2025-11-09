import type { FastifyRequest, FastifyReply } from 'fastify';
import { appConfig } from '../config/index.js';

export const apiKeyAuthMiddleware = async (request: FastifyRequest, reply: FastifyReply): Promise<void> => {
  const { apiKeys } = appConfig.security;

  // If no API keys are configured, allow all requests
  if (!apiKeys || apiKeys.length === 0) {
    return;
  }

  // Extract API key from headers
  // Support both Authorization: Bearer <key> and X-API-Key: <key>
  const authHeader = request.headers.authorization;
  const apiKeyHeader = request.headers['x-api-key'] as string | undefined;

  let providedKey: string | undefined;

  if (authHeader && authHeader.startsWith('Bearer ')) {
    providedKey = authHeader.substring(7);
  } else if (apiKeyHeader) {
    providedKey = apiKeyHeader;
  }

  // Check if the provided key is valid
  if (!providedKey || !apiKeys.includes(providedKey)) {
    reply.code(401).send({
      error: 'Unauthorized',
      message: 'Invalid or missing API key',
    });
    return;
  }

  // API key is valid, continue
};
