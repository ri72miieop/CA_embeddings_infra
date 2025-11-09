import type { FastifyRequest, FastifyReply } from 'fastify';
import { appConfig } from '../config/index.js';

export const apiKeyAuthMiddleware = async (request: FastifyRequest, reply: FastifyReply): Promise<void> => {
  const { apiKeys } = appConfig.security;

  // If no API keys are configured, reject the request with a configuration error
  if (!apiKeys || apiKeys.length === 0) {
    reply.code(500).send({
      error: 'Configuration Error',
      message: 'API key authentication is required but no API keys are configured. Please configure API_KEYS environment variable.',
    });
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
