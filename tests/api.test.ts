import { describe, it, expect, beforeAll, afterAll } from 'bun:test';
import { createServer } from '../src/server';
import type { FastifyInstance } from 'fastify';

describe('API Endpoints', () => {
  let app: FastifyInstance;

  beforeAll(async () => {
    process.env.NODE_ENV = 'test';
    process.env.VECTOR_STORE = 'qdrant';
    process.env.QDRANT_URL = 'http://localhost:6333';
    process.env.QDRANT_COLLECTION_NAME = 'test-embeddings';
    process.env.VECTOR_DIMENSION = '3';
    process.env.ENABLE_TRACING = 'false';

    const { fastify } = await createServer();
    app = fastify;
  });

  afterAll(async () => {
    await app.close();
  });

  it('should respond to health checks', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/health',
    });

    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.body);
    expect(body).toHaveProperty('status');
    expect(body).toHaveProperty('timestamp');
  });

  it('should provide metrics endpoint', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/metrics',
    });

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/plain');
  });

  it('should insert embeddings', async () => {
    const payload = {
      embeddings: [
        {
          key: 'test-embed-1',
          vector: [1, 0, 0],
        },
        {
          key: 'test-embed-2',
          vector: [0, 1, 0],
        },
      ],
    };

    const response = await app.inject({
      method: 'POST',
      url: '/embeddings',
      payload,
    });

    expect(response.statusCode).toBe(201);
    const body = JSON.parse(response.body);
    expect(body.success).toBe(true);
    expect(body.inserted).toBe(2);
  });

  it('should search embeddings', async () => {
    const payload = {
      vector: [1, 0, 0],
      k: 5,
    };

    const response = await app.inject({
      method: 'POST',
      url: '/embeddings/search',
      payload,
    });

    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.body);
    expect(body.success).toBe(true);
    expect(Array.isArray(body.results)).toBe(true);
  });

  it('should get database stats', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/embeddings/stats',
    });

    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.body);
    expect(body).toHaveProperty('vectorCount');
    expect(body).toHaveProperty('dbSize');
    expect(body).toHaveProperty('dimension');
  });

  it('should validate input data', async () => {
    const invalidPayload = {
      embeddings: [
        {
          key: '',
          vector: [],
        },
      ],
    };

    const response = await app.inject({
      method: 'POST',
      url: '/embeddings',
      payload: invalidPayload,
    });

    expect(response.statusCode).toBe(400);
  });
});