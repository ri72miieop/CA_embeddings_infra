import { describe, it, expect, beforeAll, afterAll } from 'bun:test';
import { createVectorStore } from '../src/factories/vector-store.factory';
import type { IVectorStore } from '../src/interfaces/vector-store.interface';
import type { DatabaseConfig } from '../src/types';

describe('EmbeddingService', () => {
  let service: IVectorStore;
  const config: DatabaseConfig = {
    type: 'qdrant',
    dimension: 3,
    qdrant: {
      url: 'http://localhost:6333',
      port: 6333,
      collectionName: 'test-embeddings',
    },
  };

  beforeAll(async () => {
    service = createVectorStore(config);
    await service.initialize();
  });

  afterAll(async () => {
    await service.close();
  });

  it('should initialize successfully', () => {
    expect(service).toBeDefined();
  });

  it('should insert embeddings', async () => {
    const embeddings = [
      {
        key: 'test1',
        vector: new Float32Array([1, 0, 0]),
      },
      {
        key: 'test2',
        vector: new Float32Array([0, 1, 0]),
      },
    ];

    await expect(service.insert(embeddings)).resolves.not.toThrow();
  });

  it('should search for similar embeddings', async () => {
    const query = {
      vector: new Float32Array([1, 0, 0]),
      k: 2,
    };

    const results = await service.search(query);

    expect(results).toBeDefined();
    expect(Array.isArray(results)).toBe(true);
    expect(results.length).toBeGreaterThan(0);
    expect(results[0]).toHaveProperty('key');
    expect(results[0]).toHaveProperty('distance');
  });

  it('should delete embeddings', async () => {
    const keys = ['test1'];

    await expect(service.delete(keys)).resolves.not.toThrow();
  });

  it('should get database statistics', async () => {
    const stats = await service.getStats();

    expect(stats).toBeDefined();
    expect(stats).toHaveProperty('vectorCount');
    expect(stats).toHaveProperty('dbSize');
    expect(typeof stats.vectorCount).toBe('number');
    expect(typeof stats.dbSize).toBe('string');
  });
});