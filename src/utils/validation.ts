import { z } from 'zod';

export const EmbeddingVectorSchema = z.object({
  key: z.string().min(1).max(255),
  vector: z.array(z.number()).min(1),
  metadata: z.record(z.any()).optional(),
});

export const SearchQuerySchema = z.object({
  vector: z.array(z.number()).min(1).optional(),
  searchTerm: z.string().min(1).optional(),
  k: z.number().int().min(1).max(1000).default(10),
  threshold: z.number().min(0).max(1).optional(),
  filter: z.record(z.any()).optional(), // Metadata filter for advanced search (Qdrant)
}).refine(
  (data) => data.vector || data.searchTerm,
  {
    message: "Either 'vector' or 'searchTerm' must be provided",
    path: ["vector", "searchTerm"],
  }
).refine(
  (data) => !(data.vector && data.searchTerm),
  {
    message: "Cannot provide both 'vector' and 'searchTerm' at the same time",
    path: ["vector", "searchTerm"],
  }
);

export const BulkInsertSchema = z.object({
  embeddings: z.array(EmbeddingVectorSchema).min(1).max(10000),
});

export const BulkDeleteSchema = z.object({
  keys: z.array(z.string().min(1)).min(1).max(10000),
});

export const EmbeddingGenerationItemSchema = z.object({
  key: z.string().min(1).max(255),
  content: z.string().min(1),
  contentType: z.literal('text'),
  metadata: z.record(z.any()).optional(),
});

export const GenerateEmbeddingsSchema = z.object({
  items: z.array(EmbeddingGenerationItemSchema).min(1).max(1000),
});

export const validateVector = (vector: number[], expectedDimension: number): void => {
  if (vector.length !== expectedDimension) {
    throw new Error(`Vector dimension mismatch. Expected ${expectedDimension}, got ${vector.length}`);
  }

  if (vector.some(val => !Number.isFinite(val))) {
    throw new Error('Vector contains invalid values (NaN or Infinity)');
  }
};

export const normalizeVector = (vector: number[]): Float32Array => {
  const magnitude = Math.sqrt(vector.reduce((sum, val) => sum + val * val, 0));

  if (magnitude === 0) {
    throw new Error('Cannot normalize zero vector');
  }

  return new Float32Array(vector.map(val => val / magnitude));
};

export type EmbeddingVectorInput = z.infer<typeof EmbeddingVectorSchema>;
export type SearchQueryInput = z.infer<typeof SearchQuerySchema>;
export type BulkInsertInput = z.infer<typeof BulkInsertSchema>;
export type BulkDeleteInput = z.infer<typeof BulkDeleteSchema>;
export type EmbeddingGenerationItemInput = z.infer<typeof EmbeddingGenerationItemSchema>;
export type GenerateEmbeddingsInput = z.infer<typeof GenerateEmbeddingsSchema>;