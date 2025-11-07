import type { IVectorStore } from '../interfaces/vector-store.interface.js';
import type { DatabaseConfig } from '../types/index.js';
import { CoreNNVectorStore } from '../stores/corenn-vector-store.js';
import { QdrantVectorStore } from '../stores/qdrant-vector-store.js';

/**
 * Factory function to create the appropriate vector store implementation
 * based on the configuration
 * 
 * @param config - Database configuration with store type
 * @returns Instance of the appropriate vector store
 * @throws Error if vector store type is not supported
 */
export function createVectorStore(config: DatabaseConfig): IVectorStore {
  switch (config.type) {
    case 'corenn':
      return new CoreNNVectorStore(config);
    
    case 'qdrant':
      return new QdrantVectorStore(config);
    
    default:
      throw new Error(
        `Unsupported vector store type: ${config.type}. Supported types: corenn, qdrant`
      );
  }
}

