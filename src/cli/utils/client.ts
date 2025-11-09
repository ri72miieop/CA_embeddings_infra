interface EmbeddingItem {
  key: string;
  content: string;
  contentType: 'text';
  metadata?: Record<string, any>;
}

interface GenerateEmbeddingsResponse {
  success: boolean;
  results?: Array<{
    key: string;
    vector: number[];
    metadata?: Record<string, any>;
  }>;
  error?: string;
  generated: number;
}

interface SearchResult {
  key: string;
  distance: number;
  metadata?: Record<string, any>;
}

interface SearchResponse {
  success: boolean;
  results?: SearchResult[];
  error?: string;
  count: number;
}

export class CA_EmbedClient {
  private baseUrl: string;
  private apiKey?: string;

  constructor(baseUrl: string, apiKey?: string) {
    this.baseUrl = baseUrl.replace(/\/$/, ''); // Remove trailing slash
    this.apiKey = apiKey;
  }

  private getHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };

    if (this.apiKey) {
      headers['Authorization'] = `Bearer ${this.apiKey}`;
    }

    return headers;
  }

  async generateEmbeddings(items: EmbeddingItem[]): Promise<GenerateEmbeddingsResponse> {
    const response = await fetch(`${this.baseUrl}/embeddings/generate`, {
      method: 'POST',
      headers: this.getHeaders(),
      body: JSON.stringify({ items }),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    return response.json() as Promise<GenerateEmbeddingsResponse>;
  }

  async generateAndStoreEmbeddings(items: EmbeddingItem[]): Promise<GenerateEmbeddingsResponse> {
    const response = await fetch(`${this.baseUrl}/embeddings/generate-and-store`, {
      method: 'POST',
      headers: this.getHeaders(),
      body: JSON.stringify({ items }),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    return response.json() as Promise<GenerateEmbeddingsResponse>;
  }

  async searchEmbeddings(vector: number[], k: number = 10, threshold?: number): Promise<SearchResponse> {
    const body: any = { vector, k };
    if (threshold !== undefined) {
      body.threshold = threshold;
    }

    const response = await fetch(`${this.baseUrl}/embeddings/search`, {
      method: 'POST',
      headers: this.getHeaders(),
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    return response.json() as Promise<SearchResponse>;
  }

  async checkExistingKeys(keys: string[]): Promise<Set<string>> {
    // Note: The vector database doesn't provide an efficient way to check for existing keys
    // at scale (300k+ keys). The previous approach using vector search was causing HTTP 500
    // errors and was extremely inefficient. Instead, we'll return an empty set and rely on
    // the server's duplicate handling behavior.
    //
    // If you need true deduplication, consider:
    // 1. Adding a dedicated "check keys" endpoint to the server
    // 2. Using an external cache/database for key tracking
    // 3. Processing in smaller chunks with resume functionality

    console.log(`Skipping existing key check for ${keys.length} keys (using server-side duplicate handling)`);
    return new Set<string>();
  }

  async healthCheck(): Promise<boolean> {
    try {
      const response = await fetch(`${this.baseUrl}/health`);
      return response.ok;
    } catch {
      return false;
    }
  }

  async getStats(): Promise<any> {
    const response = await fetch(`${this.baseUrl}/embeddings/stats`);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    return response.json() as Promise<GenerateEmbeddingsResponse>;
  }
}