import fs from 'fs/promises';
import path from 'path';

export interface EmbeddingRecord {
  key: string;
  vector: number[];
  metadata?: Record<string, any>;
  timestamp?: string;
}

export type ExportFormat = 'jsonl' | 'csv' | 'parquet' | 'npy';

export interface ExporterOptions {
  format: ExportFormat;
  outputPath: string;
  append: boolean;
  includeMetadata: boolean;
  batchSize?: number;
}

export class EmbeddingExporter {
  private options: ExporterOptions;
  private batchBuffer: EmbeddingRecord[] = [];

  constructor(options: ExporterOptions) {
    this.options = options;
  }

  async initialize(): Promise<void> {
    // Ensure output directory exists
    const dir = path.dirname(this.options.outputPath);
    await fs.mkdir(dir, { recursive: true });

    // If not appending, write headers for formats that need them
    if (!this.options.append) {
      await this.writeHeader();
    }
  }

  async addEmbedding(embedding: EmbeddingRecord): Promise<void> {
    this.batchBuffer.push({
      ...embedding,
      timestamp: embedding.timestamp || new Date().toISOString()
    });

    // Write batch if buffer is full
    if (this.options.batchSize && this.batchBuffer.length >= this.options.batchSize) {
      await this.flush();
    }
  }

  async addEmbeddings(embeddings: EmbeddingRecord[]): Promise<void> {
    // Batch push instead of iterating - much more efficient
    const timestampedEmbeddings = embeddings.map(embedding => ({
      ...embedding,
      timestamp: embedding.timestamp || new Date().toISOString()
    }));

    this.batchBuffer.push(...timestampedEmbeddings);

    // Write batch if buffer is full
    if (this.options.batchSize && this.batchBuffer.length >= this.options.batchSize) {
      await this.flush();
    }
  }

  async flush(): Promise<void> {
    if (this.batchBuffer.length === 0) return;

    switch (this.options.format) {
      case 'jsonl':
        await this.writeJsonLines();
        break;
      case 'csv':
        await this.writeCsv();
        break;
      case 'parquet':
        await this.writeParquet();
        break;
      case 'npy':
        await this.writeNumpy();
        break;
      default:
        throw new Error(`Unsupported format: ${this.options.format}`);
    }

    this.batchBuffer = [];
  }

  async finalize(): Promise<void> {
    await this.flush();
    await this.writeFooter();
  }

  private async writeHeader(): Promise<void> {
    switch (this.options.format) {
      case 'csv':
        const headers = ['key', 'timestamp'];

        // Add vector dimension headers
        if (this.batchBuffer.length > 0 && this.batchBuffer[0]) {
          const vectorLength = this.batchBuffer[0].vector.length;
          for (let i = 0; i < vectorLength; i++) {
            headers.push(`dim_${i}`);
          }
        }

        if (this.options.includeMetadata) {
          headers.push('metadata');
        }

        await fs.writeFile(this.options.outputPath, headers.join(',') + '\n');
        break;
    }
  }

  private async writeFooter(): Promise<void> {
    // Most formats don't need footers
  }

  private async writeJsonLines(): Promise<void> {
    const lines = this.batchBuffer.map(record => {
      const output: any = {
        key: record.key,
        vector: record.vector,
        timestamp: record.timestamp
      };

      if (this.options.includeMetadata && record.metadata) {
        output.metadata = record.metadata;
      }

      return JSON.stringify(output);
    }).join('\n') + '\n';

    await fs.appendFile(this.options.outputPath, lines);
  }

  private async writeCsv(): Promise<void> {
    const lines = this.batchBuffer.map(record => {
      const row = [
        `"${record.key}"`,
        `"${record.timestamp}"`
      ];

      // Add vector values
      row.push(...record.vector.map(v => v.toString()));

      // Add metadata as JSON string if requested
      if (this.options.includeMetadata && record.metadata) {
        row.push(`"${JSON.stringify(record.metadata).replace(/"/g, '""')}"`);
      }

      return row.join(',');
    }).join('\n') + '\n';

    await fs.appendFile(this.options.outputPath, lines);
  }

  private async writeParquet(): Promise<void> {
    // For now, we'll implement a simple approach
    // In a full implementation, you'd use a proper parquet library
    throw new Error('Parquet export not yet implemented. Use JSONL format for now.');
  }

  private async writeNumpy(): Promise<void> {
    // For now, we'll save as a simple binary format
    // In a full implementation, you'd use a proper NumPy-compatible format
    const vectors = this.batchBuffer.map(r => r.vector);
    if (vectors.length === 0 || !vectors[0]) {
      return; // Nothing to write
    }
    const buffer = Buffer.alloc(vectors.length * vectors[0].length * 4); // 4 bytes per float32

    let offset = 0;
    for (const vector of vectors) {
      for (const value of vector) {
        buffer.writeFloatLE(value, offset);
        offset += 4;
      }
    }

    await fs.appendFile(this.options.outputPath, buffer);

    // Also save metadata separately
    const metadataPath = this.options.outputPath.replace(/\.npy$/, '_metadata.jsonl');
    const metadataLines = this.batchBuffer.map(record => JSON.stringify({
      key: record.key,
      timestamp: record.timestamp,
      metadata: record.metadata
    })).join('\n') + '\n';

    await fs.appendFile(metadataPath, metadataLines);
  }

  static async exportBatch(
    embeddings: EmbeddingRecord[],
    options: ExporterOptions
  ): Promise<void> {
    const exporter = new EmbeddingExporter(options);
    await exporter.initialize();
    await exporter.addEmbeddings(embeddings);
    await exporter.finalize();
  }

  static generateFilename(format: ExportFormat, baseName: string = 'embeddings'): string {
    const timestamp = new Date().toISOString().slice(0, 19).replace(/[:.]/g, '-');
    return `${baseName}_${timestamp}.${format}`;
  }
}