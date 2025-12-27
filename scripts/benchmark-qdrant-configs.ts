#!/usr/bin/env bun

/**
 * Qdrant Configuration Benchmark Script
 *
 * Comprehensive benchmarking tool to test different Qdrant configurations:
 * - Quantization types (none, scalar int8, binary)
 * - HNSW parameters (ef values)
 * - Search parameters (rescore, oversampling)
 *
 * Features:
 * - Setup phase: copies vectors from source collection to benchmark collections
 * - Validates localhost connection (safety check)
 * - Measures latency, throughput, and recall
 * - Outputs detailed JSON and CSV reports
 *
 * Usage:
 *   bun run scripts/benchmark-qdrant-configs.ts --source=embeddings --sample=10000
 *   bun run scripts/benchmark-qdrant-configs.ts --source=embeddings --sample=50000 --skip-setup
 */

import { QdrantClient } from '@qdrant/js-client-rest';
import chalk from 'chalk';
import { promises as fs } from 'fs';

// ============================================================================
// Types & Interfaces
// ============================================================================

interface BenchmarkConfig {
  name: string;
  description: string;
  collectionSuffix: string;
  quantization: QuantizationConfig | null;
  hnswConfig?: Partial<HnswConfig>;
}

interface QuantizationConfig {
  scalar?: {
    type: 'int8';
    quantile?: number;
    always_ram?: boolean;
  };
  binary?: {
    always_ram?: boolean;
    encoding?: 'one_and_half_bits' | 'two_bits';
  };
  product?: {
    compression: 'x4' | 'x8' | 'x16' | 'x32' | 'x64';
    always_ram?: boolean;
  };
}

interface HnswConfig {
  m: number;
  ef_construct: number;
  on_disk: boolean;
}

interface SearchVariant {
  name: string;
  params: {
    hnsw_ef?: number;
    quantization?: {
      ignore?: boolean;
      rescore?: boolean;
      oversampling?: number;
    };
  };
}

interface BenchmarkResult {
  configName: string;
  searchVariant: string;
  queryIndex: number;
  latencyMs: number;
  resultsCount: number;
  topResultIds: string[];
}

interface RecallResult {
  configName: string;
  searchVariant: string;
  recall: number;
  avgLatencyMs: number;
  p50LatencyMs: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
  minLatencyMs: number;
  maxLatencyMs: number;
}

interface CollectionStats {
  name: string;
  pointsCount: number;
  indexedVectorsCount: number;
  segmentsCount: number;
  status: string;
  quantizationType: string | null;
  memoryUsageEstimate: string;
  // Actual memory metrics from Qdrant
  actualMemoryBytes?: number;
  vectorsMemoryBytes?: number;
  payloadMemoryBytes?: number;
  indexMemoryBytes?: number;
  // Disk metrics
  diskDataBytes?: number;
  // Optimizer info
  optimizerStatus?: string;
}

interface TelemetryMetrics {
  ramUsageBytes: number;
  diskReadBytes: number;
  diskWriteBytes: number;
  cpuUsagePercent?: number;
}

// ============================================================================
// Configuration
// ============================================================================

const LOCALHOST_PATTERNS = [
  'localhost',
  '127.0.0.1',
  '0.0.0.0',
  '::1',
];

const QDRANT_URL = process.env.QDRANT_URL || 'http://localhost:6333';
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;

// Top 10 configurations optimized for:
// - Limited RAM (~7.6GB free)
// - High I/O wait issues
// - Need for both speed and accuracy
// These are the most promising configs to test first
const TOP_CONFIG_NAMES = [
  'baseline',              // Reference point
  'binary_ram',            // 1-bit, 32x compression (~1.1GB for 9M vectors)
  'binary_2bit_ram',       // 2-bit, 16x compression (~2.2GB) - better recall than 1-bit
  'binary_1half_ram',      // 1.5-bit, 24x compression (~1.5GB)
  'product_x16_ram',       // Product quantization 16x (~2.2GB)
  'product_x32_ram',       // Product quantization 32x (~1.1GB)
  'scalar_int8_ram',       // 4x compression, good balance (may not fit)
  'binary_hnsw_m24',       // Binary 1-bit + better HNSW for accuracy
  'binary_hnsw_m32_ef200', // Max quality binary
  'scalar_hnsw_m24',       // Scalar + better HNSW
];

// Top search variants - most useful combinations
const TOP_SEARCH_VARIANT_NAMES = [
  'default',               // Baseline search
  'ef_64',                 // Lower ef for speed
  'ef_128',                // Balanced ef
  'quant_no_rescore',      // Fast quantized (no rescore)
  'quant_rescore_1.5x',    // Light rescoring
  'quant_rescore_2.0x',    // Recommended rescoring
  'quant_rescore_3.0x',    // Higher quality rescoring
  'ef64_rescore_2.0x',     // Combined: lower ef + rescore
];

// Benchmark configurations to test
// Organized by category for easier understanding
const BENCHMARK_CONFIGS: BenchmarkConfig[] = [
  // =========================================================================
  // BASELINE CONFIGURATIONS (no quantization)
  // =========================================================================
  {
    name: 'baseline',
    description: 'No quantization, vectors on disk, HNSW in RAM (current production)',
    collectionSuffix: 'baseline',
    quantization: null,
    hnswConfig: {
      m: 16,
      ef_construct: 100,
      on_disk: false,
    },
  },
  {
    name: 'baseline_hnsw_disk',
    description: 'No quantization, both vectors and HNSW on disk',
    collectionSuffix: 'baseline_hnsw_disk',
    quantization: null,
    hnswConfig: {
      m: 16,
      ef_construct: 100,
      on_disk: true, // HNSW graph also on disk
    },
  },

  // =========================================================================
  // SCALAR INT8 QUANTIZATION VARIANTS
  // =========================================================================
  {
    name: 'scalar_int8_disk',
    description: 'Scalar INT8, quantized on disk (mmap)',
    collectionSuffix: 'scalar_disk',
    quantization: {
      scalar: {
        type: 'int8',
        quantile: 0.99,
        always_ram: false,
      },
    },
  },
  {
    name: 'scalar_int8_ram',
    description: 'Scalar INT8, quantized in RAM (recommended)',
    collectionSuffix: 'scalar_ram',
    quantization: {
      scalar: {
        type: 'int8',
        quantile: 0.99,
        always_ram: true,
      },
    },
  },
  {
    name: 'scalar_int8_q95',
    description: 'Scalar INT8, quantile 0.95 (more aggressive clipping)',
    collectionSuffix: 'scalar_q95',
    quantization: {
      scalar: {
        type: 'int8',
        quantile: 0.95,
        always_ram: true,
      },
    },
  },
  {
    name: 'scalar_int8_q999',
    description: 'Scalar INT8, quantile 0.999 (less clipping, more accurate)',
    collectionSuffix: 'scalar_q999',
    quantization: {
      scalar: {
        type: 'int8',
        quantile: 0.999,
        always_ram: true,
      },
    },
  },

  // =========================================================================
  // BINARY QUANTIZATION VARIANTS
  // =========================================================================
  {
    name: 'binary_ram',
    description: 'Binary quantization in RAM (1-bit, 32x compression)',
    collectionSuffix: 'binary_ram',
    quantization: {
      binary: {
        always_ram: true,
      },
    },
  },
  {
    name: 'binary_disk',
    description: 'Binary quantization on disk (1-bit, 32x compression)',
    collectionSuffix: 'binary_disk',
    quantization: {
      binary: {
        always_ram: false,
      },
    },
  },
  {
    name: 'binary_2bit_ram',
    description: 'Binary 2-bit quantization in RAM (16x compression, better recall)',
    collectionSuffix: 'binary_2bit_ram',
    quantization: {
      binary: {
        always_ram: true,
        encoding: 'two_bits',
      },
    },
  },
  {
    name: 'binary_1half_ram',
    description: 'Binary 1.5-bit quantization in RAM (24x compression)',
    collectionSuffix: 'binary_1half_ram',
    quantization: {
      binary: {
        always_ram: true,
        encoding: 'one_and_half_bits',
      },
    },
  },

  // =========================================================================
  // PRODUCT QUANTIZATION VARIANTS
  // =========================================================================
  {
    name: 'product_x16_ram',
    description: 'Product quantization x16 in RAM (16x compression)',
    collectionSuffix: 'product_x16_ram',
    quantization: {
      product: {
        compression: 'x16',
        always_ram: true,
      },
    },
  },
  {
    name: 'product_x32_ram',
    description: 'Product quantization x32 in RAM (32x compression)',
    collectionSuffix: 'product_x32_ram',
    quantization: {
      product: {
        compression: 'x32',
        always_ram: true,
      },
    },
  },
  {
    name: 'product_x64_ram',
    description: 'Product quantization x64 in RAM (64x compression, most aggressive)',
    collectionSuffix: 'product_x64_ram',
    quantization: {
      product: {
        compression: 'x64',
        always_ram: true,
      },
    },
  },

  // =========================================================================
  // HNSW PARAMETER VARIATIONS (with scalar quantization)
  // =========================================================================
  {
    name: 'scalar_hnsw_m8',
    description: 'Scalar INT8 + HNSW m=8 (faster build, less accurate)',
    collectionSuffix: 'scalar_m8',
    quantization: {
      scalar: {
        type: 'int8',
        quantile: 0.99,
        always_ram: true,
      },
    },
    hnswConfig: {
      m: 8,
      ef_construct: 100,
      on_disk: false,
    },
  },
  {
    name: 'scalar_hnsw_m12',
    description: 'Scalar INT8 + HNSW m=12 (balanced)',
    collectionSuffix: 'scalar_m12',
    quantization: {
      scalar: {
        type: 'int8',
        quantile: 0.99,
        always_ram: true,
      },
    },
    hnswConfig: {
      m: 12,
      ef_construct: 100,
      on_disk: false,
    },
  },
  {
    name: 'scalar_hnsw_m24',
    description: 'Scalar INT8 + HNSW m=24 (higher accuracy)',
    collectionSuffix: 'scalar_m24',
    quantization: {
      scalar: {
        type: 'int8',
        quantile: 0.99,
        always_ram: true,
      },
    },
    hnswConfig: {
      m: 24,
      ef_construct: 100,
      on_disk: false,
    },
  },
  {
    name: 'scalar_hnsw_m32',
    description: 'Scalar INT8 + HNSW m=32 (highest accuracy, more memory)',
    collectionSuffix: 'scalar_m32',
    quantization: {
      scalar: {
        type: 'int8',
        quantile: 0.99,
        always_ram: true,
      },
    },
    hnswConfig: {
      m: 32,
      ef_construct: 100,
      on_disk: false,
    },
  },
  {
    name: 'scalar_hnsw_ef200',
    description: 'Scalar INT8 + HNSW ef_construct=200 (better index quality)',
    collectionSuffix: 'scalar_ef200',
    quantization: {
      scalar: {
        type: 'int8',
        quantile: 0.99,
        always_ram: true,
      },
    },
    hnswConfig: {
      m: 16,
      ef_construct: 200,
      on_disk: false,
    },
  },
  {
    name: 'scalar_hnsw_ef50',
    description: 'Scalar INT8 + HNSW ef_construct=50 (faster build, lower quality)',
    collectionSuffix: 'scalar_ef50',
    quantization: {
      scalar: {
        type: 'int8',
        quantile: 0.99,
        always_ram: true,
      },
    },
    hnswConfig: {
      m: 16,
      ef_construct: 50,
      on_disk: false,
    },
  },

  // =========================================================================
  // BINARY QUANTIZATION + HNSW VARIATIONS
  // =========================================================================
  {
    name: 'binary_hnsw_m24',
    description: 'Binary + HNSW m=24 (compensate for quantization loss)',
    collectionSuffix: 'binary_m24',
    quantization: {
      binary: {
        always_ram: true,
      },
    },
    hnswConfig: {
      m: 24,
      ef_construct: 100,
      on_disk: false,
    },
  },
  {
    name: 'binary_hnsw_m32_ef200',
    description: 'Binary + HNSW m=32, ef_construct=200 (max quality for binary)',
    collectionSuffix: 'binary_m32_ef200',
    quantization: {
      binary: {
        always_ram: true,
      },
    },
    hnswConfig: {
      m: 32,
      ef_construct: 200,
      on_disk: false,
    },
  },
];

// Search parameter variants to test for each configuration
const SEARCH_VARIANTS: SearchVariant[] = [
  // =========================================================================
  // BASELINE SEARCH (no special params)
  // =========================================================================
  {
    name: 'default',
    params: {},
  },

  // =========================================================================
  // HNSW ef VARIATIONS (controls search beam width)
  // Lower ef = faster but less accurate, Higher ef = slower but more accurate
  // =========================================================================
  {
    name: 'ef_32',
    params: { hnsw_ef: 32 },
  },
  {
    name: 'ef_48',
    params: { hnsw_ef: 48 },
  },
  {
    name: 'ef_64',
    params: { hnsw_ef: 64 },
  },
  {
    name: 'ef_96',
    params: { hnsw_ef: 96 },
  },
  {
    name: 'ef_128',
    params: { hnsw_ef: 128 },
  },
  {
    name: 'ef_192',
    params: { hnsw_ef: 192 },
  },
  {
    name: 'ef_256',
    params: { hnsw_ef: 256 },
  },
  {
    name: 'ef_384',
    params: { hnsw_ef: 384 },
  },
  {
    name: 'ef_512',
    params: { hnsw_ef: 512 },
  },

  // =========================================================================
  // QUANTIZATION: NO RESCORING (fastest, uses only quantized vectors)
  // =========================================================================
  {
    name: 'quant_no_rescore',
    params: {
      quantization: {
        ignore: false,
        rescore: false,
      },
    },
  },
  {
    name: 'quant_no_rescore_ef64',
    params: {
      hnsw_ef: 64,
      quantization: {
        ignore: false,
        rescore: false,
      },
    },
  },
  {
    name: 'quant_no_rescore_ef128',
    params: {
      hnsw_ef: 128,
      quantization: {
        ignore: false,
        rescore: false,
      },
    },
  },

  // =========================================================================
  // QUANTIZATION: WITH RESCORING (various oversampling levels)
  // Oversampling: fetch N× candidates, rescore with original vectors
  // =========================================================================
  {
    name: 'quant_rescore_1.0x',
    params: {
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 1.0,
      },
    },
  },
  {
    name: 'quant_rescore_1.2x',
    params: {
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 1.2,
      },
    },
  },
  {
    name: 'quant_rescore_1.5x',
    params: {
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 1.5,
      },
    },
  },
  {
    name: 'quant_rescore_2.0x',
    params: {
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 2.0,
      },
    },
  },
  {
    name: 'quant_rescore_2.5x',
    params: {
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 2.5,
      },
    },
  },
  {
    name: 'quant_rescore_3.0x',
    params: {
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 3.0,
      },
    },
  },
  {
    name: 'quant_rescore_4.0x',
    params: {
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 4.0,
      },
    },
  },
  {
    name: 'quant_rescore_5.0x',
    params: {
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 5.0,
      },
    },
  },

  // =========================================================================
  // COMBINED: ef + oversampling (finding the sweet spot)
  // =========================================================================
  {
    name: 'ef64_rescore_1.5x',
    params: {
      hnsw_ef: 64,
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 1.5,
      },
    },
  },
  {
    name: 'ef64_rescore_2.0x',
    params: {
      hnsw_ef: 64,
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 2.0,
      },
    },
  },
  {
    name: 'ef128_rescore_1.5x',
    params: {
      hnsw_ef: 128,
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 1.5,
      },
    },
  },
  {
    name: 'ef128_rescore_2.0x',
    params: {
      hnsw_ef: 128,
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 2.0,
      },
    },
  },
  {
    name: 'ef256_rescore_1.5x',
    params: {
      hnsw_ef: 256,
      quantization: {
        ignore: false,
        rescore: true,
        oversampling: 1.5,
      },
    },
  },

  // =========================================================================
  // IGNORE QUANTIZATION (force use of original vectors)
  // Useful for accuracy baseline comparison
  // =========================================================================
  {
    name: 'ignore_quant',
    params: {
      quantization: {
        ignore: true,
      },
    },
  },
  {
    name: 'ignore_quant_ef128',
    params: {
      hnsw_ef: 128,
      quantization: {
        ignore: true,
      },
    },
  },
];

// Filter test cases to benchmark filtered search performance
interface FilterTestCase {
  name: string;
  description: string;
  filter: Record<string, any>;
}

const FILTER_TEST_CASES: FilterTestCase[] = [
  {
    name: 'no_filter',
    description: 'No filter (baseline)',
    filter: {},
  },
  {
    name: 'keyword_source',
    description: 'Filter by source (keyword index)',
    filter: {
      must: [
        { key: 'metadata.source', match: { value: 'twitter' } }
      ]
    },
  },
  {
    name: 'keyword_username',
    description: 'Filter by username (keyword index)',
    filter: {
      must: [
        { key: 'metadata.username', match: { value: 'testuser' } }
      ]
    },
  },
  {
    name: 'text_contains_short',
    description: 'Filter by text contains short word (full-text)',
    filter: {
      must: [
        { key: 'metadata.text', match: { text: 'the' } }
      ]
    },
  },
  {
    name: 'text_contains_phrase',
    description: 'Filter by text contains phrase (full-text)',
    filter: {
      must: [
        { key: 'metadata.text', match: { text: 'hello world' } }
      ]
    },
  },
  {
    name: 'combined_must',
    description: 'Multiple conditions with AND',
    filter: {
      must: [
        { key: 'metadata.source', match: { value: 'twitter' } },
      ]
    },
  },
  {
    name: 'should_or',
    description: 'Multiple conditions with OR (should)',
    filter: {
      should: [
        { key: 'metadata.source', match: { value: 'twitter' } },
        { key: 'metadata.source', match: { value: 'mastodon' } },
      ]
    },
  },
];

// Different k (result limit) values to test
const K_VALUES_TO_TEST = [10, 25, 50, 100, 200];

// Concurrency levels to test
const CONCURRENCY_LEVELS = [1, 2, 4, 8, 16];

// Additional result interfaces for extended benchmarks
interface FilterBenchmarkResult {
  configName: string;
  filterName: string;
  searchVariant: string;
  queryIndex: number;
  latencyMs: number;
  resultsCount: number;
}

interface ConcurrencyBenchmarkResult {
  configName: string;
  searchVariant: string;
  concurrencyLevel: number;
  totalQueries: number;
  totalTimeMs: number;
  avgLatencyMs: number;
  throughputQps: number;
  p50LatencyMs: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
  errors: number;
}

interface KValueBenchmarkResult {
  configName: string;
  searchVariant: string;
  kValue: number;
  avgLatencyMs: number;
  p50LatencyMs: number;
  p95LatencyMs: number;
}

interface ColdWarmCacheResult {
  configName: string;
  searchVariant: string;
  runType: 'cold' | 'warm';
  avgLatencyMs: number;
  p50LatencyMs: number;
  p95LatencyMs: number;
}

// ============================================================================
// Utility Functions
// ============================================================================

function validateLocalhostUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    const hostname = parsed.hostname.toLowerCase();
    return LOCALHOST_PATTERNS.some(pattern => hostname === pattern || hostname.startsWith(pattern));
  } catch {
    return false;
  }
}

function formatTimestamp(): string {
  return new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
}

function calculatePercentile(values: number[], percentile: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.ceil((percentile / 100) * sorted.length) - 1;
  return sorted[Math.max(0, index)] ?? 0;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function estimateMemoryUsage(pointsCount: number, dimension: number, quantization: string | null): string {
  const baseVectorBytes = pointsCount * dimension * 4; // float32

  let quantizedBytes = 0;
  switch (quantization) {
    case 'scalar_int8':
      quantizedBytes = pointsCount * dimension * 1; // int8
      break;
    case 'binary':
      quantizedBytes = pointsCount * Math.ceil(dimension / 8); // 1 bit per dim
      break;
  }

  // HNSW graph overhead (rough estimate: ~100 bytes per point for m=16)
  const hnswOverhead = pointsCount * 100;

  const total = baseVectorBytes + quantizedBytes + hnswOverhead;

  return `Vectors: ${formatBytes(baseVectorBytes)}, Quantized: ${formatBytes(quantizedBytes)}, HNSW: ${formatBytes(hnswOverhead)}, Total: ${formatBytes(total)}`;
}

// ============================================================================
// Qdrant Operations
// ============================================================================

class QdrantBenchmark {
  private client: QdrantClient;
  private dimension: number = 1024;

  constructor(url: string, apiKey?: string) {
    this.client = new QdrantClient({
      url,
      apiKey,
      timeout: 300000, // 5 minutes for long operations
    });
  }

  async getSourceCollectionInfo(collectionName: string): Promise<{ dimension: number; pointsCount: number }> {
    const info = await this.client.getCollection(collectionName);
    const vectorsConfig = info.config?.params?.vectors;
    const dimension = typeof vectorsConfig === 'object' && 'size' in vectorsConfig
      ? vectorsConfig.size
      : 1024;

    return {
      dimension,
      pointsCount: info.points_count || 0,
    };
  }

  async collectionExists(name: string): Promise<boolean> {
    const collections = await this.client.getCollections();
    return collections.collections.some(c => c.name === name);
  }

  async deleteCollectionIfExists(name: string): Promise<void> {
    if (await this.collectionExists(name)) {
      console.log(chalk.yellow(`  Deleting existing collection: ${name}`));
      await this.client.deleteCollection(name);
    }
  }

  async createBenchmarkCollection(
    name: string,
    dimension: number,
    config: BenchmarkConfig
  ): Promise<void> {
    console.log(chalk.blue(`  Creating collection: ${name}`));
    console.log(chalk.gray(`    Description: ${config.description}`));

    const createParams: any = {
      vectors: {
        size: dimension,
        distance: 'Cosine',
        on_disk: true, // Always store original vectors on disk
      },
      on_disk_payload: true,
      optimizers_config: {
        indexing_threshold: 10000,
      },
    };

    // Add HNSW config if specified
    if (config.hnswConfig) {
      createParams.hnsw_config = config.hnswConfig;
    }

    // Add quantization config if specified
    if (config.quantization) {
      createParams.quantization_config = config.quantization;
    }

    await this.client.createCollection(name, createParams);
    console.log(chalk.green(`    Collection created successfully`));
  }

  async copyVectorsFromSource(
    sourceCollection: string,
    targetCollection: string,
    sampleSize: number,
    onProgress?: (copied: number, total: number) => void
  ): Promise<number> {
    console.log(chalk.blue(`  Copying vectors from ${sourceCollection} to ${targetCollection}`));

    let copied = 0;
    let offset: string | number | null = null;
    const batchSize = 1000;

    while (copied < sampleSize) {
      const scrollParams: any = {
        limit: Math.min(batchSize, sampleSize - copied),
        with_payload: true,
        with_vector: true,
      };

      if (offset !== null) {
        scrollParams.offset = offset;
      }

      const response = await this.client.scroll(sourceCollection, scrollParams);

      if (!response.points || response.points.length === 0) {
        break;
      }

      // Upsert to target collection
      const points = response.points.map(point => ({
        id: point.id,
        vector: point.vector as number[],
        payload: point.payload || {},
      }));

      await this.client.upsert(targetCollection, {
        wait: true,
        points,
      });

      copied += points.length;

      if (onProgress) {
        onProgress(copied, sampleSize);
      }

      const nextOffset = response.next_page_offset;
      if (nextOffset === undefined || nextOffset === null) {
        break;
      }
      offset = nextOffset as string | number;
    }

    return copied;
  }

  async waitForIndexing(collectionName: string, timeoutMs: number = 600000): Promise<void> {
    console.log(chalk.blue(`  Waiting for indexing to complete on ${collectionName}...`));

    const startTime = Date.now();
    let lastStatus = '';

    while (Date.now() - startTime < timeoutMs) {
      const info = await this.client.getCollection(collectionName);
      const status = info.status;

      // optimizer_status can be "ok" (string) or an object like { status: "indexing" }
      let optimizerOk = false;
      let optimizerDisplay = '';

      if (typeof info.optimizer_status === 'string') {
        optimizerDisplay = info.optimizer_status;
        optimizerOk = info.optimizer_status === 'ok';
      } else if (typeof info.optimizer_status === 'object' && info.optimizer_status !== null) {
        optimizerDisplay = JSON.stringify(info.optimizer_status);
        // Check if it's { status: "ok" } or similar
        optimizerOk = (info.optimizer_status as any).status === 'ok' ||
                      JSON.stringify(info.optimizer_status) === '"ok"';
      }

      const statusKey = `${status}-${optimizerDisplay}`;
      if (statusKey !== lastStatus) {
        console.log(chalk.gray(`    Status: ${status}, Optimizer: ${optimizerDisplay}`));
        lastStatus = statusKey;
      }

      if (status === 'green' && optimizerOk) {
        console.log(chalk.green(`    Indexing complete!`));
        return;
      }

      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    console.log(chalk.yellow(`    Warning: Indexing timeout reached, proceeding anyway`));
  }

  async getCollectionStats(collectionName: string): Promise<CollectionStats> {
    const info = await this.client.getCollection(collectionName);

    let quantizationType: string | null = null;
    if (info.config?.quantization_config) {
      if ('scalar' in info.config.quantization_config) {
        quantizationType = 'scalar_int8';
      } else if ('binary' in info.config.quantization_config) {
        quantizationType = 'binary';
      }
    }

    const vectorsConfig = info.config?.params?.vectors;
    const dimension = typeof vectorsConfig === 'object' && 'size' in vectorsConfig
      ? vectorsConfig.size
      : 1024;

    // Try to get detailed cluster info for memory metrics
    let actualMemoryBytes: number | undefined;
    let vectorsMemoryBytes: number | undefined;
    let payloadMemoryBytes: number | undefined;
    let indexMemoryBytes: number | undefined;
    let diskDataBytes: number | undefined;

    try {
      // Get cluster info for the collection which has detailed memory stats
      const clusterInfo = await this.getCollectionClusterInfo(collectionName);
      if (clusterInfo) {
        actualMemoryBytes = clusterInfo.totalMemory;
        vectorsMemoryBytes = clusterInfo.vectorsMemory;
        payloadMemoryBytes = clusterInfo.payloadMemory;
        indexMemoryBytes = clusterInfo.indexMemory;
        diskDataBytes = clusterInfo.diskData;
      }
    } catch (e) {
      // Cluster info not available, use estimates
    }

    const optimizerStatus = typeof info.optimizer_status === 'string'
      ? info.optimizer_status
      : JSON.stringify(info.optimizer_status);

    return {
      name: collectionName,
      pointsCount: info.points_count || 0,
      indexedVectorsCount: info.indexed_vectors_count || 0,
      segmentsCount: info.segments_count || 0,
      status: info.status,
      quantizationType,
      memoryUsageEstimate: estimateMemoryUsage(info.points_count || 0, dimension, quantizationType),
      actualMemoryBytes,
      vectorsMemoryBytes,
      payloadMemoryBytes,
      indexMemoryBytes,
      diskDataBytes,
      optimizerStatus,
    };
  }

  /**
   * Get detailed memory metrics from collection cluster info
   */
  async getCollectionClusterInfo(collectionName: string): Promise<{
    totalMemory: number;
    vectorsMemory: number;
    payloadMemory: number;
    indexMemory: number;
    diskData: number;
  } | null> {
    try {
      // Use the REST API directly for detailed info
      const response = await fetch(`${QDRANT_URL}/collections/${collectionName}/cluster`, {
        headers: QDRANT_API_KEY ? { 'api-key': QDRANT_API_KEY } : {},
      });

      if (!response.ok) {
        // Try alternative endpoint - collection telemetry
        return await this.getCollectionTelemetry(collectionName);
      }

      const data = await response.json();
      // Parse cluster info for memory stats
      // Structure varies by Qdrant version

      return null; // Will be parsed based on actual response structure
    } catch {
      return null;
    }
  }

  /**
   * Get collection telemetry/metrics
   */
  async getCollectionTelemetry(collectionName: string): Promise<{
    totalMemory: number;
    vectorsMemory: number;
    payloadMemory: number;
    indexMemory: number;
    diskData: number;
  } | null> {
    try {
      // Try to get telemetry from Qdrant
      const response = await fetch(`${QDRANT_URL}/telemetry`, {
        headers: QDRANT_API_KEY ? { 'api-key': QDRANT_API_KEY } : {},
      });

      if (!response.ok) return null;

      const data = await response.json();

      // Look for collection-specific metrics in telemetry
      const collections = data.result?.app?.collections;
      if (collections && collections[collectionName]) {
        const colData = collections[collectionName];
        return {
          totalMemory: colData.total_memory_bytes || 0,
          vectorsMemory: colData.vectors_memory_bytes || 0,
          payloadMemory: colData.payload_memory_bytes || 0,
          indexMemory: colData.index_memory_bytes || 0,
          diskData: colData.disk_data_bytes || 0,
        };
      }

      return null;
    } catch {
      return null;
    }
  }

  /**
   * Get global Qdrant telemetry metrics (RAM, disk IO)
   */
  async getTelemetryMetrics(): Promise<TelemetryMetrics | null> {
    try {
      const response = await fetch(`${QDRANT_URL}/telemetry`, {
        headers: QDRANT_API_KEY ? { 'api-key': QDRANT_API_KEY } : {},
      });

      if (!response.ok) return null;

      const data = await response.json();
      const app = data.result?.app;

      return {
        ramUsageBytes: app?.memory?.active_bytes || 0,
        diskReadBytes: app?.disk?.read_bytes || 0,
        diskWriteBytes: app?.disk?.write_bytes || 0,
        cpuUsagePercent: app?.cpu?.percent || undefined,
      };
    } catch {
      return null;
    }
  }

  /**
   * Measure disk IO during a search operation
   */
  async measureSearchWithIO(
    collectionName: string,
    vector: number[],
    limit: number,
    params: SearchVariant['params']
  ): Promise<{
    latencyMs: number;
    results: Array<{ id: string | number; score: number }>;
    diskReadDelta?: number;
  }> {
    // Get baseline disk reads
    const beforeMetrics = await this.getTelemetryMetrics();

    // Run search
    const searchResult = await this.runSearch(collectionName, vector, limit, params);

    // Get after disk reads
    const afterMetrics = await this.getTelemetryMetrics();

    let diskReadDelta: number | undefined;
    if (beforeMetrics && afterMetrics) {
      diskReadDelta = afterMetrics.diskReadBytes - beforeMetrics.diskReadBytes;
    }

    return {
      ...searchResult,
      diskReadDelta,
    };
  }

  async getSampleQueryVectors(collectionName: string, count: number): Promise<Array<{ id: string | number; vector: number[] }>> {
    const response = await this.client.scroll(collectionName, {
      limit: count,
      with_vector: true,
      with_payload: false,
    });

    return (response.points || []).map(point => ({
      id: point.id,
      vector: point.vector as number[],
    }));
  }

  async runSearch(
    collectionName: string,
    vector: number[],
    limit: number,
    params: SearchVariant['params'],
    filter?: Record<string, any>
  ): Promise<{ latencyMs: number; results: Array<{ id: string | number; score: number }> }> {
    const startTime = performance.now();

    const searchParams: any = {
      vector,
      limit,
      with_payload: false,
    };

    if (params.hnsw_ef || params.quantization) {
      searchParams.params = {};

      if (params.hnsw_ef) {
        searchParams.params.hnsw_ef = params.hnsw_ef;
      }

      if (params.quantization) {
        searchParams.params.quantization = params.quantization;
      }
    }

    // Add filter if provided and not empty
    if (filter && Object.keys(filter).length > 0) {
      searchParams.filter = filter;
    }

    const results = await this.client.search(collectionName, searchParams);

    const latencyMs = performance.now() - startTime;

    return {
      latencyMs,
      results: results.map(r => ({
        id: r.id,
        score: r.score,
      })),
    };
  }

  /**
   * Run multiple searches concurrently and measure throughput
   */
  async runConcurrentSearches(
    collectionName: string,
    vectors: number[][],
    limit: number,
    params: SearchVariant['params'],
    concurrency: number
  ): Promise<{ latencies: number[]; errors: number; totalTimeMs: number }> {
    const startTime = performance.now();
    const latencies: number[] = [];
    let errors = 0;

    // Process vectors in batches of 'concurrency' size
    for (let i = 0; i < vectors.length; i += concurrency) {
      const batch = vectors.slice(i, i + concurrency);
      const promises = batch.map(async (vector) => {
        try {
          const result = await this.runSearch(collectionName, vector, limit, params);
          return { success: true, latencyMs: result.latencyMs };
        } catch (e) {
          return { success: false, latencyMs: 0 };
        }
      });

      const results = await Promise.all(promises);
      for (const result of results) {
        if (result.success) {
          latencies.push(result.latencyMs);
        } else {
          errors++;
        }
      }
    }

    const totalTimeMs = performance.now() - startTime;
    return { latencies, errors, totalTimeMs };
  }

  /**
   * Clear OS page cache (requires appropriate permissions)
   * This is a best-effort operation - may not work on all systems
   */
  async clearCache(): Promise<void> {
    // Note: This is a placeholder. In practice, clearing cache requires:
    // - Linux: echo 3 > /proc/sys/vm/drop_caches (requires root)
    // - Qdrant: Restart the service or use madvise hints
    // For now, we just wait a bit to let system settle
    await new Promise(resolve => setTimeout(resolve, 1000));
  }

  /**
   * Create payload indexes for filter benchmarking
   */
  async ensurePayloadIndexes(collectionName: string): Promise<void> {
    const indexes = [
      { field: 'metadata.source', schema: 'keyword' },
      { field: 'metadata.username', schema: 'keyword' },
      { field: 'metadata.text', schema: 'text' },
      { field: 'key', schema: 'keyword' },
    ];

    for (const { field, schema } of indexes) {
      try {
        await this.client.createPayloadIndex(collectionName, {
          field_name: field,
          field_schema: schema as any,
        });
      } catch (e) {
        // Index might already exist, that's fine
      }
    }
  }
}

// ============================================================================
// Benchmark Execution
// ============================================================================

/**
 * Run benchmark for a single configuration:
 * 1. Create collection (or reuse if exists)
 * 2. Copy vectors
 * 3. Wait for indexing
 * 4. Run all search variants
 * 5. Optionally delete collection
 */
async function benchmarkSingleConfig(
  benchmark: QdrantBenchmark,
  config: BenchmarkConfig,
  sourceCollection: string,
  sampleSize: number,
  queryVectors: Array<{ id: string | number; vector: number[] }>,
  searchLimit: number,
  warmupRuns: number,
  deleteAfter: boolean = true
): Promise<{ results: BenchmarkResult[]; stats: CollectionStats }> {
  const collectionName = `benchmark_${config.collectionSuffix}`;

  console.log(chalk.cyan(`\n${'='.repeat(60)}`));
  console.log(chalk.cyan.bold(`CONFIG: ${config.name}`));
  console.log(chalk.gray(`${config.description}`));
  console.log(chalk.cyan(`${'='.repeat(60)}`));

  // Get source info for dimension
  const sourceInfo = await benchmark.getSourceCollectionInfo(sourceCollection);
  const actualSampleSize = Math.min(sampleSize, sourceInfo.pointsCount);

  // Setup: Delete if exists, create, copy, index
  console.log(chalk.blue(`\n[1/4] Setting up collection...`));
  await benchmark.deleteCollectionIfExists(collectionName);
  await benchmark.createBenchmarkCollection(collectionName, sourceInfo.dimension, config);

  // Copy vectors
  console.log(chalk.blue(`[2/4] Copying ${actualSampleSize.toLocaleString()} vectors...`));
  let lastProgress = 0;
  const copied = await benchmark.copyVectorsFromSource(
    sourceCollection,
    collectionName,
    actualSampleSize,
    (current, total) => {
      const progress = Math.floor((current / total) * 100);
      if (progress >= lastProgress + 10) {
        process.stdout.write(chalk.gray(`  Progress: ${progress}%\r`));
        lastProgress = progress;
      }
    }
  );
  console.log(chalk.green(`  Copied ${copied.toLocaleString()} vectors`));

  // Wait for indexing
  console.log(chalk.blue(`[3/4] Waiting for indexing...`));
  await benchmark.waitForIndexing(collectionName);

  // Get stats
  const stats = await benchmark.getCollectionStats(collectionName);
  console.log(chalk.gray(`  Points: ${stats.pointsCount.toLocaleString()}, Indexed: ${stats.indexedVectorsCount.toLocaleString()}`));
  console.log(chalk.gray(`  Memory estimate: ${stats.memoryUsageEstimate}`));

  // Benchmark
  console.log(chalk.blue(`[4/4] Running benchmarks...`));
  const allResults: BenchmarkResult[] = [];

  // Filter search variants based on quantization support
  const applicableVariants = SEARCH_VARIANTS.filter(variant => {
    if (variant.params.quantization && !config.quantization) {
      return false;
    }
    return true;
  });

  console.log(chalk.gray(`  Testing ${applicableVariants.length} search variants...`));

  for (const variant of applicableVariants) {
    // Warmup runs
    if (warmupRuns > 0) {
      for (let w = 0; w < warmupRuns; w++) {
        const warmupQuery = queryVectors[w % queryVectors.length]!;
        await benchmark.runSearch(collectionName, warmupQuery.vector, searchLimit, variant.params);
      }
    }

    // Actual benchmark runs
    const latencies: number[] = [];

    for (let i = 0; i < queryVectors.length; i++) {
      const query = queryVectors[i]!;
      const result = await benchmark.runSearch(collectionName, query.vector, searchLimit, variant.params);

      latencies.push(result.latencyMs);

      allResults.push({
        configName: config.name,
        searchVariant: variant.name,
        queryIndex: i,
        latencyMs: result.latencyMs,
        resultsCount: result.results.length,
        topResultIds: result.results.slice(0, 10).map(r => String(r.id)),
      });
    }

    // Print quick stats
    const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length;
    const p95 = calculatePercentile(latencies, 95);
    console.log(chalk.gray(`  ${variant.name.padEnd(25)} avg: ${avgLatency.toFixed(1).padStart(6)}ms  p95: ${p95.toFixed(1).padStart(6)}ms`));
  }

  // Cleanup
  if (deleteAfter) {
    console.log(chalk.yellow(`\nDeleting collection ${collectionName} to free resources...`));
    await benchmark.deleteCollectionIfExists(collectionName);
  }

  console.log(chalk.green(`✓ Config ${config.name} complete!`));

  return { results: allResults, stats };
}

/**
 * Get query vectors from source collection (before any benchmark collections are created)
 */
async function getQueryVectorsFromSource(
  benchmark: QdrantBenchmark,
  sourceCollection: string,
  queryCount: number
): Promise<Array<{ id: string | number; vector: number[] }>> {
  console.log(chalk.blue(`\nGetting ${queryCount} query vectors from source collection...`));
  const queryVectors = await benchmark.getSampleQueryVectors(sourceCollection, queryCount);
  console.log(chalk.green(`  Got ${queryVectors.length} query vectors`));
  return queryVectors;
}

/**
 * Build ground truth by creating a temporary baseline collection
 */
async function buildGroundTruth(
  benchmark: QdrantBenchmark,
  sourceCollection: string,
  sampleSize: number,
  queryVectors: Array<{ id: string | number; vector: number[] }>,
  searchLimit: number
): Promise<{ groundTruth: Map<number, string[]>; baselineResults: BenchmarkResult[]; baselineStats: CollectionStats }> {
  console.log(chalk.blue.bold('\n=== BUILDING GROUND TRUTH (Baseline) ===\n'));

  // Create baseline config
  const baselineConfig: BenchmarkConfig = {
    name: 'baseline',
    description: 'No quantization, vectors on disk, HNSW in RAM (ground truth)',
    collectionSuffix: 'baseline',
    quantization: null,
    hnswConfig: {
      m: 16,
      ef_construct: 100,
      on_disk: false,
    },
  };

  // Create baseline collection
  const { results: baselineResults, stats: baselineStats } = await benchmarkSingleConfig(
    benchmark,
    baselineConfig,
    sourceCollection,
    sampleSize,
    queryVectors,
    searchLimit,
    5, // warmup
    false // DON'T delete - we need it for ground truth queries
  );

  // Build ground truth from baseline with high ef
  console.log(chalk.blue('\nBuilding ground truth with ef=512...'));
  const groundTruth = new Map<number, string[]>();
  const collectionName = `benchmark_${baselineConfig.collectionSuffix}`;

  for (let i = 0; i < queryVectors.length; i++) {
    const query = queryVectors[i]!;
    const result = await benchmark.runSearch(
      collectionName,
      query.vector,
      searchLimit,
      { hnsw_ef: 512 }
    );
    groundTruth.set(i, result.results.map(r => String(r.id)));

    if ((i + 1) % 10 === 0) {
      process.stdout.write(chalk.gray(`  Progress: ${i + 1}/${queryVectors.length}\r`));
    }
  }
  console.log(chalk.green(`\n  Ground truth built for ${groundTruth.size} queries`));

  // Now delete the baseline collection
  console.log(chalk.yellow(`\nDeleting baseline collection...`));
  await benchmark.deleteCollectionIfExists(collectionName);

  return { groundTruth, baselineResults, baselineStats };
}

function calculateRecall(
  results: BenchmarkResult[],
  groundTruth: Map<number, string[]>,
  atK: number = 10
): RecallResult[] {
  // Group results by config and variant
  const grouped = new Map<string, BenchmarkResult[]>();

  for (const result of results) {
    const key = `${result.configName}|${result.searchVariant}`;
    if (!grouped.has(key)) {
      grouped.set(key, []);
    }
    grouped.get(key)!.push(result);
  }

  const recallResults: RecallResult[] = [];

  for (const [key, groupResults] of grouped) {
    const [configName, searchVariant] = key.split('|');

    let totalRecall = 0;
    const latencies: number[] = [];

    for (const result of groupResults) {
      const truth = groundTruth.get(result.queryIndex);
      if (!truth) continue;

      const truthSet = new Set(truth.slice(0, atK));
      const resultSet = new Set(result.topResultIds.slice(0, atK));

      let matches = 0;
      for (const id of resultSet) {
        if (truthSet.has(id)) {
          matches++;
        }
      }

      totalRecall += matches / Math.min(atK, truth.length);
      latencies.push(result.latencyMs);
    }

    const avgRecall = totalRecall / groupResults.length;
    const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length;

    recallResults.push({
      configName: configName!,
      searchVariant: searchVariant!,
      recall: avgRecall,
      avgLatencyMs: avgLatency,
      p50LatencyMs: calculatePercentile(latencies, 50),
      p95LatencyMs: calculatePercentile(latencies, 95),
      p99LatencyMs: calculatePercentile(latencies, 99),
      minLatencyMs: Math.min(...latencies),
      maxLatencyMs: Math.max(...latencies),
    });
  }

  // Sort by recall descending, then by latency ascending
  recallResults.sort((a, b) => {
    if (Math.abs(a.recall - b.recall) > 0.01) {
      return b.recall - a.recall;
    }
    return a.avgLatencyMs - b.avgLatencyMs;
  });

  return recallResults;
}

// ============================================================================
// Extended Benchmark Phases
// ============================================================================

/**
 * Benchmark with different k (result limit) values
 */
async function runKValueBenchmark(
  benchmark: QdrantBenchmark,
  configs: BenchmarkConfig[],
  queryVectors: Array<{ id: string | number; vector: number[] }>,
  selectedVariants: string[] = ['default', 'quant_rescore_2.0x']
): Promise<KValueBenchmarkResult[]> {
  console.log(chalk.blue.bold('\n=== K-VALUE BENCHMARK ===\n'));
  console.log(chalk.gray(`Testing k values: ${K_VALUES_TO_TEST.join(', ')}`));

  const results: KValueBenchmarkResult[] = [];

  // Test a subset of configs (baseline + best quantized)
  const configsToTest = configs.filter(c =>
    c.name === 'baseline' ||
    c.name === 'scalar_int8_ram' ||
    c.name === 'binary_ram'
  );

  for (const config of configsToTest) {
    const collectionName = `benchmark_${config.collectionSuffix}`;
    console.log(chalk.cyan(`\n${config.name}:`));

    const applicableVariants = SEARCH_VARIANTS.filter(v =>
      selectedVariants.includes(v.name) &&
      (!v.params.quantization || config.quantization)
    );

    for (const variant of applicableVariants) {
      for (const k of K_VALUES_TO_TEST) {
        const latencies: number[] = [];

        for (const query of queryVectors) {
          const result = await benchmark.runSearch(collectionName, query.vector, k, variant.params);
          latencies.push(result.latencyMs);
        }

        const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length;
        const p50 = calculatePercentile(latencies, 50);
        const p95 = calculatePercentile(latencies, 95);

        results.push({
          configName: config.name,
          searchVariant: variant.name,
          kValue: k,
          avgLatencyMs: avgLatency,
          p50LatencyMs: p50,
          p95LatencyMs: p95,
        });

        console.log(chalk.gray(`  ${variant.name} k=${k}: avg=${avgLatency.toFixed(2)}ms, p95=${p95.toFixed(2)}ms`));
      }
    }
  }

  return results;
}

/**
 * Benchmark filtered searches
 */
async function runFilterBenchmark(
  benchmark: QdrantBenchmark,
  configs: BenchmarkConfig[],
  queryVectors: Array<{ id: string | number; vector: number[] }>,
  searchLimit: number
): Promise<FilterBenchmarkResult[]> {
  console.log(chalk.blue.bold('\n=== FILTER BENCHMARK ===\n'));
  console.log(chalk.gray(`Testing ${FILTER_TEST_CASES.length} filter types`));

  const results: FilterBenchmarkResult[] = [];

  // Test a subset of configs
  const configsToTest = configs.filter(c =>
    c.name === 'baseline' ||
    c.name === 'scalar_int8_ram' ||
    c.name === 'binary_ram'
  );

  // Use default search variant for filter testing
  const searchVariant = SEARCH_VARIANTS.find(v => v.name === 'default')!;

  for (const config of configsToTest) {
    const collectionName = `benchmark_${config.collectionSuffix}`;
    console.log(chalk.cyan(`\n${config.name}:`));

    // Ensure payload indexes exist
    await benchmark.ensurePayloadIndexes(collectionName);

    for (const filterCase of FILTER_TEST_CASES) {
      const latencies: number[] = [];
      let totalResults = 0;

      for (let i = 0; i < queryVectors.length; i++) {
        const query = queryVectors[i]!;
        try {
          const result = await benchmark.runSearch(
            collectionName,
            query.vector,
            searchLimit,
            searchVariant.params,
            Object.keys(filterCase.filter).length > 0 ? filterCase.filter : undefined
          );
          latencies.push(result.latencyMs);
          totalResults += result.results.length;

          results.push({
            configName: config.name,
            filterName: filterCase.name,
            searchVariant: 'default',
            queryIndex: i,
            latencyMs: result.latencyMs,
            resultsCount: result.results.length,
          });
        } catch (e) {
          // Filter might not match any documents
          latencies.push(0);
        }
      }

      const avgLatency = latencies.filter(l => l > 0).length > 0
        ? latencies.filter(l => l > 0).reduce((a, b) => a + b, 0) / latencies.filter(l => l > 0).length
        : 0;
      const avgResults = totalResults / queryVectors.length;

      console.log(chalk.gray(`  ${filterCase.name}: avg=${avgLatency.toFixed(2)}ms, avgResults=${avgResults.toFixed(1)}`));
    }
  }

  return results;
}

/**
 * Benchmark concurrent query performance (throughput)
 */
async function runConcurrencyBenchmark(
  benchmark: QdrantBenchmark,
  configs: BenchmarkConfig[],
  queryVectors: Array<{ id: string | number; vector: number[] }>,
  searchLimit: number
): Promise<ConcurrencyBenchmarkResult[]> {
  console.log(chalk.blue.bold('\n=== CONCURRENCY BENCHMARK ===\n'));
  console.log(chalk.gray(`Testing concurrency levels: ${CONCURRENCY_LEVELS.join(', ')}`));

  const results: ConcurrencyBenchmarkResult[] = [];

  // Test a subset of configs
  const configsToTest = configs.filter(c =>
    c.name === 'baseline' ||
    c.name === 'scalar_int8_ram' ||
    c.name === 'binary_ram'
  );

  // Use a couple of representative search variants
  const variantsToTest = SEARCH_VARIANTS.filter(v =>
    v.name === 'default' || v.name === 'quant_rescore_2.0x'
  );

  const vectors = queryVectors.map(q => q.vector);
  // Repeat vectors to have enough for concurrency testing
  const extendedVectors = [...vectors, ...vectors, ...vectors, ...vectors];

  for (const config of configsToTest) {
    const collectionName = `benchmark_${config.collectionSuffix}`;
    console.log(chalk.cyan(`\n${config.name}:`));

    const applicableVariants = variantsToTest.filter(v =>
      !v.params.quantization || config.quantization
    );

    for (const variant of applicableVariants) {
      for (const concurrency of CONCURRENCY_LEVELS) {
        const result = await benchmark.runConcurrentSearches(
          collectionName,
          extendedVectors,
          searchLimit,
          variant.params,
          concurrency
        );

        const avgLatency = result.latencies.length > 0
          ? result.latencies.reduce((a, b) => a + b, 0) / result.latencies.length
          : 0;
        const throughput = result.latencies.length / (result.totalTimeMs / 1000);

        const benchmarkResult: ConcurrencyBenchmarkResult = {
          configName: config.name,
          searchVariant: variant.name,
          concurrencyLevel: concurrency,
          totalQueries: result.latencies.length + result.errors,
          totalTimeMs: result.totalTimeMs,
          avgLatencyMs: avgLatency,
          throughputQps: throughput,
          p50LatencyMs: calculatePercentile(result.latencies, 50),
          p95LatencyMs: calculatePercentile(result.latencies, 95),
          p99LatencyMs: calculatePercentile(result.latencies, 99),
          errors: result.errors,
        };

        results.push(benchmarkResult);

        console.log(chalk.gray(
          `  ${variant.name} @${concurrency}: ` +
          `throughput=${throughput.toFixed(1)} QPS, ` +
          `avg=${avgLatency.toFixed(2)}ms, ` +
          `p95=${benchmarkResult.p95LatencyMs.toFixed(2)}ms`
        ));
      }
    }
  }

  return results;
}

/**
 * Benchmark cold vs warm cache performance
 */
async function runColdWarmBenchmark(
  benchmark: QdrantBenchmark,
  configs: BenchmarkConfig[],
  queryVectors: Array<{ id: string | number; vector: number[] }>,
  searchLimit: number
): Promise<ColdWarmCacheResult[]> {
  console.log(chalk.blue.bold('\n=== COLD/WARM CACHE BENCHMARK ===\n'));

  const results: ColdWarmCacheResult[] = [];

  // Test a subset of configs
  const configsToTest = configs.filter(c =>
    c.name === 'baseline' ||
    c.name === 'scalar_int8_ram' ||
    c.name === 'binary_ram'
  );

  const searchVariant = SEARCH_VARIANTS.find(v => v.name === 'default')!;

  for (const config of configsToTest) {
    const collectionName = `benchmark_${config.collectionSuffix}`;
    console.log(chalk.cyan(`\n${config.name}:`));

    // Cold run (first queries after "clearing" cache)
    console.log(chalk.gray('  Running cold queries (first-time access)...'));
    await benchmark.clearCache();

    const coldLatencies: number[] = [];
    for (const query of queryVectors.slice(0, 20)) { // Use fewer queries for cold test
      const result = await benchmark.runSearch(collectionName, query.vector, searchLimit, searchVariant.params);
      coldLatencies.push(result.latencyMs);
    }

    const coldAvg = coldLatencies.reduce((a, b) => a + b, 0) / coldLatencies.length;
    results.push({
      configName: config.name,
      searchVariant: 'default',
      runType: 'cold',
      avgLatencyMs: coldAvg,
      p50LatencyMs: calculatePercentile(coldLatencies, 50),
      p95LatencyMs: calculatePercentile(coldLatencies, 95),
    });

    // Warm run (repeated queries, data should be cached)
    console.log(chalk.gray('  Running warm queries (cached)...'));

    // Warmup: run queries multiple times to ensure caching
    for (let w = 0; w < 3; w++) {
      for (const query of queryVectors) {
        await benchmark.runSearch(collectionName, query.vector, searchLimit, searchVariant.params);
      }
    }

    const warmLatencies: number[] = [];
    for (const query of queryVectors) {
      const result = await benchmark.runSearch(collectionName, query.vector, searchLimit, searchVariant.params);
      warmLatencies.push(result.latencyMs);
    }

    const warmAvg = warmLatencies.reduce((a, b) => a + b, 0) / warmLatencies.length;
    results.push({
      configName: config.name,
      searchVariant: 'default',
      runType: 'warm',
      avgLatencyMs: warmAvg,
      p50LatencyMs: calculatePercentile(warmLatencies, 50),
      p95LatencyMs: calculatePercentile(warmLatencies, 95),
    });

    const improvement = ((coldAvg - warmAvg) / coldAvg * 100).toFixed(1);
    console.log(chalk.gray(`  Cold: ${coldAvg.toFixed(2)}ms, Warm: ${warmAvg.toFixed(2)}ms (${improvement}% improvement)`));
  }

  return results;
}

// ============================================================================
// Output Generation
// ============================================================================

async function saveResults(
  recallResults: RecallResult[],
  rawResults: BenchmarkResult[],
  collectionStats: CollectionStats[],
  outputDir: string
): Promise<{ jsonPath: string; csvPath: string; summaryPath: string }> {
  await fs.mkdir(outputDir, { recursive: true });

  const timestamp = formatTimestamp();

  // Save raw JSON results
  const jsonPath = `${outputDir}/benchmark-raw-${timestamp}.json`;
  await fs.writeFile(jsonPath, JSON.stringify({
    timestamp: new Date().toISOString(),
    collectionStats,
    recallResults,
    rawResults,
  }, null, 2));

  // Save CSV summary
  const csvPath = `${outputDir}/benchmark-summary-${timestamp}.csv`;
  const csvHeaders = [
    'config_name',
    'search_variant',
    'recall@10',
    'avg_latency_ms',
    'p50_latency_ms',
    'p95_latency_ms',
    'p99_latency_ms',
    'min_latency_ms',
    'max_latency_ms',
  ];

  const csvRows = recallResults.map(r => [
    r.configName,
    r.searchVariant,
    r.recall.toFixed(4),
    r.avgLatencyMs.toFixed(2),
    r.p50LatencyMs.toFixed(2),
    r.p95LatencyMs.toFixed(2),
    r.p99LatencyMs.toFixed(2),
    r.minLatencyMs.toFixed(2),
    r.maxLatencyMs.toFixed(2),
  ].join(','));

  await fs.writeFile(csvPath, [csvHeaders.join(','), ...csvRows].join('\n'));

  // Save human-readable summary
  const summaryPath = `${outputDir}/benchmark-report-${timestamp}.txt`;
  let summary = `
QDRANT CONFIGURATION BENCHMARK REPORT
=====================================
Generated: ${new Date().toISOString()}

COLLECTION STATISTICS
---------------------
`;

  for (const stats of collectionStats) {
    summary += `
${stats.name}:
  Points: ${stats.pointsCount.toLocaleString()}
  Indexed: ${stats.indexedVectorsCount.toLocaleString()}
  Segments: ${stats.segmentsCount}
  Quantization: ${stats.quantizationType || 'none'}
  Status: ${stats.status}
  Memory: ${stats.memoryUsageEstimate}
`;
  }

  summary += `
RECALL & LATENCY RESULTS (sorted by recall, then latency)
---------------------------------------------------------
`;

  summary += `
${'Config'.padEnd(25)} ${'Variant'.padEnd(25)} ${'Recall@10'.padEnd(10)} ${'Avg(ms)'.padEnd(10)} ${'P50(ms)'.padEnd(10)} ${'P95(ms)'.padEnd(10)} ${'P99(ms)'.padEnd(10)}
${'-'.repeat(100)}
`;

  for (const r of recallResults) {
    summary += `${r.configName.padEnd(25)} ${r.searchVariant.padEnd(25)} ${(r.recall * 100).toFixed(2).padStart(7)}%  ${r.avgLatencyMs.toFixed(2).padStart(8)} ${r.p50LatencyMs.toFixed(2).padStart(10)} ${r.p95LatencyMs.toFixed(2).padStart(10)} ${r.p99LatencyMs.toFixed(2).padStart(10)}\n`;
  }

  // Add recommendations
  summary += `
RECOMMENDATIONS
---------------
`;

  // Find best configurations for different use cases
  const highRecall = recallResults.filter(r => r.recall >= 0.99);
  const balanced = recallResults.filter(r => r.recall >= 0.95);
  const fastest = [...recallResults].sort((a, b) => a.avgLatencyMs - b.avgLatencyMs);

  if (highRecall.length > 0) {
    const best = highRecall.sort((a, b) => a.avgLatencyMs - b.avgLatencyMs)[0]!;
    summary += `
Best for HIGH ACCURACY (>=99% recall):
  Config: ${best.configName}
  Variant: ${best.searchVariant}
  Recall: ${(best.recall * 100).toFixed(2)}%
  Latency: ${best.avgLatencyMs.toFixed(2)}ms avg, ${best.p95LatencyMs.toFixed(2)}ms p95
`;
  }

  if (balanced.length > 0) {
    const best = balanced.sort((a, b) => a.avgLatencyMs - b.avgLatencyMs)[0]!;
    summary += `
Best for BALANCED (>=95% recall, fastest):
  Config: ${best.configName}
  Variant: ${best.searchVariant}
  Recall: ${(best.recall * 100).toFixed(2)}%
  Latency: ${best.avgLatencyMs.toFixed(2)}ms avg, ${best.p95LatencyMs.toFixed(2)}ms p95
`;
  }

  if (fastest.length > 0) {
    const best = fastest[0]!;
    summary += `
Best for LOWEST LATENCY (fastest, any recall):
  Config: ${best.configName}
  Variant: ${best.searchVariant}
  Recall: ${(best.recall * 100).toFixed(2)}%
  Latency: ${best.avgLatencyMs.toFixed(2)}ms avg, ${best.p95LatencyMs.toFixed(2)}ms p95
`;
  }

  await fs.writeFile(summaryPath, summary);

  return { jsonPath, csvPath, summaryPath };
}

function printSummaryTable(recallResults: RecallResult[]): void {
  console.log(chalk.blue.bold('\n=== RESULTS SUMMARY ===\n'));

  console.log(chalk.cyan(
    'Config'.padEnd(25) +
    'Variant'.padEnd(25) +
    'Recall@10'.padEnd(12) +
    'Avg(ms)'.padEnd(10) +
    'P95(ms)'.padEnd(10)
  ));
  console.log(chalk.gray('-'.repeat(82)));

  for (const r of recallResults) {
    const recallColor = r.recall >= 0.99 ? chalk.green : r.recall >= 0.95 ? chalk.yellow : chalk.red;
    const latencyColor = r.avgLatencyMs < 50 ? chalk.green : r.avgLatencyMs < 200 ? chalk.yellow : chalk.red;

    console.log(
      chalk.white(r.configName.padEnd(25)) +
      chalk.gray(r.searchVariant.padEnd(25)) +
      recallColor(`${(r.recall * 100).toFixed(2)}%`.padStart(9) + '   ') +
      latencyColor(r.avgLatencyMs.toFixed(2).padStart(8) + '  ') +
      latencyColor(r.p95LatencyMs.toFixed(2).padStart(8))
    );
  }
}

// ============================================================================
// Main Entry Point
// ============================================================================

async function main() {
  console.log(chalk.blue.bold('\n=== QDRANT CONFIGURATION BENCHMARK ===\n'));

  // Parse arguments
  const args = process.argv.slice(2);
  let sourceCollection = 'embeddings';
  let sampleSize = 10000;
  let queryCount = 50;
  let searchLimit = 100;
  let warmupRuns = 5;
  let skipSetup = false;
  let outputDir = 'benchmarks/qdrant-configs';

  // Extended benchmark flags
  let runExtended = false;
  let runFilters = false;
  let runKValues = false;
  let runConcurrency = false;
  let runColdWarm = false;
  let quickMode = false;
  let topMode = false;

  for (const arg of args) {
    if (arg.startsWith('--source=')) {
      sourceCollection = arg.split('=')[1]!;
    } else if (arg.startsWith('--sample=')) {
      sampleSize = parseInt(arg.split('=')[1]!, 10);
    } else if (arg.startsWith('--queries=')) {
      queryCount = parseInt(arg.split('=')[1]!, 10);
    } else if (arg.startsWith('--limit=')) {
      searchLimit = parseInt(arg.split('=')[1]!, 10);
    } else if (arg.startsWith('--warmup=')) {
      warmupRuns = parseInt(arg.split('=')[1]!, 10);
    } else if (arg === '--skip-setup') {
      skipSetup = true;
    } else if (arg.startsWith('--output=')) {
      outputDir = arg.split('=')[1]!;
    } else if (arg === '--extended' || arg === '-e') {
      runExtended = true;
    } else if (arg === '--filters') {
      runFilters = true;
    } else if (arg === '--k-values') {
      runKValues = true;
    } else if (arg === '--concurrency') {
      runConcurrency = true;
    } else if (arg === '--cold-warm') {
      runColdWarm = true;
    } else if (arg === '--quick') {
      quickMode = true;
    } else if (arg === '--top' || arg === '-t') {
      topMode = true;
    } else if (arg === '--help' || arg === '-h') {
      console.log(`
Usage: bun run scripts/benchmark-qdrant-configs.ts [options]

Options:
  --source=<collection>   Source collection to copy vectors from (default: embeddings)
  --sample=<number>       Number of vectors to sample for benchmark (default: 10000)
  --queries=<number>      Number of query vectors to test (default: 50)
  --limit=<number>        Number of results to retrieve per search (default: 100)
  --warmup=<number>       Number of warmup runs before benchmarking (default: 5)
  --skip-setup            Skip setup phase (use existing benchmark collections)
  --output=<dir>          Output directory for results (default: benchmarks/qdrant-configs)
  --help, -h              Show this help message

Extended Benchmarks:
  --extended, -e          Run ALL extended benchmarks (filters, k-values, concurrency, cold-warm)
  --filters               Run filter performance benchmark
  --k-values              Run different k (result limit) benchmark
  --concurrency           Run concurrent query throughput benchmark
  --cold-warm             Run cold vs warm cache benchmark
  --quick                 Quick mode: test only 3 configs and 5 variants
  --top, -t               Top mode: test 8 most promising configs for low-RAM/high-IO scenarios

Environment Variables:
  QDRANT_URL              Qdrant server URL (default: http://localhost:6333)
  QDRANT_API_KEY          Qdrant API key (optional)

Benchmark Configurations (${BENCHMARK_CONFIGS.length} total):
  - Baseline (no quantization)
  - Scalar INT8 (various: disk, RAM, different quantiles)
  - Binary (RAM, disk)
  - HNSW variations (m=8,12,16,24,32, ef_construct variations)

Search Variants (${SEARCH_VARIANTS.length} total):
  - HNSW ef: 32, 48, 64, 96, 128, 192, 256, 384, 512
  - Quantization: no rescore, rescore 1.0x-5.0x
  - Combined: ef + oversampling variations

Examples:
  # Basic benchmark
  bun run scripts/benchmark-qdrant-configs.ts --source=embeddings --sample=5000

  # Quick test with fewer variants (3 configs, 5 variants)
  bun run scripts/benchmark-qdrant-configs.ts --sample=1000 --queries=20 --quick

  # Top 8 configs for low-RAM/high-IO scenarios (RECOMMENDED)
  bun run scripts/benchmark-qdrant-configs.ts --sample=50000 --queries=50 --top

  # Full benchmark with all extended tests
  bun run scripts/benchmark-qdrant-configs.ts --sample=10000 --extended

  # Skip setup, run only filter benchmark
  bun run scripts/benchmark-qdrant-configs.ts --skip-setup --filters

  # Production-like benchmark
  bun run scripts/benchmark-qdrant-configs.ts --sample=50000 --queries=100 --extended
`);
      process.exit(0);
    }
  }

  // If --extended is set, enable all extended benchmarks
  if (runExtended) {
    runFilters = true;
    runKValues = true;
    runConcurrency = true;
    runColdWarm = true;
  }

  // CRITICAL: Validate localhost connection
  console.log(chalk.yellow('=== SAFETY CHECK ==='));
  console.log(chalk.gray(`Qdrant URL: ${QDRANT_URL}`));

  if (!validateLocalhostUrl(QDRANT_URL)) {
    console.error(chalk.red.bold('\n!!! ERROR: NOT A LOCALHOST URL !!!\n'));
    console.error(chalk.red('This script is designed to run against a LOCAL Qdrant instance only.'));
    console.error(chalk.red('Running against a remote/production instance could cause data loss or performance issues.'));
    console.error(chalk.red(`\nDetected URL: ${QDRANT_URL}`));
    console.error(chalk.yellow('\nTo run against localhost, set:'));
    console.error(chalk.cyan('  export QDRANT_URL="http://localhost:6333"'));
    process.exit(1);
  }

  console.log(chalk.green('Localhost URL confirmed. Safe to proceed.\n'));

  // Print configuration
  console.log(chalk.cyan('Configuration:'));
  console.log(chalk.gray(`  Source collection: ${sourceCollection}`));
  console.log(chalk.gray(`  Sample size: ${sampleSize.toLocaleString()}`));
  console.log(chalk.gray(`  Query count: ${queryCount}`));
  console.log(chalk.gray(`  Search limit: ${searchLimit}`));
  console.log(chalk.gray(`  Warmup runs: ${warmupRuns}`));
  console.log(chalk.gray(`  Skip setup: ${skipSetup}`));
  console.log(chalk.gray(`  Output dir: ${outputDir}`));

  // Apply mode selection - reduce configs and variants based on mode
  let configsToUse = BENCHMARK_CONFIGS;
  let variantsToUse = SEARCH_VARIANTS;

  if (topMode) {
    console.log(chalk.yellow('\nTop mode enabled - using 8 most promising configs for low-RAM/high-IO'));
    console.log(chalk.gray('  Configs: baseline, binary (RAM/disk), scalar (RAM/disk), optimized HNSW variants'));
    configsToUse = BENCHMARK_CONFIGS.filter(c => TOP_CONFIG_NAMES.includes(c.name));
    variantsToUse = SEARCH_VARIANTS.filter(v => TOP_SEARCH_VARIANT_NAMES.includes(v.name));
  } else if (quickMode) {
    console.log(chalk.yellow('\nQuick mode enabled - using reduced config/variant set'));
    configsToUse = BENCHMARK_CONFIGS.filter(c =>
      ['baseline', 'scalar_int8_ram', 'binary_ram'].includes(c.name)
    );
    variantsToUse = SEARCH_VARIANTS.filter(v =>
      ['default', 'ef_64', 'ef_128', 'quant_no_rescore', 'quant_rescore_2.0x'].includes(v.name)
    );
  }

  console.log(chalk.cyan(`\nConfigurations to test (${configsToUse.length}):`));
  for (const config of configsToUse) {
    console.log(chalk.gray(`  - ${config.name}: ${config.description}`));
  }

  console.log(chalk.cyan(`\nSearch variants to test (${variantsToUse.length}):`));
  for (const variant of variantsToUse) {
    console.log(chalk.gray(`  - ${variant.name}`));
  }

  console.log(chalk.cyan('\nExtended benchmarks:'));
  console.log(chalk.gray(`  Filters: ${runFilters ? 'yes' : 'no'}`));
  console.log(chalk.gray(`  K-values: ${runKValues ? 'yes' : 'no'}`));
  console.log(chalk.gray(`  Concurrency: ${runConcurrency ? 'yes' : 'no'}`));
  console.log(chalk.gray(`  Cold/Warm: ${runColdWarm ? 'yes' : 'no'}`));

  // Initialize client
  const benchmark = new QdrantBenchmark(QDRANT_URL, QDRANT_API_KEY);

  // Store extended results
  let filterResults: FilterBenchmarkResult[] = [];
  let kValueResults: KValueBenchmarkResult[] = [];
  let concurrencyResults: ConcurrencyBenchmarkResult[] = [];
  let coldWarmResults: ColdWarmCacheResult[] = [];

  try {
    // =========================================================================
    // STEP 1: Get query vectors from SOURCE collection
    // =========================================================================
    const queryVectors = await getQueryVectorsFromSource(benchmark, sourceCollection, queryCount);

    // =========================================================================
    // STEP 2: Build ground truth using baseline config
    // (Creates baseline, benchmarks it, builds ground truth, then deletes it)
    // =========================================================================
    const { groundTruth, baselineResults, baselineStats } = await buildGroundTruth(
      benchmark,
      sourceCollection,
      sampleSize,
      queryVectors,
      searchLimit
    );

    // =========================================================================
    // STEP 3: Benchmark each config sequentially (create -> benchmark -> delete)
    // =========================================================================
    console.log(chalk.blue.bold('\n=== BENCHMARKING CONFIGURATIONS ===\n'));
    console.log(chalk.gray(`Testing ${configsToUse.length} configurations (excluding baseline which was already tested)`));

    const allResults: BenchmarkResult[] = [...baselineResults];
    const collectionStats: CollectionStats[] = [baselineStats];

    // Filter out baseline since we already benchmarked it
    const remainingConfigs = configsToUse.filter(c => c.name !== 'baseline');

    for (let i = 0; i < remainingConfigs.length; i++) {
      const config = remainingConfigs[i]!;
      console.log(chalk.cyan(`\n[${i + 1}/${remainingConfigs.length}] Testing: ${config.name}`));

      const { results, stats } = await benchmarkSingleConfig(
        benchmark,
        config,
        sourceCollection,
        sampleSize,
        queryVectors,
        searchLimit,
        warmupRuns,
        true // Delete after benchmarking
      );

      allResults.push(...results);
      collectionStats.push(stats);
    }

    // =========================================================================
    // STEP 4: Calculate recall
    // =========================================================================
    console.log(chalk.blue.bold('\n=== CALCULATING RECALL ===\n'));
    const recallResults = calculateRecall(allResults, groundTruth, 10);

    // Print summary
    printSummaryTable(recallResults);

    // =========================================================================
    // EXTENDED BENCHMARKS (optional)
    // Note: These require keeping collections alive, so we skip for now
    // or run on a subset with temporary collections
    // =========================================================================

    // For extended benchmarks, we need to temporarily recreate collections
    // Only do this if explicitly requested
    if (runKValues || runFilters || runConcurrency || runColdWarm) {
      console.log(chalk.blue.bold('\n=== EXTENDED BENCHMARKS ===\n'));
      console.log(chalk.yellow('Creating temporary collections for extended benchmarks...'));

      // Use just the top 3 configs for extended tests
      const extendedConfigs = configsToUse.filter(c =>
        ['baseline', 'scalar_int8_ram', 'binary_ram'].includes(c.name)
      ).slice(0, 3);

      for (const config of extendedConfigs) {
        // Temporarily create the collection
        const { stats } = await benchmarkSingleConfig(
          benchmark,
          config,
          sourceCollection,
          sampleSize,
          queryVectors,
          searchLimit,
          warmupRuns,
          false // Don't delete yet - need for extended tests
        );

        const collectionName = `benchmark_${config.collectionSuffix}`;

        if (runKValues) {
          const kResults = await runKValueBenchmark(benchmark, [config], queryVectors);
          kValueResults.push(...kResults);
        }

        if (runFilters) {
          const fResults = await runFilterBenchmark(benchmark, [config], queryVectors, searchLimit);
          filterResults.push(...fResults);
        }

        if (runConcurrency) {
          const cResults = await runConcurrencyBenchmark(benchmark, [config], queryVectors, searchLimit);
          concurrencyResults.push(...cResults);
        }

        if (runColdWarm) {
          const cwResults = await runColdWarmBenchmark(benchmark, [config], queryVectors, searchLimit);
          coldWarmResults.push(...cwResults);
        }

        // Delete collection after extended tests
        console.log(chalk.yellow(`Deleting ${collectionName}...`));
        await benchmark.deleteCollectionIfExists(collectionName);
      }
    }

    // =========================================================================
    // STEP 5: Save results
    // =========================================================================
    console.log(chalk.blue('\n=== SAVING RESULTS ===\n'));
    const { jsonPath, csvPath, summaryPath } = await saveResults(
      recallResults,
      allResults,
      collectionStats,
      outputDir
    );

    // Save extended results if any
    if (filterResults.length > 0 || kValueResults.length > 0 ||
        concurrencyResults.length > 0 || coldWarmResults.length > 0) {
      const timestamp = formatTimestamp();
      const extendedPath = `${outputDir}/benchmark-extended-${timestamp}.json`;
      await fs.writeFile(extendedPath, JSON.stringify({
        timestamp: new Date().toISOString(),
        filterResults,
        kValueResults,
        concurrencyResults,
        coldWarmResults,
      }, null, 2));
      console.log(chalk.green(`  Extended: ${extendedPath}`));
    }

    console.log(chalk.green(`  JSON: ${jsonPath}`));
    console.log(chalk.green(`  CSV: ${csvPath}`));
    console.log(chalk.green(`  Report: ${summaryPath}`));

    // Print extended summaries
    if (concurrencyResults.length > 0) {
      console.log(chalk.blue.bold('\n=== THROUGHPUT SUMMARY ===\n'));
      const best = [...concurrencyResults].sort((a, b) => b.throughputQps - a.throughputQps)[0];
      if (best) {
        console.log(chalk.green(`Best throughput: ${best.throughputQps.toFixed(1)} QPS`));
        console.log(chalk.gray(`  Config: ${best.configName}, Variant: ${best.searchVariant}, Concurrency: ${best.concurrencyLevel}`));
      }
    }

    if (coldWarmResults.length > 0) {
      console.log(chalk.blue.bold('\n=== CACHE IMPACT SUMMARY ===\n'));
      for (const config of [...new Set(coldWarmResults.map(r => r.configName))]) {
        const cold = coldWarmResults.find(r => r.configName === config && r.runType === 'cold');
        const warm = coldWarmResults.find(r => r.configName === config && r.runType === 'warm');
        if (cold && warm) {
          const improvement = ((cold.avgLatencyMs - warm.avgLatencyMs) / cold.avgLatencyMs * 100).toFixed(1);
          console.log(chalk.gray(`${config}: Cold ${cold.avgLatencyMs.toFixed(2)}ms -> Warm ${warm.avgLatencyMs.toFixed(2)}ms (${improvement}% faster)`));
        }
      }
    }

    console.log(chalk.green.bold('\n=== BENCHMARK COMPLETE ===\n'));

  } catch (error) {
    console.error(chalk.red(`\nBenchmark failed: ${error}`));
    if (error instanceof Error) {
      console.error(chalk.gray(error.stack));
    }
    process.exit(1);
  }
}

main().catch(error => {
  console.error(chalk.red(`Fatal error: ${error}`));
  process.exit(1);
});
