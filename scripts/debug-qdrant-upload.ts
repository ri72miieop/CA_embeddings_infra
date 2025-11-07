#!/usr/bin/env bun
/**
 * Debug script to test Qdrant upload with a single file
 * This will show exactly what's being sent and received
 */

import fs from 'fs/promises';
import path from 'path';
import { QdrantClient } from '@qdrant/qdrant-js';
import { parquetRead } from 'hyparquet';

const QDRANT_URL = process.env.QDRANT_URL || 'http://localhost:6333';
const QDRANT_COLLECTION_NAME = process.env.QDRANT_COLLECTION_NAME || 'embeddings';
const QUEUE_DIR = process.env.QUEUE_DIR || './data/embedding-calls/embedding-queue';

async function debugUpload() {
  console.log('🔍 Qdrant Upload Debug Script\n');
  
  // Connect to Qdrant
  const client = new QdrantClient({
    url: QDRANT_URL,
  });

  console.log(`Connected to Qdrant at ${QDRANT_URL}`);
  
  // Get first pending file
  const files = await fs.readdir(QUEUE_DIR);
  const pendingFile = files.find(f => f.endsWith('.pending.parquet'));
  
  if (!pendingFile) {
    console.log('No pending files found');
    return;
  }

  console.log(`\nReading file: ${pendingFile}\n`);
  const filePath = path.join(QUEUE_DIR, pendingFile);
  
  // Read first embedding from parquet
  const buffer = await fs.readFile(filePath);
  const arrayBuffer = buffer.buffer.slice(
    buffer.byteOffset,
    buffer.byteOffset + buffer.byteLength
  ) as ArrayBuffer;

  let firstRow: any = null;
  await parquetRead({
    file: arrayBuffer,
    rowFormat: 'object',
    onComplete: (rows: any[]) => {
      if (rows.length > 0) {
        firstRow = rows[0];
      }
    },
  });

  if (!firstRow) {
    console.log('No data in file');
    return;
  }

  console.log('📋 First Row Data:');
  console.log('-------------------');
  console.log('Key:', firstRow.key);
  console.log('Key Type:', typeof firstRow.key);
  console.log('queueItemId:', firstRow.queueItemId);
  console.log('timestamp:', firstRow.timestamp);
  console.log('correlationId:', firstRow.correlationId);
  console.log('retryCount:', firstRow.retryCount);
  
  // Extract vector
  const vectorDim = Object.keys(firstRow).filter((k) => k.startsWith('v')).length;
  const vector = new Array(vectorDim);
  for (let i = 0; i < vectorDim; i++) {
    vector[i] = firstRow[`v${i}`] as number;
  }
  
  console.log('Vector Dimension:', vectorDim);
  console.log('First 3 vector values:', vector.slice(0, 3));
  console.log('All values are numbers:', vector.every(v => typeof v === 'number' && !isNaN(v)));
  
  // Parse metadata
  let metadata = firstRow.metadata;
  if (typeof metadata === 'string') {
    try {
      metadata = JSON.parse(metadata);
    } catch (e) {
      metadata = {};
    }
  }
  
  console.log('Metadata:', metadata);
  console.log('\n📤 Attempting Upload...\n');

  // Test 1: Try with key as 64-bit unsigned integer (using BigInt)
  console.log('Test 1: Using key as 64-BIT UNSIGNED INTEGER (via BigInt)');
  const bigIntKey = BigInt(firstRow.key);
  const numericKey = Number(bigIntKey);
  
  console.log('Original key (string):', firstRow.key);
  console.log('BigInt representation:', bigIntKey.toString());
  console.log('Number (for Qdrant):', numericKey);
  console.log('Is within safe integer range:', bigIntKey <= BigInt(Number.MAX_SAFE_INTEGER));
  
  const point1 = {
    id: numericKey, // This will be handled as u64 by Qdrant backend
    vector: vector,
    payload: {
      key: firstRow.key,
      metadata: metadata || {},
      queueItemId: firstRow.queueItemId,
      timestamp: firstRow.timestamp,
      correlationId: firstRow.correlationId || undefined,
    },
  };
  
  console.log('\nPoint structure:');
  console.log('  ID:', point1.id, `(${typeof point1.id})`);
  console.log('  Vector length:', point1.vector.length);
  console.log('  Payload keys:', Object.keys(point1.payload));
  
  try {
    await client.upsert(QDRANT_COLLECTION_NAME, {
      wait: true,
      points: [point1],
    });
    console.log('✅ SUCCESS with u64 numeric ID!\n');
  } catch (error: any) {
    console.log('❌ FAILED with u64 numeric ID');
    console.log('Error:', error.message);
    if (error.data) {
      console.log('Error data:', JSON.stringify(error.data, null, 2));
    }
    if (error.response) {
      console.log('Response:', JSON.stringify(error.response, null, 2));
    }
    console.log('\n');
  }

  // Test 2: Try with key as string (for comparison)
  console.log('Test 2: Using key as STRING (for comparison)');
  const point2 = {
    id: String(firstRow.key),
    vector: vector,
    payload: {
      key: firstRow.key,
      metadata: metadata || {},
      queueItemId: firstRow.queueItemId,
      timestamp: firstRow.timestamp,
      correlationId: firstRow.correlationId || undefined,
    },
  };
  
  console.log('Point structure:');
  console.log('  ID:', point2.id, `(${typeof point2.id})`);
  console.log('  Vector length:', point2.vector.length);
  console.log('  Payload keys:', Object.keys(point2.payload));
  
  try {
    await client.upsert(QDRANT_COLLECTION_NAME, {
      wait: true,
      points: [point2],
    });
    console.log('✅ SUCCESS with string ID!\n');
  } catch (error: any) {
    console.log('❌ FAILED with string ID');
    console.log('Error:', error.message);
    if (error.data) {
      console.log('Error data:', JSON.stringify(error.data, null, 2));
    }
    if (error.response) {
      console.log('Response:', JSON.stringify(error.response, null, 2));
    }
    console.log('\n');
  }

  // Test 3: Check collection configuration
  console.log('Test 3: Collection Configuration');
  try {
    const collectionInfo = await client.getCollection(QDRANT_COLLECTION_NAME);
    console.log('Collection info:');
    console.log(JSON.stringify(collectionInfo, null, 2));
  } catch (error: any) {
    console.log('Error getting collection info:', error.message);
  }
}

debugUpload().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});

