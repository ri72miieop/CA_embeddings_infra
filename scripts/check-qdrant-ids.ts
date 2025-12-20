/**
 * Script to check all Qdrant records for ID precision loss issues
 *
 * The bug: Tweet IDs (19 digits) were converted via Number(BigInt(key)) which loses precision
 * because Number.MAX_SAFE_INTEGER is only ~16 digits.
 *
 * This script:
 * 1. Scrolls through all points in the collection
 * 2. Compares the numeric ID with payload.key
 * 3. Collects all mismatched IDs in memory
 * 4. Writes them to a file at the end
 *
 * Usage: bun run scripts/check-qdrant-ids.ts [--output=mismatched-ids.json] [--batch-size=1000]
 */

import { QdrantClient } from '@qdrant/js-client-rest';
import { config } from 'dotenv';
import { writeFileSync } from 'fs';

config();

// Configuration
const QDRANT_URL = process.env.QDRANT_URL || 'http://localhost';
const QDRANT_PORT = parseInt(process.env.QDRANT_PORT || '6333', 10);
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;
const QDRANT_COLLECTION = process.env.QDRANT_COLLECTION || 'embeddings';

// Parse CLI arguments
const args = process.argv.slice(2);
const outputArg = args.find(arg => arg.startsWith('--output='));
const OUTPUT_FILE = outputArg ? outputArg.split('=')[1] : 'mismatched-ids.json';
const batchSizeArg = args.find(arg => arg.startsWith('--batch-size='));
const BATCH_SIZE = batchSizeArg ? parseInt(batchSizeArg.split('=')[1] || '1000', 10) : 1000;

console.log('='.repeat(70));
console.log('Qdrant ID Precision Check');
console.log('='.repeat(70));
console.log(`Qdrant URL: ${QDRANT_URL}:${QDRANT_PORT}`);
console.log(`Collection: ${QDRANT_COLLECTION}`);
console.log(`Batch size: ${BATCH_SIZE}`);
console.log(`Output file: ${OUTPUT_FILE}`);
console.log('='.repeat(70));

// Initialize Qdrant client
const client = new QdrantClient({
  url: QDRANT_URL,
  port: QDRANT_PORT,
  apiKey: QDRANT_API_KEY,
});

interface MismatchedId {
  currentId: string; // Stored as string to avoid BigInt serialization issues
  expectedId: string;
}

async function main() {
  // Get collection info
  const collectionInfo = await client.getCollection(QDRANT_COLLECTION);
  const totalPoints = collectionInfo.points_count || 0;

  console.log(`\nTotal points in collection: ${totalPoints.toLocaleString()}`);
  console.log('Scanning for points with mismatched IDs...\n');

  let offset: string | number | null = null;
  let scannedCount = 0;
  let noKeyCount = 0;
  const startTime = Date.now();

  // Collect all mismatched IDs in memory
  const mismatchedIds: MismatchedId[] = [];

  while (true) {
    // Scroll through points
    const scrollParams: any = {
      limit: BATCH_SIZE,
      with_payload: true,
      with_vector: false, // Don't need vectors for this check
    };

    if (offset !== null) {
      scrollParams.offset = offset;
    }

    const response = await client.scroll(QDRANT_COLLECTION, scrollParams);

    if (!response.points || response.points.length === 0) {
      break;
    }

    for (const point of response.points) {
      scannedCount++;

      const currentId = point.id;
      const payloadKey = (point.payload as any)?.key as string;

      if (!payloadKey) {
        noKeyCount++;
        continue;
      }

      // Check if ID matches payload.key
      // Convert both to strings for comparison
      const currentIdStr = String(currentId);

      if (currentIdStr !== payloadKey) {
        mismatchedIds.push({
          currentId: currentIdStr, // Convert to string for JSON serialization (BigInt can't be serialized)
          expectedId: payloadKey,
        });
      }
    }

    // Progress update
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = scannedCount / elapsed;
    const eta = totalPoints > 0 ? ((totalPoints - scannedCount) / rate).toFixed(0) : '?';

    process.stdout.write(
      `\rScanned: ${scannedCount.toLocaleString()} / ${totalPoints.toLocaleString()} | ` +
      `Mismatches: ${mismatchedIds.length.toLocaleString()} | ` +
      `Rate: ${rate.toFixed(0)}/s | ` +
      `ETA: ${eta}s   `
    );

    // Get next page offset
    const nextOffset = response.next_page_offset;
    if (nextOffset === undefined || nextOffset === null) {
      break;
    }
    offset = nextOffset as string | number;
  }

  // Write results to file
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);

  const output = {
    timestamp: new Date().toISOString(),
    collection: QDRANT_COLLECTION,
    totalScanned: scannedCount,
    totalMismatched: mismatchedIds.length,
    noKeyCount: noKeyCount,
    scanDurationSeconds: parseFloat(totalTime),
    mismatchedIds: mismatchedIds,
  };

  writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2));

  // Final summary
  console.log('\n\n' + '='.repeat(70));
  console.log('Scan Complete');
  console.log('='.repeat(70));
  console.log(`Total scanned:      ${scannedCount.toLocaleString()}`);
  console.log(`Mismatched IDs:     ${mismatchedIds.length.toLocaleString()}`);
  console.log(`Points without key: ${noKeyCount.toLocaleString()}`);
  console.log(`Time elapsed:       ${totalTime}s`);
  console.log(`Output written to:  ${OUTPUT_FILE}`);
  console.log('='.repeat(70));

  if (mismatchedIds.length > 0) {
    console.log(`\n💡 Found ${mismatchedIds.length.toLocaleString()} points with mismatched IDs`);
    console.log(`   Run 'bun run scripts/fix-qdrant-ids.ts' to fix them`);
  } else {
    console.log('\n✅ All IDs match their payload.key - no issues found!');
  }
}

// Run the check
main().catch(error => {
  console.error('\n❌ Check failed:', error);
  process.exit(1);
});

