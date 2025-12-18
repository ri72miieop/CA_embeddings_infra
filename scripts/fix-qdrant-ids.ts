/**
 * Migration script to fix Qdrant point IDs that lost precision due to Number conversion
 *
 * The bug: Tweet IDs (19 digits) were converted via Number(BigInt(key)) which loses precision
 * because Number.MAX_SAFE_INTEGER is only ~16 digits.
 *
 * This script:
 * 1. Scrolls through all points in the collection
 * 2. Compares the numeric ID with payload.key
 * 3. If they don't match, re-inserts with the correct string ID and deletes the old one
 *
 * Usage: bun run scripts/fix-qdrant-ids.ts [--dry-run] [--batch-size=1000]
 */

import { QdrantClient } from '@qdrant/js-client-rest';
import { config } from 'dotenv';

config();

// Configuration
const QDRANT_URL = process.env.QDRANT_URL || 'http://localhost:6333';
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;
const QDRANT_COLLECTION = process.env.QDRANT_COLLECTION || 'embeddings';

// Parse CLI arguments
const args = process.argv.slice(2);
const isDryRun = args.includes('--dry-run');
const batchSizeArg = args.find(arg => arg.startsWith('--batch-size='));
const BATCH_SIZE = batchSizeArg ? parseInt(batchSizeArg.split('=')[1] || '1000', 10) : 1000;

console.log('='.repeat(70));
console.log('Qdrant ID Precision Fix Migration');
console.log('='.repeat(70));
console.log(`Qdrant URL: ${QDRANT_URL}`);
console.log(`Collection: ${QDRANT_COLLECTION}`);
console.log(`Batch size: ${BATCH_SIZE}`);
console.log(`Dry run: ${isDryRun}`);
console.log('='.repeat(70));

if (isDryRun) {
  console.log('\n⚠️  DRY RUN MODE - No changes will be made\n');
}

// Initialize Qdrant client
const client = new QdrantClient({
  url: QDRANT_URL,
  apiKey: QDRANT_API_KEY,
});

interface PointToFix {
  oldId: string | number;
  correctId: string;
  vector: number[];
  payload: Record<string, any>;
}

async function main() {
  // Get collection info
  const collectionInfo = await client.getCollection(QDRANT_COLLECTION);
  const totalPoints = collectionInfo.points_count || 0;

  console.log(`Total points in collection: ${totalPoints.toLocaleString()}`);
  console.log('\nScanning for points with incorrect IDs...\n');

  let offset: string | number | null = null;
  let scannedCount = 0;
  let mismatchCount = 0;
  let fixedCount = 0;
  let errorCount = 0;
  const startTime = Date.now();

  // Batch points to fix for efficiency
  let pointsToFix: PointToFix[] = [];

  while (true) {
    // Scroll through points
    const scrollParams: any = {
      limit: BATCH_SIZE,
      with_payload: true,
      with_vector: true,
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
        console.warn(`⚠️  Point ${currentId} has no payload.key, skipping`);
        continue;
      }

      // Check if ID matches payload.key
      // Convert both to strings for comparison
      const currentIdStr = String(currentId);

      if (currentIdStr !== payloadKey) {
        mismatchCount++;

        if (isDryRun) {
          console.log(`[DRY RUN] Would fix: ID ${currentId} -> ${payloadKey}`);
        } else {
          pointsToFix.push({
            oldId: currentId,
            correctId: payloadKey,
            vector: point.vector as number[],
            payload: point.payload as Record<string, any>,
          });
        }
      }

      // Process batch of fixes
      if (!isDryRun && pointsToFix.length >= 100) {
        const result = await fixPoints(pointsToFix);
        fixedCount += result.fixed;
        errorCount += result.errors;
        pointsToFix = [];
      }
    }

    // Progress update
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = scannedCount / elapsed;
    const eta = totalPoints > 0 ? ((totalPoints - scannedCount) / rate).toFixed(0) : '?';

    process.stdout.write(
      `\rScanned: ${scannedCount.toLocaleString()} / ${totalPoints.toLocaleString()} | ` +
      `Mismatches: ${mismatchCount.toLocaleString()} | ` +
      `Fixed: ${fixedCount.toLocaleString()} | ` +
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

  // Process remaining points
  if (!isDryRun && pointsToFix.length > 0) {
    const result = await fixPoints(pointsToFix);
    fixedCount += result.fixed;
    errorCount += result.errors;
  }

  // Final summary
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log('\n\n' + '='.repeat(70));
  console.log('Migration Complete');
  console.log('='.repeat(70));
  console.log(`Total scanned:    ${scannedCount.toLocaleString()}`);
  console.log(`Mismatched IDs:   ${mismatchCount.toLocaleString()}`);
  if (!isDryRun) {
    console.log(`Successfully fixed: ${fixedCount.toLocaleString()}`);
    console.log(`Errors:           ${errorCount.toLocaleString()}`);
  }
  console.log(`Time elapsed:     ${totalTime}s`);
  console.log('='.repeat(70));

  if (isDryRun && mismatchCount > 0) {
    console.log(`\n💡 Run without --dry-run to fix ${mismatchCount.toLocaleString()} points`);
  }
}

async function fixPoints(points: PointToFix[]): Promise<{ fixed: number; errors: number }> {
  let fixed = 0;
  let errors = 0;

  // Step 1: Insert points with correct IDs
  const newPoints = points.map(p => ({
    id: p.correctId,
    vector: p.vector,
    payload: p.payload,
  }));

  try {
    await client.upsert(QDRANT_COLLECTION, {
      wait: true,
      points: newPoints,
    });

    // Step 2: Delete old points with wrong IDs
    const oldIds = points.map(p => p.oldId);

    await client.delete(QDRANT_COLLECTION, {
      wait: true,
      points: oldIds,
    });

    // Log each fixed point
    for (const point of points) {
      console.log(`\n✅ Reinserted: ${point.oldId} -> ${point.correctId}`);
    }

    fixed = points.length;
  } catch (error) {
    // If batch fails, try one by one
    console.error(`\nBatch operation failed, trying individually...`);

    for (const point of points) {
      try {
        // Insert with correct ID
        await client.upsert(QDRANT_COLLECTION, {
          wait: true,
          points: [{
            id: point.correctId,
            vector: point.vector,
            payload: point.payload,
          }],
        });

        // Delete old point
        await client.delete(QDRANT_COLLECTION, {
          wait: true,
          points: [point.oldId],
        });

        console.log(`\n✅ Reinserted: ${point.oldId} -> ${point.correctId}`);
        fixed++;
      } catch (individualError) {
        errors++;
        console.error(`\nFailed to fix point ${point.oldId} -> ${point.correctId}: ${individualError}`);
      }
    }
  }

  return { fixed, errors };
}

// Run the migration
main().catch(error => {
  console.error('\n❌ Migration failed:', error);
  process.exit(1);
});
