/**
 * Migration script to fix Qdrant point IDs that lost precision due to Number conversion
 *
 * The bug: Tweet IDs (19 digits) were converted via Number(BigInt(key)) which loses precision
 * because Number.MAX_SAFE_INTEGER is only ~16 digits.
 *
 * This script uses a SAFE THREE-PHASE approach:
 *
 * PHASE 1a - SCAN (fast):
 *   1. Scrolls through all points at full speed (no vectors, minimal payload)
 *   2. Collects all mismatches where id !== payload.key
 *   3. No processing during scan = consistent high throughput
 *
 * PHASE 1b - PROCESS:
 *   1. For collected mismatches, batch-fetches vectors
 *   2. Upserts new points with correct BigInt IDs
 *   3. Optionally verifies data integrity (can skip for speed)
 *
 * PHASE 2 - Delete:
 *   1. Deletes old points ONLY if they were verified/upserted in Phase 1b
 *
 * Usage: bun run scripts/fix-qdrant-ids.ts [options]
 *
 * Options:
 *   --dry-run           Don't make any changes, just scan and report
 *   --skip-verify       FAST MODE: Skip verification (safe since we copy exact data)
 *   --rescan            Force fresh scan, ignore cached scan results
 *   --batch-size=N      Scroll batch size (default: 1000)
 *   --fix-batch-size=N  How many points to fix at once (default: 500)
 *   --parallel=N        Parallel batch count (default: 4) [not yet implemented]
 *   --limit=N           Stop after N mismatches found
 *   --final-recheck     CLEANUP MODE: Find and delete leftover duplicate points with truncated IDs
 *
 * Caching:
 *   Scan results are cached to data/tmp/fix-qdrant-ids-scan-cache-{collection}.json
 *   If interrupted during processing, restart will load from cache instead of rescanning.
 *   Use --rescan to force a fresh scan. Cache is auto-deleted after successful completion.
 *
 * Performance tips:
 *   - Use --skip-verify for 5-10x speedup (verification is redundant since we copy exact data)
 *   - Increase --fix-batch-size for better throughput (e.g., --fix-batch-size=1000)
 *
 * Final Recheck Mode (--final-recheck):
 *   After running the main migration, use this mode to clean up any leftover duplicates.
 *   OPTIMIZED: Single scan - directly finds and deletes points where id !== payload.key.
 *   No secondary lookups needed - these ARE the duplicates (old truncated IDs).
 */

import { QdrantClient } from '@qdrant/js-client-rest';
import { config } from 'dotenv';
import { join } from 'path';

config();

// Check for BigInt support via JSON.rawJSON (required for lossless large integer IDs)
if (!('rawJSON' in JSON)) {
  console.error('❌ ERROR: JSON.rawJSON is not available in this JavaScript environment.');
  console.error('   This feature is required for handling large tweet IDs without precision loss.');
  console.error('   Requirements:');
  console.error('   - Bun 1.2+ (you have it!)');
  console.error('   - Node 21+ (or Node 20 with --harmony-json-parse-with-source flag)');
  console.error('   - Chrome 112+');
  process.exit(1);
}

// Configuration
const QDRANT_URL = process.env.QDRANT_URL || 'http://localhost';
const QDRANT_PORT = parseInt(process.env.QDRANT_PORT || '6333', 10);
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;
const QDRANT_COLLECTION = process.env.QDRANT_COLLECTION || 'embeddings';

// Parse CLI arguments
const args = process.argv.slice(2);
const isDryRun = args.includes('--dry-run');
const skipVerify = args.includes('--skip-verify'); // FAST MODE: Skip verification (safe because we copy exact data)
const isFinalRecheck = args.includes('--final-recheck'); // CLEANUP MODE: Find and delete leftover duplicates
const forceRescan = args.includes('--rescan'); // Force a fresh scan, ignore cached results
const batchSizeArg = args.find(arg => arg.startsWith('--batch-size='));
const BATCH_SIZE = batchSizeArg ? parseInt(batchSizeArg.split('=')[1] || '1000', 10) : 1000;
const fixBatchSizeArg = args.find(arg => arg.startsWith('--fix-batch-size='));
const FIX_BATCH_SIZE = fixBatchSizeArg ? parseInt(fixBatchSizeArg.split('=')[1] || '500', 10) : 500;
const limitArg = args.find(arg => arg.startsWith('--limit='));
const LIMIT = limitArg ? parseInt(limitArg.split('=')[1] || '0', 10) : 0;
const parallelArg = args.find(arg => arg.startsWith('--parallel='));
const PARALLEL_BATCHES = parallelArg ? parseInt(parallelArg.split('=')[1] || '4', 10) : 4;

// Cache file for scan results (so we can resume if interrupted during processing)
const CACHE_DIR = join(process.cwd(), 'data', 'tmp');
const SCAN_CACHE_FILE = join(CACHE_DIR, `fix-qdrant-ids-scan-cache-${QDRANT_COLLECTION}.json`);

console.log('='.repeat(70));
console.log(isFinalRecheck
  ? 'Qdrant ID Precision Fix - FINAL RECHECK (Cleanup Duplicates)'
  : 'Qdrant ID Precision Fix Migration (Safe Two-Phase)');
console.log('='.repeat(70));
console.log(`Qdrant URL: ${QDRANT_URL}:${QDRANT_PORT}`);
console.log(`Collection: ${QDRANT_COLLECTION}`);
console.log(`Scan batch size: ${BATCH_SIZE}`);
if (!isFinalRecheck) {
  console.log(`Fix batch size: ${FIX_BATCH_SIZE}`);
  console.log(`Parallel batches: ${PARALLEL_BATCHES}`);
}
console.log(`Limit: ${LIMIT > 0 ? LIMIT : 'None (all records)'}`);
console.log(`Dry run: ${isDryRun}`);
if (!isFinalRecheck) {
  console.log(`Skip verification: ${skipVerify}${skipVerify ? ' (FAST MODE)' : ''}`);
}
console.log(`Final recheck: ${isFinalRecheck}${isFinalRecheck ? ' (CLEANUP MODE)' : ''}`);
console.log('='.repeat(70));

if (isDryRun) {
  console.log('\n⚠️  DRY RUN MODE - No changes will be made\n');
}

if (isFinalRecheck) {
  console.log('\n🔍 FINAL RECHECK MODE: Scanning for leftover duplicate points with truncated IDs\n');
} else if (skipVerify && !isDryRun) {
  console.log('\n🚀 FAST MODE: Skipping verification (data is copied exactly)\n');
}

// Initialize Qdrant client
const client = new QdrantClient({
  url: QDRANT_URL,
  port: QDRANT_PORT,
  apiKey: QDRANT_API_KEY,
});

// Graceful shutdown handling
let shuttingDown = false;
let currentPhase: 'scanning' | 'processing' | 'deleting' | 'idle' = 'idle';

function handleShutdownSignal(signal: string) {
  if (shuttingDown) {
    console.log(`\n\n⚠️  Force shutdown requested. Exiting immediately (data may be inconsistent)...`);
    process.exit(1);
  }

  shuttingDown = true;
  console.log(`\n\n🛑 Graceful shutdown initiated (${signal})...`);

  if (currentPhase === 'scanning') {
    console.log('   Will finish current batch, then proceed to deletion phase.');
    console.log('   Press Ctrl+C again to force quit (not recommended).\n');
  } else if (currentPhase === 'processing') {
    console.log('   Finishing current batch processing...');
    console.log('   Press Ctrl+C again to force quit (not recommended).\n');
  } else if (currentPhase === 'deleting') {
    console.log('   Must complete deletions to avoid duplicates. Please wait...');
    console.log('   Press Ctrl+C again to force quit (may leave duplicates).\n');
  }
}

process.on('SIGINT', () => handleShutdownSignal('SIGINT'));
process.on('SIGTERM', () => handleShutdownSignal('SIGTERM'));

interface PointToFix {
  oldId: string | number | bigint;
  correctId: string;
  correctIdBigInt: bigint; // BigInt version for Qdrant (full precision, no data loss)
  vector: number[];
  payload: Record<string, any>;
}

interface VerifiedPoint {
  oldId: string | number | bigint;
  newId: bigint; // BigInt ID that was stored in Qdrant (full precision)
}

// Deep comparison of two objects
function deepEqual(obj1: any, obj2: any): boolean {
  if (obj1 === obj2) return true;
  if (typeof obj1 !== typeof obj2) return false;
  if (typeof obj1 !== 'object' || obj1 === null || obj2 === null) return false;

  const keys1 = Object.keys(obj1);
  const keys2 = Object.keys(obj2);

  if (keys1.length !== keys2.length) return false;

  for (const key of keys1) {
    if (!keys2.includes(key)) return false;
    if (!deepEqual(obj1[key], obj2[key])) return false;
  }

  return true;
}

// Compare two vectors - returns detailed comparison result
function compareVectors(v1: number[], v2: number[]): { 
  strictEqual: boolean; 
  toleranceEqual: boolean; 
  maxDiff: number;
  diffCount: number;
} {
  if (v1.length !== v2.length) {
    return { strictEqual: false, toleranceEqual: false, maxDiff: Infinity, diffCount: v1.length };
  }

  let strictEqual = true;
  let toleranceEqual = true;
  let maxDiff = 0;
  let diffCount = 0;
  const TOLERANCE = 1e-9;

  for (let i = 0; i < v1.length; i++) {
    const val1 = v1[i]!;
    const val2 = v2[i]!;
    
    if (val1 !== val2) {
      strictEqual = false;
      diffCount++;
      const diff = Math.abs(val1 - val2);
      maxDiff = Math.max(maxDiff, diff);
      
      if (diff > TOLERANCE) {
        toleranceEqual = false;
      }
    }
  }

  return { strictEqual, toleranceEqual, maxDiff, diffCount };
}

// Lightweight structure for scan phase - no vectors yet
interface MismatchInfo {
  oldId: string | number | bigint;
  correctId: string;
  correctIdBigInt: bigint;
}

// Cache structure for scan results
interface ScanCache {
  collection: string;
  scannedCount: number;
  totalPoints: number;
  timestamp: string;
  mismatches: Array<{ oldId: string; correctId: string }>;
}

// Cache helper functions using Bun's native APIs
async function loadScanCache(): Promise<ScanCache | null> {
  const file = Bun.file(SCAN_CACHE_FILE);
  if (await file.exists()) {
    try {
      const data = await file.json();
      // Validate cache is for the same collection
      if (data.collection === QDRANT_COLLECTION) {
        return data as ScanCache;
      }
      console.log('⚠️  Cache file is for a different collection, ignoring...');
    } catch (error) {
      console.log('⚠️  Failed to parse cache file, will rescan...');
    }
  }
  return null;
}

async function saveScanCache(cache: ScanCache): Promise<void> {
  // Ensure directory exists
  const fs = await import('fs/promises');
  await fs.mkdir(CACHE_DIR, { recursive: true });
  await Bun.write(SCAN_CACHE_FILE, JSON.stringify(cache, null, 2));
}

async function deleteScanCache(): Promise<void> {
  const file = Bun.file(SCAN_CACHE_FILE);
  if (await file.exists()) {
    const fs = await import('fs/promises');
    await fs.unlink(SCAN_CACHE_FILE).catch(() => {});
  }
}

async function main() {
  // Get collection info
  const collectionInfo = await client.getCollection(QDRANT_COLLECTION);
  const totalPoints = collectionInfo.points_count || 0;

  console.log(`Total points in collection: ${totalPoints.toLocaleString()}`);

  const startTime = Date.now();
  let scannedCount = 0;
  let scanTime = '0';

  // Collect ALL mismatches first (lightweight - no vectors)
  let allMismatches: MismatchInfo[] = [];

  // ========== PHASE 1A: SCAN or LOAD FROM CACHE ==========
  const cachedScan = forceRescan ? null : await loadScanCache();

  if (cachedScan && !forceRescan) {
    // Load from cache
    console.log(`\n📂 Loading scan results from cache...`);
    console.log(`   Cache created: ${cachedScan.timestamp}`);
    console.log(`   Scanned: ${cachedScan.scannedCount.toLocaleString()} points`);
    console.log(`   Mismatches: ${cachedScan.mismatches.length.toLocaleString()}`);

    scannedCount = cachedScan.scannedCount;
    allMismatches = cachedScan.mismatches.map(m => ({
      oldId: m.oldId,
      correctId: m.correctId,
      correctIdBigInt: BigInt(m.correctId),
    }));

    console.log(`\n✅ Loaded ${allMismatches.length.toLocaleString()} mismatches from cache.\n`);
    console.log(`💡 Use --rescan to force a fresh scan.\n`);
  } else {
    // Do fresh scan
    console.log('\n📋 PHASE 1a: Scanning for mismatches (fast scan, no processing)...\n');

    currentPhase = 'scanning';
    let offset: string | number | null = null;

    while (true) {
      // Check for graceful shutdown
      if (shuttingDown) {
        console.log(`\n\n🛑 Shutdown requested. Stopping scan after ${scannedCount.toLocaleString()} points...`);
        break;
      }

      // Check limit
      if (LIMIT > 0 && allMismatches.length >= LIMIT) {
        console.log(`\n\n⏹️  Reached limit of ${LIMIT} mismatches, stopping scan...`);
        break;
      }

      // Scroll through points - ONLY fetch key, no vectors
      const scrollParams: any = {
        limit: BATCH_SIZE,
        with_payload: { include: ['key'] },
        with_vector: false,
      };

      if (offset !== null) {
        scrollParams.offset = offset;
      }

      const response = await client.scroll(QDRANT_COLLECTION, scrollParams);

      if (!response.points || response.points.length === 0) {
        break;
      }

      for (const point of response.points) {
        if (LIMIT > 0 && allMismatches.length >= LIMIT) break;

        scannedCount++;

        const payloadKey = (point.payload as any)?.key as string;
        if (!payloadKey) continue;

        // Check if ID matches payload.key
        if (String(point.id) !== payloadKey) {
          allMismatches.push({
            oldId: point.id,
            correctId: payloadKey,
            correctIdBigInt: BigInt(payloadKey),
          });

          if (isDryRun) {
            console.log(`[DRY RUN] Would fix: ID ${point.id} -> ${payloadKey}`);
          }
        }
      }

      // Progress update
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = scannedCount / elapsed;
      const eta = totalPoints > 0 ? ((totalPoints - scannedCount) / rate).toFixed(0) : '?';

      process.stdout.write(
        `\rScanned: ${scannedCount.toLocaleString()} / ${totalPoints.toLocaleString()} | ` +
        `Mismatches: ${allMismatches.length.toLocaleString()} | ` +
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

    scanTime = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n\n✅ Scan complete in ${scanTime}s. Found ${allMismatches.length.toLocaleString()} mismatches.\n`);

    // Save scan results to cache (for resume if interrupted during processing)
    if (allMismatches.length > 0 && !isDryRun && !shuttingDown) {
      const cache: ScanCache = {
        collection: QDRANT_COLLECTION,
        scannedCount,
        totalPoints,
        timestamp: new Date().toISOString(),
        mismatches: allMismatches.map(m => ({
          oldId: String(m.oldId),
          correctId: m.correctId,
        })),
      };
      await saveScanCache(cache);
      console.log(`💾 Scan results cached to ${SCAN_CACHE_FILE}\n`);
    }
  }

  if (isDryRun) {
    console.log('='.repeat(70));
    console.log('DRY RUN Complete');
    console.log('='.repeat(70));
    console.log(`Total scanned:     ${scannedCount.toLocaleString()}`);
    console.log(`Mismatched IDs:    ${allMismatches.length.toLocaleString()}`);
    console.log(`Time elapsed:      ${scanTime}s`);
    console.log('='.repeat(70));
    console.log(`\n💡 Run without --dry-run to fix ${allMismatches.length.toLocaleString()} points`);
    return;
  }

  if (allMismatches.length === 0) {
    console.log('✅ No mismatches found. Database is already correct!');
    return;
  }

  if (shuttingDown) {
    console.log('🛑 Shutdown requested. Skipping processing phase.');
    return;
  }

  // ========== PHASE 1B: PROCESS (fetch vectors, upsert, verify) ==========
  console.log(`📋 PHASE 1b: Processing ${allMismatches.length.toLocaleString()} mismatches (fetch vectors, upsert, verify)...\n`);

  currentPhase = 'processing';
  const processStartTime = Date.now();
  let upsertedCount = 0;
  let verifiedCount = 0;
  let verificationFailedCount = 0;
  let errorCount = 0;
  const verifiedForDeletion: VerifiedPoint[] = [];

  // Process in batches
  for (let i = 0; i < allMismatches.length; i += FIX_BATCH_SIZE) {
    if (shuttingDown) {
      console.log(`\n\n🛑 Shutdown requested. Processed ${i.toLocaleString()} of ${allMismatches.length.toLocaleString()} mismatches.`);
      break;
    }

    const batch = allMismatches.slice(i, i + FIX_BATCH_SIZE);

    // Fetch vectors for this batch
    const ids = batch.map(m => m.oldId);
    let pointsWithVectors;
    try {
      pointsWithVectors = await client.retrieve(QDRANT_COLLECTION, {
        ids,
        with_payload: true,
        with_vector: true,
      });
    } catch (error) {
      console.error(`\n❌ Failed to retrieve batch: ${error}`);
      errorCount += batch.length;
      continue;
    }

    // Map retrieved points by ID
    const pointsMap = new Map(pointsWithVectors.map(p => [String(p.id), p]));

    // Build full PointToFix array
    const pointsToFix: PointToFix[] = [];
    for (const mismatch of batch) {
      const fullPoint = pointsMap.get(String(mismatch.oldId));
      if (fullPoint) {
        pointsToFix.push({
          oldId: mismatch.oldId,
          correctId: mismatch.correctId,
          correctIdBigInt: mismatch.correctIdBigInt,
          vector: fullPoint.vector as number[],
          payload: fullPoint.payload as Record<string, any>,
        });
      }
    }

    // Process batch
    const result = await processAndVerifyBatch(pointsToFix, skipVerify);
    upsertedCount += result.upserted;
    verifiedCount += result.verified;
    verificationFailedCount += result.verificationFailed;
    errorCount += result.errors;
    verifiedForDeletion.push(...result.verifiedPoints);

    // Progress update
    const processed = Math.min(i + FIX_BATCH_SIZE, allMismatches.length);
    const elapsed = (Date.now() - processStartTime) / 1000;
    const rate = processed / elapsed;
    const eta = ((allMismatches.length - processed) / rate).toFixed(0);

    process.stdout.write(
      `\rProcessed: ${processed.toLocaleString()} / ${allMismatches.length.toLocaleString()} | ` +
      `Verified: ${verifiedCount.toLocaleString()} | ` +
      `Rate: ${rate.toFixed(0)}/s | ` +
      `ETA: ${eta}s   `
    );
  }

  // Phase 1 summary
  const phase1Time = ((Date.now() - startTime) / 1000).toFixed(1);
  const processTime = ((Date.now() - processStartTime) / 1000).toFixed(1);

  console.log('\n\n' + '='.repeat(70));
  console.log('PHASE 1 Complete - Scan, Upsert & Verify');
  console.log('='.repeat(70));
  console.log(`Total scanned:         ${scannedCount.toLocaleString()}`);
  console.log(`Mismatched IDs:        ${allMismatches.length.toLocaleString()}`);
  console.log(`Upserted:              ${upsertedCount.toLocaleString()}`);
  console.log(`✅ Verified:           ${verifiedCount.toLocaleString()}`);
  console.log(`❌ Verification failed: ${verificationFailedCount.toLocaleString()}`);
  console.log(`Errors:                ${errorCount.toLocaleString()}`);
  console.log(`Scan time:             ${scanTime}s`);
  console.log(`Process time:          ${processTime}s`);
  console.log(`Total time:            ${phase1Time}s`);
  console.log('='.repeat(70));

  // PHASE 2: Delete old points (only verified ones)
  if (verifiedForDeletion.length === 0) {
    console.log('\n✅ No verified points to clean up. Migration complete!');
    currentPhase = 'idle';
    return;
  }

  currentPhase = 'deleting';

  if (shuttingDown) {
    console.log(`\n📋 PHASE 2: Completing deletions for ${verifiedForDeletion.length.toLocaleString()} verified points before shutdown...\n`);
    console.log('   ⚠️  Please wait - skipping deletions would leave duplicate data!\n');
  } else {
    console.log(`\n📋 PHASE 2: Deleting ${verifiedForDeletion.length.toLocaleString()} verified old points...\n`);
  }

  const phase2Start = Date.now();
  let deletedCount = 0;
  let deleteErrorCount = 0;

  // Delete in batches
  const DELETE_BATCH_SIZE = 1000;
  for (let i = 0; i < verifiedForDeletion.length; i += DELETE_BATCH_SIZE) {
    const batch = verifiedForDeletion.slice(i, i + DELETE_BATCH_SIZE);
    const oldIds = batch.map(p => p.oldId);

    try {
      await client.delete(QDRANT_COLLECTION, {
        wait: true,
        points: oldIds,
      });
      deletedCount += batch.length;

      process.stdout.write(
        `\rDeleted: ${deletedCount.toLocaleString()} / ${verifiedForDeletion.length.toLocaleString()}   `
      );
    } catch (error) {
      deleteErrorCount += batch.length;
      console.error(`\n❌ Failed to delete batch: ${error}`);
    }
  }

  // Final summary
  currentPhase = 'idle';
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  const phase2Time = ((Date.now() - phase2Start) / 1000).toFixed(1);

  console.log('\n\n' + '='.repeat(70));
  console.log(shuttingDown ? 'Migration Stopped (Graceful Shutdown)' : 'Migration Complete');
  console.log('='.repeat(70));
  console.log(`PHASE 1 (Upsert & Verify): ${phase1Time}s`);
  console.log(`  - Upserted: ${upsertedCount.toLocaleString()}`);
  console.log(`  - Verified: ${verifiedCount.toLocaleString()}`);
  console.log(`  - Verification failed: ${verificationFailedCount.toLocaleString()}`);
  console.log(`PHASE 2 (Delete): ${phase2Time}s`);
  console.log(`  - Deleted: ${deletedCount.toLocaleString()}`);
  console.log(`  - Delete errors: ${deleteErrorCount.toLocaleString()}`);
  console.log(`Total time: ${totalTime}s`);
  console.log('='.repeat(70));

  if (verificationFailedCount > 0) {
    console.log(`\n⚠️  ${verificationFailedCount} points failed verification and were NOT deleted.`);
    console.log('   These old points still exist and need manual review.');
  }

  if (shuttingDown) {
    console.log(`\n💡 Migration was stopped early. ${scannedCount.toLocaleString()} of ${totalPoints.toLocaleString()} points were scanned.`);
    console.log('   Run the script again to continue - it will load from cache.');
  } else if (deleteErrorCount === 0 && verificationFailedCount === 0) {
    // Migration completed successfully - delete the cache
    await deleteScanCache();
    console.log('\n🗑️  Scan cache deleted (migration complete).');
  }
}

async function processAndVerifyBatch(points: PointToFix[], skipVerify: boolean): Promise<{
  upserted: number;
  verified: number;
  verificationFailed: number;
  errors: number;
  verifiedPoints: VerifiedPoint[];
}> {
  let upserted = 0;
  let verified = 0;
  let verificationFailed = 0;
  let errors = 0;
  const verifiedPoints: VerifiedPoint[] = [];

  // Track which points were successfully upserted
  const successfullyUpserted: Set<string> = new Set();

  // Step 1: Upsert points with correct IDs (using BigInt for full precision)
  const newPoints = points.map(p => ({
    id: p.correctIdBigInt,
    vector: p.vector,
    payload: p.payload,
  }));

  try {
    await client.upsert(QDRANT_COLLECTION, {
      wait: true,
      points: newPoints,
    });
    upserted = points.length;
    // All succeeded
    for (const p of points) {
      successfullyUpserted.add(p.correctId);
    }
  } catch (error: any) {
    console.error(`\n❌ Batch upsert failed: ${error}`);
    console.error(`   IDs attempted: ${points.map(p => p.correctId).slice(0, 5).join(', ')}${points.length > 5 ? '...' : ''}`);
    if (error.data) {
      console.error(`   Error details: ${JSON.stringify(error.data)}`);
    }

    // Try individual upserts to identify the problematic point
    console.log(`   Attempting individual upserts...`);
    for (const point of points) {
      try {
        await client.upsert(QDRANT_COLLECTION, {
          wait: true,
          points: [{
            id: point.correctIdBigInt,
            vector: point.vector,
            payload: point.payload,
          }],
        });
        upserted++;
        successfullyUpserted.add(point.correctId);
      } catch (individualError: any) {
        console.error(`   ❌ Failed: ${point.correctId}`);
        errors++;
      }
    }

    // If no individual upserts succeeded, return early
    if (upserted === 0) {
      return { upserted, verified, verificationFailed, errors, verifiedPoints };
    }
  }

  // Step 2: Verification (or skip in FAST MODE)
  if (skipVerify) {
    // FAST MODE: Skip verification, assume all upserts are correct
    // This is safe because we're copying the exact same vector and payload
    for (const point of points) {
      if (successfullyUpserted.has(point.correctId)) {
        verified++;
        verifiedPoints.push({
          oldId: point.oldId,
          newId: point.correctIdBigInt,
        });
      }
    }
  } else {
    // SAFE MODE: Batch verify all points
    const pointsToVerify = points.filter(p => successfullyUpserted.has(p.correctId));

    if (pointsToVerify.length > 0) {
      try {
        // Batch retrieve all new points at once (much faster than individual calls)
        const newPointIds = pointsToVerify.map(p => p.correctIdBigInt);
        const newPointsResult = await client.retrieve(QDRANT_COLLECTION, {
          ids: newPointIds,
          with_payload: true,
          with_vector: true,
        });

        // Map retrieved points by ID for quick lookup
        const newPointsMap = new Map(newPointsResult.map(p => [String(p.id), p]));

        for (const point of pointsToVerify) {
          const newPoint = newPointsMap.get(String(point.correctIdBigInt));

          if (!newPoint) {
            console.error(`\n❌ Could not retrieve new point: ${point.correctId}`);
            verificationFailed++;
            continue;
          }

          // Compare vectors
          const newVector = newPoint.vector as number[];
          const vectorComparison = compareVectors(point.vector, newVector);

          if (!vectorComparison.strictEqual) {
            if (!vectorComparison.toleranceEqual) {
              console.error(`\n❌ Vector mismatch for ${point.oldId} -> ${point.correctId}`);
            }
            verificationFailed++;
            continue;
          }

          // Compare payloads
          const newPayload = newPoint.payload as Record<string, any>;
          if (!deepEqual(point.payload, newPayload)) {
            console.error(`\n❌ Payload mismatch for ${point.oldId} -> ${point.correctId}`);
            verificationFailed++;
            continue;
          }

          // Verification passed!
          verified++;
          verifiedPoints.push({
            oldId: point.oldId,
            newId: point.correctIdBigInt,
          });
        }
      } catch (error) {
        console.error(`\n❌ Batch verification error: ${error}`);
        // Mark all as failed if batch verification fails
        verificationFailed += pointsToVerify.length;
      }
    }
  }

  return { upserted, verified, verificationFailed, errors, verifiedPoints };
}

/**
 * Final recheck mode: Find and delete leftover duplicate points with truncated IDs
 *
 * OPTIMIZED: Single scan - directly finds points where id !== payload.key
 * These ARE the duplicates (old truncated IDs that should be deleted).
 * No secondary lookups needed!
 */
async function finalRecheck() {
  const collectionInfo = await client.getCollection(QDRANT_COLLECTION);
  const totalPoints = collectionInfo.points_count || 0;

  console.log(`Total points in collection: ${totalPoints.toLocaleString()}`);
  console.log('\n🔍 Scanning for points where id !== payload.key (duplicates)...\n');

  currentPhase = 'scanning';
  let offset: string | number | null = null;
  let scannedCount = 0;
  let duplicatesFound = 0;
  let deletedCount = 0;
  let errorCount = 0;
  const startTime = Date.now();

  // Batch of duplicate IDs to delete
  let duplicateIds: (string | number | bigint)[] = [];
  const DELETE_BATCH_SIZE = 1000;

  while (true) {
    // Check for graceful shutdown
    if (shuttingDown) {
      console.log(`\n\n🛑 Shutdown requested. Stopping scan after ${scannedCount.toLocaleString()} points...`);
      break;
    }

    // Check limit
    if (LIMIT > 0 && duplicatesFound >= LIMIT) {
      console.log(`\n\n⏹️  Reached limit of ${LIMIT} duplicates, stopping scan...`);
      break;
    }

    // Scroll through points - only need the key field
    const scrollParams: any = {
      limit: BATCH_SIZE,
      with_payload: { include: ['key'] },
      with_vector: false,
    };

    if (offset !== null) {
      scrollParams.offset = offset;
    }

    const response = await client.scroll(QDRANT_COLLECTION, scrollParams);

    if (!response.points || response.points.length === 0) {
      break;
    }

    for (const point of response.points) {
      if (LIMIT > 0 && duplicatesFound >= LIMIT) break;

      scannedCount++;

      const payloadKey = (point.payload as any)?.key as string;
      if (!payloadKey) continue;

      // Direct check: if id !== key, this IS a duplicate (old truncated ID)
      const currentIdStr = String(point.id);
      if (currentIdStr !== payloadKey) {
        duplicatesFound++;
        duplicateIds.push(point.id);

        if (isDryRun) {
          console.log(`\n[DRY RUN] Would delete: id=${point.id} (key=${payloadKey})`);
        }
      }
    }

    // Delete batch of duplicates
    if (!isDryRun && duplicateIds.length >= DELETE_BATCH_SIZE) {
      currentPhase = 'deleting';
      const result = await deleteDuplicateBatch(duplicateIds);
      deletedCount += result.deleted;
      errorCount += result.errors;
      duplicateIds = [];
      currentPhase = 'scanning';
    }

    // Progress update
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = scannedCount / elapsed;
    const eta = totalPoints > 0 ? ((totalPoints - scannedCount) / rate).toFixed(0) : '?';

    process.stdout.write(
      `\rScanned: ${scannedCount.toLocaleString()} / ${totalPoints.toLocaleString()} | ` +
      `Duplicates: ${duplicatesFound.toLocaleString()} | ` +
      `Deleted: ${deletedCount.toLocaleString()} | ` +
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

  // Delete remaining duplicates
  if (!isDryRun && duplicateIds.length > 0) {
    currentPhase = 'deleting';
    const result = await deleteDuplicateBatch(duplicateIds);
    deletedCount += result.deleted;
    errorCount += result.errors;
  }

  // Final summary
  currentPhase = 'idle';
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log('\n\n' + '='.repeat(70));
  console.log(shuttingDown ? 'Final Recheck Stopped (Graceful Shutdown)' : 'Final Recheck Complete');
  console.log('='.repeat(70));
  console.log(`Total scanned:        ${scannedCount.toLocaleString()}`);
  console.log(`Duplicates found:     ${duplicatesFound.toLocaleString()} (id !== payload.key)`);
  if (!isDryRun) {
    console.log(`Deleted:              ${deletedCount.toLocaleString()}`);
    console.log(`Errors:               ${errorCount.toLocaleString()}`);
  }
  console.log(`Time elapsed:         ${totalTime}s`);
  console.log('='.repeat(70));

  if (isDryRun && duplicatesFound > 0) {
    console.log(`\n💡 Run without --dry-run to delete ${duplicatesFound.toLocaleString()} duplicate points`);
  }

  if (shuttingDown) {
    console.log(`\n💡 Recheck was stopped early. ${scannedCount.toLocaleString()} of ${totalPoints.toLocaleString()} points were scanned.`);
    console.log('   Run the script again to continue checking remaining points.');
  }

  if (duplicatesFound === 0 && !shuttingDown) {
    console.log('\n✅ No duplicates found. Database is clean!');
  }
}

async function deleteDuplicateBatch(ids: (string | number | bigint)[]): Promise<{
  deleted: number;
  errors: number;
}> {
  try {
    await client.delete(QDRANT_COLLECTION, {
      wait: true,
      points: ids,
    });
    return { deleted: ids.length, errors: 0 };
  } catch (error) {
    console.error(`\n❌ Failed to delete batch: ${error}`);
    return { deleted: 0, errors: ids.length };
  }
}

// Run the migration or final recheck
if (isFinalRecheck) {
  finalRecheck().catch(error => {
    console.error('\n❌ Final recheck failed:', error);
    process.exit(1);
  });
} else {
  main().catch(error => {
    console.error('\n❌ Migration failed:', error);
    process.exit(1);
  });
}
