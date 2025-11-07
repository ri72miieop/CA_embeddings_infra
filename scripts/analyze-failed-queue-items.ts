#!/usr/bin/env bun

import fs from 'fs/promises';
import path from 'path';
import { CoreNN } from 'corenn/corenn-node/src/index.js';
import { parseArgs } from 'util';
import type { EmbeddingVector } from '../src/types/index.ts';

interface QueuedEmbedding {
  id: string;
  timestamp: string;
  embeddings: EmbeddingVector[];
  retryCount: number;
  correlationId?: string;
}

interface FileAnalysis {
  fileName: string;
  totalItems: number;
  itemsInDb: number;
  itemsNotInDb: number;
  percentageInDb: number;
  sampleExistingKeys: string[];
  sampleMissingKeys: string[];
}

interface OverallStats {
  analysisType: 'failed' | 'completed';
  totalFiles: number;
  totalItems: number;
  totalItemsInDb: number;
  totalItemsNotInDb: number;
  overallPercentageInDb: number;
  fileAnalyses: FileAnalysis[];
}

async function loadConfig() {
  // Load config similar to your app
  const configPath = path.join(process.cwd(), 'src/config/index.ts');
  
  // For now, use default values - you can adjust these
  return {
    database: {
      path: process.env.CORENN_DB_PATH || './data/embeddings.db',
      dimension: parseInt(process.env.VECTOR_DIMENSION || '1024'),
      indexType: process.env.INDEX_TYPE || 'hnsw'
    },
    queueDir: process.env.QUEUE_DIR || './data/embedding-calls/embedding-queue'
  };
}

async function initializeDatabase(dbPath: string) {
  console.log(`🔌 Connecting to CoreNN database: ${dbPath}`);
  
  try {
    // Try to open existing database
    const db = CoreNN.open(dbPath);
    console.log('✅ Database connection established');
    return db;
  } catch (error) {
    console.error('❌ Failed to connect to database:', error);
    console.error('   Make sure the database exists and is accessible');
    throw error;
  }
}

async function getQueueFiles(queueDir: string, type: 'failed' | 'completed'): Promise<string[]> {
  try {
    let targetDir = queueDir;
    let filePattern = '';
    
    if (type === 'failed') {
      filePattern = '.failed.jsonl';
    } else {
      targetDir = path.join(queueDir, 'completed');
      filePattern = '.completed.jsonl';
    }
    
    const files = await fs.readdir(targetDir);
    const queueFiles = files.filter(f => f.endsWith(filePattern));
    console.log(`📁 Found ${queueFiles.length} ${type} queue files`);
    return queueFiles.map(f => path.join(targetDir, f));
  } catch (error) {
    console.error(`❌ Failed to read ${type} queue directory:`, error);
    throw error;
  }
}

async function parseQueueFile(filePath: string): Promise<QueuedEmbedding[]> {
  try {
    const data = await fs.readFile(filePath, 'utf-8');
    const lines = data.trim().split('\n').filter(line => line);
    return lines.map(line => JSON.parse(line));
  } catch (error) {
    console.error(`❌ Failed to parse file ${filePath}:`, error);
    return [];
  }
}

async function checkEmbeddingsInDatabase(db: CoreNN, embeddings: Array<{key: string, vector: number[]}>): Promise<Set<string>> {
  const existingKeys = new Set<string>();
  
  console.log(`   🔍 Checking ${embeddings.length} embeddings against database using vector search...`);
  
  // Check embeddings in batches to avoid overwhelming the database
  const batchSize = 50; // Smaller batch size for vector searches
  
  for (let i = 0; i < embeddings.length; i += batchSize) {
    const batch = embeddings.slice(i, i + batchSize);
    
    for (const embedding of batch) {
      try {
        // Convert to Float32Array for querying
        const queryVector = new Float32Array(embedding.vector);
        
        // Search for the most similar vector (k=1)
        const results = db.query(queryVector, 1);
        
        if (results && results.length > 0) {
          const topResult = results[0];
          
          // If the distance is very small (essentially 0), it's likely the same vector
          // We use a small threshold to account for floating point precision
          if (topResult && topResult.distance < 0.0001 && topResult.key === embedding.key) {
            existingKeys.add(embedding.key);
          }
        }
      } catch (error) {
        // Vector search failed, continue with next embedding
        console.debug(`   ⚠️  Failed to search for key ${embedding.key}:`, error);
      }
    }
    
    // Progress indicator
    if (i % (batchSize * 4) === 0 || i + batchSize >= embeddings.length) {
      const checked = Math.min(i + batchSize, embeddings.length);
      console.log(`   🔍 Checked ${checked}/${embeddings.length} embeddings... (${existingKeys.size} found so far)`);
    }
  }
  
  console.log(`   ✅ Found ${existingKeys.size}/${embeddings.length} embeddings in database`);
  return existingKeys;
}

async function analyzeQueueFile(db: CoreNN, filePath: string): Promise<FileAnalysis> {
  const fileName = path.basename(filePath);
  console.log(`\n📊 Analyzing ${fileName}...`);
  
  const queuedItems = await parseQueueFile(filePath);
  if (queuedItems.length === 0) {
    console.log(`   ⚠️  No items found in ${fileName}`);
    return {
      fileName,
      totalItems: 0,
      itemsInDb: 0,
      itemsNotInDb: 0,
      percentageInDb: 0,
      sampleExistingKeys: [],
      sampleMissingKeys: []
    };
  }
  
  // Debug: Show structure of first item
  if (queuedItems.length > 0) {
    const firstItem = queuedItems[0]!;
    console.log(`   🔍 Sample queue item structure:`);
    console.log(`      - ID: ${firstItem.id}`);
    console.log(`      - Timestamp: ${firstItem.timestamp}`);
    console.log(`      - Embeddings count: ${firstItem.embeddings?.length || 0}`);
    console.log(`      - Retry count: ${firstItem.retryCount}`);
    
    if (firstItem.embeddings && firstItem.embeddings.length > 0) {
      const firstEmbedding = firstItem.embeddings[0]!;
      console.log(`      - First embedding key: ${firstEmbedding.key}`);
      console.log(`      - First embedding vector type: ${typeof firstEmbedding.vector}`);
      console.log(`      - First embedding vector length: ${firstEmbedding.vector?.length || 'undefined'}`);
      
      if (firstEmbedding.vector && firstEmbedding.vector.length > 0) {
        console.log(`      - First few vector values: [${firstEmbedding.vector.slice(0, 3).join(', ')}...]`);
      }
    }
  }
  
  // Extract all embeddings (key + vector) from all queued items
  const allEmbeddings: Array<{key: string, vector: number[]}> = [];
  let emptyVectors = 0;
  let invalidVectors = 0;
  let totalEmbeddingsInFile = 0;
  
  for (const item of queuedItems) {
    for (const embedding of item.embeddings) {
      totalEmbeddingsInFile++;
      
      if (!embedding.vector || !Array.isArray(embedding.vector)) {
        invalidVectors++;
        console.warn(`   ⚠️  Invalid vector for key ${embedding.key}: ${typeof embedding.vector}`);
        continue;
      }
      
      if (embedding.vector.length === 0) {
        emptyVectors++;
        console.warn(`   ⚠️  Empty vector for key ${embedding.key}`);
        continue;
      }
      
      if (embedding.vector.length !== 1024) {
        console.warn(`   ⚠️  Wrong vector dimension for key ${embedding.key}: ${embedding.vector.length} (expected 1024)`);
        continue;
      }
      
      allEmbeddings.push({
        key: embedding.key,
        vector: embedding.vector
      });
    }
  }
  
  console.log(`   📝 Found ${allEmbeddings.length} valid embeddings out of ${totalEmbeddingsInFile} total in ${queuedItems.length} queue items`);
  if (emptyVectors > 0) {
    console.log(`   ⚠️  Skipped ${emptyVectors} empty vectors`);
  }
  if (invalidVectors > 0) {
    console.log(`   ⚠️  Skipped ${invalidVectors} invalid vectors`);
  }
  
  // Check which embeddings exist in database using vector search
  let existingKeys: Set<string>;
  
  if (allEmbeddings.length === 0) {
    console.log(`   ⚠️  No valid embeddings to check - all vectors were empty or invalid`);
    existingKeys = new Set();
  } else {
    existingKeys = await checkEmbeddingsInDatabase(db, allEmbeddings);
  }
  
  const itemsInDb = existingKeys.size;
  const itemsNotInDb = allEmbeddings.length - itemsInDb;
  const percentageInDb = allEmbeddings.length > 0 ? (itemsInDb / allEmbeddings.length) * 100 : 0;
  
  // Get samples for debugging
  const existingKeysArray = Array.from(existingKeys);
  const allKeys = allEmbeddings.map(emb => emb.key);
  const missingKeys = allKeys.filter(key => !existingKeys.has(key));
  
  const analysis: FileAnalysis = {
    fileName,
    totalItems: totalEmbeddingsInFile, // Use total count including invalid ones
    itemsInDb,
    itemsNotInDb: totalEmbeddingsInFile - itemsInDb,
    percentageInDb: Math.round(percentageInDb * 100) / 100,
    sampleExistingKeys: existingKeysArray.slice(0, 5),
    sampleMissingKeys: missingKeys.slice(0, 5)
  };
  
  console.log(`   ✅ ${fileName}: ${itemsInDb}/${totalEmbeddingsInFile} (${analysis.percentageInDb}%) already in DB (${allEmbeddings.length} valid vectors checked)`);
  
  return analysis;
}

async function generateReport(stats: OverallStats) {
  const typeLabel = stats.analysisType.toUpperCase();
  const typeEmoji = stats.analysisType === 'failed' ? '❌' : '✅';
  
  console.log('\n' + '='.repeat(80));
  console.log(`📊 ${typeEmoji} ${typeLabel} QUEUE ITEMS ANALYSIS REPORT`);
  console.log('='.repeat(80));
  
  console.log(`\n🔢 OVERALL STATISTICS:`);
  console.log(`   Total ${stats.analysisType} files: ${stats.totalFiles}`);
  console.log(`   Total ${stats.analysisType} items: ${stats.totalItems}`);
  console.log(`   Items already in DB: ${stats.totalItemsInDb} (${stats.overallPercentageInDb}%)`);
  console.log(`   Items missing from DB: ${stats.totalItemsNotInDb} (${(100 - stats.overallPercentageInDb).toFixed(2)}%)`);
  
  console.log(`\n📁 PER-FILE BREAKDOWN:`);
  console.log('   File Name'.padEnd(50) + 'Total'.padEnd(8) + 'In DB'.padEnd(8) + 'Missing'.padEnd(8) + '% In DB');
  console.log('   ' + '-'.repeat(78));
  
  for (const analysis of stats.fileAnalyses) {
    const fileName = analysis.fileName.length > 47 ? 
      '...' + analysis.fileName.slice(-44) : 
      analysis.fileName;
    
    console.log(
      `   ${fileName.padEnd(50)}${analysis.totalItems.toString().padEnd(8)}${analysis.itemsInDb.toString().padEnd(8)}${analysis.itemsNotInDb.toString().padEnd(8)}${analysis.percentageInDb.toFixed(1)}%`
    );
  }
  
  // Show patterns
  const highExistingFiles = stats.fileAnalyses.filter(f => f.percentageInDb > 80);
  const lowExistingFiles = stats.fileAnalyses.filter(f => f.percentageInDb < 20);
  
  if (highExistingFiles.length > 0) {
    console.log(`\n🔍 FILES WITH HIGH DB PRESENCE (>80%):`);
    highExistingFiles.forEach(f => {
      console.log(`   ${f.fileName}: ${f.percentageInDb}% already in DB`);
      if (f.sampleExistingKeys.length > 0) {
        console.log(`      Sample existing keys: ${f.sampleExistingKeys.slice(0, 3).join(', ')}`);
      }
    });
  }
  
  if (lowExistingFiles.length > 0) {
    console.log(`\n❌ FILES WITH LOW DB PRESENCE (<20%):`);
    lowExistingFiles.forEach(f => {
      console.log(`   ${f.fileName}: ${f.percentageInDb}% in DB`);
      if (f.sampleMissingKeys.length > 0) {
        console.log(`      Sample missing keys: ${f.sampleMissingKeys.slice(0, 3).join(', ')}`);
      }
    });
  }
  
  console.log(`\n💡 ANALYSIS:`);
  if (stats.analysisType === 'failed') {
    if (stats.overallPercentageInDb > 80) {
      console.log(`   🎯 HIGH DUPLICATION: ${stats.overallPercentageInDb}% of failed items already exist in DB`);
      console.log(`   📋 This suggests a double-processing issue - items are being processed successfully`);
      console.log(`      but then retried and failing on the retry attempt.`);
    } else if (stats.overallPercentageInDb < 20) {
      console.log(`   🆕 LOW DUPLICATION: ${stats.overallPercentageInDb}% of failed items exist in DB`);
      console.log(`   📋 This suggests genuine processing failures - items are failing to insert initially.`);
    } else {
      console.log(`   🔄 MIXED PATTERN: ${stats.overallPercentageInDb}% of failed items exist in DB`);
      console.log(`   📋 This suggests a combination of double-processing and genuine failures.`);
    }
  } else {
    if (stats.overallPercentageInDb > 95) {
      console.log(`   ✅ EXCELLENT: ${stats.overallPercentageInDb}% of completed items are properly stored in DB`);
      console.log(`   📋 The queue processing is working correctly for completed items.`);
    } else if (stats.overallPercentageInDb < 80) {
      console.log(`   ⚠️  CONCERNING: Only ${stats.overallPercentageInDb}% of completed items are in DB`);
      console.log(`   📋 This suggests completed items are not being properly stored in the database.`);
    } else {
      console.log(`   🔄 MOSTLY GOOD: ${stats.overallPercentageInDb}% of completed items are in DB`);
      console.log(`   📋 Most completed items are properly stored, but some may be missing.`);
    }
  }
  
  console.log('\n' + '='.repeat(80));
}

async function saveReportToFile(stats: OverallStats) {
  const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-');
  const reportPath = `${stats.analysisType}-queue-analysis-${timestamp}.json`;
  await fs.writeFile(reportPath, JSON.stringify(stats, null, 2));
  console.log(`💾 Detailed report saved to: ${reportPath}`);
}

async function main() {
  // Parse command line arguments
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      type: {
        type: 'string',
        short: 't',
        default: 'failed'
      },
      help: {
        type: 'boolean',
        short: 'h',
        default: false
      }
    }
  });

  if (values.help) {
    console.log(`
🔍 Queue Items Analysis Tool

Usage: bun run scripts/analyze-failed-queue-items.ts [options]

Options:
  -t, --type <type>    Type of files to analyze: 'failed' or 'completed' (default: 'failed')
  -h, --help          Show this help message

Examples:
  bun run scripts/analyze-failed-queue-items.ts --type failed
  bun run scripts/analyze-failed-queue-items.ts --type completed
  bun run scripts/analyze-failed-queue-items.ts -t completed
`);
    return;
  }

  const analysisType = values.type as 'failed' | 'completed';
  if (analysisType !== 'failed' && analysisType !== 'completed') {
    console.error('❌ Invalid type. Must be "failed" or "completed"');
    process.exit(1);
  }

  const typeEmoji = analysisType === 'failed' ? '❌' : '✅';
  console.log(`🚀 Starting ${typeEmoji} ${analysisType.toUpperCase()} Queue Items Analysis...\n`);
  
  try {
    // Load configuration
    const config = await loadConfig();
    console.log(`📂 Queue directory: ${config.queueDir}`);
    console.log(`🗄️  Database path: ${config.database.path}`);
    
    // Initialize database
    const db = await initializeDatabase(config.database.path);
    
    // Get queue files
    const queueFiles = await getQueueFiles(config.queueDir, analysisType);
    
    if (queueFiles.length === 0) {
      console.log(`✅ No ${analysisType} files found! Queue is clean.`);
      return;
    }
    
    // Analyze each queue file
    const fileAnalyses: FileAnalysis[] = [];
    for (const filePath of queueFiles) {
      const analysis = await analyzeQueueFile(db, filePath);
      fileAnalyses.push(analysis);
    }
    
    // Calculate overall statistics
    const totalItems = fileAnalyses.reduce((sum, f) => sum + f.totalItems, 0);
    const totalItemsInDb = fileAnalyses.reduce((sum, f) => sum + f.itemsInDb, 0);
    const totalItemsNotInDb = fileAnalyses.reduce((sum, f) => sum + f.itemsNotInDb, 0);
    const overallPercentageInDb = totalItems > 0 ? 
      Math.round((totalItemsInDb / totalItems) * 10000) / 100 : 0;
    
    const stats: OverallStats = {
      analysisType,
      totalFiles: queueFiles.length,
      totalItems,
      totalItemsInDb,
      totalItemsNotInDb,
      overallPercentageInDb,
      fileAnalyses: fileAnalyses.sort((a, b) => b.percentageInDb - a.percentageInDb)
    };
    
    // Generate and display report
    await generateReport(stats);
    
    // Save detailed report
    await saveReportToFile(stats);
    
    console.log('\n✅ Analysis complete!');
    
  } catch (error) {
    console.error('\n❌ Analysis failed:', error);
    process.exit(1);
  }
}

// Run the script
if (import.meta.main) {
  main();
}
