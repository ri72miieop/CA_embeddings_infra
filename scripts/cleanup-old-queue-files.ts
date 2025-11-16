#!/usr/bin/env bun

/**
 * Cleanup script for old file-based queue Parquet files
 * 
 * This script helps clean up or archive the old .pending.parquet, .processing.parquet,
 * .completed.parquet, and .failed.parquet files from the deprecated RotatingEmbeddingWriteQueue.
 * 
 * Usage:
 *   bun run scripts/cleanup-old-queue-files.ts --queue-dir ./data/embedding-queue --action [archive|delete|list]
 * 
 * Actions:
 *   - list: List all old queue files (default)
 *   - archive: Move old files to an archive directory
 *   - delete: Permanently delete old files (use with caution!)
 */

import fs from 'fs/promises';
import path from 'path';

interface CleanupOptions {
  queueDir: string;
  action: 'list' | 'archive' | 'delete';
  archiveDir?: string;
}

async function listOldQueueFiles(queueDir: string): Promise<string[]> {
  const files = await fs.readdir(queueDir);
  
  const oldQueueFiles = files.filter(f => 
    f.endsWith('.pending.parquet') || 
    f.endsWith('.processing.parquet') || 
    f.endsWith('.completed.parquet') || 
    f.endsWith('.failed.parquet')
  );
  
  return oldQueueFiles;
}

async function getFileStats(filePath: string) {
  const stats = await fs.stat(filePath);
  return {
    size: stats.size,
    modified: stats.mtime,
    sizeMB: Math.round(stats.size / (1024 * 1024) * 100) / 100
  };
}

async function listFiles(options: CleanupOptions): Promise<void> {
  console.log(`\n📁 Scanning directory: ${options.queueDir}\n`);
  
  const files = await listOldQueueFiles(options.queueDir);
  
  if (files.length === 0) {
    console.log('✅ No old queue files found.\n');
    return;
  }
  
  console.log(`Found ${files.length} old queue file(s):\n`);
  
  let totalSize = 0;
  const filesByType: Record<string, number> = {
    pending: 0,
    processing: 0,
    completed: 0,
    failed: 0
  };
  
  for (const file of files) {
    const filePath = path.join(options.queueDir, file);
    const stats = await getFileStats(filePath);
    
    // Determine file type
    let fileType = 'unknown';
    if (file.includes('.pending.')) fileType = 'pending';
    else if (file.includes('.processing.')) fileType = 'processing';
    else if (file.includes('.completed.')) fileType = 'completed';
    else if (file.includes('.failed.')) fileType = 'failed';
    
    filesByType[fileType]++;
    totalSize += stats.size;
    
    console.log(`  • ${file}`);
    console.log(`    Size: ${stats.sizeMB} MB | Modified: ${stats.modified.toISOString()}`);
  }
  
  console.log(`\n📊 Summary:`);
  console.log(`  Pending files: ${filesByType.pending}`);
  console.log(`  Processing files: ${filesByType.processing}`);
  console.log(`  Completed files: ${filesByType.completed}`);
  console.log(`  Failed files: ${filesByType.failed}`);
  console.log(`  Total size: ${Math.round(totalSize / (1024 * 1024) * 100) / 100} MB\n`);
}

async function archiveFiles(options: CleanupOptions): Promise<void> {
  const archiveDir = options.archiveDir || path.join(options.queueDir, 'archived-old-queue');
  
  console.log(`\n📦 Archiving old queue files to: ${archiveDir}\n`);
  
  // Create archive directory
  await fs.mkdir(archiveDir, { recursive: true });
  
  const files = await listOldQueueFiles(options.queueDir);
  
  if (files.length === 0) {
    console.log('✅ No old queue files found to archive.\n');
    return;
  }
  
  let archivedCount = 0;
  let totalSize = 0;
  
  for (const file of files) {
    const sourcePath = path.join(options.queueDir, file);
    const destPath = path.join(archiveDir, file);
    
    try {
      const stats = await getFileStats(sourcePath);
      await fs.rename(sourcePath, destPath);
      console.log(`  ✓ Archived: ${file} (${stats.sizeMB} MB)`);
      archivedCount++;
      totalSize += stats.size;
    } catch (error) {
      console.error(`  ✗ Failed to archive: ${file}`, error);
    }
  }
  
  console.log(`\n✅ Archived ${archivedCount} file(s) (${Math.round(totalSize / (1024 * 1024) * 100) / 100} MB)`);
  console.log(`   Location: ${archiveDir}\n`);
}

async function deleteFiles(options: CleanupOptions): Promise<void> {
  console.log(`\n🗑️  Deleting old queue files from: ${options.queueDir}\n`);
  console.log('⚠️  WARNING: This action is PERMANENT and cannot be undone!\n');
  
  const files = await listOldQueueFiles(options.queueDir);
  
  if (files.length === 0) {
    console.log('✅ No old queue files found to delete.\n');
    return;
  }
  
  // Ask for confirmation
  const readline = require('readline').createInterface({
    input: process.stdin,
    output: process.stdout
  });
  
  const answer = await new Promise<string>((resolve) => {
    readline.question(`Are you sure you want to delete ${files.length} file(s)? (yes/no): `, resolve);
  });
  
  readline.close();
  
  if (answer.toLowerCase() !== 'yes') {
    console.log('\n❌ Deletion cancelled.\n');
    return;
  }
  
  let deletedCount = 0;
  let totalSize = 0;
  
  for (const file of files) {
    const filePath = path.join(options.queueDir, file);
    
    try {
      const stats = await getFileStats(filePath);
      await fs.unlink(filePath);
      console.log(`  ✓ Deleted: ${file} (${stats.sizeMB} MB)`);
      deletedCount++;
      totalSize += stats.size;
    } catch (error) {
      console.error(`  ✗ Failed to delete: ${file}`, error);
    }
  }
  
  console.log(`\n✅ Deleted ${deletedCount} file(s) (${Math.round(totalSize / (1024 * 1024) * 100) / 100} MB)\n`);
}

async function main() {
  const args = process.argv.slice(2);
  
  // Parse arguments
  let queueDir = './data/embedding-queue';
  let action: 'list' | 'archive' | 'delete' = 'list';
  let archiveDir: string | undefined;
  
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--queue-dir' && args[i + 1]) {
      queueDir = args[i + 1];
      i++;
    } else if (args[i] === '--action' && args[i + 1]) {
      action = args[i + 1] as 'list' | 'archive' | 'delete';
      i++;
    } else if (args[i] === '--archive-dir' && args[i + 1]) {
      archiveDir = args[i + 1];
      i++;
    } else if (args[i] === '--help' || args[i] === '-h') {
      console.log(`
Cleanup script for old file-based queue Parquet files

Usage:
  bun run scripts/cleanup-old-queue-files.ts [options]

Options:
  --queue-dir <path>      Path to queue directory (default: ./data/embedding-queue)
  --action <action>       Action to perform: list, archive, or delete (default: list)
  --archive-dir <path>    Custom archive directory (default: <queue-dir>/archived-old-queue)
  --help, -h              Show this help message

Examples:
  # List old queue files
  bun run scripts/cleanup-old-queue-files.ts

  # Archive old files
  bun run scripts/cleanup-old-queue-files.ts --action archive

  # Delete old files (with confirmation)
  bun run scripts/cleanup-old-queue-files.ts --action delete

  # Use custom queue directory
  bun run scripts/cleanup-old-queue-files.ts --queue-dir ./custom/path --action archive
      `);
      process.exit(0);
    }
  }
  
  const options: CleanupOptions = {
    queueDir,
    action,
    archiveDir
  };
  
  // Check if queue directory exists
  try {
    await fs.access(queueDir);
  } catch (error) {
    console.error(`\n❌ Queue directory not found: ${queueDir}\n`);
    process.exit(1);
  }
  
  // Execute action
  switch (action) {
    case 'list':
      await listFiles(options);
      break;
    case 'archive':
      await archiveFiles(options);
      break;
    case 'delete':
      await deleteFiles(options);
      break;
    default:
      console.error(`\n❌ Unknown action: ${action}\n`);
      process.exit(1);
  }
}

main().catch((error) => {
  console.error('\n❌ Error:', error);
  process.exit(1);
});

