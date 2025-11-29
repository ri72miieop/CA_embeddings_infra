#!/usr/bin/env bun

/**
 * Memory Monitor Script
 * Monitors memory usage of the current process and logs it periodically
 */

const INTERVAL_MS = 2000; // Check every 2 seconds

interface MemoryStats {
  timestamp: string;
  heapUsed: number;
  heapTotal: number;
  external: number;
  arrayBuffers: number;
  rss: number;
  heapUsedMB: number;
  rssMB: number;
}

const stats: MemoryStats[] = [];
let lastHeapUsed = 0;
let lastRSS = 0;

function formatBytes(bytes: number): string {
  return (bytes / 1024 / 1024).toFixed(2);
}

function getMemoryStats(): MemoryStats {
  const usage = process.memoryUsage();
  return {
    timestamp: new Date().toISOString(),
    heapUsed: usage.heapUsed,
    heapTotal: usage.heapTotal,
    external: usage.external,
    arrayBuffers: usage.arrayBuffers,
    rss: usage.rss,
    heapUsedMB: parseFloat(formatBytes(usage.heapUsed)),
    rssMB: parseFloat(formatBytes(usage.rss)),
  };
}

console.log('🔍 Memory Monitor Started');
console.log('Format: RSS (Total Memory) | Heap Used | Heap Total | External | Array Buffers');
console.log('='.repeat(100));

const interval = setInterval(() => {
  const memStats = getMemoryStats();
  stats.push(memStats);

  const heapDelta = memStats.heapUsed - lastHeapUsed;
  const rssDelta = memStats.rss - lastRSS;

  const heapDeltaStr = heapDelta > 0 ? `+${formatBytes(heapDelta)}` : formatBytes(heapDelta);
  const rssDeltaStr = rssDelta > 0 ? `+${formatBytes(rssDelta)}` : formatBytes(rssDelta);

  console.log(
    `RSS: ${formatBytes(memStats.rss)} MB (${rssDeltaStr}) | ` +
    `Heap: ${formatBytes(memStats.heapUsed)} MB (${heapDeltaStr}) / ${formatBytes(memStats.heapTotal)} MB | ` +
    `Ext: ${formatBytes(memStats.external)} MB | ` +
    `AB: ${formatBytes(memStats.arrayBuffers)} MB`
  );

  lastHeapUsed = memStats.heapUsed;
  lastRSS = memStats.rss;

  // Trigger GC if available (run with --expose-gc flag)
  if (stats.length % 10 === 0 && global.gc) {
    console.log('🗑️  Manual GC triggered');
    global.gc();
  }
}, INTERVAL_MS);

// Handle shutdown
process.on('SIGINT', () => {
  clearInterval(interval);
  console.log('\n\n📊 Memory Statistics Summary:');
  console.log('='.repeat(100));

  if (stats.length > 0) {
    const first = stats[0];
    const last = stats[stats.length - 1];
    const maxHeap = Math.max(...stats.map(s => s.heapUsed));
    const maxRSS = Math.max(...stats.map(s => s.rss));

    console.log(`Initial Heap: ${formatBytes(first.heapUsed)} MB | RSS: ${formatBytes(first.rss)} MB`);
    console.log(`Final Heap:   ${formatBytes(last.heapUsed)} MB | RSS: ${formatBytes(last.rss)} MB`);
    console.log(`Peak Heap:    ${formatBytes(maxHeap)} MB`);
    console.log(`Peak RSS:     ${formatBytes(maxRSS)} MB`);
    console.log(`Total Growth: Heap: ${formatBytes(last.heapUsed - first.heapUsed)} MB | RSS: ${formatBytes(last.rss - first.rss)} MB`);
  }

  process.exit(0);
});

// Keep the process alive
setInterval(() => {}, 1000000);
