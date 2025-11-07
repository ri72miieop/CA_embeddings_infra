#!/usr/bin/env bun

/**
 * Vector Store Benchmark Script
 * 
 * Performs comprehensive performance testing of the vector store search endpoint
 * with real-time resource monitoring via SSH.
 * 
 * Usage:
 *   bun run scripts/benchmark-vector-store.ts <machine-ip>
 * 
 * Required Environment Variables:
 *   - SSH_USERNAME: SSH username for remote machine
 *   - SSH_CERT_PATH: Path to SSH private key/certificate
 *   - API_SERVER_URL: URL of the API server to benchmark (e.g., http://api.example.com:3000)
 */

import { Client, type ConnectConfig } from 'ssh2';
import chalk from 'chalk';
import { promises as fs } from 'fs';
import { promisify } from 'util';
import * as readline from 'readline';

// ============================================================================
// Types & Interfaces
// ============================================================================

interface SearchResult {
  key: string;
  distance: number;
  metadata?: Record<string, any>;
}

interface SearchResponse {
  success: boolean;
  results: SearchResult[];
  count: number;
  error?: string;
}

interface ResourceSnapshot {
  timestamp: string;
  cpuUsagePercent: number;
  ramUsedMB: number;
  ramTotalMB: number;
  ramUsagePercent: number;
  offsetMs: number; // Time offset from query start
}

interface HardwareInfo {
  hostname: string;
  cpuModel: string;
  cpuCores: number;
  cpuArchitecture: string;
  ramTotalMB: number;
  osVersion: string;
  platform: string;
}

interface SearchTestCase {
  description: string;
  searchTerm: string;
  k?: number;
  threshold?: number;
  filter?: Record<string, any>;
}

interface BenchmarkResult {
  testCase: SearchTestCase;
  timestamp: string;
  responseTimeMs: number;
  resultCount: number;
  success: boolean;
  error?: string;
  resourceSnapshots: ResourceSnapshot[]; // Multiple snapshots during query execution
}

interface BenchmarkSummary {
  machineIp: string;
  hardware: HardwareInfo;
  benchmarkDate: string;
  totalDurationMs: number;
  totalTests: number;
  successfulTests: number;
  failedTests: number;
  successRate: number;
  responseTimeStats: {
    min: number;
    max: number;
    avg: number;
    p50: number;
    p95: number;
    p99: number;
  };
  resourceStats: {
    cpu: {
      min: number;
      max: number;
      avg: number;
      p95: number;
    };
    ram: {
      min: number;
      max: number;
      avg: number;
      p95: number;
    };
  };
  results: BenchmarkResult[];
}

// ============================================================================
// Configuration
// ============================================================================

const SSH_USERNAME = process.env.SSH_USERNAME;
const SSH_CERT_PATH = process.env.SSH_CERT_PATH;
const API_SERVER_URL = process.env.API_SERVER_URL?? "localhost:3000";

// ============================================================================
// Utility Functions
// ============================================================================

function getTimestamp(): string {
  return new Date().toISOString();
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

function calculateStats(values: number[]): { min: number; max: number; avg: number; p50: number; p95: number; p99: number } {
  if (values.length === 0) {
    return { min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0 };
  }
  
  const sorted = [...values].sort((a, b) => a - b);
  return {
    min: sorted[0]!,
    max: sorted[sorted.length - 1]!,
    avg: values.reduce((a, b) => a + b, 0) / values.length,
    p50: calculatePercentile(values, 50),
    p95: calculatePercentile(values, 95),
    p99: calculatePercentile(values, 99),
  };
}

async function promptForInput(question: string): Promise<string> {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.close();
      resolve(answer.trim());
    });
  });
}

// ============================================================================
// SSH Connection & Remote Command Execution
// ============================================================================

class SSHConnection {
  private client: Client;
  private connected: boolean = false;
  private config: ConnectConfig;

  constructor(host: string, username: string, privateKeyPath: string) {
    this.client = new Client();
    this.config = {
      host,
      port: 22,
      username,
      privateKey: undefined as any, // Will be loaded in connect()
    };
  }

  async connect(): Promise<void> {
    try {
      // Read the private key
      const privateKey = await fs.readFile(SSH_CERT_PATH!, 'utf-8');
      
      // Check if it's a .ppk file
      if (SSH_CERT_PATH!.toLowerCase().endsWith('.ppk')) {
        throw new Error(
          'PuTTY Private Key (.ppk) format is not supported.\n\n' +
          'Please convert your .ppk file to OpenSSH format:\n\n' +
          '  On Windows (using PuTTYgen):\n' +
          '    1. Open PuTTYgen\n' +
          '    2. Load your .ppk file (File → Load private key)\n' +
          '    3. Go to Conversions → Export OpenSSH key\n' +
          '    4. Save it (e.g., as id_rsa)\n' +
          '    5. Update SSH_CERT_PATH to point to the new file\n\n' +
          '  On Linux/Mac:\n' +
          '    puttygen your_key.ppk -O private-openssh -o id_rsa\n' +
          '    chmod 600 id_rsa\n' +
          '    export SSH_CERT_PATH="/path/to/id_rsa"'
        );
      }

      // Check if the key looks like an OpenSSH or PEM format
      if (!privateKey.includes('BEGIN') || !privateKey.includes('PRIVATE KEY')) {
        throw new Error(
          'Invalid private key format. Expected OpenSSH or PEM format.\n' +
          'The key should start with "-----BEGIN ... PRIVATE KEY-----"'
        );
      }

      this.config.privateKey = privateKey;

      return new Promise((resolve, reject) => {
        this.client.on('ready', () => {
          this.connected = true;
          resolve();
        });

        this.client.on('error', (err) => {
          reject(err);
        });

        this.client.connect(this.config);
      });
    } catch (error) {
      throw new Error(`Failed to connect via SSH: ${error}`);
    }
  }

  async executeCommand(command: string): Promise<string> {
    if (!this.connected) {
      throw new Error('SSH connection not established');
    }

    return new Promise((resolve, reject) => {
      this.client.exec(command, (err, stream) => {
        if (err) {
          reject(err);
          return;
        }

        let output = '';
        let errorOutput = '';

        stream.on('close', (code: number) => {
          if (code !== 0 && !output) {
            reject(new Error(`Command failed with code ${code}: ${errorOutput}`));
          } else {
            resolve(output.trim());
          }
        });

        stream.on('data', (data: Buffer) => {
          output += data.toString();
        });

        stream.stderr.on('data', (data: Buffer) => {
          errorOutput += data.toString();
        });
      });
    });
  }

  async disconnect(): Promise<void> {
    this.client.end();
    this.connected = false;
  }
}

// ============================================================================
// Hardware & Resource Collection
// ============================================================================

async function detectOS(ssh: SSHConnection): Promise<'linux' | 'windows'> {
  try {
    await ssh.executeCommand('uname');
    return 'linux';
  } catch {
    return 'windows';
  }
}

async function collectHardwareInfo(ssh: SSHConnection): Promise<HardwareInfo> {
  console.log(chalk.blue('📊 Collecting hardware information...'));

  const os = await detectOS(ssh);
  const hostname = await ssh.executeCommand('hostname');

  let cpuModel = '';
  let cpuCores = 0;
  let cpuArchitecture = '';
  let ramTotalMB = 0;
  let osVersion = '';
  let platform = '';

  if (os === 'linux') {
    // Linux hardware collection
    try {
      const cpuInfo = await ssh.executeCommand('lscpu');
      const lines = cpuInfo.split('\n');
      
      for (const line of lines) {
        if (line.includes('Model name:')) {
          cpuModel = line.split(':')[1]?.trim() || 'Unknown';
        } else if (line.includes('CPU(s):') && !line.includes('NUMA')) {
          const match = line.match(/CPU\(s\):\s+(\d+)/);
          if (match && match[1]) cpuCores = parseInt(match[1], 10);
        } else if (line.includes('Architecture:')) {
          cpuArchitecture = line.split(':')[1]?.trim() || 'Unknown';
        }
      }

      const memInfo = await ssh.executeCommand('free -m | grep Mem:');
      const memMatch = memInfo.match(/Mem:\s+(\d+)/);
      if (memMatch && memMatch[1]) {
        ramTotalMB = parseInt(memMatch[1], 10);
      }

      osVersion = await ssh.executeCommand('uname -r');
      platform = 'Linux';
    } catch (error) {
      console.warn(chalk.yellow(`⚠️  Some hardware info could not be collected: ${error}`));
    }
  } else {
    // Windows hardware collection
    try {
      const cpuModelRaw = await ssh.executeCommand('wmic cpu get name /value');
      cpuModel = cpuModelRaw.split('=')[1]?.trim() || 'Unknown';

      const cores = await ssh.executeCommand('wmic cpu get NumberOfCores /value');
      const coresValue = cores.split('=')[1]?.trim();
      cpuCores = coresValue ? parseInt(coresValue, 10) : 0;

      const cpuArchRaw = await ssh.executeCommand('wmic os get OSArchitecture /value');
      cpuArchitecture = cpuArchRaw.split('=')[1]?.trim() || 'Unknown';

      const memInfo = await ssh.executeCommand('wmic OS get TotalVisibleMemorySize /value');
      const memValue = memInfo.split('=')[1]?.trim();
      const memKB = memValue ? parseInt(memValue, 10) : 0;
      ramTotalMB = Math.round(memKB / 1024);

      const osVerRaw = await ssh.executeCommand('wmic os get Version /value');
      osVersion = osVerRaw.split('=')[1]?.trim() || 'Unknown';
      
      platform = 'Windows';
    } catch (error) {
      console.warn(chalk.yellow(`⚠️  Some hardware info could not be collected: ${error}`));
    }
  }

  const hardware: HardwareInfo = {
    hostname,
    cpuModel,
    cpuCores,
    cpuArchitecture,
    ramTotalMB,
    osVersion,
    platform,
  };

  console.log(chalk.green('✓ Hardware info collected:'));
  console.log(chalk.gray(`  Hostname: ${hostname}`));
  console.log(chalk.gray(`  CPU: ${cpuModel} (${cpuCores} cores, ${cpuArchitecture})`));
  console.log(chalk.gray(`  RAM: ${ramTotalMB} MB`));
  console.log(chalk.gray(`  OS: ${platform} ${osVersion}`));

  return hardware;
}

async function collectResourceUsage(ssh: SSHConnection, os: 'linux' | 'windows', offsetMs: number): Promise<ResourceSnapshot> {
  let cpuUsagePercent = 0;
  let ramUsedMB = 0;
  let ramTotalMB = 0;

  try {
    if (os === 'linux') {
      // Get CPU usage from top
      const topOutput = await ssh.executeCommand('top -bn1 | grep "Cpu(s)"');
      const cpuMatch = topOutput.match(/(\d+\.\d+)\s+id/);
      if (cpuMatch && cpuMatch[1]) {
        const idlePercent = parseFloat(cpuMatch[1]);
        cpuUsagePercent = 100 - idlePercent;
      }

      // Get RAM usage from free
      const freeOutput = await ssh.executeCommand('free -m | grep Mem:');
      const memMatch = freeOutput.match(/Mem:\s+(\d+)\s+(\d+)/);
      if (memMatch && memMatch[1] && memMatch[2]) {
        ramTotalMB = parseInt(memMatch[1], 10);
        ramUsedMB = parseInt(memMatch[2], 10);
      }
    } else {
      // Windows CPU usage
      const cpuOutput = await ssh.executeCommand('wmic cpu get loadpercentage /value');
      const cpuMatch = cpuOutput.match(/LoadPercentage=(\d+)/);
      if (cpuMatch && cpuMatch[1]) {
        cpuUsagePercent = parseInt(cpuMatch[1], 10);
      }

      // Windows RAM usage
      const memOutput = await ssh.executeCommand('wmic OS get FreePhysicalMemory,TotalVisibleMemorySize /value');
      const totalMatch = memOutput.match(/TotalVisibleMemorySize=(\d+)/);
      const freeMatch = memOutput.match(/FreePhysicalMemory=(\d+)/);
      
      if (totalMatch && totalMatch[1] && freeMatch && freeMatch[1]) {
        const totalKB = parseInt(totalMatch[1], 10);
        const freeKB = parseInt(freeMatch[1], 10);
        ramTotalMB = Math.round(totalKB / 1024);
        ramUsedMB = Math.round((totalKB - freeKB) / 1024);
      }
    }
  } catch (error) {
    // Silently fail to not spam logs during polling
  }

  const ramUsagePercent = ramTotalMB > 0 ? (ramUsedMB / ramTotalMB) * 100 : 0;

  return {
    timestamp: getTimestamp(),
    cpuUsagePercent: Math.round(cpuUsagePercent * 100) / 100,
    ramUsedMB,
    ramTotalMB,
    ramUsagePercent: Math.round(ramUsagePercent * 100) / 100,
    offsetMs,
  };
}

/**
 * Collect resource snapshots while a query is running
 */
async function collectResourcesDuringQuery(
  ssh: SSHConnection,
  os: 'linux' | 'windows',
  queryStartTime: number,
  intervalMs: number = 5000
): Promise<{ snapshots: ResourceSnapshot[]; stopCollecting: () => void }> {
  const snapshots: ResourceSnapshot[] = [];
  let collecting = true;

  const stopCollecting = () => {
    collecting = false;
  };

  // Start collecting in the background
  (async () => {
    while (collecting) {
      const offsetMs = Date.now() - queryStartTime;
      const snapshot = await collectResourceUsage(ssh, os, offsetMs);
      snapshots.push(snapshot);
      
      // Wait for the interval, but check frequently if we should stop
      const waitUntil = Date.now() + intervalMs;
      while (collecting && Date.now() < waitUntil) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
  })();

  return { snapshots, stopCollecting };
}

// ============================================================================
// Search Test Cases
// ============================================================================

function generateTestCases(): SearchTestCase[] {
  const testCases: SearchTestCase[] = [];

  // 1. Basic searches with different k values (5 tests)
  testCases.push(
    { description: 'Basic search with k=1', searchTerm: 'twitter', k: 1 },
    { description: 'Basic search with k=5', searchTerm: 'hello world', k: 5 },
    { description: 'Basic search with k=10', searchTerm: 'machine learning', k: 10 },
    { description: 'Basic search with k=20', searchTerm: 'data analysis', k: 20 },
    { description: 'Basic search with k=50', searchTerm: 'artificial intelligence', k: 50 }
  );

  // 2. Searches with different thresholds (3 tests)
  testCases.push(
    { description: 'Search with threshold=0.7', searchTerm: 'programming', k: 10, threshold: 0.7 },
    { description: 'Search with threshold=0.8', searchTerm: 'coding', k: 10, threshold: 0.8 },
    { description: 'Search with threshold=0.9', searchTerm: 'software', k: 10, threshold: 0.9 }
  );

  // 3. Filtered searches by text field (4 tests)
  testCases.push(
    { description: 'Filter by text: twitter', searchTerm: 'social media', k: 10, filter: { text: 'twitter' } },
    { description: 'Filter by text: hello', searchTerm: 'greeting', k: 10, filter: { text: 'hello' } },
    { description: 'Filter by text: machine learning', searchTerm: 'AI', k: 10, filter: { text: 'machine learning' } },
    { description: 'Filter by text: data', searchTerm: 'information', k: 10, filter: { text: 'data' } }
  );

  // 4. Filtered searches by has_quotes (2 tests)
  testCases.push(
    { description: 'Filter by has_quotes=true', searchTerm: 'quotes', k: 10, filter: { has_quotes: true } },
    { description: 'Filter by has_quotes=false', searchTerm: 'no quotes', k: 10, filter: { has_quotes: false } }
  );

  // 5. Filtered searches by is_truncated (2 tests)
  testCases.push(
    { description: 'Filter by is_truncated=true', searchTerm: 'truncated', k: 10, filter: { is_truncated: true } },
    { description: 'Filter by is_truncated=false', searchTerm: 'full text', k: 10, filter: { is_truncated: false } }
  );

  // 6. Combined filters (4 tests)
  testCases.push(
    { description: 'Combined: has_quotes=true & is_truncated=false', searchTerm: 'combined', k: 10, filter: { has_quotes: true, is_truncated: false } },
    { description: 'Combined: has_quotes=false & is_truncated=true', searchTerm: 'multi-filter', k: 10, filter: { has_quotes: false, is_truncated: true } },
    { description: 'Combined: text + has_quotes', searchTerm: 'complex', k: 10, filter: { text: 'twitter', has_quotes: true } },
    { description: 'Combined: all filters', searchTerm: 'comprehensive', k: 10, filter: { text: 'data', has_quotes: false, is_truncated: false } }
  );

  // 7. Additional varied searches (2 tests)
  testCases.push(
    { description: 'Simple search: technology', searchTerm: 'technology', k: 15 },
    { description: 'Simple search: database', searchTerm: 'database query optimization', k: 25 }
  );

  return testCases;
}

// ============================================================================
// Benchmark Execution
// ============================================================================

async function executeSearchQuery(
  serverUrl: string,
  testCase: SearchTestCase,
  ssh: SSHConnection,
  os: 'linux' | 'windows',
  retries: number = 1
): Promise<{ responseTimeMs: number; resultCount: number; success: boolean; error?: string; resourceSnapshots: ResourceSnapshot[] }> {
  const startTime = Date.now();
  
  try {
    const requestBody: any = {
      searchTerm: testCase.searchTerm,
      k: testCase.k || 10,
    };

    if (testCase.threshold !== undefined) {
      requestBody.threshold = testCase.threshold;
    }

    if (testCase.filter) {
      requestBody.filter = testCase.filter;
    }

    // Start collecting resources while the query executes
    const resourceCollection = await collectResourcesDuringQuery(ssh, os, startTime, 5000);

    const response = await fetch(`${serverUrl}/embeddings/search`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(requestBody),
    });

    const responseTimeMs = Date.now() - startTime;

    // Stop collecting resources
    resourceCollection.stopCollecting();
    
    // Give it a moment to finish the last collection
    await new Promise(resolve => setTimeout(resolve, 1000));

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`HTTP ${response.status}: ${errorText}`);
    }

    const data = await response.json() as SearchResponse;

    return {
      responseTimeMs,
      resultCount: data.count || data.results.length,
      success: true,
      resourceSnapshots: resourceCollection.snapshots,
    };
  } catch (error) {
    const responseTimeMs = Date.now() - startTime;
    
    if (retries > 0) {
      console.log(chalk.yellow(`  ↻ Retrying... (${retries} attempts left)`));
      await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second before retry
      return executeSearchQuery(serverUrl, testCase, ssh, os, retries - 1);
    }

    return {
      responseTimeMs,
      resultCount: 0,
      success: false,
      error: error instanceof Error ? error.message : String(error),
      resourceSnapshots: [], // No snapshots on error
    };
  }
}

async function runBenchmark(
  machineIp: string,
  serverUrl: string,
  ssh: SSHConnection,
  os: 'linux' | 'windows',
  hardware: HardwareInfo
): Promise<BenchmarkSummary> {
  console.log(chalk.blue('\n🚀 Starting benchmark tests...\n'));

  const testCases = generateTestCases();
  const results: BenchmarkResult[] = [];
  const benchmarkStartTime = Date.now();

  let successCount = 0;
  let failCount = 0;

  for (let i = 0; i < testCases.length; i++) {
    const testCase = testCases[i]!;
    
    console.log(chalk.cyan(`[${i + 1}/${testCases.length}] ${testCase.description}`));

    // Execute the search query (resource collection happens during execution)
    const result = await executeSearchQuery(serverUrl, testCase, ssh, os);

    if (result.success) {
      successCount++;
      console.log(chalk.green(`  ✓ Success: ${result.responseTimeMs}ms, ${result.resultCount} results`));
    } else {
      failCount++;
      console.log(chalk.red(`  ✗ Failed: ${result.error}`));
    }

    // Show resource usage summary (average from snapshots)
    if (result.resourceSnapshots.length > 0) {
      const avgCpu = result.resourceSnapshots.reduce((sum: number, s: ResourceSnapshot) => sum + s.cpuUsagePercent, 0) / result.resourceSnapshots.length;
      const avgRam = result.resourceSnapshots.reduce((sum: number, s: ResourceSnapshot) => sum + s.ramUsagePercent, 0) / result.resourceSnapshots.length;
      console.log(chalk.gray(`  Resources: ${result.resourceSnapshots.length} snapshots, Avg CPU: ${avgCpu.toFixed(2)}%, Avg RAM: ${avgRam.toFixed(2)}%`));
    } else {
      console.log(chalk.gray(`  Resources: No snapshots collected (query too fast or error)`));
    }

    results.push({
      testCase,
      timestamp: getTimestamp(),
      responseTimeMs: result.responseTimeMs,
      resultCount: result.resultCount,
      success: result.success,
      error: result.error,
      resourceSnapshots: result.resourceSnapshots,
    });

    // Small delay between tests
    await new Promise(resolve => setTimeout(resolve, 1000));
  }

  const benchmarkEndTime = Date.now();
  const totalDurationMs = benchmarkEndTime - benchmarkStartTime;

  // Calculate statistics
  const successfulResults = results.filter(r => r.success);
  const responseTimes = successfulResults.map(r => r.responseTimeMs);
  
  // Flatten all resource snapshots to calculate overall stats
  const allCpuUsages: number[] = [];
  const allRamUsages: number[] = [];
  
  for (const result of results) {
    for (const snapshot of result.resourceSnapshots) {
      allCpuUsages.push(snapshot.cpuUsagePercent);
      allRamUsages.push(snapshot.ramUsagePercent);
    }
  }

  const responseTimeStats = calculateStats(responseTimes);
  const cpuStats = allCpuUsages.length > 0 ? {
    min: Math.min(...allCpuUsages),
    max: Math.max(...allCpuUsages),
    avg: allCpuUsages.reduce((a, b) => a + b, 0) / allCpuUsages.length,
    p95: calculatePercentile(allCpuUsages, 95),
  } : { min: 0, max: 0, avg: 0, p95: 0 };
  
  const ramStats = allRamUsages.length > 0 ? {
    min: Math.min(...allRamUsages),
    max: Math.max(...allRamUsages),
    avg: allRamUsages.reduce((a, b) => a + b, 0) / allRamUsages.length,
    p95: calculatePercentile(allRamUsages, 95),
  } : { min: 0, max: 0, avg: 0, p95: 0 };

  return {
    machineIp,
    hardware,
    benchmarkDate: new Date().toISOString(),
    totalDurationMs,
    totalTests: testCases.length,
    successfulTests: successCount,
    failedTests: failCount,
    successRate: (successCount / testCases.length) * 100,
    responseTimeStats,
    resourceStats: {
      cpu: cpuStats,
      ram: ramStats,
    },
    results,
  };
}

// ============================================================================
// Output Generation
// ============================================================================

async function saveJSONOutput(summary: BenchmarkSummary): Promise<string> {
  const filename = `benchmark-${summary.hardware.hostname}-${formatTimestamp()}.json`;
  const filepath = `benchmarks/${filename}`;

  await fs.mkdir('benchmarks', { recursive: true });
  await fs.writeFile(filepath, JSON.stringify(summary, null, 2), 'utf-8');

  return filepath;
}

async function saveCSVOutput(summary: BenchmarkSummary): Promise<string> {
  const filename = `benchmark-${summary.hardware.hostname}-${formatTimestamp()}.csv`;
  const filepath = `benchmarks/${filename}`;

  const headers = [
    'machine_ip',
    'hostname',
    'timestamp',
    'search_term',
    'k',
    'threshold',
    'filter_json',
    'response_time_ms',
    'result_count',
    'snapshot_offset_ms',
    'cpu_usage_pct',
    'ram_usage_pct',
    'success',
    'error_message',
  ];

  // Create rows - one per resource snapshot
  const rows: any[] = [];
  
  for (const result of summary.results) {
    if (result.resourceSnapshots.length === 0) {
      // No snapshots - add single row with no resource data
      rows.push([
        summary.machineIp,
        summary.hardware.hostname,
        result.timestamp,
        result.testCase.searchTerm,
        result.testCase.k || 10,
        result.testCase.threshold || '',
        result.testCase.filter ? JSON.stringify(result.testCase.filter) : '',
        result.responseTimeMs,
        result.resultCount,
        '',  // snapshot_offset_ms
        '',  // cpu_usage_pct
        '',  // ram_usage_pct
        result.success,
        result.error || '',
      ]);
    } else {
      // Add a row for each resource snapshot
      for (const snapshot of result.resourceSnapshots) {
        rows.push([
          summary.machineIp,
          summary.hardware.hostname,
          result.timestamp,
          result.testCase.searchTerm,
          result.testCase.k || 10,
          result.testCase.threshold || '',
          result.testCase.filter ? JSON.stringify(result.testCase.filter) : '',
          result.responseTimeMs,
          result.resultCount,
          snapshot.offsetMs,
          snapshot.cpuUsagePercent,
          snapshot.ramUsagePercent,
          result.success,
          result.error || '',
        ]);
      }
    }
  }

  const csvContent = [
    headers.join(','),
    ...rows.map(row => row.map((cell: any) => {
      const cellStr = String(cell);
      // Escape cells containing commas, quotes, or newlines
      if (cellStr.includes(',') || cellStr.includes('"') || cellStr.includes('\n')) {
        return `"${cellStr.replace(/"/g, '""')}"`;
      }
      return cellStr;
    }).join(',')),
  ].join('\n');

  await fs.mkdir('benchmarks', { recursive: true });
  await fs.writeFile(filepath, csvContent, 'utf-8');

  return filepath;
}

function printSummary(summary: BenchmarkSummary): void {
  console.log(chalk.blue('\n' + '='.repeat(70)));
  console.log(chalk.blue.bold('📊 BENCHMARK SUMMARY'));
  console.log(chalk.blue('='.repeat(70)));

  console.log(chalk.cyan('\n🖥️  Machine Information:'));
  console.log(chalk.gray(`  IP: ${summary.machineIp}`));
  console.log(chalk.gray(`  Hostname: ${summary.hardware.hostname}`));
  console.log(chalk.gray(`  CPU: ${summary.hardware.cpuModel}`));
  console.log(chalk.gray(`  Cores: ${summary.hardware.cpuCores}`));
  console.log(chalk.gray(`  RAM: ${summary.hardware.ramTotalMB} MB`));
  console.log(chalk.gray(`  OS: ${summary.hardware.platform} ${summary.hardware.osVersion}`));

  console.log(chalk.cyan('\n📈 Test Results:'));
  console.log(chalk.gray(`  Total Tests: ${summary.totalTests}`));
  console.log(chalk.green(`  Successful: ${summary.successfulTests}`));
  console.log(chalk.red(`  Failed: ${summary.failedTests}`));
  console.log(chalk.gray(`  Success Rate: ${summary.successRate.toFixed(2)}%`));
  console.log(chalk.gray(`  Total Duration: ${(summary.totalDurationMs / 1000).toFixed(2)}s`));

  console.log(chalk.cyan('\n⚡ Response Time Statistics (ms):'));
  console.log(chalk.gray(`  Min: ${summary.responseTimeStats.min.toFixed(2)}`));
  console.log(chalk.gray(`  Max: ${summary.responseTimeStats.max.toFixed(2)}`));
  console.log(chalk.gray(`  Avg: ${summary.responseTimeStats.avg.toFixed(2)}`));
  console.log(chalk.gray(`  P50: ${summary.responseTimeStats.p50.toFixed(2)}`));
  console.log(chalk.gray(`  P95: ${summary.responseTimeStats.p95.toFixed(2)}`));
  console.log(chalk.gray(`  P99: ${summary.responseTimeStats.p99.toFixed(2)}`));

  console.log(chalk.cyan('\n💻 CPU Usage Statistics (%):'));
  console.log(chalk.gray(`  Min: ${summary.resourceStats.cpu.min.toFixed(2)}`));
  console.log(chalk.gray(`  Max: ${summary.resourceStats.cpu.max.toFixed(2)}`));
  console.log(chalk.gray(`  Avg: ${summary.resourceStats.cpu.avg.toFixed(2)}`));
  console.log(chalk.gray(`  P95: ${summary.resourceStats.cpu.p95.toFixed(2)}`));

  console.log(chalk.cyan('\n🧠 RAM Usage Statistics (%):'));
  console.log(chalk.gray(`  Min: ${summary.resourceStats.ram.min.toFixed(2)}`));
  console.log(chalk.gray(`  Max: ${summary.resourceStats.ram.max.toFixed(2)}`));
  console.log(chalk.gray(`  Avg: ${summary.resourceStats.ram.avg.toFixed(2)}`));
  console.log(chalk.gray(`  P95: ${summary.resourceStats.ram.p95.toFixed(2)}`));

  console.log(chalk.blue('\n' + '='.repeat(70) + '\n'));
}

// ============================================================================
// Main Entry Point
// ============================================================================

async function main() {
  console.log(chalk.blue.bold('\n🔬 Vector Store Benchmark Tool\n'));

  // 1. Validate environment variables
  if (!SSH_USERNAME || !SSH_CERT_PATH) {
    console.error(chalk.red('❌ Error: Required environment variables not set'));
    console.log(chalk.yellow('\nPlease set the following environment variables:'));
    console.log(chalk.gray('  - SSH_USERNAME: SSH username for remote machine'));
    console.log(chalk.gray('  - SSH_CERT_PATH: Path to SSH private key/certificate'));
    console.log(chalk.gray('  - API_SERVER_URL: URL of the API server (e.g., http://api.example.com:3000)'));
    process.exit(1);
  }

  if (!API_SERVER_URL) {
    console.error(chalk.red('❌ Error: API_SERVER_URL environment variable not set'));
    console.log(chalk.yellow('\nPlease set API_SERVER_URL to point to your API server:'));
    console.log(chalk.gray('  export API_SERVER_URL="http://your-api-server:3000"'));
    process.exit(1);
  }

  // 2. Get machine IP
  let machineIp = process.argv[2];
  if (!machineIp) {
    machineIp = await promptForInput('Enter machine IP address: ');
    if (!machineIp) {
      console.error(chalk.red('❌ Error: Machine IP is required'));
      process.exit(1);
    }
  }

  // 3. Set API server URL (separate from the machine being monitored)
  const apiServerUrl = API_SERVER_URL;
  console.log(chalk.gray(`Machine IP (for monitoring): ${machineIp}`));
  console.log(chalk.gray(`API Server URL: ${apiServerUrl}`));

  // 4. Establish SSH connection
  console.log(chalk.blue(`\n🔐 Connecting to ${machineIp} via SSH...`));
  const ssh = new SSHConnection(machineIp, SSH_USERNAME, SSH_CERT_PATH);

  try {
    await ssh.connect();
    console.log(chalk.green('✓ SSH connection established'));
  } catch (error) {
    console.error(chalk.red(`❌ Failed to establish SSH connection: ${error}`));
    console.log(chalk.yellow('\nPlease check:'));
    console.log(chalk.gray('  - Machine IP is correct and reachable'));
    console.log(chalk.gray('  - SSH username is valid'));
    console.log(chalk.gray('  - SSH certificate path is correct'));
    console.log(chalk.gray('  - SSH port 22 is accessible'));
    process.exit(1);
  }

  try {
    // 5. Collect hardware information
    const hardware = await collectHardwareInfo(ssh);
    const os = await detectOS(ssh);

    // 6. Run benchmark
    const summary = await runBenchmark(machineIp, apiServerUrl, ssh, os, hardware);

    // 7. Print summary
    printSummary(summary);

    // 8. Save outputs
    console.log(chalk.blue('💾 Saving results...'));
    const jsonPath = await saveJSONOutput(summary);
    const csvPath = await saveCSVOutput(summary);

    console.log(chalk.green(`✓ JSON output saved: ${jsonPath}`));
    console.log(chalk.green(`✓ CSV output saved: ${csvPath}`));

    console.log(chalk.green.bold('\n✅ Benchmark completed successfully!\n'));
  } catch (error) {
    console.error(chalk.red(`\n❌ Benchmark failed: ${error}`));
    process.exit(1);
  } finally {
    // 9. Cleanup SSH connection
    await ssh.disconnect();
  }
}

// Run the main function
main().catch((error) => {
  console.error(chalk.red(`Fatal error: ${error}`));
  process.exit(1);
});

