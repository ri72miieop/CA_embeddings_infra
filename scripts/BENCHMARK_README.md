# Vector Store Benchmark Script

A comprehensive benchmarking tool that measures vector store search performance across different machines with real-time resource monitoring via SSH.

## Features

- **SSH-based Hardware Collection**: Automatically retrieves hostname, CPU, RAM, and OS details from remote machines
- **Real-time Resource Monitoring**: Collects CPU and RAM usage snapshots **every 5 seconds while each search query is executing** to capture actual resource consumption during query processing
- **Comprehensive Test Suite**: 22+ diverse search test cases covering:
  - Different k values (1, 5, 10, 20, 50)
  - Score thresholds (0.7, 0.8, 0.9)
  - Metadata filters (text, has_quotes, is_truncated)
  - Combined filters
- **Performance Statistics**: Calculates min, max, avg, p50, p95, p99 for response times
- **Dual Output Formats**: Generates both JSON and CSV reports

## Prerequisites

### Required Environment Variables

Set these environment variables before running the benchmark:

```bash
export SSH_USERNAME="your_ssh_username"
export SSH_CERT_PATH="/path/to/your/private_key"  # Must be OpenSSH format, NOT .ppk
export API_SERVER_URL="http://your-api-server:3000"  # URL of the API server to benchmark
```

**Important**: The SSH private key must be in OpenSSH or PEM format. If you have a `.ppk` file, convert it first:

**Windows (using PuTTYgen):**
1. Open PuTTYgen
2. Load your .ppk file (File → Load private key)
3. Go to Conversions → Export OpenSSH key
4. Save it (e.g., as `id_rsa`)

**Linux/Mac:**
```bash
puttygen your_key.ppk -O private-openssh -o id_rsa
chmod 600 id_rsa
export SSH_CERT_PATH="/path/to/id_rsa"
```

### SSH Setup

1. Ensure SSH access is configured on the target machine
2. The SSH private key should have appropriate permissions (e.g., `chmod 600`)
3. SSH port 22 should be accessible from your machine

## Usage

### Basic Usage

```bash
# Set environment variables first
export SSH_USERNAME="root"
export SSH_CERT_PATH="/path/to/id_rsa"
export API_SERVER_URL="http://api-server.example.com:3000"

# Run benchmark with machine IP (for SSH monitoring)
bun run benchmark 192.168.1.100

# Or directly
bun run scripts/benchmark-vector-store.ts 192.168.1.100
```

**Note**: The machine IP is used for SSH connection to monitor resources. The API_SERVER_URL is where your ca_embed API server is running (these can be different machines).

### Interactive Mode

If you don't provide the machine IP, you'll be prompted:

```bash
bun run benchmark
# Enter machine IP address: 192.168.1.100
```

## Output

The script generates two files in the `benchmarks/` directory:

### 1. JSON Output

**File**: `benchmarks/benchmark-{hostname}-{timestamp}.json`

Contains:
- Complete machine hardware specifications
- All search test results with detailed timing
- Resource usage snapshots for each query
- Comprehensive statistics (response times, CPU, RAM)
- Benchmark metadata

### 2. CSV Output

**File**: `benchmarks/benchmark-{hostname}-{timestamp}.csv`

**Note**: If a query execution takes multiple resource snapshots, there will be multiple CSV rows for that query (one per snapshot).

Columns:
- `machine_ip`: IP address of the tested machine
- `hostname`: Machine hostname
- `timestamp`: When the query was executed
- `search_term`: Search query text
- `k`: Number of results requested
- `threshold`: Score threshold (if any)
- `filter_json`: Applied filters as JSON
- `response_time_ms`: Query response time
- `result_count`: Number of results returned
- `snapshot_offset_ms`: Time offset from query start when this snapshot was taken (in milliseconds)
- `cpu_usage_pct`: CPU usage at this snapshot
- `ram_usage_pct`: RAM usage at this snapshot
- `success`: Whether the query succeeded
- `error_message`: Error details (if failed)

## Test Cases

The benchmark includes 22 test cases:

1. **Basic searches** (5 tests): Different k values (1, 5, 10, 20, 50)
2. **Threshold searches** (3 tests): Different thresholds (0.7, 0.8, 0.9)
3. **Text filters** (4 tests): Filter by specific text values
4. **Boolean filters** (4 tests): Filter by has_quotes and is_truncated
5. **Combined filters** (4 tests): Multiple filters simultaneously
6. **Additional varied searches** (2 tests): Different search terms and k values

## Example Output

```
🔬 Vector Store Benchmark Tool

🔐 Connecting to 192.168.1.100 via SSH...
✓ SSH connection established

📊 Collecting hardware information...
✓ Hardware info collected:
  Hostname: server-01
  CPU: Intel(R) Xeon(R) CPU @ 2.30GHz (8 cores, x86_64)
  RAM: 16384 MB
  OS: Linux 5.15.0

🚀 Starting benchmark tests...

[1/22] Basic search with k=1
  ✓ Success: 45ms, 1 results
  Resources: No snapshots collected (query too fast or error)

[2/22] Basic search with k=5
  ✓ Success: 7852ms, 5 results
  Resources: 2 snapshots, Avg CPU: 45.32%, Avg RAM: 52.10%
...

======================================================================
📊 BENCHMARK SUMMARY
======================================================================

🖥️  Machine Information:
  IP: 192.168.1.100
  Hostname: server-01
  CPU: Intel(R) Xeon(R) CPU @ 2.30GHz
  Cores: 8
  RAM: 16384 MB
  OS: Linux 5.15.0

📈 Test Results:
  Total Tests: 22
  Successful: 22
  Failed: 0
  Success Rate: 100.00%
  Total Duration: 15.43s

⚡ Response Time Statistics (ms):
  Min: 23.45
  Max: 127.89
  Avg: 52.34
  P50: 48.12
  P95: 98.76
  P99: 115.23

💻 CPU Usage Statistics (%):
  Min: 8.50
  Max: 25.30
  Avg: 15.67
  P95: 22.15

🧠 RAM Usage Statistics (%):
  Min: 42.10
  Max: 48.20
  Avg: 45.30
  P95: 47.50

======================================================================

💾 Saving results...
✓ JSON output saved: benchmarks/benchmark-server-01-2025-11-06T12-30-45.json
✓ CSV output saved: benchmarks/benchmark-server-01-2025-11-06T12-30-45.csv

✅ Benchmark completed successfully!
```

## Troubleshooting

### SSH Connection Failed

```
❌ Failed to establish SSH connection
```

**Solutions**:
- Verify the machine IP is correct and reachable
- Check that SSH_USERNAME is valid
- Ensure SSH_CERT_PATH points to the correct private key
- Verify SSH port 22 is accessible
- Check private key permissions (`chmod 600 /path/to/key`)

### Environment Variables Not Set

```
❌ Error: Required environment variables not set
```

**Solution**: Set SSH_USERNAME and SSH_CERT_PATH as shown in Prerequisites

### Search Queries Failing

If individual queries fail, the benchmark will:
1. Automatically retry once
2. Continue with remaining tests
3. Report failures in the summary

Check:
- The vector store server is running at the correct URL
- The server has embeddings data
- Network connectivity is stable

## Notes

- The benchmark runs 22 searches sequentially with a 1 second delay between tests
- **Resource collection happens every 5 seconds DURING query execution**, not before
- For fast queries (<5s), you may get 0-1 snapshots; slower queries will have multiple snapshots
- Failed requests are automatically retried once
- SSH connection must succeed or the benchmark will exit
- Resource monitoring requires appropriate system commands (top/free on Linux, wmic on Windows)
- The script works with both Linux and Windows target machines
- The CSV file will have multiple rows per query if multiple resource snapshots were collected

