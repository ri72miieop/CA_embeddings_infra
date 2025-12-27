#!/usr/bin/env bun

/**
 * Benchmark Visualization Dashboard Generator
 *
 * Reads benchmark JSON results and generates an interactive HTML dashboard
 * with Plotly.js charts.
 *
 * Usage:
 *   bun run scripts/visualize-benchmark.ts benchmarks/qdrant-configs/benchmark-raw-*.json
 *   bun run scripts/visualize-benchmark.ts --latest
 */

import { promises as fs } from 'fs';
import path from 'path';
import chalk from 'chalk';

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
  // Actual memory metrics
  actualMemoryBytes?: number;
  vectorsMemoryBytes?: number;
  payloadMemoryBytes?: number;
  indexMemoryBytes?: number;
  diskDataBytes?: number;
}

interface BenchmarkData {
  timestamp: string;
  collectionStats: CollectionStats[];
  recallResults: RecallResult[];
  rawResults: BenchmarkResult[];
}

interface ExtendedData {
  timestamp: string;
  filterResults?: any[];
  kValueResults?: any[];
  concurrencyResults?: any[];
  coldWarmResults?: any[];
}

function generateHTML(data: BenchmarkData, extendedData?: ExtendedData): string {
  const { recallResults, collectionStats } = data;

  // Prepare data for charts
  const scatterData = recallResults.map(r => ({
    x: r.avgLatencyMs,
    y: r.recall * 100,
    config: r.configName,
    variant: r.searchVariant,
    p95: r.p95LatencyMs,
    label: `${r.configName} / ${r.searchVariant}`,
  }));

  // Group by config for bar chart
  const configGroups = new Map<string, RecallResult[]>();
  for (const r of recallResults) {
    if (!configGroups.has(r.configName)) {
      configGroups.set(r.configName, []);
    }
    configGroups.get(r.configName)!.push(r);
  }

  // Best variant per config (highest recall with lowest latency)
  const bestPerConfig = Array.from(configGroups.entries()).map(([config, results]) => {
    const sorted = results
      .filter(r => r.recall >= 0.95)
      .sort((a, b) => a.avgLatencyMs - b.avgLatencyMs);
    return sorted[0] || results.sort((a, b) => b.recall - a.recall)[0];
  }).filter(Boolean);

  // Heatmap data: config x variant
  const configs = [...new Set(recallResults.map(r => r.configName))];
  const variants = [...new Set(recallResults.map(r => r.searchVariant))];

  // Create latency matrix for heatmap
  const latencyMatrix: number[][] = [];
  const recallMatrix: number[][] = [];

  for (const config of configs) {
    const latencyRow: number[] = [];
    const recallRow: number[] = [];
    for (const variant of variants) {
      const result = recallResults.find(r => r.configName === config && r.searchVariant === variant);
      latencyRow.push(result ? result.avgLatencyMs : NaN);
      recallRow.push(result ? result.recall * 100 : NaN);
    }
    latencyMatrix.push(latencyRow);
    recallMatrix.push(recallRow);
  }

  // Find Pareto optimal points
  const paretoOptimal = findParetoOptimal(scatterData);

  // Color scheme for configs
  const configColors: Record<string, string> = {};
  const colorPalette = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
    '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
  ];
  configs.forEach((config, i) => {
    configColors[config] = colorPalette[i % colorPalette.length]!;
  });

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Qdrant Benchmark Results</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
      background: #0d1117;
      color: #c9d1d9;
      padding: 20px;
      line-height: 1.6;
    }
    .container {
      max-width: 1600px;
      margin: 0 auto;
    }
    h1 {
      color: #58a6ff;
      margin-bottom: 10px;
      font-size: 2em;
    }
    h2 {
      color: #8b949e;
      margin: 30px 0 15px;
      font-size: 1.4em;
      border-bottom: 1px solid #30363d;
      padding-bottom: 10px;
    }
    .timestamp {
      color: #8b949e;
      font-size: 0.9em;
      margin-bottom: 20px;
    }
    .chart-container {
      background: #161b22;
      border-radius: 8px;
      padding: 20px;
      margin-bottom: 20px;
      border: 1px solid #30363d;
    }
    .chart {
      width: 100%;
      height: 500px;
    }
    .chart-tall {
      height: 700px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
      gap: 20px;
    }
    .stats-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 15px;
      margin-bottom: 20px;
    }
    .stat-card {
      background: #161b22;
      border-radius: 8px;
      padding: 15px;
      border: 1px solid #30363d;
    }
    .stat-card h3 {
      color: #8b949e;
      font-size: 0.85em;
      margin-bottom: 5px;
      text-transform: uppercase;
    }
    .stat-card .value {
      color: #58a6ff;
      font-size: 1.8em;
      font-weight: bold;
    }
    .stat-card .sub {
      color: #8b949e;
      font-size: 0.85em;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.9em;
    }
    th, td {
      padding: 10px 12px;
      text-align: left;
      border-bottom: 1px solid #30363d;
    }
    th {
      background: #0d1117;
      color: #8b949e;
      font-weight: 600;
      position: sticky;
      top: 0;
    }
    tr:hover {
      background: #1f2428;
    }
    .good { color: #3fb950; }
    .warn { color: #d29922; }
    .bad { color: #f85149; }
    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 15px;
      margin: 15px 0;
    }
    .legend-item {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 0.85em;
    }
    .legend-color {
      width: 14px;
      height: 14px;
      border-radius: 3px;
    }
    .tabs {
      display: flex;
      gap: 10px;
      margin-bottom: 20px;
      flex-wrap: wrap;
    }
    .tab {
      padding: 8px 16px;
      background: #21262d;
      border: 1px solid #30363d;
      border-radius: 6px;
      cursor: pointer;
      color: #c9d1d9;
      transition: all 0.2s;
    }
    .tab:hover {
      background: #30363d;
    }
    .tab.active {
      background: #58a6ff;
      color: #0d1117;
      border-color: #58a6ff;
    }
    .tab-content {
      display: none;
    }
    .tab-content.active {
      display: block;
    }
    .recommendations {
      background: #1c2128;
      border-left: 4px solid #58a6ff;
      padding: 15px 20px;
      margin: 20px 0;
      border-radius: 0 8px 8px 0;
    }
    .recommendations h3 {
      color: #58a6ff;
      margin-bottom: 10px;
    }
    .recommendations ul {
      list-style: none;
      padding: 0;
    }
    .recommendations li {
      padding: 8px 0;
      border-bottom: 1px solid #30363d;
    }
    .recommendations li:last-child {
      border-bottom: none;
    }
    .badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 12px;
      font-size: 0.75em;
      font-weight: 600;
    }
    .badge-best { background: #238636; color: white; }
    .badge-good { background: #1f6feb; color: white; }
    .badge-warn { background: #9e6a03; color: white; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Qdrant Configuration Benchmark Results</h1>
    <p class="timestamp">Generated: ${data.timestamp}</p>

    <div class="stats-grid">
      <div class="stat-card">
        <h3>Configurations Tested</h3>
        <div class="value">${configs.length}</div>
      </div>
      <div class="stat-card">
        <h3>Search Variants</h3>
        <div class="value">${variants.length}</div>
      </div>
      <div class="stat-card">
        <h3>Total Combinations</h3>
        <div class="value">${recallResults.length}</div>
      </div>
      <div class="stat-card">
        <h3>Best Recall</h3>
        <div class="value">${(Math.max(...recallResults.map(r => r.recall)) * 100).toFixed(1)}%</div>
      </div>
      <div class="stat-card">
        <h3>Lowest Latency</h3>
        <div class="value">${Math.min(...recallResults.map(r => r.avgLatencyMs)).toFixed(1)}ms</div>
      </div>
    </div>

    ${generateRecommendations(recallResults)}

    <div class="tabs">
      <button class="tab active" onclick="showTab('overview')">Overview</button>
      <button class="tab" onclick="showTab('scatter')">Recall vs Latency</button>
      <button class="tab" onclick="showTab('heatmap')">Heatmaps</button>
      <button class="tab" onclick="showTab('comparison')">Config Comparison</button>
      <button class="tab" onclick="showTab('resources')">Memory & Resources</button>
      <button class="tab" onclick="showTab('table')">Full Data</button>
    </div>

    <div id="overview" class="tab-content active">
      <h2>Key Insights</h2>
      <div class="grid">
        <div class="chart-container">
          <h3 style="margin-bottom: 15px; color: #8b949e;">Pareto Frontier: Recall vs Latency</h3>
          <div id="pareto-chart" class="chart"></div>
        </div>
        <div class="chart-container">
          <h3 style="margin-bottom: 15px; color: #8b949e;">Best Config per Recall Threshold</h3>
          <div id="best-config-chart" class="chart"></div>
        </div>
      </div>
    </div>

    <div id="scatter" class="tab-content">
      <h2>Recall vs Latency (All Combinations)</h2>
      <div class="chart-container">
        <div class="legend">
          ${configs.map(c => `<div class="legend-item"><div class="legend-color" style="background: ${configColors[c]}"></div>${c}</div>`).join('')}
        </div>
        <div id="scatter-chart" class="chart chart-tall"></div>
      </div>
    </div>

    <div id="heatmap" class="tab-content">
      <h2>Performance Heatmaps</h2>
      <div class="grid">
        <div class="chart-container">
          <h3 style="margin-bottom: 15px; color: #8b949e;">Average Latency (ms)</h3>
          <div id="latency-heatmap" class="chart chart-tall"></div>
        </div>
        <div class="chart-container">
          <h3 style="margin-bottom: 15px; color: #8b949e;">Recall (%)</h3>
          <div id="recall-heatmap" class="chart chart-tall"></div>
        </div>
      </div>
    </div>

    <div id="comparison" class="tab-content">
      <h2>Configuration Comparison</h2>
      <div class="chart-container">
        <h3 style="margin-bottom: 15px; color: #8b949e;">Latency Distribution by Config (Best Variant Each)</h3>
        <div id="config-comparison-chart" class="chart"></div>
      </div>
      <div class="chart-container">
        <h3 style="margin-bottom: 15px; color: #8b949e;">P95 Latency Comparison</h3>
        <div id="p95-chart" class="chart"></div>
      </div>
    </div>

    <div id="resources" class="tab-content">
      <h2>Memory & Resource Usage</h2>
      ${generateResourcesSection(collectionStats)}
    </div>

    <div id="table" class="tab-content">
      <h2>Full Results Table</h2>
      <div class="chart-container" style="max-height: 600px; overflow-y: auto;">
        <table>
          <thead>
            <tr>
              <th>Config</th>
              <th>Variant</th>
              <th>Recall</th>
              <th>Avg Latency</th>
              <th>P50</th>
              <th>P95</th>
              <th>P99</th>
            </tr>
          </thead>
          <tbody>
            ${recallResults
              .sort((a, b) => b.recall - a.recall || a.avgLatencyMs - b.avgLatencyMs)
              .map(r => `
                <tr>
                  <td>${r.configName}</td>
                  <td>${r.searchVariant}</td>
                  <td class="${r.recall >= 0.99 ? 'good' : r.recall >= 0.95 ? 'warn' : 'bad'}">${(r.recall * 100).toFixed(2)}%</td>
                  <td class="${r.avgLatencyMs < 50 ? 'good' : r.avgLatencyMs < 200 ? 'warn' : 'bad'}">${r.avgLatencyMs.toFixed(1)}ms</td>
                  <td>${r.p50LatencyMs.toFixed(1)}ms</td>
                  <td>${r.p95LatencyMs.toFixed(1)}ms</td>
                  <td>${r.p99LatencyMs.toFixed(1)}ms</td>
                </tr>
              `).join('')}
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    const darkTheme = {
      paper_bgcolor: '#161b22',
      plot_bgcolor: '#161b22',
      font: { color: '#c9d1d9' },
      xaxis: {
        gridcolor: '#30363d',
        linecolor: '#30363d',
        zerolinecolor: '#30363d'
      },
      yaxis: {
        gridcolor: '#30363d',
        linecolor: '#30363d',
        zerolinecolor: '#30363d'
      },
      margin: { t: 40, r: 20, b: 60, l: 60 }
    };

    const configs = ${JSON.stringify(configs)};
    const variants = ${JSON.stringify(variants)};
    const configColors = ${JSON.stringify(configColors)};
    const scatterData = ${JSON.stringify(scatterData)};
    const paretoOptimal = ${JSON.stringify(paretoOptimal)};
    const latencyMatrix = ${JSON.stringify(latencyMatrix)};
    const recallMatrix = ${JSON.stringify(recallMatrix)};
    const bestPerConfig = ${JSON.stringify(bestPerConfig)};

    function showTab(tabId) {
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
      document.querySelector(\`.tab[onclick="showTab('\${tabId}')"]\`).classList.add('active');
      document.getElementById(tabId).classList.add('active');
    }

    // Pareto frontier chart
    function renderParetoChart() {
      const traces = [];

      // All points (faded)
      traces.push({
        x: scatterData.map(d => d.x),
        y: scatterData.map(d => d.y),
        mode: 'markers',
        type: 'scatter',
        marker: {
          size: 8,
          color: scatterData.map(d => configColors[d.config]),
          opacity: 0.3
        },
        text: scatterData.map(d => d.label),
        hovertemplate: '%{text}<br>Latency: %{x:.1f}ms<br>Recall: %{y:.2f}%<extra></extra>',
        name: 'All configs',
        showlegend: false
      });

      // Pareto optimal points
      traces.push({
        x: paretoOptimal.map(d => d.x),
        y: paretoOptimal.map(d => d.y),
        mode: 'markers+lines',
        type: 'scatter',
        marker: { size: 14, color: '#58a6ff', symbol: 'star' },
        line: { color: '#58a6ff', dash: 'dot', width: 2 },
        text: paretoOptimal.map(d => d.label),
        hovertemplate: '<b>%{text}</b><br>Latency: %{x:.1f}ms<br>Recall: %{y:.2f}%<extra></extra>',
        name: 'Pareto Optimal'
      });

      Plotly.newPlot('pareto-chart', traces, {
        ...darkTheme,
        xaxis: { ...darkTheme.xaxis, title: 'Average Latency (ms)', type: 'log' },
        yaxis: { ...darkTheme.yaxis, title: 'Recall (%)', range: [90, 101] },
        showlegend: true,
        legend: { x: 1, xanchor: 'right', y: 1, bgcolor: 'rgba(0,0,0,0)' }
      }, { responsive: true });
    }

    // Best config per threshold
    function renderBestConfigChart() {
      const thresholds = [99, 98, 97, 95, 90];
      const bestConfigs = thresholds.map(thresh => {
        const candidates = scatterData.filter(d => d.y >= thresh);
        if (candidates.length === 0) return null;
        return candidates.sort((a, b) => a.x - b.x)[0];
      }).filter(Boolean);

      Plotly.newPlot('best-config-chart', [{
        x: bestConfigs.map(d => d.label),
        y: bestConfigs.map(d => d.x),
        type: 'bar',
        marker: { color: bestConfigs.map(d => configColors[d.config]) },
        text: bestConfigs.map(d => \`\${d.y.toFixed(1)}% recall\`),
        textposition: 'outside',
        hovertemplate: '%{x}<br>Latency: %{y:.1f}ms<extra></extra>'
      }], {
        ...darkTheme,
        xaxis: { ...darkTheme.xaxis, title: 'Configuration', tickangle: -45 },
        yaxis: { ...darkTheme.yaxis, title: 'Average Latency (ms)' },
        margin: { ...darkTheme.margin, b: 120 }
      }, { responsive: true });
    }

    // Full scatter chart
    function renderScatterChart() {
      const traces = configs.map(config => {
        const points = scatterData.filter(d => d.config === config);
        return {
          x: points.map(d => d.x),
          y: points.map(d => d.y),
          mode: 'markers',
          type: 'scatter',
          name: config,
          marker: { size: 10, color: configColors[config] },
          text: points.map(d => d.variant),
          hovertemplate: '<b>' + config + '</b><br>Variant: %{text}<br>Latency: %{x:.1f}ms<br>Recall: %{y:.2f}%<extra></extra>'
        };
      });

      Plotly.newPlot('scatter-chart', traces, {
        ...darkTheme,
        xaxis: { ...darkTheme.xaxis, title: 'Average Latency (ms)', type: 'log' },
        yaxis: { ...darkTheme.yaxis, title: 'Recall (%)' },
        showlegend: true,
        legend: { bgcolor: 'rgba(0,0,0,0)' }
      }, { responsive: true });
    }

    // Heatmaps
    function renderHeatmaps() {
      const heatmapLayout = {
        ...darkTheme,
        xaxis: { ...darkTheme.xaxis, title: 'Search Variant', tickangle: -45, dtick: 1 },
        yaxis: { ...darkTheme.yaxis, title: 'Configuration', dtick: 1 },
        margin: { ...darkTheme.margin, b: 150, l: 150 }
      };

      Plotly.newPlot('latency-heatmap', [{
        z: latencyMatrix,
        x: variants,
        y: configs,
        type: 'heatmap',
        colorscale: [[0, '#238636'], [0.5, '#d29922'], [1, '#f85149']],
        hovertemplate: 'Config: %{y}<br>Variant: %{x}<br>Latency: %{z:.1f}ms<extra></extra>'
      }], heatmapLayout, { responsive: true });

      Plotly.newPlot('recall-heatmap', [{
        z: recallMatrix,
        x: variants,
        y: configs,
        type: 'heatmap',
        colorscale: [[0, '#f85149'], [0.5, '#d29922'], [1, '#238636']],
        zmin: 90,
        zmax: 100,
        hovertemplate: 'Config: %{y}<br>Variant: %{x}<br>Recall: %{z:.2f}%<extra></extra>'
      }], heatmapLayout, { responsive: true });
    }

    // Config comparison
    function renderComparisonCharts() {
      const sorted = [...bestPerConfig].sort((a, b) => a.avgLatencyMs - b.avgLatencyMs);

      Plotly.newPlot('config-comparison-chart', [{
        x: sorted.map(d => d.configName),
        y: sorted.map(d => d.avgLatencyMs),
        type: 'bar',
        marker: { color: sorted.map(d => configColors[d.configName]) },
        text: sorted.map(d => \`\${(d.recall * 100).toFixed(1)}%\`),
        textposition: 'outside',
        hovertemplate: '%{x}<br>Avg: %{y:.1f}ms<br>Recall: %{text}<extra></extra>'
      }], {
        ...darkTheme,
        xaxis: { ...darkTheme.xaxis, tickangle: -45 },
        yaxis: { ...darkTheme.yaxis, title: 'Average Latency (ms)' },
        margin: { ...darkTheme.margin, b: 120 }
      }, { responsive: true });

      Plotly.newPlot('p95-chart', [{
        x: sorted.map(d => d.configName),
        y: sorted.map(d => d.p95LatencyMs),
        type: 'bar',
        marker: { color: sorted.map(d => configColors[d.configName]) }
      }], {
        ...darkTheme,
        xaxis: { ...darkTheme.xaxis, tickangle: -45 },
        yaxis: { ...darkTheme.yaxis, title: 'P95 Latency (ms)' },
        margin: { ...darkTheme.margin, b: 120 }
      }, { responsive: true });
    }

    // Render all charts
    renderParetoChart();
    renderBestConfigChart();
    renderScatterChart();
    renderHeatmaps();
    renderComparisonCharts();
  </script>
</body>
</html>`;
}

function findParetoOptimal(points: Array<{ x: number; y: number; label: string; config: string }>): typeof points {
  // Sort by recall descending
  const sorted = [...points].sort((a, b) => b.y - a.y);
  const pareto: typeof points = [];
  let minLatency = Infinity;

  for (const point of sorted) {
    if (point.x < minLatency) {
      pareto.push(point);
      minLatency = point.x;
    }
  }

  return pareto.sort((a, b) => a.x - b.x);
}

function generateRecommendations(results: RecallResult[]): string {
  const recommendations: string[] = [];

  // Find best for different use cases
  const highRecall = results.filter(r => r.recall >= 0.99).sort((a, b) => a.avgLatencyMs - b.avgLatencyMs)[0];
  const balanced = results.filter(r => r.recall >= 0.95).sort((a, b) => a.avgLatencyMs - b.avgLatencyMs)[0];
  const fastest = [...results].sort((a, b) => a.avgLatencyMs - b.avgLatencyMs)[0];
  const lowestP95 = [...results].sort((a, b) => a.p95LatencyMs - b.p95LatencyMs)[0];

  if (highRecall) {
    recommendations.push(`
      <li>
        <span class="badge badge-best">Best Accuracy</span>
        <strong>${highRecall.configName}</strong> with <strong>${highRecall.searchVariant}</strong>
        — ${(highRecall.recall * 100).toFixed(2)}% recall @ ${highRecall.avgLatencyMs.toFixed(1)}ms avg
      </li>
    `);
  }

  if (balanced && balanced !== highRecall) {
    recommendations.push(`
      <li>
        <span class="badge badge-good">Balanced</span>
        <strong>${balanced.configName}</strong> with <strong>${balanced.searchVariant}</strong>
        — ${(balanced.recall * 100).toFixed(2)}% recall @ ${balanced.avgLatencyMs.toFixed(1)}ms avg
      </li>
    `);
  }

  if (fastest && fastest !== balanced && fastest !== highRecall) {
    recommendations.push(`
      <li>
        <span class="badge badge-warn">Fastest</span>
        <strong>${fastest.configName}</strong> with <strong>${fastest.searchVariant}</strong>
        — ${(fastest.recall * 100).toFixed(2)}% recall @ ${fastest.avgLatencyMs.toFixed(1)}ms avg
      </li>
    `);
  }

  if (lowestP95) {
    recommendations.push(`
      <li>
        <span class="badge badge-good">Lowest P95</span>
        <strong>${lowestP95.configName}</strong> with <strong>${lowestP95.searchVariant}</strong>
        — P95: ${lowestP95.p95LatencyMs.toFixed(1)}ms
      </li>
    `);
  }

  return `
    <div class="recommendations">
      <h3>Recommendations</h3>
      <ul>${recommendations.join('')}</ul>
    </div>
  `;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function generateResourcesSection(stats: CollectionStats[]): string {
  // Check if we have actual memory data
  const hasActualMemory = stats.some(s => s.actualMemoryBytes !== undefined);
  const hasDiskData = stats.some(s => s.diskDataBytes !== undefined);

  // Prepare data for charts
  const configNames = stats.map(s => s.name.replace('benchmark_', ''));

  // Memory breakdown data
  const memoryData = stats.map(s => ({
    config: s.name.replace('benchmark_', ''),
    vectors: s.vectorsMemoryBytes || 0,
    payload: s.payloadMemoryBytes || 0,
    index: s.indexMemoryBytes || 0,
    total: s.actualMemoryBytes || 0,
    disk: s.diskDataBytes || 0,
    estimate: s.memoryUsageEstimate,
    quantization: s.quantizationType || 'none',
  }));

  return `
    <div class="grid">
      <div class="chart-container">
        <h3 style="margin-bottom: 15px; color: #8b949e;">Memory Usage by Configuration</h3>
        <div id="memory-chart" class="chart"></div>
      </div>
      <div class="chart-container">
        <h3 style="margin-bottom: 15px; color: #8b949e;">Memory Breakdown</h3>
        <div id="memory-breakdown-chart" class="chart"></div>
      </div>
    </div>

    <div class="chart-container">
      <h3 style="margin-bottom: 15px; color: #8b949e;">Resource Details by Configuration</h3>
      <table>
        <thead>
          <tr>
            <th>Config</th>
            <th>Quantization</th>
            <th>Points</th>
            <th>Segments</th>
            <th>Vectors Memory</th>
            <th>Index Memory</th>
            <th>Payload Memory</th>
            <th>Total RAM</th>
            <th>Disk Usage</th>
          </tr>
        </thead>
        <tbody>
          ${stats.map(s => `
            <tr>
              <td>${s.name.replace('benchmark_', '')}</td>
              <td><span class="badge ${s.quantizationType === 'binary' ? 'badge-best' : s.quantizationType === 'scalar_int8' ? 'badge-good' : 'badge-warn'}">${s.quantizationType || 'none'}</span></td>
              <td>${s.pointsCount.toLocaleString()}</td>
              <td>${s.segmentsCount}</td>
              <td>${s.vectorsMemoryBytes ? formatBytes(s.vectorsMemoryBytes) : '-'}</td>
              <td>${s.indexMemoryBytes ? formatBytes(s.indexMemoryBytes) : '-'}</td>
              <td>${s.payloadMemoryBytes ? formatBytes(s.payloadMemoryBytes) : '-'}</td>
              <td class="${s.actualMemoryBytes && s.actualMemoryBytes < 1024*1024*1024 ? 'good' : 'warn'}">${s.actualMemoryBytes ? formatBytes(s.actualMemoryBytes) : s.memoryUsageEstimate}</td>
              <td>${s.diskDataBytes ? formatBytes(s.diskDataBytes) : '-'}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>

    ${!hasActualMemory ? `
    <div class="recommendations" style="border-left-color: #d29922;">
      <h3 style="color: #d29922;">Note: Estimated Memory Values</h3>
      <p>Actual memory metrics were not available from Qdrant telemetry. The values shown are estimates based on vector dimensions and quantization type. To get actual metrics, ensure Qdrant telemetry is enabled.</p>
    </div>
    ` : ''}

    <script>
      // Memory comparison chart
      const memoryData = ${JSON.stringify(memoryData)};

      function renderMemoryCharts() {
        // Total memory bar chart
        const totalMemory = memoryData.map(d => d.total || 0);
        const hasData = totalMemory.some(v => v > 0);

        if (hasData) {
          Plotly.newPlot('memory-chart', [{
            x: memoryData.map(d => d.config),
            y: memoryData.map(d => d.total / (1024 * 1024)), // Convert to MB
            type: 'bar',
            marker: {
              color: memoryData.map(d =>
                d.quantization === 'binary' ? '#238636' :
                d.quantization === 'scalar_int8' ? '#1f6feb' : '#d29922'
              )
            },
            text: memoryData.map(d => d.quantization),
            hovertemplate: '%{x}<br>Memory: %{y:.1f} MB<br>Quantization: %{text}<extra></extra>'
          }], {
            ...darkTheme,
            xaxis: { ...darkTheme.xaxis, tickangle: -45 },
            yaxis: { ...darkTheme.yaxis, title: 'Total Memory (MB)' },
            margin: { ...darkTheme.margin, b: 120 }
          }, { responsive: true });

          // Stacked breakdown
          Plotly.newPlot('memory-breakdown-chart', [
            {
              x: memoryData.map(d => d.config),
              y: memoryData.map(d => d.vectors / (1024 * 1024)),
              name: 'Vectors',
              type: 'bar',
              marker: { color: '#1f77b4' }
            },
            {
              x: memoryData.map(d => d.config),
              y: memoryData.map(d => d.index / (1024 * 1024)),
              name: 'Index (HNSW)',
              type: 'bar',
              marker: { color: '#ff7f0e' }
            },
            {
              x: memoryData.map(d => d.config),
              y: memoryData.map(d => d.payload / (1024 * 1024)),
              name: 'Payload',
              type: 'bar',
              marker: { color: '#2ca02c' }
            }
          ], {
            ...darkTheme,
            barmode: 'stack',
            xaxis: { ...darkTheme.xaxis, tickangle: -45 },
            yaxis: { ...darkTheme.yaxis, title: 'Memory (MB)' },
            margin: { ...darkTheme.margin, b: 120 },
            legend: { bgcolor: 'rgba(0,0,0,0)' }
          }, { responsive: true });
        } else {
          // Show placeholder with estimates
          document.getElementById('memory-chart').innerHTML = '<p style="color: #8b949e; text-align: center; padding: 50px;">Actual memory data not available. See table below for estimates.</p>';
          document.getElementById('memory-breakdown-chart').innerHTML = '<p style="color: #8b949e; text-align: center; padding: 50px;">Memory breakdown requires Qdrant telemetry.</p>';
        }
      }

      // Render on tab switch
      const originalShowTab = showTab;
      showTab = function(tabId) {
        originalShowTab(tabId);
        if (tabId === 'resources') {
          renderMemoryCharts();
        }
      };
    </script>
  `;
}

async function findLatestBenchmark(dir: string): Promise<string | null> {
  try {
    const files = await fs.readdir(dir);
    const jsonFiles = files
      .filter(f => f.startsWith('benchmark-raw-') && f.endsWith('.json'))
      .sort()
      .reverse();

    if (jsonFiles.length > 0) {
      return path.join(dir, jsonFiles[0]!);
    }
  } catch {
    // Directory doesn't exist
  }
  return null;
}

async function main() {
  const args = process.argv.slice(2);

  if (args.includes('--help') || args.includes('-h')) {
    console.log(`
Usage: bun run scripts/visualize-benchmark.ts [options] [json-file]

Options:
  --latest          Use the latest benchmark file from benchmarks/qdrant-configs/
  --output=<file>   Output HTML file (default: benchmark-dashboard.html)
  --help, -h        Show this help message

Examples:
  bun run scripts/visualize-benchmark.ts benchmarks/qdrant-configs/benchmark-raw-2024-01-15.json
  bun run scripts/visualize-benchmark.ts --latest
  bun run scripts/visualize-benchmark.ts --latest --output=results.html
`);
    process.exit(0);
  }

  let inputFile: string | null = null;
  let outputFile = 'benchmark-dashboard.html';

  for (const arg of args) {
    if (arg === '--latest') {
      inputFile = await findLatestBenchmark('benchmarks/qdrant-configs');
      if (!inputFile) {
        console.error(chalk.red('No benchmark files found in benchmarks/qdrant-configs/'));
        process.exit(1);
      }
    } else if (arg.startsWith('--output=')) {
      outputFile = arg.split('=')[1]!;
    } else if (!arg.startsWith('-')) {
      inputFile = arg;
    }
  }

  if (!inputFile) {
    console.error(chalk.red('Please specify a benchmark JSON file or use --latest'));
    console.log(chalk.gray('Run with --help for usage information'));
    process.exit(1);
  }

  console.log(chalk.blue(`Reading benchmark data from: ${inputFile}`));

  try {
    const content = await fs.readFile(inputFile, 'utf-8');
    const data: BenchmarkData = JSON.parse(content);

    // Check for extended data
    const extendedFile = inputFile.replace('benchmark-raw-', 'benchmark-extended-');
    let extendedData: ExtendedData | undefined;
    try {
      const extContent = await fs.readFile(extendedFile, 'utf-8');
      extendedData = JSON.parse(extContent);
      console.log(chalk.gray(`Found extended benchmark data`));
    } catch {
      // No extended data
    }

    console.log(chalk.blue(`Generating dashboard...`));
    console.log(chalk.gray(`  Configs: ${[...new Set(data.recallResults.map(r => r.configName))].length}`));
    console.log(chalk.gray(`  Variants: ${[...new Set(data.recallResults.map(r => r.searchVariant))].length}`));
    console.log(chalk.gray(`  Total results: ${data.recallResults.length}`));

    const html = generateHTML(data, extendedData);

    await fs.writeFile(outputFile, html);
    console.log(chalk.green(`\n✓ Dashboard saved to: ${outputFile}`));
    console.log(chalk.gray(`Open in browser to view interactive charts`));

  } catch (error) {
    console.error(chalk.red(`Error: ${error}`));
    process.exit(1);
  }
}

main();
