#!/usr/bin/env bun

/**
 * Standalone semantic search script
 * 
 * Usage:
 *   bun run scripts/semantic-search.ts "your search query"
 *   bun run scripts/semantic-search.ts "your search query" --k=5 --threshold=0.8
 */

import chalk from 'chalk';

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

async function semanticSearch(searchTerm: string, options: {
  serverUrl?: string;
  k?: number;
  threshold?: number;
  filter?: Record<string, any>;
}): Promise<void> {
  const {
    serverUrl = process.env.SERVER_URL || 'http://localhost:3000',
    k = 10,
    threshold,
    filter,
  } = options;

  try {
    console.log(chalk.blue('🔍 Semantic Search'));
    console.log(chalk.gray('─'.repeat(50)));
    console.log(chalk.cyan('Query:'), searchTerm);
    console.log(chalk.cyan('Server:'), serverUrl);
    console.log(chalk.cyan('Results (k):'), k);
    if (threshold !== undefined) {
      console.log(chalk.cyan('Threshold:'), threshold);
    }
    if (filter) {
      console.log(chalk.cyan('Filter:'), JSON.stringify(filter));
    }
    console.log(chalk.gray('─'.repeat(50)));
    console.log('');

    const requestBody: any = {
      searchTerm,
      k,
    };

    if (threshold !== undefined) {
      requestBody.threshold = threshold;
    }

    if (filter) {
      requestBody.filter = filter;
    }

    const startTime = Date.now();
    const response = await fetch(`${serverUrl}/embeddings/search`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(requestBody),
    });

    const duration = Date.now() - startTime;

    if (!response.ok) {
      const errorData = await response.json();
      console.error(chalk.red('❌ Search failed:'));
      console.error(chalk.red(JSON.stringify(errorData, null, 2)));
      process.exit(1);
    }

    const data = await response.json() as SearchResponse;

    if (!data.success) {
      console.error(chalk.red('❌ Search failed:'));
      console.error(chalk.red(data.error || 'Unknown error'));
      process.exit(1);
    }

    console.log(chalk.green(`✅ Search completed in ${duration}ms`));
    console.log(chalk.green(`Found ${data.count} result(s)`));
    console.log('');

    if (data.results.length === 0) {
      console.log(chalk.yellow('No results found'));
      return;
    }

    // Display results without vectors
    data.results.forEach((result, index) => {
      console.log(chalk.cyan(`─── Result ${index + 1} ───`));
      console.log(chalk.white('Key:'), chalk.yellow(result.key));
      console.log(chalk.white('Distance:'), chalk.yellow(result.distance.toFixed(4)));
      console.log(chalk.white('Similarity:'), chalk.yellow(`${(result.distance * 100).toFixed(2)}%`));
      
      if (result.metadata && Object.keys(result.metadata).length > 0) {
        console.log(chalk.white('Metadata:'));
        for (const [key, value] of Object.entries(result.metadata)) {
          console.log(chalk.gray(`  ${key}:`), formatValue(value));
        }
      }
      console.log('');
    });

    // Summary
    console.log(chalk.gray('─'.repeat(50)));
    console.log(chalk.blue('Summary:'));
    console.log(chalk.gray(`  Total Results: ${data.count}`));
    if (data.results.length > 0) {
      const avgDistance = data.results.reduce((sum, r) => sum + r.distance, 0) / data.results.length;
      console.log(chalk.gray(`  Average Similarity: ${(avgDistance * 100).toFixed(2)}%`));
      const bestMatch = data.results[0];
      if (bestMatch) {
        console.log(chalk.gray(`  Best Match: ${bestMatch.key} (${(bestMatch.distance * 100).toFixed(2)}%)`));
      }
    }
    console.log(chalk.gray('─'.repeat(50)));

  } catch (error) {
    if (error instanceof Error) {
      console.error(chalk.red('❌ Error:'), error.message);
      
      if (error.message.includes('fetch') || error.message.includes('ECONNREFUSED')) {
        console.error('');
        console.error(chalk.yellow('💡 Troubleshooting:'));
        console.error(chalk.gray('  1. Make sure the server is running:'), chalk.cyan('bun start'));
        console.error(chalk.gray('  2. Verify the server URL:'), chalk.cyan(options.serverUrl || 'http://localhost:3000'));
        console.error(chalk.gray('  3. Check if embedding generation is enabled in .env'));
      }
    } else {
      console.error(chalk.red('❌ Unknown error occurred'));
    }
    process.exit(1);
  }
}

function formatValue(value: any): string {
  if (typeof value === 'string') {
    return chalk.yellow(value);
  } else if (typeof value === 'number') {
    return chalk.yellow(value.toString());
  } else if (typeof value === 'boolean') {
    return chalk.yellow(value.toString());
  } else {
    return chalk.yellow(JSON.stringify(value));
  }
}

// Parse command line arguments
function parseArgs(): { searchTerm: string; options: any } {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    console.error(chalk.red('❌ Error: Search term is required'));
    console.log('');
    console.log(chalk.yellow('Usage:'));
    console.log(chalk.cyan('  bun run scripts/semantic-search.ts "your search query"'));
    console.log('');
    console.log(chalk.yellow('Options:'));
    console.log(chalk.gray('  --url=<url>         Server URL (default: http://localhost:3000)'));
    console.log(chalk.gray('  --k=<number>        Number of results (default: 10)'));
    console.log(chalk.gray('  --threshold=<num>   Minimum similarity threshold (0.0-1.0)'));
    console.log(chalk.gray('  --filter=<json>     Metadata filter as JSON string'));
    console.log('');
    console.log(chalk.yellow('Examples:'));
    console.log(chalk.cyan('  bun run scripts/semantic-search.ts "machine learning"'));
    console.log(chalk.cyan('  bun run scripts/semantic-search.ts "AI" --k=5 --threshold=0.8'));
    console.log(chalk.cyan('  bun run scripts/semantic-search.ts "tech" --filter=\'{"category":"technology"}\''));
    process.exit(1);
  }

  const searchTerm = args[0];
  if (!searchTerm) {
    console.error(chalk.red('❌ Error: Search term is required'));
    process.exit(1);
  }
  
  const options: any = {};

  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    if (arg && arg.startsWith('--')) {
      const parts = arg.slice(2).split('=');
      const key = parts[0];
      const value = parts[1];
      
      if (!value) {
        console.warn(chalk.yellow(`⚠️  Missing value for option: --${key}`));
        continue;
      }
      
      switch (key) {
        case 'url':
          options.serverUrl = value;
          break;
        case 'k':
          options.k = parseInt(value, 10);
          break;
        case 'threshold':
          options.threshold = parseFloat(value);
          break;
        case 'filter':
          try {
            options.filter = JSON.parse(value);
          } catch (error) {
            console.error(chalk.red('❌ Invalid JSON in --filter parameter'));
            process.exit(1);
          }
          break;
        default:
          console.warn(chalk.yellow(`⚠️  Unknown option: --${key}`));
      }
    }
  }

  return { searchTerm, options };
}

// Main execution
const { searchTerm, options } = parseArgs();
await semanticSearch(searchTerm, options);

