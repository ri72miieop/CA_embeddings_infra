#!/usr/bin/env bun

/**
 * Example: Semantic Search Usage
 * 
 * This example demonstrates how to use the semantic search API
 * to query your vector database using natural language.
 */

const SERVER_URL = process.env.SERVER_URL || 'http://localhost:3000';

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

/**
 * Perform a semantic search query
 */
async function semanticSearch(
  searchTerm: string,
  options: {
    k?: number;
    threshold?: number;
    filter?: Record<string, any>;
  } = {}
): Promise<SearchResponse> {
  const { k = 10, threshold, filter } = options;

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

  const response = await fetch(`${SERVER_URL}/embeddings/search`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(requestBody),
  });

  if (!response.ok) {
    throw new Error(`Search failed: ${response.statusText}`);
  }

  return await response.json() as SearchResponse;
}

/**
 * Example 1: Basic search
 */
async function example1() {
  console.log('\n=== Example 1: Basic Search ===');
  
  try {
    const results = await semanticSearch('machine learning', { k: 5 });
    
    console.log(`Found ${results.count} results:`);
    results.results.forEach((result, i) => {
      console.log(`  ${i + 1}. ${result.key} (similarity: ${(result.distance * 100).toFixed(2)}%)`);
    });
  } catch (error) {
    console.error('Error:', error);
  }
}

/**
 * Example 2: Search with threshold
 */
async function example2() {
  console.log('\n=== Example 2: High Precision Search ===');
  
  try {
    // Only return results with >80% similarity
    const results = await semanticSearch('artificial intelligence', {
      k: 10,
      threshold: 0.8,
    });
    
    console.log(`Found ${results.count} highly relevant results (>80% similar):`);
    results.results.forEach((result, i) => {
      console.log(`  ${i + 1}. ${result.key} (${(result.distance * 100).toFixed(2)}%)`);
      if (result.metadata) {
        console.log(`     Metadata:`, result.metadata);
      }
    });
  } catch (error) {
    console.error('Error:', error);
  }
}

/**
 * Example 3: Search with metadata filter (Qdrant only)
 */
async function example3() {
  console.log('\n=== Example 3: Filtered Search ===');
  
  try {
    // Search only within a specific category
    const results = await semanticSearch('me and who', {
      k: 5,
      filter: {
        has_quotes: 'true',
      },
    });
    
    console.log(`Found ${results.count} "me and who" quotestweets:`);
    results.results.forEach((result, i) => {
      console.log(`  ${i + 1}. ${result.key}`);
      if (result.metadata) {
        console.log(`     ${result.metadata.title || 'No title'}`);
      }
    });
  } catch (error) {
    console.error('Error:', error);
  }
}

/**
 * Example 4: Batch searching
 */
async function example4() {
  console.log('\n=== Example 4: Batch Searching ===');
  
  const queries = [
    'deep learning',
    'neural networks',
    'computer vision',
    'natural language processing',
  ];
  
  try {
    console.log(`Searching for ${queries.length} queries...`);
    
    // Search all queries in parallel
    const allResults = await Promise.all(
      queries.map(query => semanticSearch(query, { k: 3 }))
    );
    
    // Display results
    queries.forEach((query, i) => {
      const results = allResults[i];
      if (results) {
        console.log(`\n"${query}" -> ${results.count} results`);
        results.results.forEach((result, j) => {
          console.log(`  ${j + 1}. ${result.key} (${(result.distance * 100).toFixed(2)}%)`);
        });
      }
    });
  } catch (error) {
    console.error('Error:', error);
  }
}

/**
 * Example 5: Finding the best match
 */
async function example5() {
  console.log('\n=== Example 5: Finding Best Match ===');
  
  try {
    const results = await semanticSearch('introduction to python programming', { k: 1 });
    
    if (results.count > 0) {
      const bestMatch = results.results[0];
      if (bestMatch) {
        console.log('Best matching document:');
        console.log(`  Key: ${bestMatch.key}`);
        console.log(`  Similarity: ${(bestMatch.distance * 100).toFixed(2)}%`);
        if (bestMatch.metadata) {
          console.log(`  Metadata:`, JSON.stringify(bestMatch.metadata, null, 2));
        }
      }
    } else {
      console.log('No matches found');
    }
  } catch (error) {
    console.error('Error:', error);
  }
}

/**
 * Main function - Run all examples
 */
async function main() {
  console.log('===========================================');
  console.log('  Semantic Search Examples');
  console.log('===========================================');
  console.log(`Server: ${SERVER_URL}`);
  console.log('===========================================');

  // Check if server is running
  try {
    const healthResponse = await fetch(`${SERVER_URL}/health`);
    if (!healthResponse.ok) {
      throw new Error('Server is not healthy');
    }
    console.log('✅ Server is running and healthy\n');
  } catch (error) {
    console.error('❌ Error: Cannot connect to server at', SERVER_URL);
    console.error('   Make sure the server is running: bun start');
    console.error('   And embedding generation is enabled in .env\n');
    process.exit(1);
  }

  // Run examples
  await example1();
  await example2();
  await example3();
  await example4();
  await example5();

  console.log('\n===========================================');
  console.log('  Examples completed!');
  console.log('===========================================\n');
}

// Run if executed directly
if (import.meta.main) {
  await main();
}

// Export for use in other scripts
export { semanticSearch };

