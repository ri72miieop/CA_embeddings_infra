#!/usr/bin/env bun
/**
 * Test Qdrant connectivity and diagnose connection issues
 */

import chalk from 'chalk';

const QDRANT_URL = process.env.QDRANT_URL || 'http://localhost:6333';
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;

console.log(chalk.bold.cyan('\n🔍 QDRANT CONNECTION DIAGNOSTICS\n'));
console.log(chalk.bold('Configuration:'));
console.log(`   QDRANT_URL: ${chalk.cyan(QDRANT_URL)}`);
console.log(`   QDRANT_API_KEY: ${QDRANT_API_KEY ? chalk.green('Set (' + QDRANT_API_KEY.substring(0, 8) + '...)') : chalk.yellow('Not set')}`);
console.log();

// Parse URL
let urlToTest = QDRANT_URL;
let alternativeUrls: string[] = [];

try {
  const url = new URL(QDRANT_URL);
  console.log(chalk.bold('Parsed URL:'));
  console.log(`   Protocol: ${url.protocol}`);
  console.log(`   Hostname: ${url.hostname}`);
  console.log(`   Port: ${url.port || '(default)'}`);
  console.log();

  // Generate alternative URLs to test
  if (url.port === '6333') {
    // If using port 6333, try without port (Coolify might proxy)
    alternativeUrls.push(`${url.protocol}//${url.hostname}`);
    
    // Try HTTPS version
    if (url.protocol === 'http:') {
      alternativeUrls.push(`https://${url.hostname}`);
    }
  }
  
  // If using HTTP, suggest HTTPS
  if (url.protocol === 'http:' && url.hostname.includes('sslip.io')) {
    alternativeUrls.push(`https://${url.hostname}${url.port ? ':' + url.port : ''}`);
  }
} catch (e) {
  console.log(chalk.red('⚠️  Invalid URL format'));
}

console.log(chalk.bold.cyan('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n'));

/**
 * Test connection to a URL
 */
async function testConnection(url: string, label: string): Promise<boolean> {
  console.log(chalk.bold(`Testing: ${label}`));
  console.log(chalk.dim(`   URL: ${url}`));
  
  try {
    // Test 1: Basic fetch to root
    console.log(chalk.dim('   → GET /'));
    const rootResponse = await fetch(url, {
      method: 'GET',
      headers: QDRANT_API_KEY ? { 'api-key': QDRANT_API_KEY } : {},
      signal: AbortSignal.timeout(5000),
    });
    
    console.log(chalk.green(`   ✓ Root endpoint responded: ${rootResponse.status}`));
    const rootText = await rootResponse.text();
    if (rootText) {
      console.log(chalk.dim(`   Response preview: ${rootText.substring(0, 100)}...`));
    }
  } catch (error: any) {
    console.log(chalk.red(`   ✗ Root endpoint failed: ${error.code || error.message}`));
  }

  try {
    // Test 2: Qdrant collections endpoint
    console.log(chalk.dim('   → GET /collections'));
    const collectionsUrl = url.endsWith('/') ? `${url}collections` : `${url}/collections`;
    const response = await fetch(collectionsUrl, {
      method: 'GET',
      headers: QDRANT_API_KEY ? { 'api-key': QDRANT_API_KEY } : {},
      signal: AbortSignal.timeout(5000),
    });
    
    console.log(chalk.green(`   ✓ Collections endpoint responded: ${response.status}`));
    
    if (response.ok) {
      const data = await response.json();
      console.log(chalk.green(`   ✓ Valid Qdrant response!`));
      console.log(chalk.dim(`   Collections: ${JSON.stringify(data, null, 2)}`));
      return true;
    } else {
      const text = await response.text();
      console.log(chalk.yellow(`   ⚠️  Non-OK response: ${text.substring(0, 200)}`));
    }
  } catch (error: any) {
    console.log(chalk.red(`   ✗ Collections endpoint failed: ${error.code || error.message}`));
    if (error.cause) {
      console.log(chalk.red(`   Cause: ${error.cause}`));
    }
  }
  
  console.log();
  return false;
}

// Test connections
async function runTests() {
  let success = false;
  
  // Test primary URL
  success = await testConnection(urlToTest, 'Primary URL');
  
  // Test alternatives if primary failed
  if (!success && alternativeUrls.length > 0) {
    console.log(chalk.bold.yellow('Primary URL failed. Testing alternatives...\n'));
    
    for (let i = 0; i < alternativeUrls.length; i++) {
      const altUrl = alternativeUrls[i]!;
      const result = await testConnection(altUrl, `Alternative ${i + 1}`);
      if (result) {
        success = true;
        console.log(chalk.bold.green(`\n✓ SUCCESS! Use this URL:\n   ${altUrl}\n`));
        console.log(chalk.bold.cyan('Update your environment variable:'));
        console.log(chalk.white(`   export QDRANT_URL="${altUrl}"\n`));
        break;
      }
    }
  }
  
  if (success) {
    console.log(chalk.bold.green('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'));
    console.log(chalk.bold.green('✓ Connection successful!'));
    console.log(chalk.bold.green('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n'));
  } else {
    console.log(chalk.bold.red('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'));
    console.log(chalk.bold.red('✗ All connection attempts failed'));
    console.log(chalk.bold.red('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n'));
    
    console.log(chalk.bold.yellow('Troubleshooting suggestions:'));
    console.log(chalk.yellow('1. Check Coolify dashboard - is Qdrant service running?'));
    console.log(chalk.yellow('2. Verify the domain/URL in Coolify settings'));
    console.log(chalk.yellow('3. Check if Qdrant needs port 6333 exposed in Coolify'));
    console.log(chalk.yellow('4. Try accessing the Qdrant dashboard in your browser'));
    console.log(chalk.yellow('5. Check Coolify logs for Qdrant service'));
    console.log(chalk.yellow('6. Ensure API key is set correctly if authentication is enabled'));
    console.log();
  }
}

runTests().catch(error => {
  console.error(chalk.red('\n❌ Test script error:'), error);
  process.exit(1);
});

