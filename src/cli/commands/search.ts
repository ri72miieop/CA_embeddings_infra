import chalk from 'chalk';

interface SearchOptions {
  url: string;
  apiKey?: string;
  k: number;
  threshold?: string;
  filter?: string;
  pretty: boolean;
}

export async function searchCommand(searchTerm: string, options: SearchOptions) {
  const { url, apiKey, k, threshold, filter, pretty } = options;

  try {
    console.log(chalk.blue('🔍 Performing semantic search...'));
    console.log(chalk.gray(`Search term: "${searchTerm}"`));
    console.log(chalk.gray(`Server URL: ${url}`));
    console.log(chalk.gray(`Results (k): ${k}`));
    
    if (threshold) {
      console.log(chalk.gray(`Threshold: ${threshold}`));
    }
    
    if (filter) {
      console.log(chalk.gray(`Filter: ${filter}`));
    }
    
    console.log('');

    // Prepare request body
    const requestBody: any = {
      searchTerm,
      k,
    };

    if (threshold) {
      requestBody.threshold = parseFloat(threshold);
    }

    if (filter) {
      try {
        requestBody.filter = JSON.parse(filter);
      } catch (error) {
        console.error(chalk.red('❌ Invalid JSON in filter parameter'));
        process.exit(1);
      }
    }

    // Prepare headers
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };

    if (apiKey) {
      headers['Authorization'] = `Bearer ${apiKey}`;
    }

    // Make API request
    const response = await fetch(`${url}/embeddings/search`, {
      method: 'POST',
      headers,
      body: JSON.stringify(requestBody),
    });

    if (!response.ok) {
      const errorData = await response.json();
      console.error(chalk.red('❌ Search failed:'));
      console.error(chalk.red(JSON.stringify(errorData, null, 2)));
      process.exit(1);
    }

    const data = await response.json();

    if (!data.success) {
      console.error(chalk.red('❌ Search failed:'));
      console.error(chalk.red(data.error || 'Unknown error'));
      process.exit(1);
    }

    console.log(chalk.green(`✅ Search completed successfully`));
    console.log(chalk.green(`Found ${data.count} result(s)\n`));

    // Filter out vector field from results
    const resultsWithoutVectors = data.results.map((result: any) => {
      const { vector, ...rest } = result;
      return rest;
    });

    // Display results
    if (pretty) {
      // Pretty print with colors and formatting
      resultsWithoutVectors.forEach((result: any, index: number) => {
        console.log(chalk.cyan(`Result ${index + 1}:`));
        console.log(chalk.yellow(`  Key: ${result.key}`));
        console.log(chalk.yellow(`  Distance: ${result.distance.toFixed(4)}`));
        
        if (result.metadata && Object.keys(result.metadata).length > 0) {
          console.log(chalk.yellow(`  Metadata:`));
          for (const [key, value] of Object.entries(result.metadata)) {
            console.log(chalk.gray(`    ${key}: ${JSON.stringify(value)}`));
          }
        }
        console.log('');
      });
    } else {
      // Raw JSON output
      console.log(JSON.stringify({
        success: true,
        results: resultsWithoutVectors,
        count: data.count,
      }, null, 2));
    }

  } catch (error) {
    if (error instanceof Error) {
      console.error(chalk.red('❌ Error:'), error.message);
      
      if (error.message.includes('fetch')) {
        console.error(chalk.yellow('\n💡 Tip: Make sure the server is running at'), chalk.cyan(url));
        console.error(chalk.yellow('You can start it with:'), chalk.cyan('bun start'));
      }
    } else {
      console.error(chalk.red('❌ Unknown error occurred'));
    }
    process.exit(1);
  }
}

