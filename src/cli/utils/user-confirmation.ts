import chalk from 'chalk';

/**
 * Prompts user for confirmation before sending data to DeepInfra
 */
export async function confirmDeepInfraUsage(
  recordCount: number,
  estimatedCost?: number
): Promise<boolean> {
  console.log(chalk.cyan('\n🤖 DeepInfra API Usage Confirmation'));
  console.log(chalk.gray('─'.repeat(50)));

  console.log(`📊 Records to process: ${chalk.bold(recordCount.toLocaleString())}`);

  if (estimatedCost) {
    console.log(`💰 Estimated cost: ${chalk.bold(`$${estimatedCost.toFixed(4)}`)}`);
  } else {
    // Rough estimate: ~$0.0001 per 1K tokens, assume ~250 tokens per record
    const estimatedTokens = recordCount * 250;
    const roughCost = (estimatedTokens / 1000) * 0.0001;
    console.log(`💰 Estimated cost: ${chalk.gray(`~$${roughCost.toFixed(4)} (rough estimate)`)}`);
  }

  console.log(`🌐 Provider: ${chalk.bold('DeepInfra')}`);
  console.log(`🔗 Data will be sent to external API`);

  console.log(chalk.gray('─'.repeat(50)));
  console.log(chalk.yellow('⚠️  This will send your data to DeepInfra\'s servers for embedding generation.'));
  console.log(chalk.gray('   Make sure you\'re comfortable with their data handling policies.'));

  const answer = await promptUser('\n❓ Do you want to continue? (y/N): ');

  return answer.toLowerCase() === 'y' || answer.toLowerCase() === 'yes';
}

/**
 * Prompts user for confirmation when existing temporary files are found
 */
export async function confirmProcessExistingBatchFiles(
  batchCount: number,
  totalRecords: number,
  pendingRecords: number
): Promise<boolean> {
  console.log(chalk.cyan('\n📦 Existing Batch Files Found'));
  console.log(chalk.gray('─'.repeat(50)));

  console.log(`📁 Total batches: ${chalk.bold(batchCount.toString())}`);
  console.log(`📊 Total records: ${chalk.bold(totalRecords.toLocaleString())}`);
  console.log(`🔄 Pending records: ${chalk.bold(pendingRecords.toLocaleString())}`);

  if (pendingRecords === 0) {
    console.log(chalk.green('✅ All records have been processed!'));
    return false;
  }

  console.log(chalk.gray('─'.repeat(50)));
  console.log(chalk.yellow('🔄 Found existing batch files with pending records.'));
  console.log(chalk.gray('   You can continue processing where you left off without re-reading the parquet file.'));

  const answer = await promptUser('\n❓ Continue processing existing batches? (Y/n): ');

  return answer.toLowerCase() !== 'n' && answer.toLowerCase() !== 'no';
}

/**
 * Simple prompt utility using readline
 */
function promptUser(question: string): Promise<string> {
  const readline = require('readline');

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  return new Promise((resolve) => {
    rl.question(question, (answer: string) => {
      rl.close();
      resolve(answer.trim());
    });
  });
}

/**
 * Display batch processing information
 */
export function displayBatchInfo(
  totalRecords: number,
  totalBatches: number,
  pendingRecords: number,
  completedRecords: number
): void {
  console.log(chalk.cyan('\n📊 Batch Processing Status:'));
  console.log(chalk.gray('─'.repeat(40)));
  console.log(`📁 Total records: ${totalRecords.toLocaleString()}`);
  console.log(`📦 Total batches: ${totalBatches}`);
  console.log(`✅ Completed: ${chalk.green(completedRecords.toLocaleString())} (${((completedRecords / totalRecords) * 100).toFixed(1)}%)`);
  console.log(`🔄 Pending: ${chalk.yellow(pendingRecords.toLocaleString())} (${((pendingRecords / totalRecords) * 100).toFixed(1)}%)`);

  if (pendingRecords > 0) {
    console.log(chalk.gray('\n💡 You can resume processing anytime by running the same command.'));
  }
}