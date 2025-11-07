import chalk from 'chalk';

export interface BatchProgress {
  batchId: string;
  status: 'pending' | 'preparing' | 'sending' | 'processing' | 'completed' | 'failed' | 'retrying';
  recordCount: number;
  attempt: number;
  maxAttempts: number;
  error?: string;
  startTime?: number;
  endTime?: number;
}

export class BatchProgressDisplay {
  private batches: Map<string, BatchProgress> = new Map();
  private displayInterval: NodeJS.Timeout | null = null;
  private isDisplaying = false;
  private lastDisplayHeight = 0;

  constructor() {}

  /**
   * Start displaying progress
   */
  start(): void {
    if (this.isDisplaying) return;

    this.isDisplaying = true;
    this.displayInterval = setInterval(() => {
      this.updateDisplay();
    }, 200); // Update every 200ms for smooth animation
  }

  /**
   * Stop displaying progress
   */
  stop(): void {
    if (this.displayInterval) {
      clearInterval(this.displayInterval);
      this.displayInterval = null;
    }
    this.isDisplaying = false;
    this.clearDisplay();
  }

  /**
   * Add or update batch progress
   */
  updateBatch(batchId: string, updates: Partial<BatchProgress>): void {
    const existing = this.batches.get(batchId);
    const batch: BatchProgress = {
      batchId,
      status: 'pending',
      recordCount: 0,
      attempt: 1,
      maxAttempts: 3,
      ...existing,
      ...updates
    };

    if (batch.status === 'sending' && !batch.startTime) {
      batch.startTime = Date.now();
    }

    if ((batch.status === 'completed' || batch.status === 'failed') && !batch.endTime) {
      batch.endTime = Date.now();
    }

    this.batches.set(batchId, batch);
  }

  /**
   * Get summary statistics
   */
  getStats() {
    const stats = {
      total: this.batches.size,
      pending: 0,
      preparing: 0,
      sending: 0,
      processing: 0,
      completed: 0,
      failed: 0,
      retrying: 0
    };

    for (const batch of this.batches.values()) {
      stats[batch.status]++;
    }

    return stats;
  }

  /**
   * Clear the display area
   */
  private clearDisplay(): void {
    if (this.lastDisplayHeight > 0) {
      // Move cursor up and clear lines
      process.stdout.write('\x1b[' + this.lastDisplayHeight + 'A');
      for (let i = 0; i < this.lastDisplayHeight; i++) {
        process.stdout.write('\x1b[2K\x1b[1B'); // Clear line and move down
      }
      process.stdout.write('\x1b[' + this.lastDisplayHeight + 'A'); // Move back up
      this.lastDisplayHeight = 0;
    }
  }

  /**
   * Update the display
   */
  private updateDisplay(): void {
    if (!this.isDisplaying) return;

    const stats = this.getStats();
    const lines: string[] = [];

    // Summary header
    lines.push(chalk.cyan('📊 Batch Processing Status:'));
    lines.push(chalk.gray('─'.repeat(60)));

    // Progress bar
    const completedRatio = (stats.completed + stats.failed) / Math.max(stats.total, 1);
    const progressBarWidth = 40;
    const completedWidth = Math.floor(completedRatio * progressBarWidth);
    const progressBar = '█'.repeat(completedWidth) + '░'.repeat(progressBarWidth - completedWidth);

    lines.push(`${chalk.green('█'.repeat(completedWidth))}${chalk.gray('░'.repeat(progressBarWidth - completedWidth))} ${(completedRatio * 100).toFixed(1)}%`);

    // Status counts
    const statusLine = [
      stats.pending > 0 ? chalk.gray(`⏳ ${stats.pending} pending`) : '',
      stats.preparing > 0 ? chalk.yellow(`🔧 ${stats.preparing} preparing`) : '',
      stats.sending > 0 ? chalk.blue(`📡 ${stats.sending} sending`) : '',
      stats.processing > 0 ? chalk.cyan(`⚡ ${stats.processing} processing`) : '',
      stats.retrying > 0 ? chalk.yellow(`🔄 ${stats.retrying} retrying`) : '',
      stats.completed > 0 ? chalk.green(`✅ ${stats.completed} completed`) : '',
      stats.failed > 0 ? chalk.red(`❌ ${stats.failed} failed`) : ''
    ].filter(s => s).join('  ');

    lines.push(statusLine);

    // Recent activity (last 10 batches that are active)
    const recentBatches = Array.from(this.batches.values())
      .filter(b => b.status !== 'completed' && b.status !== 'failed')
      .sort((a, b) => (b.startTime || 0) - (a.startTime || 0))
      .slice(0, 8);

    if (recentBatches.length > 0) {
      lines.push('');
      lines.push(chalk.cyan('🔄 Active Batches:'));

      for (const batch of recentBatches) {
        const duration = batch.startTime ? ((Date.now() - batch.startTime) / 1000).toFixed(1) : '0.0';
        const statusIcon = this.getStatusIcon(batch.status);
        const statusColor = this.getStatusColor(batch.status);

        let line = `  ${statusIcon} ${batch.batchId} (${batch.recordCount} records)`;

        if (batch.status === 'retrying') {
          line += ` - Attempt ${batch.attempt}/${batch.maxAttempts}`;
        }

        if (batch.status === 'sending' || batch.status === 'processing') {
          line += ` - ${duration}s`;
        }

        lines.push(statusColor(line));
      }
    }

    // Recent errors (last 3 failed batches)
    const recentErrors = Array.from(this.batches.values())
      .filter(b => b.status === 'failed' && b.error)
      .sort((a, b) => (b.endTime || 0) - (a.endTime || 0))
      .slice(0, 3);

    if (recentErrors.length > 0) {
      lines.push('');
      lines.push(chalk.red('❌ Recent Errors:'));

      for (const batch of recentErrors) {
        const errorMsg = batch.error?.length! > 50 ? batch.error?.substring(0, 50) + '...' : batch.error;
        lines.push(chalk.red(`  ${batch.batchId}: ${errorMsg}`));
      }
    }

    lines.push(''); // Add a blank line at the end

    // Clear previous display and show new content
    this.clearDisplay();
    const output = lines.join('\n');
    process.stdout.write(output);
    this.lastDisplayHeight = lines.length;
  }

  /**
   * Get status icon for a batch status
   */
  private getStatusIcon(status: string): string {
    switch (status) {
      case 'pending': return '⏳';
      case 'preparing': return '🔧';
      case 'sending': return '📡';
      case 'processing': return '⚡';
      case 'retrying': return '🔄';
      case 'completed': return '✅';
      case 'failed': return '❌';
      default: return '❓';
    }
  }

  /**
   * Get status color function for a batch status
   */
  private getStatusColor(status: string): (text: string) => string {
    switch (status) {
      case 'pending': return chalk.gray;
      case 'preparing': return chalk.yellow;
      case 'sending': return chalk.blue;
      case 'processing': return chalk.cyan;
      case 'retrying': return chalk.yellow;
      case 'completed': return chalk.green;
      case 'failed': return chalk.red;
      default: return chalk.white;
    }
  }

  /**
   * Show final summary
   */
  showFinalSummary(): void {
    this.stop();

    const stats = this.getStats();
    const totalRecords = Array.from(this.batches.values()).reduce((sum, b) => sum + b.recordCount, 0);

    console.log(chalk.cyan('\n📊 Final Processing Summary:'));
    console.log(chalk.gray('─'.repeat(50)));
    console.log(`📦 Total batches: ${stats.total}`);
    console.log(`📄 Total records: ${totalRecords.toLocaleString()}`);
    console.log(`${chalk.green('✅ Completed:')} ${stats.completed} batches`);

    if (stats.failed > 0) {
      console.log(`${chalk.red('❌ Failed:')} ${stats.failed} batches`);
    }

    // Calculate total processing time
    const batches = Array.from(this.batches.values()).filter(b => b.startTime && b.endTime);
    if (batches.length > 0) {
      const totalTime = Math.max(...batches.map(b => b.endTime!)) - Math.min(...batches.map(b => b.startTime!));
      console.log(`⏱️  Total time: ${(totalTime / 1000).toFixed(1)}s`);
      console.log(`🚀 Average: ${(totalRecords / (totalTime / 1000)).toFixed(1)} records/sec`);
    }
  }
}