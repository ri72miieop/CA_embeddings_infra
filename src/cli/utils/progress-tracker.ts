import chalk from 'chalk';

export class ProgressTracker {
  private total: number = 0;
  private processed: number = 0;
  private startTime: number = 0;

  start(total: number): void {
    this.total = total;
    this.processed = 0;
    this.startTime = Date.now();
  }

  update(count: number): void {
    this.processed += count;
    this.printProgress();
  }

  finish(): void {
    const elapsed = Date.now() - this.startTime;
    const elapsedSeconds = elapsed / 1000;
    const rate = elapsedSeconds > 0 ? this.processed / elapsedSeconds : 0;
    const safeRate = isFinite(rate) ? rate : 0;

    console.log(chalk.green(`\\n✅ Completed ${this.processed}/${this.total} records`));
    console.log(chalk.gray(`   ⏱️  Total time: ${this.formatDuration(elapsed)}`));
    console.log(chalk.gray(`   📊 Rate: ${safeRate.toFixed(2)} records/second`));
  }

  private printProgress(): void {
    const percentage = this.total > 0 ? (this.processed / this.total) * 100 : 0;
    const elapsed = Date.now() - this.startTime;
    const elapsedSeconds = elapsed / 1000;
    const rate = elapsedSeconds > 0 ? this.processed / elapsedSeconds : 0;
    const remainingTime = this.total > this.processed && rate > 0 ?
      ((this.total - this.processed) / rate) * 1000 : 0;

    const progressBar = this.createProgressBar(percentage);

    // Ensure all values are finite for display
    const safePercentage = isFinite(percentage) ? percentage : 0;
    const safeRate = isFinite(rate) ? rate : 0;
    const safeRemainingTime = isFinite(remainingTime) ? remainingTime : 0;

    console.log(
      `${progressBar} ${safePercentage.toFixed(1)}% ` +
      `(${this.processed}/${this.total}) ` +
      chalk.gray(`[${safeRate.toFixed(1)}/s, ETA: ${this.formatDuration(safeRemainingTime)}]`)
    );
  }

  private createProgressBar(percentage: number): string {
    const width = 30;

    // Ensure percentage is valid and within bounds
    if (!isFinite(percentage) || isNaN(percentage)) {
      percentage = 0;
    }
    percentage = Math.max(0, Math.min(100, percentage));

    const filled = Math.round((percentage / 100) * width);
    const empty = width - filled;

    // Ensure filled and empty are valid non-negative integers
    const safeFilled = Math.max(0, Math.min(width, filled));
    const safeEmpty = Math.max(0, width - safeFilled);

    const filledBar = chalk.green('█'.repeat(safeFilled));
    const emptyBar = chalk.gray('░'.repeat(safeEmpty));

    return `[${filledBar}${emptyBar}]`;
  }

  private formatDuration(ms: number): string {
    if (ms < 1000) {
      return `${Math.round(ms)}ms`;
    }

    const seconds = Math.floor(ms / 1000);
    if (seconds < 60) {
      return `${seconds}s`;
    }

    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;

    if (minutes < 60) {
      return `${minutes}m ${remainingSeconds}s`;
    }

    const hours = Math.floor(minutes / 60);
    const remainingMinutes = minutes % 60;

    return `${hours}h ${remainingMinutes}m ${remainingSeconds}s`;
  }
}