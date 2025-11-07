import chalk from 'chalk';

export enum CircuitState {
  CLOSED = 'CLOSED',     // Normal operation
  OPEN = 'OPEN',         // API is down, reject requests immediately
  HALF_OPEN = 'HALF_OPEN' // Testing if API is back up
}

export interface CircuitBreakerOptions {
  failureThreshold: number;     // Number of failures before opening circuit
  failureRate: number;          // Percentage of failures before opening (0-1)
  resetTimeout: number;         // Time to wait before trying again (ms)
  monitorWindow: number;        // Time window to monitor failures (ms)
  minRequestsBeforeTrip: number; // Minimum requests before considering failure rate
}

export interface CircuitBreakerStats {
  state: CircuitState;
  failures: number;
  successes: number;
  totalRequests: number;
  failureRate: number;
  lastFailureTime: number | null;
  timeUntilRetry: number | null;
}

export class CircuitBreaker {
  private state: CircuitState = CircuitState.CLOSED;
  private failures: number = 0;
  private successes: number = 0;
  private lastFailureTime: number | null = null;
  private requestHistory: Array<{ timestamp: number; success: boolean }> = [];

  constructor(private options: CircuitBreakerOptions) {}

  /**
   * Execute a function with circuit breaker protection
   */
  async execute<T>(fn: () => Promise<T>): Promise<T> {
    if (this.state === CircuitState.OPEN) {
      if (this.shouldAttemptReset()) {
        this.state = CircuitState.HALF_OPEN;
        console.log(chalk.yellow('🔄 Circuit breaker: Attempting to reset (HALF_OPEN)'));
      } else {
        const timeLeft = this.getTimeUntilRetry();
        throw new Error(`Circuit breaker is OPEN. API appears to be down. Retrying in ${Math.ceil(timeLeft / 1000)}s`);
      }
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  /**
   * Check if a request should be allowed
   */
  shouldAllowRequest(): boolean {
    if (this.state === CircuitState.CLOSED) return true;
    if (this.state === CircuitState.HALF_OPEN) return true;
    if (this.state === CircuitState.OPEN) return this.shouldAttemptReset();
    return false;
  }

  /**
   * Record a successful request
   */
  private onSuccess(): void {
    this.successes++;
    this.addToHistory(true);

    if (this.state === CircuitState.HALF_OPEN) {
      this.state = CircuitState.CLOSED;
      this.failures = 0;
      console.log(chalk.green('✅ Circuit breaker: API recovered (CLOSED)'));
    }
  }

  /**
   * Record a failed request
   */
  private onFailure(): void {
    this.failures++;
    this.lastFailureTime = Date.now();
    this.addToHistory(false);

    const stats = this.getStats();

    // Check if we should open the circuit
    if (this.state === CircuitState.CLOSED || this.state === CircuitState.HALF_OPEN) {
      const shouldOpen =
        (this.failures >= this.options.failureThreshold) ||
        (stats.totalRequests >= this.options.minRequestsBeforeTrip && stats.failureRate >= this.options.failureRate);

      if (shouldOpen) {
        this.state = CircuitState.OPEN;
        console.log(chalk.red(`🚨 Circuit breaker: OPENED due to high failure rate (${(stats.failureRate * 100).toFixed(1)}%)`));
        console.log(chalk.yellow(`⏸️  API requests paused for ${this.options.resetTimeout / 1000}s`));
      }
    }
  }

  /**
   * Add request result to sliding window history
   */
  private addToHistory(success: boolean): void {
    const now = Date.now();
    this.requestHistory.push({ timestamp: now, success });

    // Remove old entries outside the monitor window
    const cutoff = now - this.options.monitorWindow;
    this.requestHistory = this.requestHistory.filter(entry => entry.timestamp > cutoff);
  }

  /**
   * Check if enough time has passed to attempt reset
   */
  private shouldAttemptReset(): boolean {
    if (!this.lastFailureTime) return true;
    return Date.now() - this.lastFailureTime >= this.options.resetTimeout;
  }

  /**
   * Get time remaining until retry attempt
   */
  private getTimeUntilRetry(): number {
    if (!this.lastFailureTime) return 0;
    const elapsed = Date.now() - this.lastFailureTime;
    return Math.max(0, this.options.resetTimeout - elapsed);
  }

  /**
   * Get current circuit breaker statistics
   */
  getStats(): CircuitBreakerStats {
    const recentRequests = this.requestHistory;
    const totalRequests = recentRequests.length;
    const recentFailures = recentRequests.filter(r => !r.success).length;
    const failureRate = totalRequests > 0 ? recentFailures / totalRequests : 0;

    return {
      state: this.state,
      failures: this.failures,
      successes: this.successes,
      totalRequests,
      failureRate,
      lastFailureTime: this.lastFailureTime,
      timeUntilRetry: this.state === CircuitState.OPEN ? this.getTimeUntilRetry() : null
    };
  }

  /**
   * Force reset the circuit breaker (for manual intervention)
   */
  reset(): void {
    this.state = CircuitState.CLOSED;
    this.failures = 0;
    this.successes = 0;
    this.lastFailureTime = null;
    this.requestHistory = [];
    console.log(chalk.blue('🔄 Circuit breaker: Manually reset'));
  }

  /**
   * Get a human-readable status message
   */
  getStatusMessage(): string {
    const stats = this.getStats();

    switch (this.state) {
      case CircuitState.CLOSED:
        return chalk.green(`✅ API Healthy (${stats.totalRequests} requests, ${(stats.failureRate * 100).toFixed(1)}% failure rate)`);

      case CircuitState.OPEN:
        const timeLeft = Math.ceil((stats.timeUntilRetry || 0) / 1000);
        return chalk.red(`🚨 API Down - Circuit OPEN (retry in ${timeLeft}s)`);

      case CircuitState.HALF_OPEN:
        return chalk.yellow(`🔄 API Testing - Circuit HALF_OPEN`);

      default:
        return 'Unknown state';
    }
  }
}

/**
 * Default circuit breaker configuration for embedding APIs
 */
export const DEFAULT_CIRCUIT_BREAKER_OPTIONS: CircuitBreakerOptions = {
  failureThreshold: 5,        // Open after 5 consecutive failures
  failureRate: 0.7,          // Open if 70% of requests fail
  resetTimeout: 30000,       // Wait 30 seconds before retry
  monitorWindow: 60000,      // Monitor failures over 1 minute window
  minRequestsBeforeTrip: 10  // Need at least 10 requests before considering failure rate
};