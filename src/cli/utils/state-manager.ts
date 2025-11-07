import fs from 'fs/promises';
import path from 'path';

interface ProcessingState {
  startTime: string;
  lastUpdate: string;
  processedKeys: string[];
  totalProcessed: number;
  errors: string[];
}

export class StateManager {
  private stateFile?: string;

  constructor(stateFile?: string) {
    this.stateFile = stateFile;
  }

  async load(): Promise<ProcessingState> {
    if (!this.stateFile) {
      return this.createEmptyState();
    }

    try {
      await fs.access(this.stateFile);
      const content = await fs.readFile(this.stateFile, 'utf-8');
      const state = JSON.parse(content) as ProcessingState;

      // Validate state structure
      if (!state.processedKeys || !Array.isArray(state.processedKeys)) {
        console.warn('Invalid state file format, starting fresh');
        return this.createEmptyState();
      }

      console.log(`📂 Resumed from state file: ${state.processedKeys.length} keys already processed`);
      return state;

    } catch (error) {
      if ((error as any).code === 'ENOENT') {
        // File doesn't exist, create new state
        return this.createEmptyState();
      }

      console.warn(`Warning: Could not load state file: ${error}. Starting fresh.`);
      return this.createEmptyState();
    }
  }

  async update(newKeys: string[], errors: string[] = []): Promise<void> {
    if (!this.stateFile) {
      return;
    }

    try {
      const currentState = await this.load();

      const updatedState: ProcessingState = {
        ...currentState,
        lastUpdate: new Date().toISOString(),
        processedKeys: [...currentState.processedKeys, ...newKeys],
        totalProcessed: currentState.totalProcessed + newKeys.length,
        errors: [...currentState.errors, ...errors]
      };

      // Ensure directory exists
      const dir = path.dirname(this.stateFile);
      await fs.mkdir(dir, { recursive: true });

      // Write state file
      await fs.writeFile(this.stateFile, JSON.stringify(updatedState, null, 2));

    } catch (error) {
      console.warn(`Warning: Could not save state: ${error}`);
    }
  }

  async clear(): Promise<void> {
    if (!this.stateFile) {
      return;
    }

    try {
      await fs.unlink(this.stateFile);
    } catch (error) {
      // Ignore if file doesn't exist
      if ((error as any).code !== 'ENOENT') {
        console.warn(`Warning: Could not clear state file: ${error}`);
      }
    }
  }

  getStateFile(): string | undefined {
    return this.stateFile;
  }

  private createEmptyState(): ProcessingState {
    return {
      startTime: new Date().toISOString(),
      lastUpdate: new Date().toISOString(),
      processedKeys: [],
      totalProcessed: 0,
      errors: []
    };
  }
}