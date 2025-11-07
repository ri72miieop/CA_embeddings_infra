import fs from 'fs/promises';
import crypto from 'crypto';
import path from 'path';

export interface FileHashInfo {
  filePath: string;
  hash: string;
  size: number;
  lastModified: string;
}

export interface CacheManifest {
  version: string;
  createdAt: string;
  sourceFile: FileHashInfo;
  intermediaryFile: FileHashInfo;
  processingOptions: {
    textColumn: string;
    idColumn: string;
    maxTextLength: number;
    includeDate: boolean;
    processConversationContext: boolean;
  };
}

export class FileHasher {
  /**
   * Calculate SHA-256 hash of a file
   */
  static async hashFile(filePath: string): Promise<string> {
    const fileBuffer = await fs.readFile(filePath);
    const hashSum = crypto.createHash('sha256');
    hashSum.update(fileBuffer);
    return hashSum.digest('hex');
  }

  /**
   * Get file info including hash, size, and modification time
   */
  static async getFileInfo(filePath: string): Promise<FileHashInfo> {
    const stats = await fs.stat(filePath);
    const hash = await this.hashFile(filePath);

    return {
      filePath: path.resolve(filePath),
      hash,
      size: stats.size,
      lastModified: stats.mtime.toISOString()
    };
  }

  /**
   * Create cache directory structure
   */
  static async createCacheDirectory(sourceFilePath: string): Promise<string> {
    const sourceFileName = path.basename(sourceFilePath, '.parquet');
    const cacheDir = path.join('./data/parquet-cache', sourceFileName);
    await fs.mkdir(cacheDir, { recursive: true });
    return cacheDir;
  }

  /**
   * Generate cache manifest file path
   */
  static getCacheManifestPath(cacheDir: string): string {
    return path.join(cacheDir, 'cache-manifest.json');
  }

  /**
   * Generate intermediary parquet file path
   */
  static getIntermediaryFilePath(cacheDir: string): string {
    return path.join(cacheDir, 'processed-tweets.parquet');
  }

  /**
   * Check if cache is valid for given processing options
   */
  static async isCacheValid(
    sourceFilePath: string,
    processingOptions: CacheManifest['processingOptions']
  ): Promise<{ valid: boolean; cacheDir?: string; manifest?: CacheManifest }> {
    try {
      const cacheDir = await this.createCacheDirectory(sourceFilePath);
      const manifestPath = this.getCacheManifestPath(cacheDir);
      const intermediaryPath = this.getIntermediaryFilePath(cacheDir);

      // Check if manifest and intermediary file exist
      try {
        await fs.access(manifestPath);
        await fs.access(intermediaryPath);
      } catch {
        return { valid: false, cacheDir };
      }

      // Read and validate manifest
      const manifestContent = await fs.readFile(manifestPath, 'utf-8');
      const manifest: CacheManifest = JSON.parse(manifestContent);

      // Check if source file hash matches
      const currentSourceInfo = await this.getFileInfo(sourceFilePath);
      if (currentSourceInfo.hash !== manifest.sourceFile.hash) {
        return { valid: false, cacheDir, manifest };
      }

      // Check if intermediary file hash matches
      const currentIntermediaryInfo = await this.getFileInfo(intermediaryPath);
      if (currentIntermediaryInfo.hash !== manifest.intermediaryFile.hash) {
        return { valid: false, cacheDir, manifest };
      }

      // Check if processing options match
      const optionsMatch = (
        manifest.processingOptions.textColumn === processingOptions.textColumn &&
        manifest.processingOptions.idColumn === processingOptions.idColumn &&
        manifest.processingOptions.maxTextLength === processingOptions.maxTextLength &&
        manifest.processingOptions.includeDate === processingOptions.includeDate &&
        manifest.processingOptions.processConversationContext === processingOptions.processConversationContext
      );

      if (!optionsMatch) {
        return { valid: false, cacheDir, manifest };
      }

      return { valid: true, cacheDir, manifest };
    } catch (error) {
      console.warn(`Cache validation failed: ${error}`);
      return { valid: false };
    }
  }

  /**
   * Create cache manifest
   */
  static async createCacheManifest(
    sourceFilePath: string,
    intermediaryFilePath: string,
    processingOptions: CacheManifest['processingOptions']
  ): Promise<CacheManifest> {
    const sourceInfo = await this.getFileInfo(sourceFilePath);
    const intermediaryInfo = await this.getFileInfo(intermediaryFilePath);

    const manifest: CacheManifest = {
      version: '1.0.0',
      createdAt: new Date().toISOString(),
      sourceFile: sourceInfo,
      intermediaryFile: intermediaryInfo,
      processingOptions
    };

    return manifest;
  }

  /**
   * Save cache manifest to disk
   */
  static async saveCacheManifest(manifest: CacheManifest, cacheDir: string): Promise<void> {
    const manifestPath = this.getCacheManifestPath(cacheDir);
    await fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2));
  }

  /**
   * Clear cache for a specific source file
   */
  static async clearCache(sourceFilePath: string): Promise<void> {
    try {
      const cacheDir = await this.createCacheDirectory(sourceFilePath);
      await fs.rm(cacheDir, { recursive: true, force: true });
    } catch (error) {
      console.warn(`Failed to clear cache: ${error}`);
    }
  }

  /**
   * Get cache statistics
   */
  static async getCacheStats(): Promise<{
    totalCaches: number;
    totalSize: number;
    caches: Array<{
      sourceFile: string;
      createdAt: string;
      size: number;
    }>;
  }> {
    try {
      const baseCacheDir = './data/parquet-cache';
      const cacheEntries = await fs.readdir(baseCacheDir, { withFileTypes: true });

      const caches = [];
      let totalSize = 0;

      for (const entry of cacheEntries) {
        if (entry.isDirectory()) {
          const cacheDir = path.join(baseCacheDir, entry.name);
          const manifestPath = this.getCacheManifestPath(cacheDir);

          try {
            const manifestContent = await fs.readFile(manifestPath, 'utf-8');
            const manifest: CacheManifest = JSON.parse(manifestContent);

            const stats = await fs.stat(this.getIntermediaryFilePath(cacheDir));
            const size = stats.size;
            totalSize += size;

            caches.push({
              sourceFile: manifest.sourceFile.filePath,
              createdAt: manifest.createdAt,
              size
            });
          } catch {
            // Skip invalid cache entries
          }
        }
      }

      return {
        totalCaches: caches.length,
        totalSize,
        caches
      };
    } catch {
      return {
        totalCaches: 0,
        totalSize: 0,
        caches: []
      };
    }
  }
}