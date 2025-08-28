const path = require('path');
const fs = require('fs-extra');
const { promisify } = require('util');
const glob = promisify(require('glob'));
const extract = require('extract-zip');
const zlib = require('zlib');
const gunzip = promisify(zlib.gunzip);
const logger = require('../config/logger');

/**
 * FileProcessor Service
 * Handles all file-related operations including finding, processing, and archiving files
 */
class FileProcessor {
  /**
   * Create a new FileProcessor instance
   * @param {Object} config - Configuration object
   * @param {string} config.workingDir - Directory containing source files
   * @param {string} config.outputDir - Directory for processed files
   * @param {string} config.archiveDir - Directory for archived files
   */
  constructor(config) {
    this.workingDir = config.workingDir;
    this.outputDir = config.outputDir;
    this.archiveDir = config.archiveDir;

    // Supported file types
    this.supportedExtensions = ['.zip', '.csv', '.gz', '.gzip'];
  }

  /**
   * Initialize the file processor
   * @returns {Promise<void>}
   */
  async initialize() {
    try {
      // Ensure all required directories exist
      await Promise.all([
        fs.ensureDir(this.workingDir),
        fs.ensureDir(this.outputDir),
        fs.ensureDir(this.archiveDir)
      ]);

      logger.info('FileProcessor initialized', {
        workingDir: this.workingDir,
        outputDir: this.outputDir,
        archiveDir: this.archiveDir
      });
    } catch (error) {
      logger.error('Failed to initialize FileProcessor', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Find files for a specific date
   * @param {string} searchDate - Date in YYYY-MM-DD format
   * @returns {Promise<string[]>} Array of file paths
   */
  async findFilesForDate(searchDate) {
    try {
      const pattern = `**/*${searchDate}*.@(${this.supportedExtensions.join('|').replace(/\./g, '')})`;
      const files = await glob(path.join(this.workingDir, pattern));

      logger.info('Found files for date', {
        date: searchDate,
        count: files.length,
        pattern
      });

      return files;
    } catch (error) {
      logger.error('Error finding files', {
        date: searchDate,
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Process a single file
   * @param {string} filePath - Path to the file
   * @returns {Promise<string>} Path to the processed file
   */
  async processFile(filePath) {
    try {
      const stats = await fs.stat(filePath);
      if (!stats.isFile()) {
        logger.warn('Skipping non-file', { path: filePath });
        return null;
      }

      const fileName = path.basename(filePath);
      const fileExt = path.extname(fileName).toLowerCase();
      const processingStartTime = Date.now();

      // Process based on file type
      let processedPath;
      if (fileExt === '.zip') {
        processedPath = await this.processZipFile(filePath);
      } else if (fileExt === '.csv') {
        processedPath = await this.processCsvFile(filePath);
      } else if (['.gz', '.gzip'].includes(fileExt)) {
        processedPath = await this.processGzipFile(filePath);
      } else {
        logger.warn('Unsupported file type', { path: filePath, extension: fileExt });
        return null;
      }

      const processingTime = Date.now() - processingStartTime;
      logger.info('File processed successfully', {
        source: filePath,
        destination: processedPath,
        size: stats.size,
        processingTime
      });

      // Archive the original file
      await this.archiveFile(filePath);

      return processedPath;
    } catch (error) {
      logger.error('Error processing file', {
        path: filePath,
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Process a ZIP file
   * @private
   * @param {string} filePath - Path to the ZIP file
   * @returns {Promise<string>} Path to the extracted directory
   */
  async processZipFile(filePath) {
    const extractDir = path.join(this.outputDir, path.basename(filePath, '.zip'));
    await fs.ensureDir(extractDir);
    await extract(filePath, { dir: extractDir });
    return extractDir;
  }

  /**
   * Process a CSV file
   * @private
   * @param {string} filePath - Path to the CSV file
   * @returns {Promise<string>} Path to the processed file
   */
  async processCsvFile(filePath) {
    const targetPath = path.join(this.outputDir, path.basename(filePath));
    await fs.copy(filePath, targetPath);
    return targetPath;
  }

  /**
   * Process a GZIP file
   * @private
   * @param {string} filePath - Path to the GZIP file
   * @returns {Promise<string>} Path to the decompressed file
   */
  async processGzipFile(filePath) {
    const targetPath = path.join(
      this.outputDir,
      path.basename(filePath).replace(/\.gz$|\.gzip$/, '')
    );

    const readStream = fs.createReadStream(filePath);
    const writeStream = fs.createWriteStream(targetPath);
    const gunzipStream = zlib.createGunzip();

    await pipeline(readStream, gunzipStream, writeStream);
    return targetPath;
  }

  /**
   * Archive a processed file
   * @private
   * @param {string} filePath - Path to the file to archive
   * @returns {Promise<string>} Path to the archived file
   */
  async archiveFile(filePath) {
    try {
      const archivePath = path.join(
        this.archiveDir,
        path.basename(filePath)
      );

      await fs.move(filePath, archivePath, { overwrite: true });

      logger.debug('File archived', {
        original: filePath,
        archived: archivePath
      });

      return archivePath;
    } catch (error) {
      logger.error('Error archiving file', {
        path: filePath,
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Clean up temporary files
   * @returns {Promise<void>}
   */
  async cleanup() {
    try {
      // Remove processed files older than 24 hours
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);

      const files = await glob(path.join(this.outputDir, '*'));

      for (const file of files) {
        const stats = await fs.stat(file);
        if (stats.mtime < yesterday) {
          await fs.remove(file);
          logger.debug('Removed old processed file', { path: file });
        }
      }

      logger.info('Cleanup completed');
    } catch (error) {
      logger.error('Error during cleanup', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }
}

module.exports = FileProcessor;
