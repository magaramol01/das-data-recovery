const fs = require('fs-extra');
const path = require('path');
const logger = require('../config/logger');

/**
 * Common Helper Functions
 * Provides utility functions used across the application
 */
class Helpers {
  /**
   * Generate a date range between two dates
   * @param {string} startDate - Start date in YYYY-MM-DD format
   * @param {string} endDate - End date in YYYY-MM-DD format
   * @returns {string[]} Array of dates in YYYY-MM-DD format
   */
  static generateDateRange(startDate, endDate) {
    const dates = [];
    const currentDate = new Date(startDate);
    const end = new Date(endDate);

    while (currentDate <= end) {
      dates.push(currentDate.toISOString().split('T')[0]);
      currentDate.setDate(currentDate.getDate() + 1);
    }

    return dates;
  }


  /**
   * Format timestamp to specific format
   * @param {Date|string} date - Date to format
   * @param {string} format - Target format ('iso', 'date', 'time', 'datetime')
   * @returns {string} Formatted date string
   */
  static formatTimestamp(date, format = 'iso') {
    const d = date instanceof Date ? date : new Date(date);

    switch (format.toLowerCase()) {
      case 'iso':
        return d.toISOString();
      case 'date':
        return d.toISOString().split('T')[0];
      case 'time':
        return d.toISOString().split('T')[1].split('.')[0];
      case 'datetime':
        return d.toISOString().replace('T', ' ').split('.')[0];
      default:
        return d.toISOString();
    }
  }

  /**
   * Ensure directory exists and is writable
   * @param {string} dirPath - Directory path
   * @returns {Promise<void>}
   */
  static async ensureDirectoryAccess(dirPath) {
    try {
      await fs.ensureDir(dirPath);
      await fs.access(dirPath, fs.constants.W_OK);
    } catch (error) {
      logger.error('Directory access error', {
        path: dirPath,
        error: error.message
      });
      throw new Error(`Cannot access directory: ${dirPath}`);
    }
  }

  /**
   * Check if file exists and get its stats
   * @param {string} filePath - File path
   * @returns {Promise<Object>} File stats
   */
  static async getFileInfo(filePath) {
    try {
      const stats = await fs.stat(filePath);
      return {
        exists: true,
        isFile: stats.isFile(),
        isDirectory: stats.isDirectory(),
        size: stats.size,
        created: stats.birthtime,
        modified: stats.mtime
      };
    } catch (error) {
      return {
        exists: false,
        error: error.message
      };
    }
  }

  /**
   * Calculate MD5 hash of a file
   * @param {string} filePath - Path to the file
   * @returns {Promise<string>} MD5 hash
   */
  static async calculateFileHash(filePath) {
    const crypto = require('crypto');
    const fileStream = fs.createReadStream(filePath);
    const hash = crypto.createHash('md5');

    return new Promise((resolve, reject) => {
      fileStream.on('error', reject);
      fileStream.on('data', chunk => hash.update(chunk));
      fileStream.on('end', () => resolve(hash.digest('hex')));
    });
  }

  /**
   * Parse CSV header to determine column mapping
   * @param {string} headerLine - CSV header line
   * @param {Object} expectedColumns - Expected column mapping
   * @returns {Object} Column mapping
   */
  static parseCSVHeader(headerLine, expectedColumns) {
    const headers = headerLine.split(',').map(h => h.trim());
    const mapping = {};

    for (const [key, value] of Object.entries(expectedColumns)) {
      const index = headers.findIndex(h =>
        h.toLowerCase() === value.toLowerCase());

      if (index !== -1) {
        mapping[key] = index;
      }
    }

    return mapping;
  }

  /**
   * Chunk array into smaller arrays
   * @param {Array} array - Array to chunk
   * @param {number} size - Chunk size
   * @returns {Array[]} Array of chunks
   */
  static chunkArray(array, size) {
    return array.reduce((chunks, item, index) => {
      const chunkIndex = Math.floor(index / size);
      if (!chunks[chunkIndex]) {
        chunks[chunkIndex] = [];
      }
      chunks[chunkIndex].push(item);
      return chunks;
    }, []);
  }

  /**
   * Retry an async function with exponential backoff
   * @param {Function} fn - Function to retry
   * @param {Object} options - Retry options
   * @returns {Promise} Function result
   */
  static async retry(fn, options = {}) {
    const {
      maxAttempts = 3,
      initialDelay = 1000,
      maxDelay = 10000,
      factor = 2
    } = options;

    let attempt = 1;
    let delay = initialDelay;

    while (attempt <= maxAttempts) {
      try {
        return await fn();
      } catch (error) {
        if (attempt === maxAttempts) throw error;

        logger.warn('Retry attempt failed', {
          attempt,
          nextDelay: delay,
          error: error.message
        });

        await new Promise(resolve => setTimeout(resolve, delay));
        delay = Math.min(delay * factor, maxDelay);
        attempt++;
      }
    }
  }

  /**
   * Deep merge two objects
   * @param {Object} target - Target object
   * @param {Object} source - Source object
   * @returns {Object} Merged object
   */
  static deepMerge(target, source) {
    const merged = { ...target };

    for (const [key, value] of Object.entries(source)) {
      if (value && typeof value === 'object' && !Array.isArray(value)) {
        merged[key] = Helpers.deepMerge(merged[key] || {}, value);
      } else {
        merged[key] = value;
      }
    }

    return merged;
  }

  /**
   * Sanitize filename
   * @param {string} filename - Original filename
   * @returns {string} Sanitized filename
   */
  static sanitizeFilename(filename) {
    return filename
      .replace(/[^a-z0-9.-]/gi, '_')
      .replace(/_+/g, '_')
      .toLowerCase();
  }

  /**
   * Get human readable file size
   * @param {number} bytes - Size in bytes
   * @returns {string} Human readable size
   */
  static formatFileSize(bytes) {
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let size = bytes;
    let unitIndex = 0;

    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex++;
    }

    return `${size.toFixed(2)} ${units[unitIndex]}`;
  }
}

module.exports = Helpers;
