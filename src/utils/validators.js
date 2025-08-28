const logger = require('../config/logger');

/**
 * Input Validators
 * Contains validation functions for various inputs
 */
class Validators {
  /**
   * Validate configuration object
   * @param {Object} config - Configuration object to validate
   * @param {string[]} requiredFields - Array of required field names
   * @throws {Error} If validation fails
   */
  static validateConfig(config, requiredFields) {
    if (!config || typeof config !== 'object') {
      throw new Error('Configuration must be an object');
    }

    const missingFields = requiredFields.filter(field => !(field in config));
    if (missingFields.length > 0) {
      throw new Error(`Missing required configuration fields: ${missingFields.join(', ')}`);
    }
  }

  /**
   * Validate date string format
   * @param {string} date - Date string to validate
   * @param {string} format - Expected format (e.g., 'YYYY-MM-DD')
   * @returns {boolean} True if valid, false otherwise
   */
  static isValidDateFormat(date, format = 'YYYY-MM-DD') {
    if (!date || typeof date !== 'string') return false;

    // Basic ISO date format validation
    if (format === 'YYYY-MM-DD') {
      const regex = /^\d{4}-\d{2}-\d{2}$/;
      if (!regex.test(date)) return false;

      const parsedDate = new Date(date);
      return parsedDate instanceof Date && !isNaN(parsedDate);
    }

    // Add more format validations as needed
    return false;
  }

  /**
   * Validate timestamp string
   * @param {string} timestamp - Timestamp to validate
   * @returns {boolean} True if valid, false otherwise
   */
  static isValidTimestamp(timestamp) {
    if (!timestamp || typeof timestamp !== 'string') return false;

    // ISO 8601 format validation
    const regex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z?$/;
    if (!regex.test(timestamp)) return false;

    const parsedDate = new Date(timestamp);
    return parsedDate instanceof Date && !isNaN(parsedDate);
  }

  /**
   * Validate file path
   * @param {string} filePath - File path to validate
   * @param {string[]} allowedExtensions - Array of allowed file extensions
   * @returns {boolean} True if valid, false otherwise
   */
  static isValidFilePath(filePath, allowedExtensions = []) {
    if (!filePath || typeof filePath !== 'string') return false;

    // Basic path validation
    if (filePath.includes('..')) return false;

    // Extension validation
    if (allowedExtensions.length > 0) {
      const ext = filePath.toLowerCase().split('.').pop();
      return allowedExtensions.includes(ext);
    }

    return true;
  }

  /**
   * Validate batch size
   * @param {number} size - Batch size to validate
   * @param {number} min - Minimum allowed size
   * @param {number} max - Maximum allowed size
   * @returns {boolean} True if valid, false otherwise
   */
  static isValidBatchSize(size, min = 1, max = 1000) {
    return Number.isInteger(size) && size >= min && size <= max;
  }

  /**
   * Validate URL
   * @param {string} url - URL to validate
   * @returns {boolean} True if valid, false otherwise
   */
  static isValidUrl(url) {
    try {
      new URL(url);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Validate API key format
   * @param {string} apiKey - API key to validate
   * @param {Object} options - Validation options
   * @returns {boolean} True if valid, false otherwise
   */
  static isValidApiKey(apiKey, options = { minLength: 32, requireSpecialChars: true }) {
    if (!apiKey || typeof apiKey !== 'string') return false;

    if (apiKey.length < options.minLength) return false;

    if (options.requireSpecialChars) {
      const specialChars = /[!@#$%^&*(),.?":{}|<>]/;
      if (!specialChars.test(apiKey)) return false;
    }

    return true;
  }

  /**
   * Validate CSV record format
   * @param {Object} record - Record to validate
   * @param {string[]} requiredFields - Array of required field names
   * @returns {boolean} True if valid, false otherwise
   */
  static isValidCsvRecord(record, requiredFields) {
    if (!record || typeof record !== 'object') return false;

    return requiredFields.every(field => {
      const value = record[field];
      return value !== undefined && value !== null && value !== '';
    });
  }

  /**
   * Validate environment variables
   * @param {string[]} requiredVars - Array of required environment variable names
   * @throws {Error} If validation fails
   */
  static validateEnv(requiredVars) {
    const missingVars = requiredVars.filter(varName => !process.env[varName]);

    if (missingVars.length > 0) {
      throw new Error(`Missing required environment variables: ${missingVars.join(', ')}`);
    }

    logger.debug('Environment variables validated successfully');
  }

  /**
   * Validate data mapping configuration
   * @param {Object} mapping - Mapping configuration to validate
   * @returns {boolean} True if valid, false otherwise
   */
  static isValidMapping(mapping) {
    if (!mapping || typeof mapping !== 'object') return false;

    const requiredFields = ['sourceField', 'targetField', 'type'];

    return Object.values(mapping).every(map =>
      requiredFields.every(field => field in map));
  }
}

module.exports = Validators;
