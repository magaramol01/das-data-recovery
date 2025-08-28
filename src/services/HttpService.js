const axios = require('axios');
const logger = require('../config/logger');

/**
 * HttpService
 * Handles all HTTP communications with external endpoints
 */
class HttpService {
  /**
   * Create a new HttpService instance
   * @param {Object} config - Configuration object
   * @param {string} config.endpoint - Base URL for the API endpoint
   * @param {string} config.apiKey - API key for authentication
   * @param {number} config.timeout - Request timeout in milliseconds
   * @param {number} config.maxRetries - Maximum number of retry attempts
   */
  constructor(config) {
    this.endpoint = config.endpoint;
    this.apiKey = config.apiKey;
    this.tenantId = config.tenantId;
    this.timeout = config.timeout || 30000;
    this.maxRetries = config.maxRetries || 3;

    this.client = axios.create({
      timeout: this.timeout,
      headers: {
        'Content-Type': 'application/json',
        'tenant-id': this.tenantId
      }
    });

    // Add response interceptor for logging
    this.client.interceptors.response.use(
      this.handleResponse.bind(this),
      this.handleError.bind(this)
    );
  }

  /**
   * Send data to the endpoint
   * @param {Object} data - Data to send
   * @param {Object} options - Additional options
   * @returns {Promise<Object>} Response from the endpoint
   */
  async sendData(data, options = {}) {
    const requestId = this.generateRequestId();
    let attempt = 1;

    while (attempt <= this.maxRetries) {
      try {
        logger.debug('Sending data to endpoint', {
          requestId,
          attempt,
          endpoint: this.endpoint,
          dataSize: JSON.stringify(data).length,
          headers: {
            ...this.client.defaults.headers,
            'X-Request-ID': requestId,
            'request-id': requestId,
            ...options.headers
          }
        });

        const response = await this.client.post(this.endpoint, data, {
          ...options,
          headers: {
            ...this.client.defaults.headers,
            'X-Request-ID': requestId,
            'request-id': requestId,
            ...options.headers
          }
        });

        logger.info('Data sent successfully', {
          requestId,
          attempt,
          statusCode: response.status
        });

        return response.data;
      } catch (error) {
        if (attempt === this.maxRetries) {
          throw error;
        }

        const delay = this.calculateRetryDelay(attempt);
        logger.warn('Retrying failed request', {
          requestId,
          attempt,
          nextAttemptDelay: delay,
          error: error.message
        });

        await this.sleep(delay);
        attempt++;
      }
    }
  }

  /**
   * Send batch of data to the endpoint
   * @param {Object[]} batch - Array of data objects to send
   * @param {Object} options - Additional options
   * @returns {Promise<Object[]>} Array of responses
   */
  async sendBatch(batch, options = {}) {
    const results = [];
    const errors = [];

    for (const [index, item] of batch.entries()) {
      try {
        const result = await this.sendData(item, options);
        results.push({ index, success: true, data: result });
      } catch (error) {
        logger.error('Error sending batch item', {
          index,
          error: error.message,
          stack: error.stack
        });

        errors.push({
          index,
          success: false,
          error: error.message,
          item
        });
      }
    }

    logger.info('Batch processing completed', {
      total: batch.length,
      successful: results.length,
      failed: errors.length
    });

    if (errors.length > 0) {
      logger.warn('Some batch items failed', { errors });
    }

    return {
      results,
      errors,
      summary: {
        total: batch.length,
        successful: results.length,
        failed: errors.length
      }
    };
  }

  /**
   * Handle successful response
   * @private
   * @param {Object} response - Axios response object
   * @returns {Object} Response data
   */
  handleResponse(response) {
    logger.debug('Received response', {
      status: response.status,
      url: response.config.url,
      method: response.config.method,
      duration: response.duration
    });
    return response;
  }

  /**
   * Handle request error
   * @private
   * @param {Error} error - Axios error object
   * @returns {Promise<never>} Rejected promise
   */
  handleError(error) {
    const errorInfo = {
      message: error.message,
      code: error.code,
      stack: error.stack
    };

    if (error.response) {
      // Server responded with non-2xx status
      errorInfo.status = error.response.status;
      errorInfo.data = error.response.data;
      errorInfo.headers = error.response.headers;
    } else if (error.request) {
      // Request was made but no response received
      errorInfo.request = {
        method: error.request.method,
        path: error.request.path
      };
    }

    logger.error('HTTP request failed', errorInfo);
    return Promise.reject(error);
  }

  /**
   * Generate a unique request ID
   * @returns {string} Request ID
   */
  generateRequestId() {
    return `test-${Date.now()}`;
  }

  /**
   * Calculate delay before retry
   * @private
   * @param {number} attempt - Current attempt number
   * @returns {number} Delay in milliseconds
   */
  calculateRetryDelay(attempt) {
    return Math.min(1000 * Math.pow(2, attempt - 1), 10000);
  }

  /**
   * Sleep for specified duration
   * @private
   * @param {number} ms - Milliseconds to sleep
   * @returns {Promise<void>}
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = HttpService;
