const logger = require('../config/logger');
const fs = require('fs-extra');
const path = require('path');
const { BATCH_SIZES, DEFAULT_CONFIG } = require('../constants');

/**
 * QueueService
 * Handles data queuing, aggregation, and persistence before sending to HTTP endpoint
 */
class QueueService {
  /**
   * Create a new QueueService instance
   * @param {Object} config - Configuration object
   * @param {string} config.outputDir - Directory for queue persistence
   * @param {string} config.queueFileName - Name of the queue persistence file
   * @param {number} config.batchSize - Size of processing batches
   */
  constructor(config = {}) {
    this.outputDir = config.outputDir || './output';
    this.queueFileName = config.queueFileName || 'data_queue.jsonl';
    this.batchSize = config.batchSize || BATCH_SIZES.MEDIUM;
    this.queue = [];
    this.persistPath = path.join(this.outputDir, this.queueFileName);
  }

  /**
   * Initialize the queue service
   * @returns {Promise<void>}
   */
  async initialize() {
    try {
      await fs.ensureDir(this.outputDir);
      // Load any existing queued data
      await this.loadPersistedQueue();
      logger.info('Queue service initialized', {
        outputDir: this.outputDir,
        queueSize: this.queue.length
      });
    } catch (error) {
      logger.error('Failed to initialize queue service', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Load previously persisted queue data
   * @private
   * @returns {Promise<void>}
   */
  async loadPersistedQueue() {
    try {
      if (await fs.pathExists(this.persistPath)) {
        const content = await fs.readFile(this.persistPath, 'utf8');
        this.queue = content
          .split('\n')
          .filter(line => line.trim())
          .map(line => JSON.parse(line));

        logger.info('Loaded persisted queue data', {
          itemsLoaded: this.queue.length
        });
      }
    } catch (error) {
      logger.error('Error loading persisted queue', {
        error: error.message,
        path: this.persistPath
      });
      // Continue with empty queue
      this.queue = [];
    }
  }

  /**
   * Add data to the queue and persist
   * @param {Object} data - Data to add to queue
   * @returns {Promise<void>}
   */
  async addToQueue(data) {
    try {
      // Add metadata
      const queueItem = {
        ...data,
        queuedAt: new Date().toISOString(),
        attempts: 0
      };

      this.queue.push(queueItem);
      await this.persistQueueItem(queueItem);

      logger.debug('Added item to queue', {
        queueSize: this.queue.length,
        timestamp: data.Timestamp
      });
    } catch (error) {
      logger.error('Error adding to queue', {
        error: error.message,
        data: JSON.stringify(data)
      });
      throw error;
    }
  }

  /**
   * Add multiple items to queue
   * @param {Object[]} items - Array of items to add to queue
   * @returns {Promise<void>}
   */
  async addBatchToQueue(items) {
    try {
      const timestamp = new Date().toISOString();
      const queueItems = items.map(item => ({
        ...item,
        queuedAt: timestamp,
        attempts: 0
      }));

      this.queue.push(...queueItems);
      await this.persistQueueBatch(queueItems);

      logger.info('Added batch to queue', {
        itemsAdded: items.length,
        newQueueSize: this.queue.length
      });
    } catch (error) {
      logger.error('Error adding batch to queue', {
        error: error.message,
        itemCount: items.length
      });
      throw error;
    }
  }

  /**
   * Persist a single queue item to storage
   * @private
   * @param {Object} item - Item to persist
   * @returns {Promise<void>}
   */
  async persistQueueItem(item) {
    try {
      await fs.appendFile(
        this.persistPath,
        JSON.stringify(item) + '\n'
      );
    } catch (error) {
      logger.error('Error persisting queue item', {
        error: error.message,
        item: JSON.stringify(item)
      });
      throw error;
    }
  }

  /**
   * Persist multiple queue items to storage
   * @private
   * @param {Object[]} items - Items to persist
   * @returns {Promise<void>}
   */
  async persistQueueBatch(items) {
    try {
      const content = items
        .map(item => JSON.stringify(item))
        .join('\n') + '\n';

      await fs.appendFile(this.persistPath, content);
    } catch (error) {
      logger.error('Error persisting queue batch', {
        error: error.message,
        itemCount: items.length
      });
      throw error;
    }
  }

  /**
   * Get items from queue in batches
   * @param {number} size - Batch size
   * @returns {Object[]} Array of queue items
   */
  getBatch(size = this.batchSize) {
    return this.queue.splice(0, size);
  }

  /**
   * Get queue statistics
   * @returns {Object} Queue statistics
   */
  getStats() {
    if (this.queue.length === 0) {
      return { totalItems: 0 };
    }

    const timestamps = this.queue.map(item => new Date(item.Timestamp));
    const attempts = this.queue.map(item => item.attempts);

    return {
      totalItems: this.queue.length,
      dateRange: {
        earliest: new Date(Math.min(...timestamps)),
        latest: new Date(Math.max(...timestamps))
      },
      attempts: {
        average: attempts.reduce((a, b) => a + b, 0) / attempts.length,
        max: Math.max(...attempts)
      }
    };
  }

  /**
   * Clear the queue
   * @returns {Promise<void>}
   */
  async clearQueue() {
    try {
      this.queue = [];
      await fs.writeFile(this.persistPath, '');
      logger.info('Queue cleared successfully');
    } catch (error) {
      logger.error('Error clearing queue', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Update queue persistence file to match current queue state
   * @private
   * @returns {Promise<void>}
   */
  async updatePersistence() {
    try {
      const content = this.queue
        .map(item => JSON.stringify(item))
        .join('\n') + '\n';

      await fs.writeFile(this.persistPath, content);
      logger.debug('Queue persistence updated', {
        itemCount: this.queue.length
      });
    } catch (error) {
      logger.error('Error updating queue persistence', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Clean up old processed items from the queue
   * @param {number} maxAge - Maximum age in milliseconds
   * @returns {Promise<number>} Number of items cleaned
   */
  async cleanup(maxAge = DEFAULT_CONFIG.FILE.MAX_AGE) {
    try {
      const cutoff = new Date(Date.now() - maxAge);
      const originalLength = this.queue.length;

      this.queue = this.queue.filter(item =>
        new Date(item.queuedAt) > cutoff);

      const removedCount = originalLength - this.queue.length;

      if (removedCount > 0) {
        await this.updatePersistence();
        logger.info('Queue cleanup completed', {
          itemsRemoved: removedCount,
          remainingItems: this.queue.length
        });
      }

      return removedCount;
    } catch (error) {
      logger.error('Error during queue cleanup', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }
}

module.exports = QueueService;
