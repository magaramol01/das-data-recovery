require('dotenv').config();
const path = require('path');
const logger = require('./config/logger');
const FileProcessor = require('./services/FileProcessor');
const DataProcessor = require('./services/DataProcessor');
const QueueService = require('./services/QueueService');
const HttpService = require('./services/HttpService');
const Validators = require('./utils/validators');
const Helpers = require('./utils/helpers');
const {
  ENVIRONMENT,
  BATCH_SIZES,
  ERROR_MESSAGES
} = require('./constants');

/**
 * Main application class
 * Orchestrates the data recovery and processing workflow
 */
class Application {
  constructor() {
    this.validateEnvironment();
    this.initializeServices();
  }

  /**
   * Validate required environment variables
   */
  validateEnvironment() {
    const requiredVars = [
      'NODE_ENV',
      'WORKING_DIR',
      'OUTPUT_DIR',
      'DB_PATH',
      'API_ENDPOINT',
      'API_KEY'
    ];

    Validators.validateEnv(requiredVars);

    logger.info('Environment configured', {
      env: process.env.NODE_ENV,
      workingDir: process.env.WORKING_DIR,
      outputDir: process.env.OUTPUT_DIR,
      dbPath: process.env.DB_PATH,
      apiEndpoint: process.env.API_ENDPOINT
    });
  }

  /**
   * Initialize all required services
   */
  async initializeServices() {
    try {
      // Initialize File Processor
      this.fileProcessor = new FileProcessor({
        workingDir: process.env.WORKING_DIR,
        outputDir: process.env.OUTPUT_DIR,
        archiveDir: process.env.ARCHIVE_DIR || path.join(process.env.OUTPUT_DIR, 'archive')
      });

      // Initialize Data Processor
      this.dataProcessor = new DataProcessor({
        dbPath: process.env.DB_PATH,
        mappingName: process.env.MAPPING_NAME,
        batchSize: parseInt(process.env.BATCH_SIZE) || BATCH_SIZES.MEDIUM
      });

      // Initialize Queue Service
      this.queueService = new QueueService({
        outputDir: process.env.OUTPUT_DIR,
        queueFileName: 'data_queue.jsonl',
        batchSize: parseInt(process.env.QUEUE_BATCH_SIZE) || BATCH_SIZES.MEDIUM
      });

      // Initialize HTTP Service
      this.httpService = new HttpService({
        endpoint: process.env.API_ENDPOINT,
        apiKey: process.env.API_KEY,
        timeout: parseInt(process.env.API_TIMEOUT) || 30000
      });

      await this.fileProcessor.initialize();
      await this.dataProcessor.initialize();
      await this.queueService.initialize();

      logger.info('All services initialized successfully');
    } catch (error) {
      logger.error('Service initialization failed', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Process data for a specific date range
   * @param {string} fromDate - Start date (YYYY-MM-DD)
   * @param {string} toDate - End date (YYYY-MM-DD)
   */
  async processDateRange(fromDate, toDate) {
    try {
      logger.info('Starting data recovery process', { fromDate, toDate });

      // Generate date range
      const dates = Helpers.generateDateRange(fromDate, toDate);
      let totalProcessed = 0;

      // Process each date
      for (const date of dates) {
        try {
          const processed = await this.processDate(date);
          totalProcessed += processed;
        } catch (error) {
          logger.error('Error processing date', {
            date,
            error: error.message
          });
          // Continue with next date
          continue;
        }
      }

      logger.info('Data recovery process completed', {
        totalDates: dates.length,
        totalProcessed
      });

    } catch (error) {
      logger.error('Fatal error in data recovery process', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Process data for a specific date
   * @param {string} date - Date to process (YYYY-MM-DD)
   * @returns {Promise<number>} Number of records processed
   */
  async processDate(date) {
    logger.info('Processing date', { date });

    // Find files for the date
    const files = await this.fileProcessor.findFilesForDate(date);
    if (files.length === 0) {
      logger.info('No files found for date', { date });
      return 0;
    }

    // Process each file
    let processed = 0;
    for (const file of files) {
      try {
        // Process and move to output directory
        const processedFile = await this.fileProcessor.processFile(file);
        if (!processedFile) continue;

        // Process data from file
        const records = await this.dataProcessor.processCsvFiles([processedFile]);
        processed += records;

        // Aggregate and send data
        if (records > 0) {
          await this.sendAggregatedData(date);
        }
      } catch (error) {
        logger.error('Error processing file', {
          file,
          error: error.message
        });
        // Continue with next file
        continue;
      }
    }

    return processed;
  }

  /**
   * Send aggregated data for a specific date
   * @param {string} date - Date to send data for
   */
  async sendAggregatedData(date) {
    const startTime = `${date}T00:00:00.000`;
    const endTime = `${date}T23:59:59.999`;

    try {
      // Get aggregated data
      const data = await this.dataProcessor.aggregateData(startTime, endTime);

      if (data.length === 0) {
        logger.info('No data to send', { date });
        return;
      }

      // Add aggregated data to queue
      await this.queueService.addBatchToQueue(data);
      logger.info('Data added to queue', {
        date,
        recordCount: data.length,
        queueStats: this.queueService.getStats()
      });

      // Process queue in batches
      await this.processQueue();
    } catch (error) {
      logger.error('Error sending aggregated data', {
        date,
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Process queued data and send to HTTP endpoint
   * @returns {Promise<void>}
   */
  async processQueue() {
    try {
      let batch;
      let totalProcessed = 0;
      let totalFailed = 0;

      while ((batch = this.queueService.getBatch()).length > 0) {
        try {
          const result = await this.httpService.sendBatch(batch);
          totalProcessed += result.summary.successful;
          totalFailed += result.summary.failed;

          // Handle failed items by adding them back to queue with increased attempt count
          if (result.errors.length > 0) {
            const failedItems = result.errors.map(error => ({
              ...batch[error.index],
              attempts: (batch[error.index].attempts || 0) + 1
            }));

            // Only requeue items that haven't exceeded max attempts
            const itemsToRequeue = failedItems.filter(item => item.attempts < 3);
            if (itemsToRequeue.length > 0) {
              await this.queueService.addBatchToQueue(itemsToRequeue);
            }

            // Log items that exceeded max attempts
            const failedPermanently = failedItems.filter(item => item.attempts >= 3);
            if (failedPermanently.length > 0) {
              logger.error('Items exceeded maximum retry attempts', {
                count: failedPermanently.length,
                items: failedPermanently
              });
            }
          }

          logger.info('Processed queue batch', {
            batchSize: batch.length,
            successful: result.summary.successful,
            failed: result.summary.failed,
            remainingInQueue: this.queueService.getStats().totalItems
          });
        } catch (error) {
          logger.error('Error processing queue batch', {
            error: error.message,
            batchSize: batch.length
          });
          // Requeue the entire batch
          await this.queueService.addBatchToQueue(
            batch.map(item => ({
              ...item,
              attempts: (item.attempts || 0) + 1
            }))
          );
        }
      }

      logger.info('Queue processing completed', {
        totalProcessed,
        totalFailed,
        queueStats: this.queueService.getStats()
      });
    } catch (error) {
      logger.error('Fatal error processing queue', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Cleanup old data and resources
   */
  async cleanup() {
    try {
      await this.dataProcessor.cleanup();
      await this.fileProcessor.cleanup();
      await this.dataProcessor.close();

      logger.info('Cleanup completed successfully');
    } catch (error) {
      logger.error('Error during cleanup', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }
}

// Create and run the application
const app = new Application();

// Handle the main process
const main = async () => {
  try {
    const fromDate = process.env.FROM;
    const toDate = process.env.TO;

    if (!fromDate || !toDate) {
      throw new Error('FROM and TO dates are required');
    }

    await app.processDateRange(fromDate, toDate);
    await app.cleanup();

    logger.info('Application completed successfully');
    process.exit(0);
  } catch (error) {
    logger.error('Application failed', {
      error: error.message,
      stack: error.stack
    });
    process.exit(1);
  }
};

// Handle process signals
process.on('SIGTERM', async () => {
  logger.info('Received SIGTERM signal');
  await app.cleanup();
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('Received SIGINT signal');
  await app.cleanup();
  process.exit(0);
});

// Start the application
main();
