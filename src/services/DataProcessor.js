const logger = require('../config/logger');
const SqliteAdapter = require('../db/SqliteAdapter');
const { pipeline } = require('stream/promises');
const csv = require('csv-parser');
const fs = require('fs-extra');
const path = require('path');

/**
 * DataProcessor Service
 * Handles data processing, aggregation, and transformation
 */
class DataProcessor {
  /**
   * Create a new DataProcessor instance
   * @param {Object} config - Configuration object
   * @param {string} config.dbPath - Path to SQLite database
   * @param {string} config.mappingName - Name of the data mapping configuration
   * @param {number} config.batchSize - Size of batches for processing
   */
  constructor(config) {
    this.dbPath = config.dbPath;
    this.mappingName = config.mappingName;
    this.batchSize = config.batchSize || 5000; // Increased from 100 to 5000
    this.maxConcurrency = config.maxConcurrency || 4; // Number of concurrent CSV processors
    this.dbAdapter = new SqliteAdapter(this.dbPath);
  }

  /**
   * Initialize the data processor
   * @returns {Promise<void>}
   */
  async initialize() {
    try {
      await this.dbAdapter.connect();
      await this.ensureTablesExist();
      logger.info('DataProcessor initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize DataProcessor', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Ensure required database tables exist
   * @private
   * @returns {Promise<void>}
   */
  async ensureTablesExist() {
    const createTableQuery = `
            CREATE TABLE IF NOT EXISTS recovery (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                tagName TEXT NOT NULL,
                value TEXT,
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_recovery_timestamp ON recovery(timestamp);
            CREATE INDEX IF NOT EXISTS idx_recovery_tagname ON recovery(tagName);
        `;

    await this.dbAdapter.run(createTableQuery);
    logger.debug('Database tables verified');
  }

  /**
   * Process CSV files and store data in SQLite with parallel processing
   * @param {string[]} filePaths - Array of CSV file paths to process
   * @returns {Promise<number>} Number of records processed
   */
  async processCsvFiles(filePaths) {
    if (!filePaths || filePaths.length === 0) {
      return 0;
    }

    let totalProcessed = 0;

    try {
      logger.info('Starting parallel CSV processing', {
        fileCount: filePaths.length,
        maxConcurrency: this.maxConcurrency,
        actualConcurrency: Math.min(this.maxConcurrency, filePaths.length)
      });

      // Process files in parallel with limited concurrency
      let fileIndex = 0;
      const processNextFile = async () => {
        const currentIndex = fileIndex++;
        if (currentIndex >= filePaths.length) {
          return null; // Signal that no more files to process
        }

        const filePath = filePaths[currentIndex];

        try {
          logger.info('Processing CSV file', {
            file: path.basename(filePath),
            progress: `${currentIndex + 1}/${filePaths.length}`
          });

          const processed = await this.processSingleCsvFileOptimized(filePath);

          logger.info('CSV file completed', {
            file: path.basename(filePath),
            recordsProcessed: processed,
            progress: `${currentIndex + 1}/${filePaths.length}`
          });

          return processed;
        } catch (error) {
          logger.error('Error processing CSV file', {
            file: path.basename(filePath),
            error: error.message,
            progress: `${currentIndex + 1}/${filePaths.length}`
          });
          return 0; // Return 0 for failed files, but continue processing
        }
      };

      // Start concurrent processors
      const concurrentProcessors = Math.min(this.maxConcurrency, filePaths.length);
      const processors = Array.from({ length: concurrentProcessors }, () =>
        (async () => {
          let totalProcessedByWorker = 0;
          let result;

          // Keep processing files until no more files available
          while ((result = await processNextFile()) !== null) {
            totalProcessedByWorker += result;
          }

          return totalProcessedByWorker;
        })()
      );      // Wait for all processors to complete
      const results = await Promise.all(processors);
      totalProcessed = results.reduce((sum, count) => sum + count, 0);

      logger.info('All CSV processing completed', {
        filesProcessed: filePaths.length,
        totalRecords: totalProcessed,
        avgRecordsPerFile: Math.round(totalProcessed / filePaths.length)
      });

      return totalProcessed;
    } catch (error) {
      logger.error('Error in parallel CSV processing', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Simple parallel CSV processing with Promise.all and chunking
   * Alternative implementation that's easier to understand and debug
   * @param {string[]} filePaths - Array of CSV file paths to process
   * @returns {Promise<number>} Number of records processed
   */
  async processCsvFilesSimple(filePaths) {
    if (!filePaths || filePaths.length === 0) {
      return 0;
    }

    try {
      logger.info('Starting simple parallel CSV processing', {
        fileCount: filePaths.length,
        maxConcurrency: this.maxConcurrency
      });

      let totalProcessed = 0;

      // Process files in chunks to limit concurrency
      for (let i = 0; i < filePaths.length; i += this.maxConcurrency) {
        const chunk = filePaths.slice(i, i + this.maxConcurrency);

        logger.info('Processing file chunk', {
          chunkSize: chunk.length,
          progress: `${Math.min(i + this.maxConcurrency, filePaths.length)}/${filePaths.length}`,
          files: chunk.map(f => path.basename(f))
        });

        // Process chunk in parallel
        const chunkResults = await Promise.allSettled(
          chunk.map(async (filePath, index) => {
            const globalIndex = i + index;
            try {
              logger.info('Processing CSV file', {
                file: path.basename(filePath),
                progress: `${globalIndex + 1}/${filePaths.length}`
              });

              const processed = await this.processSingleCsvFileOptimized(filePath);

              logger.info('CSV file completed', {
                file: path.basename(filePath),
                recordsProcessed: processed,
                progress: `${globalIndex + 1}/${filePaths.length}`
              });

              return processed;
            } catch (error) {
              logger.error('Error processing CSV file', {
                file: path.basename(filePath),
                error: error.message,
                progress: `${globalIndex + 1}/${filePaths.length}`
              });
              return 0;
            }
          })
        );

        // Sum up results from this chunk
        const chunkTotal = chunkResults.reduce((sum, result) => {
          return sum + (result.status === 'fulfilled' ? result.value : 0);
        }, 0);

        totalProcessed += chunkTotal;

        logger.info('Chunk processing completed', {
          chunkSize: chunk.length,
          chunkRecords: chunkTotal,
          totalSoFar: totalProcessed,
          progress: `${Math.min(i + this.maxConcurrency, filePaths.length)}/${filePaths.length}`
        });
      }

      logger.info('Simple parallel CSV processing completed', {
        filesProcessed: filePaths.length,
        totalRecords: totalProcessed,
        avgRecordsPerFile: filePaths.length > 0 ? Math.round(totalProcessed / filePaths.length) : 0
      });

      return totalProcessed;
    } catch (error) {
      logger.error('Error in simple parallel CSV processing', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Process a single CSV file
   * @private
   * @param {string} filePath - Path to CSV file
   * @returns {Promise<number>} Number of records processed
   */
  async processSingleCsvFile(filePath) {
    let recordCount = 0;
    let batch = [];

    logger.debug('Starting CSV processing', { filePath });

    try {
      // Begin transaction for the entire file processing
      await this.dbAdapter.beginTransaction();

      await pipeline(
        fs.createReadStream(filePath),
        csv(),
        async (source) => {
          for await (const record of source) {
            logger.debug('Processing record', { record });
            const transformed = this.transformRecord(record);
            logger.debug('Transformed record', { transformed });
            batch.push(transformed);
            recordCount++;

            if (batch.length >= this.batchSize) {
              logger.debug('Processing batch', { batchSize: batch.length });
              await this.insertBatch(batch);
              batch = [];
            }
          }

          // Insert remaining records
          if (batch.length > 0) {
            logger.debug('Processing final batch', { batchSize: batch.length });
            await this.insertBatch(batch);
          }
        }
      );

      // Commit the entire file transaction
      await this.dbAdapter.commit();

      logger.info('CSV file processed successfully', {
        file: filePath,
        recordsProcessed: recordCount
      });

      return recordCount;
    } catch (error) {
      // Rollback transaction on error
      try {
        await this.dbAdapter.rollback();
      } catch (rollbackError) {
        logger.error('Error rolling back transaction', {
          file: filePath,
          error: rollbackError.message,
          stack: rollbackError.stack
        });
      }

      logger.error('Error processing CSV file', {
        file: filePath,
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Optimized CSV file processing with bulk database inserts
   * Uses a single database connection and transaction per file
   * @private
   * @param {string} filePath - Path to CSV file
   * @returns {Promise<number>} Number of records processed
   */
  async processSingleCsvFileOptimized(filePath) {
    let recordCount = 0;
    const allRecords = [];

    const startTime = Date.now();
    logger.debug('Starting optimized CSV processing', { filePath });

    try {
      // Read and parse entire CSV file into memory (streaming with accumulation)
      await pipeline(
        fs.createReadStream(filePath),
        csv(),
        async (source) => {
          for await (const record of source) {
            const transformed = this.transformRecord(record);
            if (transformed) {
              allRecords.push([
                transformed.timestamp,
                transformed.tagName,
                transformed.value,
                transformed.metadata || null
              ]);
              recordCount++;
            }
          }
        }
      );

      // Bulk insert all records at once if we have any
      if (allRecords.length > 0) {
        logger.debug('Starting bulk database insert', {
          file: path.basename(filePath),
          recordCount: allRecords.length
        });

        // Use a dedicated database adapter instance for each file to avoid transaction conflicts
        const fileDbAdapter = new SqliteAdapter(this.dbAdapter.dbPath);
        await fileDbAdapter.connect();

        try {
          await fileDbAdapter.batchInsert(
            'recovery',
            ['timestamp', 'tagName', 'value', 'metadata'],
            allRecords,
            this.batchSize
          );
        } finally {
          await fileDbAdapter.close();
        }
      }

      const duration = Date.now() - startTime;
      logger.info('Optimized CSV file processed successfully', {
        file: path.basename(filePath),
        recordsProcessed: recordCount,
        duration,
        recordsPerSecond: recordCount > 0 ? Math.round(recordCount / (duration / 1000)) : 0
      });

      return recordCount;
    } catch (error) {
      logger.error('Error in optimized CSV processing', {
        file: path.basename(filePath),
        error: error.message,
        recordsProcessed: recordCount,
        stack: error.stack
      });
      throw error;
    }
  }  /**
   * Transform a record before insertion
   * @private
   * @param {Object} record - Raw record from CSV
   * @returns {Object} Transformed record
   */
  transformRecord(record) {
    // Find timestamp field
    const timestamp = record.Timestamp || record.timestamp;
    if (!timestamp) {
      throw new Error('CSV file missing required Timestamp column');
    }

    // Find tagName field
    const tagName = record.TagName || record.tagName || record.Tag || record.tag;
    if (!tagName) {
      throw new Error('CSV file missing required TagName column');
    }

    // Find value field
    const value = record.Value || record.value;
    if (!value) {
      throw new Error('CSV file missing required Value column');
    }

    return {
      timestamp,
      tagName,
      value,
      metadata: JSON.stringify({
        updated: new Date().toISOString()
      })
    };
  }

  /**
   * Insert a batch of records into the database
   * @private
   * @param {Object[]} batch - Array of records to insert
   * @returns {Promise<void>}
   */
  async insertBatch(batch) {
    if (batch.length === 0) {
      return;
    }

    const query = `
            INSERT INTO recovery (timestamp, tagName, value, metadata)
            VALUES (?, ?, ?, ?)
        `;

    logger.debug('Processing batch', {
      batchSize: batch.length,
      firstRecord: batch[0],
      lastRecord: batch[batch.length - 1]
    });

    for (const record of batch) {
      const params = [
        record.timestamp,
        record.tagName,
        record.value,
        record.metadata
      ];

      try {
        const result = await this.dbAdapter.run(query, params);
        logger.debug('Insert result', {
          changes: result?.changes,
          lastID: result?.lastID,
          timestamp: record.timestamp,
          tagName: record.tagName
        });
      } catch (error) {
        logger.error('Failed to insert record', {
          error: error.message,
          record,
          stack: error.stack
        });
        throw error;
      }
    }
  }

  /**
   * Aggregate data by timestamp
   * @param {string} startTime - Start timestamp
   * @param {string} endTime - End timestamp
   * @returns {Promise<Object[]>} Aggregated data
   */
  async aggregateData(startTime, endTime) {
    const query = `
            SELECT 
                datetime(substr(timestamp, 1, 16) || ':00') as formatted_timestamp,
                json_group_object(tagName, value) as aggregated_data,
                COUNT(*) as record_count
            FROM recovery
            WHERE timestamp >= ? AND timestamp < ?
            GROUP BY datetime(substr(timestamp, 1, 16) || ':00')
            ORDER BY formatted_timestamp;
        `;

    try {
      const results = await this.dbAdapter.all(query, [startTime, endTime]);

      logger.info('Data aggregation completed', {
        startTime,
        endTime,
        recordsAggregated: results.length
      });

      return results.map(row => ({
        timestamp: row.formatted_timestamp,
        data: JSON.parse(row.aggregated_data),
        recordCount: row.record_count,
        metadata: {
          aggregatedAt: new Date().toISOString(),
          timeRange: `${startTime} to ${endTime}`
        }
      }));
    } catch (error) {
      logger.error('Error aggregating data', {
        error: error.message,
        startTime,
        endTime,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Clean up old data
   * @param {number} daysToKeep - Number of days of data to retain
   * @returns {Promise<number>} Number of records deleted
   */
  async cleanup(daysToKeep = 30) {
    try {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);

      const result = await this.dbAdapter.run(
        'DELETE FROM recovery WHERE timestamp < ?',
        [cutoffDate.toISOString()]
      );

      logger.info('Database cleanup completed', {
        daysKept: daysToKeep,
        recordsDeleted: result.changes
      });

      return result.changes;
    } catch (error) {
      logger.error('Error during database cleanup', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Close the data processor and database connection
   * @returns {Promise<void>}
   */
  async close() {
    try {
      if (this.dbAdapter) {
        await this.dbAdapter.close();
        logger.info('Database connection closed successfully');
      }
      logger.info('DataProcessor closed successfully');
    } catch (error) {
      logger.error('Error closing DataProcessor', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }
}

module.exports = DataProcessor;
