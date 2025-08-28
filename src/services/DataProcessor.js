const logger = require('../config/logger');
const SqliteAdapter = require('../db/SqliteAdapter');
const { pipeline } = require('stream/promises');
const csv = require('csv-parser');
const fs = require('fs-extra');

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
    this.batchSize = config.batchSize || 100;
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
   * Process CSV files and store data in SQLite
   * @param {string[]} filePaths - Array of CSV file paths to process
   * @returns {Promise<number>} Number of records processed
   */
  async processCsvFiles(filePaths) {
    let totalProcessed = 0;

    try {
      for (const filePath of filePaths) {
        const processed = await this.processSingleCsvFile(filePath);
        totalProcessed += processed;
      }

      logger.info('CSV processing completed', {
        filesProcessed: filePaths.length,
        totalRecords: totalProcessed
      });

      return totalProcessed;
    } catch (error) {
      logger.error('Error processing CSV files', {
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

    try {
      await this.dbAdapter.beginTransaction();

      await pipeline(
        fs.createReadStream(filePath),
        csv(),
        async function* (source) {
          for await (const record of source) {
            batch.push(this.transformRecord(record));
            recordCount++;

            if (batch.length >= this.batchSize) {
              await this.insertBatch(batch);
              yield batch.length;
              batch = [];
            }
          }

          // Insert remaining records
          if (batch.length > 0) {
            await this.insertBatch(batch);
            yield batch.length;
          }
        }.bind(this)
      );

      await this.dbAdapter.commit();

      logger.info('CSV file processed successfully', {
        file: filePath,
        recordsProcessed: recordCount
      });

      return recordCount;
    } catch (error) {
      await this.dbAdapter.rollback();
      logger.error('Error processing CSV file', {
        file: filePath,
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
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
    const query = `
            INSERT INTO recovery (timestamp, tagName, value, metadata)
            VALUES (?, ?, ?, ?)
        `;

    for (const record of batch) {
      await this.dbAdapter.run(query, [
        record.timestamp,
        record.tagName,
        record.value,
        record.metadata
      ]);
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
      await this.dbAdapter.close();
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
