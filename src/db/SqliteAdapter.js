const sqlite3 = require("sqlite3");
const { open } = require("sqlite");
const fs = require("fs-extra");
const path = require("path");
const baseLogger = require("../config/logger");

// Create service-specific logger
const logger = baseLogger.withService("SqliteAdapter");

/**
 * SQLite Database Adapter
 * Handles database operations with proper error handling and connection management
 */
class SqliteAdapter {
  /**
   * Create a new SQLite adapter instance
   * @param {string} dbPath - Path to the SQLite database file
   */
  constructor(dbPath) {
    this.dbPath = dbPath;
    this.db = null;
    this.isConnected = false;
  }

  /**
   * Connect to the SQLite database
   * @returns {Promise<void>}
   * @throws {Error} If connection fails
   */
  async connect() {
    try {
      if (this.isConnected) {
        logger.debug("Already connected to database");
        return;
      }

      // Create the sqlite directory if it doesn't exist
      const dbDir = path.dirname(this.dbPath);
      await fs.mkdirp(dbDir);

      logger.debug("Connecting to SQLite database", { path: this.dbPath });

      // Initialize database connection
      this.db = await open({
        filename: this.dbPath,
        driver: sqlite3.Database,
      });

      // Create tables if they don't exist
      await this.db.exec(`
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
      `);

      // Enable foreign keys
      await this.db.run("PRAGMA foreign_keys = ON");

      // Performance and concurrency optimizations
      await this.db.run("PRAGMA journal_mode = WAL"); // Write-Ahead Logging for better concurrency
      await this.db.run("PRAGMA synchronous = NORMAL"); // Reduce sync overhead
      await this.db.run("PRAGMA cache_size = -64000"); // 64MB cache (negative = KB)
      await this.db.run("PRAGMA temp_store = MEMORY"); // Store temp tables in memory
      await this.db.run("PRAGMA mmap_size = 268435456"); // 256MB memory-mapped I/O
      await this.db.run("PRAGMA page_size = 4096"); // Optimal page size
      await this.db.run("PRAGMA auto_vacuum = INCREMENTAL"); // Incremental vacuum
      await this.db.run("PRAGMA busy_timeout = 30000"); // 30 second timeout for locks
      await this.db.run("PRAGMA wal_autocheckpoint = 1000"); // Checkpoint WAL after 1000 pages

      this.isConnected = true;
      logger.info("Successfully connected to SQLite database with performance optimizations", { path: this.dbPath });
    } catch (error) {
      logger.error("Failed to connect to SQLite database", {
        error: error.message,
        path: this.dbPath,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Execute a query and return all results
   * @param {string} query - SQL query to execute
   * @param {Array} params - Query parameters
   * @returns {Promise<Array>} Query results
   */
  async all(query, params = []) {
    try {
      await this.ensureConnection();
      const startTime = Date.now();

      const results = await this.db.all(query, params);

      const duration = Date.now() - startTime;
      logger.debug("Query executed successfully", {
        duration,
        rowCount: results.length,
        query: query.replace(/\s+/g, " ").trim(),
      });

      return results;
    } catch (error) {
      logger.error("Query execution failed", {
        error: error.message,
        query: query.replace(/\s+/g, " ").trim(),
        parameters: params,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Execute a query and return the first result
   * @param {string} query - SQL query to execute
   * @param {Array} params - Query parameters
   * @returns {Promise<Object>} First row of results
   */
  async get(query, params = []) {
    try {
      await this.ensureConnection();
      const startTime = Date.now();

      const result = await this.db.get(query, params);

      const duration = Date.now() - startTime;
      logger.debug("Query executed successfully", {
        duration,
        query: query.replace(/\s+/g, " ").trim(),
      });

      return result;
    } catch (error) {
      logger.error("Query execution failed", {
        error: error.message,
        query: query.replace(/\s+/g, " ").trim(),
        parameters: params,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Execute a query that modifies the database
   * @param {string} query - SQL query to execute
   * @param {Array} params - Query parameters
   * @returns {Promise<Object>} Result of the operation
   */
  async run(query, params = []) {
    try {
      await this.ensureConnection();
      const startTime = Date.now();

      const result = await this.db.run(query, params);

      const duration = Date.now() - startTime;
      logger.debug("Query executed successfully", {
        duration,
        changes: result.changes,
        lastID: result.lastID,
        query: query.replace(/\s+/g, " ").trim(),
      });

      return result;
    } catch (error) {
      logger.error("Query execution failed", {
        error: error.message,
        query: query.replace(/\s+/g, " ").trim(),
        parameters: params,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Retry database operation with exponential backoff
   * @param {Function} operation - The database operation to retry
   * @param {number} maxRetries - Maximum number of retry attempts
   * @param {number} baseDelay - Base delay in milliseconds
   * @returns {Promise<any>} - Result of the operation
   */
  async retryOperation(operation, maxRetries = 5, baseDelay = 100) {
    let lastError;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;

        // Only retry on SQLITE_BUSY errors
        if (error.code === "SQLITE_BUSY" && attempt < maxRetries) {
          const delay = baseDelay * Math.pow(2, attempt) + Math.random() * 100;
          logger.warn(`Database busy, retrying in ${delay}ms (attempt ${attempt + 1}/${maxRetries + 1})`);
          await this.sleep(delay);
          continue;
        }

        // Re-throw for other errors or max retries exceeded
        throw error;
      }
    }

    throw lastError;
  }

  /**
   * Sleep for specified milliseconds
   * @param {number} ms - Milliseconds to sleep
   */
  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Begin a database transaction with retry logic
   * @param {string} mode - Transaction mode ('DEFERRED', 'IMMEDIATE', 'EXCLUSIVE')
   * @returns {Promise<void>}
   */
  async beginTransaction(mode = "IMMEDIATE") {
    return this.retryOperation(async () => {
      await this.db.run(`BEGIN ${mode} TRANSACTION`);
      logger.debug(`Started ${mode} transaction`);
    });
  }

  /**
   * Commit a database transaction with retry logic
   * @returns {Promise<void>}
   */
  async commitTransaction() {
    return this.retryOperation(async () => {
      await this.db.run("COMMIT");
      logger.debug("Committed transaction");
    });
  }

  /**
   * Rollback a database transaction with retry logic
   * @returns {Promise<void>}
   */
  async rollbackTransaction() {
    return this.retryOperation(async () => {
      await this.db.run("ROLLBACK");
      logger.debug("Rolled back transaction");
    });
  }

  /**
   * Ensure database connection is established
   * @private
   * @returns {Promise<void>}
   */
  async ensureConnection() {
    if (!this.isConnected) {
      await this.connect();
    }
  }

  /**
   * Close the database connection
   * @returns {Promise<void>}
   */
  async close() {
    try {
      if (this.db && this.isConnected) {
        await this.db.close();
        this.isConnected = false;
        logger.info("Database connection closed");
      }
    } catch (error) {
      logger.error("Failed to close database connection", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Bulk insert records using prepared statements for better performance
   * @param {string} table - Table name
   * @param {Array} columns - Column names
   * @param {Array} records - Array of record arrays
   * @returns {Promise<number>} Number of records inserted
   */
  async bulkInsert(table, columns, records) {
    if (!records || records.length === 0) {
      return 0;
    }

    return this.retryOperation(async () => {
      await this.ensureConnection();
      const startTime = Date.now();

      // Create prepared statement
      const placeholders = columns.map(() => "?").join(", ");
      const sql = `INSERT INTO ${table} (${columns.join(", ")}) VALUES (${placeholders})`;

      const stmt = await this.db.prepare(sql);

      let insertedCount = 0;

      // Use IMMEDIATE transaction for better concurrency
      await this.beginTransaction("IMMEDIATE");

      try {
        for (const record of records) {
          await stmt.run(record);
          insertedCount++;
        }

        await this.commitTransaction();
        await stmt.finalize();

        const duration = Date.now() - startTime;
        logger.info("Bulk insert completed successfully", {
          table,
          recordCount: insertedCount,
          duration,
          recordsPerSecond: Math.round(insertedCount / (duration / 1000)),
        });

        return insertedCount;
      } catch (error) {
        await this.rollbackTransaction();
        await stmt.finalize();
        throw error;
      }
    });
  }

  /**
   * Batch insert records using single INSERT statement with multiple VALUES
   * Most efficient for large datasets with improved concurrency handling
   * @param {string} table - Table name
   * @param {Array} columns - Column names
   * @param {Array} records - Array of record arrays
   * @param {number} batchSize - Number of records per batch (default: 5000)
   * @returns {Promise<number>} Number of records inserted
   */
  async batchInsert(table, columns, records, batchSize = 5000) {
    if (!records || records.length === 0) {
      return 0;
    }

    return this.retryOperation(async () => {
      await this.ensureConnection();
      const startTime = Date.now();
      let totalInserted = 0;

      // Use IMMEDIATE transaction for better concurrency than EXCLUSIVE
      await this.beginTransaction("IMMEDIATE");

      try {
        // Process records in batches
        for (let i = 0; i < records.length; i += batchSize) {
          const batch = records.slice(i, i + batchSize);
          const valuePlaceholders = batch.map(() => `(${columns.map(() => "?").join(", ")})`).join(", ");
          const sql = `INSERT INTO ${table} (${columns.join(", ")}) VALUES ${valuePlaceholders}`;

          // Flatten the batch for parameters
          const params = batch.flat();

          await this.db.run(sql, params);
          totalInserted += batch.length;

          // Log progress for large batches
          if (records.length > 10000) {
            logger.debug("Batch insert progress", {
              table,
              processed: totalInserted,
              total: records.length,
              progress: `${Math.round((totalInserted / records.length) * 100)}%`,
            });
          }
        }

        await this.commitTransaction();

        const duration = Date.now() - startTime;
        logger.info("Batch insert completed successfully", {
          table,
          recordCount: totalInserted,
          batchSize,
          duration,
          recordsPerSecond: Math.round(totalInserted / (duration / 1000)),
        });

        return totalInserted;
      } catch (error) {
        await this.rollbackTransaction();
        throw error;
      }
    });
  }
}

module.exports = SqliteAdapter;
