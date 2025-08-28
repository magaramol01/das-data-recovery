const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const fs = require('fs-extra');
const path = require('path');
const logger = require('../config/logger');

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
        logger.debug('Already connected to database');
        return;
      }

      // Create the sqlite directory if it doesn't exist
      const dbDir = path.dirname(this.dbPath);
      await fs.mkdirp(dbDir);

      logger.debug('Connecting to SQLite database', { path: this.dbPath });

      // Initialize database connection
      this.db = await open({
        filename: this.dbPath,
        driver: sqlite3.Database
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
      await this.db.run('PRAGMA foreign_keys = ON');

      this.isConnected = true;
      logger.info('Successfully connected to SQLite database', { path: this.dbPath });
    } catch (error) {
      logger.error('Failed to connect to SQLite database', {
        error: error.message,
        path: this.dbPath,
        stack: error.stack
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
      logger.debug('Query executed successfully', {
        duration,
        rowCount: results.length,
        query: query.replace(/\s+/g, ' ').trim()
      });

      return results;
    } catch (error) {
      logger.error('Query execution failed', {
        error: error.message,
        query: query.replace(/\s+/g, ' ').trim(),
        parameters: params,
        stack: error.stack
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
      logger.debug('Query executed successfully', {
        duration,
        query: query.replace(/\s+/g, ' ').trim()
      });

      return result;
    } catch (error) {
      logger.error('Query execution failed', {
        error: error.message,
        query: query.replace(/\s+/g, ' ').trim(),
        parameters: params,
        stack: error.stack
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
      logger.debug('Query executed successfully', {
        duration,
        changes: result.changes,
        lastID: result.lastID,
        query: query.replace(/\s+/g, ' ').trim()
      });

      return result;
    } catch (error) {
      logger.error('Query execution failed', {
        error: error.message,
        query: query.replace(/\s+/g, ' ').trim(),
        parameters: params,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Begin a transaction
   * @returns {Promise<void>}
   */
  async beginTransaction() {
    try {
      await this.ensureConnection();
      const startTime = Date.now();
      const result = await this.db.run('BEGIN EXCLUSIVE TRANSACTION');
      const duration = Date.now() - startTime;

      logger.debug('Transaction started', {
        result,
        duration
      });
    } catch (error) {
      logger.error('Failed to start transaction', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Commit a transaction
   * @returns {Promise<void>}
   */
  async commit() {
    try {
      const startTime = Date.now();
      await this.db.run('COMMIT');
      const duration = Date.now() - startTime;

      logger.debug('Transaction committed', {
        duration
      });
    } catch (error) {
      logger.error('Failed to commit transaction', {
        error: error.message,
        stack: error.stack
      });

      // Try to rollback if commit fails
      try {
        await this.rollback();
      } catch (rollbackError) {
        logger.error('Failed to rollback after commit failure', {
          error: rollbackError.message,
          stack: rollbackError.stack
        });
      }

      throw error;
    }
  }

  /**
   * Rollback a transaction
   * @returns {Promise<void>}
   */
  async rollback() {
    try {
      const startTime = Date.now();
      await this.db.run('ROLLBACK');
      const duration = Date.now() - startTime;

      logger.debug('Transaction rolled back', {
        duration
      });
    } catch (error) {
      logger.error('Failed to rollback transaction', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
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
        logger.info('Database connection closed');
      }
    } catch (error) {
      logger.error('Failed to close database connection', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }
}

module.exports = SqliteAdapter;
