const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
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

      logger.debug('Connecting to SQLite database', { path: this.dbPath });

      this.db = await open({
        filename: this.dbPath,
        driver: sqlite3.Database
      });

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
      await this.db.run('BEGIN TRANSACTION');
      logger.debug('Transaction started');
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
      await this.db.run('COMMIT');
      logger.debug('Transaction committed');
    } catch (error) {
      logger.error('Failed to commit transaction', {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  /**
   * Rollback a transaction
   * @returns {Promise<void>}
   */
  async rollback() {
    try {
      await this.db.run('ROLLBACK');
      logger.debug('Transaction rolled back');
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
