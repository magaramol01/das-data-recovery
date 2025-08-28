/**
 * Application Constants
 * Central location for all application constants
 */

const FILE_TYPES = {
  CSV: 'csv',
  ZIP: 'zip',
  GZIP: 'gzip',
  GZ: 'gz'
};

const SUPPORTED_EXTENSIONS = ['.csv', '.zip', '.gz', '.gzip'];

const TIME_CONSTANTS = {
  MILLISECOND: 1,
  SECOND: 1000,
  MINUTE: 60 * 1000,
  HOUR: 60 * 60 * 1000,
  DAY: 24 * 60 * 60 * 1000
};

const DATE_FORMATS = {
  ISO: 'iso',
  DATE: 'date',
  TIME: 'time',
  DATETIME: 'datetime'
};

const HTTP_STATUS = {
  OK: 200,
  CREATED: 201,
  BAD_REQUEST: 400,
  UNAUTHORIZED: 401,
  FORBIDDEN: 403,
  NOT_FOUND: 404,
  SERVER_ERROR: 500
};

const LOG_LEVELS = {
  ERROR: 'error',
  WARN: 'warn',
  INFO: 'info',
  DEBUG: 'debug'
};

const BATCH_SIZES = {
  TINY: 10,
  SMALL: 50,
  MEDIUM: 100,
  LARGE: 500,
  HUGE: 1000
};

const DEFAULT_CONFIG = {
  HTTP: {
    TIMEOUT: 30000,
    MAX_RETRIES: 3,
    RETRY_DELAY: 1000
  },
  DB: {
    MAX_CONNECTIONS: 10,
    IDLE_TIMEOUT: 10000
  },
  FILE: {
    MAX_SIZE: 100 * 1024 * 1024, // 100MB
    CHUNK_SIZE: 64 * 1024 // 64KB
  }
};

const CSV_HEADERS = {
  TIMESTAMP: 'timestamp',
  TAG_NAME: 'tag_name',
  VALUE: 'value',
  QUALITY: 'quality',
  SOURCE: 'source'
};

const ERROR_MESSAGES = {
  INVALID_CONFIG: 'Invalid configuration provided',
  INVALID_DATE: 'Invalid date format',
  INVALID_FILE: 'Invalid file type or format',
  DB_CONNECTION: 'Database connection error',
  HTTP_REQUEST: 'HTTP request failed',
  FILE_ACCESS: 'File access error',
  PROCESS_ERROR: 'Processing error occurred'
};

const DATA_TYPES = {
  STRING: 'string',
  NUMBER: 'number',
  BOOLEAN: 'boolean',
  DATE: 'date',
  OBJECT: 'object'
};

const CLEANUP_CONFIG = {
  MAX_AGE_DAYS: 30,
  BATCH_SIZE: 1000,
  MAX_DELETED_PER_RUN: 10000
};

const ENVIRONMENT = {
  DEVELOPMENT: 'development',
  TESTING: 'testing',
  STAGING: 'staging',
  PRODUCTION: 'production'
};

module.exports = {
  FILE_TYPES,
  SUPPORTED_EXTENSIONS,
  TIME_CONSTANTS,
  DATE_FORMATS,
  HTTP_STATUS,
  LOG_LEVELS,
  BATCH_SIZES,
  DEFAULT_CONFIG,
  CSV_HEADERS,
  ERROR_MESSAGES,
  DATA_TYPES,
  CLEANUP_CONFIG,
  ENVIRONMENT
};
