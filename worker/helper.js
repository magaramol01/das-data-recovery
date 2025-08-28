const logger = require('../utils/logger');
const fs = require('fs-extra');
const path = require('path');
const { promisify } = require('util');
const glob = promisify(require('glob'));

const FROM = process.env.FROM;
const TO = process.env.TO;

/**
 * Get an array of dates between two dates
 * @param {string} from - Start date in YYYY-MM-DD format
 * @param {string} to - End date in YYYY-MM-DD format
 * @returns {string[]} Array of dates in YYYY-MM-DD format
 */
function getDateRange(from = FROM, to = TO) {
  try {
    const start = new Date(from);
    const end = new Date(to);
    const result = [];

    if (isNaN(start.getTime()) || isNaN(end.getTime())) {
      throw new Error('Invalid date format provided');
    }

    if (start > end) {
      throw new Error('Start date must be before or equal to end date');
    }

    let current = new Date(start);
    while (current <= end) {
      result.push(current.toISOString().slice(0, 10)); // format YYYY-MM-DD
      current.setDate(current.getDate() + 1);
    }

    return result;
  } catch (error) {
    logger.error('Error in getDateRange', {
      from,
      to,
      error: error.message
    });
    throw error;
  }
}

/**
 * Find files matching the date pattern in the working directory
 * @param {string} searchDate - Date to search for in YYYY-MM-DD format
 * @param {string} workingDir - Working directory to search in
 * @returns {Promise<string[]>} Array of matching file paths
 */
async function findFilesForDate(searchDate, workingDir) {
  try {
    const files = await glob(path.join(workingDir, `**/*${searchDate}*`));
    logger.info('Found files for date', { date: searchDate, count: files.length });
    return files;
  } catch (error) {
    logger.error('Error finding files', { date: searchDate, error: error.message });
    throw error;
  }
}

/**
 * Process a single file
 * @param {string} filePath - Path to the file to process
 * @param {string} date - Date associated with the file
 * @param {string} outputDir - Output directory for processed files
 * @returns {Promise<void>}
 */
async function processFile(filePath, date, outputDir) {
  try {
    const stats = await fs.stat(filePath);
    if (!stats.isFile()) return logger.warn('Skipping non-file', { path: filePath });

    const fileName = path.basename(filePath);
    const targetDir = path.join(outputDir, date);
    const targetPath = path.join(targetDir, fileName);

    await fs.ensureDir(targetDir);
    await fs.copy(filePath, targetPath);
    logger.info('File processed successfully', { source: filePath, destination: targetPath, size: stats.size });
  } catch (error) {
    logger.error('Error processing file', { file: filePath, date: date, error: error.message });
    throw error;
  }
}

/**
 * Process files for a specific date with concurrency control
 * @param {string} date - Date to process files for
 * @param {string} workingDir - Working directory to search in
 * @param {string} outputDir - Output directory for processed files
 * @param {number} concurrencyLimit - Maximum number of files to process concurrently
 * @returns {Promise<number>} Number of files processed
 */
async function processDateFiles(date, workingDir, outputDir, concurrencyLimit = 5) {
  try {
    logger.info('Processing date', { date });
    const files = await findFilesForDate(date, workingDir);

    // Process files in chunks for concurrency control
    for (let i = 0; i < files.length; i += concurrencyLimit) {
      await Promise.all(files.slice(i, i + concurrencyLimit).map(file => processFile(file, date, outputDir)));
    }

    logger.info('Completed processing date', { date, filesProcessed: files.length });
    return files.length;
  } catch (error) {
    logger.error('Error processing date', { date, error: error.message });
    throw error;
  }
}

/**
 * Validate required environment variables
 * @throws {Error} If required environment variables are missing
 */
function validateEnvironmentVariables() {
  const workingDir = process.env.WORKING_DIR;
  
  if (!workingDir) {
    const error = new Error('WORKING_DIR environment variable is required');
    logger.error(error.message);
    throw error;
  }

  if (!FROM || !TO) {
    const error = new Error('FROM and TO environment variables are required');
    logger.error(error.message);
    throw error;
  }

  return workingDir;
}

module.exports = {
  getDateRange,
  findFilesForDate,
  processFile,
  processDateFiles,
  validateEnvironmentVariables
};