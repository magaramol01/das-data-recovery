const logger = require('../utils/logger');
const fs = require('fs-extra');
const path = require('path');
const {
  getDateRange,
  processDateFiles,
  validateEnvironmentVariables,
  processCsvFiles
} = require('./helper');

const TOTAL_TIME_PERIOD = getDateRange();
logger.info('Time period calculated', { TOTAL_TIME_PERIOD });

const OUTPUT_DIR = process.env.OUTPUT_DIR

/**
 * Main worker function
 */
async function main() {
  try {
    const WORKING_DIR = validateEnvironmentVariables();

    logger.info('Starting data recovery process', {
      workingDir: WORKING_DIR,
      outputDir: OUTPUT_DIR,
      dateRange: TOTAL_TIME_PERIOD
    });

    await fs.ensureDir(OUTPUT_DIR);

    for (const date of TOTAL_TIME_PERIOD) {
      try {
        // Step 1: Copy and extract the required files from source to destination
        const filesProcessed = await processDateFiles(date, WORKING_DIR, OUTPUT_DIR);

        // Step 2: If files were processed, handle the CSV files and store in SQLite
        if (filesProcessed > 0) {
          await processCsvFiles(OUTPUT_DIR);
        }
      } catch (error) {
        logger.error('Error processing date', { date, error: error.message });
        continue; // Continue with next date even if current fails
      }
    }

    logger.info('Data recovery process completed successfully');
  } catch (error) {
    logger.error('Fatal error in data recovery process', { error: error.message, stack: error.stack });
    process.exit(1);
  }
}

// Run the main function
main().catch(error => {
  logger.error('Unhandled error in main function', { error: error.message, stack: error.stack });
  process.exit(1);
});