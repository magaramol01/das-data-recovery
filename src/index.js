require("dotenv").config();
const path = require("path");
const baseLogger = require("./config/logger");
const FileProcessor = require("./services/FileProcessor");
const DataProcessor = require("./services/DataProcessor");
const QueueService = require("./services/QueueService");
const HttpService = require("./services/HttpService");
const Validators = require("./utils/validators");
const Helpers = require("./utils/helpers");
const { ENVIRONMENT, BATCH_SIZES, ERROR_MESSAGES } = require("./constants");

// Create application-specific logger
const logger = baseLogger.withService("Application");

/**
 * Print environment configuration in table format
 */
function printEnvironmentTable() {
  const envConfig = {
    Application: process.env.SERVICE_NAME || "das-data-recovery",
    Environment: process.env.NODE_ENV || "development",
    "Tenant ID": process.env.TENANT_ID || "N/A",
    "Mapping Name": process.env.MAPPING_NAME || "N/A",
    "Working Directory": process.env.WORKING_DIR || "N/A",
    "Output Directory": process.env.OUTPUT_DIR || "N/A",
    "Database Path": process.env.DB_PATH || "N/A",
    "API Endpoint": process.env.API_ENDPOINT || "N/A",
    "From Date": process.env.FROM || "N/A",
    "To Date": process.env.TO || "N/A",
  };

  console.log("\n╭─────────────────────────────────────────────────────────────╮");
  console.log("│                    ENVIRONMENT CONFIGURATION                   │");
  console.log("├─────────────────────────────────────────────────────────────┤");

  Object.entries(envConfig).forEach(([key, value]) => {
    const paddedKey = key.padEnd(18);
    const paddedValue = value.toString().padEnd(40);
    console.log(`│ ${paddedKey} │ ${paddedValue} │`);
  });

  console.log("╰─────────────────────────────────────────────────────────────╯\n");
}

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
    const requiredVars = ["NODE_ENV", "WORKING_DIR", "OUTPUT_DIR", "DB_PATH", "API_ENDPOINT"];

    Validators.validateEnv(requiredVars);

    logger.info("Environment configured", {
      env: process.env.NODE_ENV,
      workingDir: process.env.WORKING_DIR,
      outputDir: process.env.OUTPUT_DIR,
      dbPath: process.env.DB_PATH,
      apiEndpoint: process.env.API_ENDPOINT,
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
        archiveDir: process.env.ARCHIVE_DIR || path.join(process.env.OUTPUT_DIR, "archive"),
      });

      // Initialize Data Processor with optimized settings
      this.dataProcessor = new DataProcessor({
        dbPath: process.env.DB_PATH,
        mappingName: process.env.MAPPING_NAME,
        batchSize: parseInt(process.env.BATCH_SIZE) || BATCH_SIZES.LARGE, // Increased default
        maxConcurrency: parseInt(process.env.MAX_CONCURRENCY) || 1, // Reduced from 4 to 2 for better database concurrency
      });

      // Initialize Queue Service
      this.queueService = new QueueService({
        outputDir: process.env.OUTPUT_DIR,
        queueFileName: "data_queue.jsonl",
        batchSize: parseInt(process.env.QUEUE_BATCH_SIZE) || BATCH_SIZES.MEDIUM,
      });

      // Initialize HTTP Service
      this.httpService = new HttpService({
        endpoint: process.env.API_ENDPOINT,
        tenantId: process.env.TENANT_ID,
        timeout: parseInt(process.env.API_TIMEOUT) || 30000,
      });

      await this.fileProcessor.initialize();
      await this.dataProcessor.initialize();
      await this.queueService.initialize();

      logger.info("All services initialized successfully");
    } catch (error) {
      logger.error("Service initialization failed", {
        error: error.message,
        stack: error.stack,
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
      logger.info("Starting data recovery process", { fromDate, toDate });

      // Generate date range
      const dates = Helpers.generateDateRange(fromDate, toDate);
      let totalProcessed = 0;

      // Process each date
      for (const date of dates) {
        try {
          const processed = await this.processDate(date);
          totalProcessed += processed;

          // Clean up after each date completion
          logger.info("Starting cleanup after date completion", { date });

          try {
            // Clean the output directory after processing each date
            await this.fileProcessor.cleanOutputDirectory();

            // Clear processed data from database to free up space
            const recordsDeleted = await this.dataProcessor.clearAllData();

            logger.info("Date cleanup completed successfully", {
              date,
              recordsDeleted,
              outputDirCleaned: true,
            });
          } catch (cleanupError) {
            logger.error("Error during date cleanup", {
              date,
              error: cleanupError.message,
              stack: cleanupError.stack,
            });
            // Continue with next date even if cleanup fails
          }
        } catch (error) {
          logger.error("Error processing date", {
            date,
            error: error.message,
          });

          // Attempt cleanup even if date processing failed
          try {
            logger.info("Attempting cleanup after failed date processing", { date });
            await this.fileProcessor.cleanOutputDirectory();
            await this.dataProcessor.clearAllData();
            logger.info("Cleanup after failed date completed", { date });
          } catch (cleanupError) {
            logger.error("Cleanup after failed date also failed", {
              date,
              error: cleanupError.message,
            });
          }

          // Continue with next date
          continue;
        }
      }

      logger.info("Data recovery process completed", {
        totalDates: dates.length,
        totalProcessed,
      });
    } catch (error) {
      logger.error("Fatal error in data recovery process", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Process data for a specific date
   * @param {string} date - Date in YYYY-MM-DD format
   * @returns {Promise<number>} Number of records processed
   */
  async processDate(date) {
    logger.info("Processing date", { date });

    // Find files for the date
    const files = await this.fileProcessor.findFilesForDate(date);

    if (files.length === 0) {
      logger.info("No files found for date", { date });
      return 0;
    }

    logger.info("Starting file processing", {
      date,
      totalFiles: files.length,
      files: files.map((f) => path.basename(f)),
    });

    // STEP 1: Copy ALL files to output directory first
    let copiedFiles = [];
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      try {
        logger.info("Copying file", {
          file: path.basename(file),
          progress: `${i + 1}/${files.length}`,
          date,
        });

        // Copy file to output directory
        const copiedFile = await this.fileProcessor.copyFileToOutput(file);
        if (copiedFile) {
          copiedFiles.push(copiedFile);
        }
      } catch (error) {
        logger.error("Error copying file", {
          file: path.basename(file),
          error: error.message,
        });
        continue;
      }
    }

    logger.info("File copying completed", {
      date,
      totalSourceFiles: files.length,
      copiedFiles: copiedFiles.length,
      copiedFileNames: copiedFiles.map((f) => path.basename(f)),
    });

    // STEP 2: Extract ZIP files in output directory
    let extractedDirs = [];
    for (let i = 0; i < copiedFiles.length; i++) {
      const copiedFile = copiedFiles[i];
      const fileExt = path.extname(copiedFile).toLowerCase();

      // Check if file is actually a ZIP archive (including .gzip files that are ZIP archives)
      const isZipArchive =
        fileExt === ".zip" || (fileExt === ".gzip" && (await this.fileProcessor.isZipFile(copiedFile)));

      if (isZipArchive) {
        try {
          logger.info("Extracting ZIP file", {
            file: path.basename(copiedFile),
            progress: `${i + 1}/${copiedFiles.length}`,
            actualType: fileExt === ".zip" ? "ZIP" : "ZIP-as-GZIP",
            date,
          });

          // Extract ZIP to subdirectory in output directory
          const extractedDir = await this.fileProcessor.extractZipToOutput(copiedFile);
          if (extractedDir) {
            extractedDirs.push(extractedDir);
          }
        } catch (error) {
          logger.error("Error extracting ZIP file", {
            file: path.basename(copiedFile),
            error: error.message,
          });
          continue;
        }
      }
    }

    logger.info("ZIP extraction completed", {
      date,
      totalZipFiles: extractedDirs.length,
      extractedDirs: extractedDirs.length,
      extractedDirNames: extractedDirs.map((d) => path.basename(d)),
    });

    // STEP 3: Find and process all CSV files in output directory
    logger.info("Starting CSV data processing", { date });

    // Find all CSV files in output directory (including subdirectories)
    const csvFiles = await this.findAllCsvFiles(this.fileProcessor.outputDir);

    logger.info("Found CSV files for processing", {
      date,
      csvFileCount: csvFiles.length,
      csvFiles: csvFiles.map((f) => path.relative(this.fileProcessor.outputDir, f)),
    });

    // Process CSV files in parallel for better performance
    let totalRecordsProcessed = 0;

    if (csvFiles.length > 0) {
      logger.info("Starting parallel CSV processing", {
        date,
        totalFiles: csvFiles.length,
      });

      try {
        // Process all CSV files in parallel with the DataProcessor's built-in concurrency control
        logger.info("Starting CSV file processing with simple parallel method", {
          date,
          totalFiles: csvFiles.length,
          maxConcurrency: this.dataProcessor.maxConcurrency,
        });

        totalRecordsProcessed = await this.dataProcessor.processCsvFilesSimple(csvFiles);

        logger.info("All CSV files processed successfully", {
          date,
          totalFiles: csvFiles.length,
          totalRecordsProcessed,
        });
      } catch (error) {
        logger.error("Error during parallel CSV processing", {
          date,
          error: error.message,
          totalFiles: csvFiles.length,
        });
        // Try to continue with aggregation even if some files failed
      }
    }

    logger.info("CSV data processing completed", {
      date,
      totalCsvFiles: csvFiles.length,
      totalRecordsProcessed,
    });

    // Step 4: Data Aggregation and API Preparation
    logger.info("Starting data aggregation", { date });

    const startTime = process.env.FROM;
    const endTime = process.env.TO;

    try {
      const aggregatedData = await this.dataProcessor.aggregateData(startTime, endTime);

      logger.info("Data aggregation completed", {
        date,
        totalAggregatedRecords: aggregatedData.length,
        timeRange: `${startTime} to ${endTime}`,
      });

      // Debug logging for aggregated data details
      logger.debug("Aggregated data ready for transmission", {
        date,
        totalTimePeriods: aggregatedData.length,
        dateRange: `${startTime} to ${endTime}`,
        sampleRecords: aggregatedData.slice(0, 3).map((record, index) => ({
          recordNumber: index + 1,
          timestamp: record.timestamp,
          recordCount: record.recordCount,
          sampleTags: Object.keys(record.data).slice(0, 5),
          totalTags: Object.keys(record.data).length,
        })),
        hasMoreRecords: aggregatedData.length > 3,
        totalRecords: aggregatedData.length,
      });

      // Remaining single end marker (replaced detailed output with debug logging above)

      // Step 5: Send aggregated data to API
      logger.info("Starting API data transmission", {
        date,
        aggregatedRecords: aggregatedData.length,
        totalSourceRecords: totalRecordsProcessed,
        endpoint: process.env.API_ENDPOINT,
      });

      try {
        // Transform aggregated data to AlertData format
        const alertData = aggregatedData.map((record) => {
          // Add timestamp to the data object
          const alertRecord = {
            ...record.data,
            Timestamp: record.timestamp,
          };
          return alertRecord;
        });

        // Validate AlertData before sending
        if (!alertData || alertData.length === 0) {
          logger.info("Skipping API transmission: No AlertData to send", {
            date,
            mappingName: process.env.MAPPING_NAME,
            aggregatedRecords: aggregatedData.length,
            alertDataLength: alertData ? alertData.length : 0,
          });

          return;
        }

        // Send data in batches to avoid 413 Payload Too Large errors
        const BATCH_SIZE = parseInt(process.env.API_BATCH_SIZE) || 50; // Default to 50 records per batch
        const totalBatches = Math.ceil(alertData.length / BATCH_SIZE);

        logger.info("Preparing to send data in batches", {
          date,
          totalRecords: alertData.length,
          batchSize: BATCH_SIZE,
          totalBatches: totalBatches,
          mappingName: process.env.MAPPING_NAME,
        });

        let successfulBatches = 0;
        let failedBatches = 0;
        let totalRecordsSent = 0;

        // Process each batch
        for (let i = 0; i < totalBatches; i++) {
          const batchStart = i * BATCH_SIZE;
          const batchEnd = Math.min(batchStart + BATCH_SIZE, alertData.length);
          const batchData = alertData.slice(batchStart, batchEnd);

          // Prepare data payload for API with required structure
          const apiPayload = {
            [process.env.MAPPING_NAME]: {
              AlertData: batchData,
            },
          };

          try {
            logger.info("Sending batch to API", {
              date,
              batchNumber: i + 1,
              totalBatches,
              batchSize: batchData.length,
              progress: `${i + 1}/${totalBatches}`,
              mappingName: process.env.MAPPING_NAME,
            });

            // Send batch to API endpoint
            const response = await this.httpService.sendData(apiPayload);

            // Handle skipped response
            if (response.skipped) {
              logger.info("API batch transmission skipped by service", {
                date,
                batchNumber: i + 1,
                reason: response.reason,
                message: response.message,
                batchSize: batchData.length,
              });
            } else {
              logger.info("API batch transmission completed successfully", {
                date,
                batchNumber: i + 1,
                totalBatches,
                response: response,
                recordsSent: batchData.length,
                payloadSize: JSON.stringify(apiPayload).length,
                mappingName: process.env.MAPPING_NAME,
              });
            }

            successfulBatches++;
            totalRecordsSent += batchData.length;

            // Small delay between batches to avoid overwhelming the API
            if (i < totalBatches - 1) {
              await new Promise((resolve) => setTimeout(resolve, 100)); // 100ms delay between batches
            }
          } catch (batchError) {
            failedBatches++;
            logger.error("API batch transmission failed", {
              date,
              batchNumber: i + 1,
              totalBatches,
              error: batchError.message,
              stack: batchError.stack,
              endpoint: process.env.API_ENDPOINT,
              batchSize: batchData.length,
              mappingName: process.env.MAPPING_NAME,
              isPayloadTooLarge: batchError.response?.status === 413,
            });

            // If it's a 413 error and batch size is already small, log warning
            if (batchError.response?.status === 413 && BATCH_SIZE <= 50) {
              logger.warn("413 error even with small batch size - consider reducing API_BATCH_SIZE further", {
                currentBatchSize: BATCH_SIZE,
                recommendedBatchSize: Math.min(25, BATCH_SIZE / 2),
              });
            }

            // Continue with next batch instead of failing entirely
            continue;
          }
        }

        // Summary logging
        logger.info("API transmission batching completed", {
          date,
          totalBatches,
          successfulBatches,
          failedBatches,
          totalRecordsSent,
          totalRecords: alertData.length,
          successRate: `${Math.round((successfulBatches / totalBatches) * 100)}%`,
          mappingName: process.env.MAPPING_NAME,
        });

        // Log warning if some batches failed
        if (failedBatches > 0) {
          logger.warn("Some batches failed during API transmission", {
            date,
            failedBatches,
            totalBatches,
            recordsNotSent: alertData.length - totalRecordsSent,
          });
        }
      } catch (error) {
        logger.error("API transmission failed", {
          date,
          error: error.message,
          stack: error.stack,
          endpoint: process.env.API_ENDPOINT,
          recordsCount: alertData ? alertData.length : aggregatedData.length,
          mappingName: process.env.MAPPING_NAME,
        });
        // Don't throw error - log and continue
      }

      logger.info("Data ready for API transmission", {
        date,
        aggregatedRecords: aggregatedData.length,
        totalSourceRecords: totalRecordsProcessed,
      });
    } catch (error) {
      logger.error("Error during data aggregation", {
        date,
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }

    return {
      filesProcessed: copiedFiles.length + extractedDirs.length,
      csvFilesProcessed: csvFiles.length,
      recordsProcessed: totalRecordsProcessed,
    };
  }

  /**
   * Find all CSV files in a directory recursively
   * @private
   * @param {string} directory - Directory to search
   * @returns {Promise<string[]>} Array of CSV file paths
   */
  async findAllCsvFiles(directory) {
    try {
      return await this.fileProcessor.findAllFiles(directory, [".csv"]);
    } catch (error) {
      logger.error("Error finding CSV files", {
        directory,
        error: error.message,
      });
      return [];
    }
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
        logger.info("No data to send", { date });
        return;
      }

      // Add aggregated data to queue
      await this.queueService.addBatchToQueue(data);
      logger.info("Data added to queue", {
        date,
        recordCount: data.length,
        queueStats: this.queueService.getStats(),
      });

      // Process queue in batches
      await this.processQueue();
    } catch (error) {
      logger.error("Error sending aggregated data", {
        date,
        error: error.message,
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

          logger.info("Processed queue batch", {
            batchSize: batch.length,
            successful: result.summary.successful,
            failed: result.summary.failed,
            skipped: result.summary.skipped || 0,
          });

          // Handle failed items by adding them back to queue with increased attempt count
          if (result.errors.length > 0) {
            const failedItems = result.errors.map((error) => ({
              ...batch[error.index],
              attempts: (batch[error.index].attempts || 0) + 1,
            }));

            // Only requeue items that haven't exceeded max attempts
            const itemsToRequeue = failedItems.filter((item) => item.attempts < 3);
            if (itemsToRequeue.length > 0) {
              await this.queueService.addBatchToQueue(itemsToRequeue);
            }

            // Log items that exceeded max attempts
            const failedPermanently = failedItems.filter((item) => item.attempts >= 3);
            if (failedPermanently.length > 0) {
              logger.error("Items exceeded maximum retry attempts", {
                count: failedPermanently.length,
                items: failedPermanently,
              });
            }
          }
        } catch (error) {
          logger.error("Error processing queue batch", {
            error: error.message,
            batchSize: batch.length,
          });
          // Requeue the entire batch
          await this.queueService.addBatchToQueue(
            batch.map((item) => ({
              ...item,
              attempts: (item.attempts || 0) + 1,
            }))
          );
        }
      }

      logger.info("Queue processing completed", {
        totalProcessed,
        totalFailed,
        queueStats: this.queueService.getStats(),
      });
    } catch (error) {
      logger.error("Fatal error processing queue", {
        error: error.message,
        stack: error.stack,
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

      logger.info("Cleanup completed successfully");
    } catch (error) {
      logger.error("Error during cleanup", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Archive output directory and clean up database and files
   * @param {string} fromDate - Start date for archive name
   * @param {string} toDate - End date for archive name
   */
  async archiveAndCleanup(fromDate, toDate) {
    try {
      logger.info("Starting archive and cleanup process", { fromDate, toDate });

      // Step 1: Create archive of output directory
      const archivePath = await this.fileProcessor.createArchive(fromDate, toDate);

      logger.info("Archive created successfully", {
        archivePath: archivePath,
        outputDir: this.fileProcessor.outputDir,
      });

      // Step 2: Clean the output directory
      await this.fileProcessor.cleanOutputDirectory();

      // Step 3: Clear all data from database
      const recordsDeleted = await this.dataProcessor.clearAllData();

      logger.info("Archive and cleanup process completed successfully", {
        archivePath,
        recordsDeleted,
        fromDate,
        toDate,
      });

      logger.info("Archive and cleanup completed successfully", {
        archiveName: path.basename(archivePath),
        archiveLocation: path.dirname(archivePath),
        outputDirectoryCleaned: this.fileProcessor.outputDir,
        recordsDeleted,
        fromDate,
        toDate,
      });

      return {
        archivePath,
        recordsDeleted,
      };
    } catch (error) {
      logger.error("Error during archive and cleanup process", {
        error: error.message,
        stack: error.stack,
        fromDate,
        toDate,
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
    // Print environment configuration table at startup
    printEnvironmentTable();

    const fromDate = process.env.FROM.split(" ")[0];
    const toDate = process.env.TO.split(" ")[0];

    if (!fromDate || !toDate) {
      throw new Error("FROM and TO dates are required");
    }

    await app.processDateRange(fromDate, toDate);

    // Archive output directory and clean up database and files
    await app.archiveAndCleanup(fromDate, toDate);

    await app.cleanup();

    logger.info("Application completed successfully");
    process.exit(0);
  } catch (error) {
    logger.error("Application failed", {
      error: error.message,
      stack: error.stack,
    });
    process.exit(1);
  }
};

// Handle process signals
process.on("SIGTERM", async () => {
  logger.info("Received SIGTERM signal");
  await app.cleanup();
  process.exit(0);
});

process.on("SIGINT", async () => {
  logger.info("Received SIGINT signal");
  await app.cleanup();
  process.exit(0);
});

// Start the application
main();
