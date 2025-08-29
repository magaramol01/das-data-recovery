const path = require("path");
const fs = require("fs-extra");
const { promisify } = require("util");
const { pipeline } = require("stream");
const glob = require("glob");
const extract = require("extract-zip");
const archiver = require("archiver");
const zlib = require("zlib");
const gunzip = promisify(zlib.gunzip);
const baseLogger = require("../config/logger");

// Create service-specific logger
const logger = baseLogger.withService("FileProcessor");

/**
 * FileProcessor Service
 * Handles all file-related operations including finding, processing, and archiving files
 */
class FileProcessor {
  /**
   * Create a new FileProcessor instance
   * @param {Object} config - Configuration object
   * @param {string} config.workingDir - Directory containing source files
   * @param {string} config.outputDir - Directory for processed files
   * @param {string} config.archiveDir - Directory for archived files
   */
  constructor(config) {
    this.workingDir = config.workingDir;
    this.outputDir = config.outputDir;
    this.archiveDir = config.archiveDir;

    // Supported file types
    this.supportedExtensions = [".zip", ".csv", ".gz", ".gzip"];
  }

  /**
   * Initialize the file processor
   * @returns {Promise<void>}
   */
  async initialize() {
    try {
      // Ensure all required directories exist
      await Promise.all([fs.ensureDir(this.workingDir), fs.ensureDir(this.outputDir), fs.ensureDir(this.archiveDir)]);

      logger.info("FileProcessor initialized", {
        workingDir: this.workingDir,
        outputDir: this.outputDir,
        archiveDir: this.archiveDir,
      });
    } catch (error) {
      logger.error("Failed to initialize FileProcessor", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Find files for a specific date
   * @param {string} searchDate - Date in YYYY-MM-DD format
   * @returns {Promise<string[]>} Array of file paths
   */
  async findFilesForDate(searchDate) {
    try {
      let allFiles = [];

      // Search for each extension separately to avoid glob pattern issues
      // Using recursive pattern to search in all subdirectories
      for (const ext of this.supportedExtensions) {
        const pattern = `**/*${searchDate}*${ext}`;
        try {
          const files = glob.sync(pattern, {
            cwd: this.workingDir,
            nocase: true, // Case insensitive matching
            dot: false, // Don't match hidden files
            nonull: false, // Don't return pattern if no match
          });

          const fullPaths = files.map((match) => path.join(this.workingDir, match));
          allFiles = allFiles.concat(fullPaths);

          if (files.length > 0) {
            logger.debug(`Found ${files.length} files with extension ${ext}`, {
              files: files,
              pattern,
            });
          }
        } catch (extError) {
          logger.warn(`Error searching for ${ext} files`, {
            extension: ext,
            pattern,
            error: extError.message,
          });
        }
      }

      // Remove duplicates (in case of overlapping patterns)
      const uniqueFiles = [...new Set(allFiles)];

      logger.info("Found files for date", {
        date: searchDate,
        count: uniqueFiles.length,
        extensions: this.supportedExtensions,
        files: uniqueFiles.map((f) => path.basename(f)),
        directories: uniqueFiles.map((f) => path.dirname(f)).filter((dir, index, arr) => arr.indexOf(dir) === index),
      });

      return uniqueFiles;
    } catch (error) {
      logger.error("Error finding files", {
        date: searchDate,
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Process a single file
   * @param {string} filePath - Path to the file
   * @returns {Promise<string>} Path to the processed file
   */
  async processFile(filePath) {
    try {
      const stats = await fs.stat(filePath);
      if (!stats.isFile()) {
        logger.warn("Skipping non-file", { path: filePath });
        return null;
      }

      const fileName = path.basename(filePath);
      const fileExt = path.extname(fileName).toLowerCase();
      const processingStartTime = Date.now();

      // Check if it's actually a ZIP file (many .gzip files are actually ZIP archives)
      const isZipFile = await this.isZipFile(filePath);

      // Process based on actual file type
      let processedPath;
      if (isZipFile || fileExt === ".zip") {
        processedPath = await this.processZipFile(filePath);
      } else if (fileExt === ".csv") {
        processedPath = await this.processCsvFile(filePath);
      } else if ([".gz", ".gzip"].includes(fileExt)) {
        processedPath = await this.processGzipFile(filePath);
      } else {
        logger.warn("Unsupported file type", { path: filePath, extension: fileExt });
        return null;
      }

      const processingTime = Date.now() - processingStartTime;
      logger.info("File processed successfully", {
        source: filePath,
        destination: processedPath,
        size: stats.size,
        processingTime,
        actualType: isZipFile ? "ZIP" : fileExt.toUpperCase(),
      });

      // Original file is kept in place, no archiving needed
      return processedPath;
    } catch (error) {
      logger.error("Error processing file", {
        path: filePath,
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Check if a file is actually a ZIP file by reading its header
   * @param {string} filePath - Path to the file
   * @returns {Promise<boolean>} True if the file is a ZIP file
   */
  async isZipFile(filePath) {
    try {
      const readStream = fs.createReadStream(filePath, { start: 0, end: 3 });
      const buffer = await new Promise((resolve, reject) => {
        const chunks = [];
        readStream.on("data", (chunk) => chunks.push(chunk));
        readStream.on("end", () => resolve(Buffer.concat(chunks)));
        readStream.on("error", reject);
      });

      if (buffer.length < 4) {
        return false;
      }

      // Check for ZIP file signature (PK\x03\x04 or PK\x05\x06 or PK\x07\x08)
      return (
        buffer[0] === 0x50 && buffer[1] === 0x4b && (buffer[2] === 0x03 || buffer[2] === 0x05 || buffer[2] === 0x07)
      );
    } catch (error) {
      logger.debug("Error checking file type", { path: filePath, error: error.message });
      return false;
    }
  }

  /**
   * Process a ZIP file
   * @private
   * @param {string} filePath - Path to the ZIP file
   * @returns {Promise<string>} Path to the extracted directory
   */
  async processZipFile(filePath) {
    const extractDir = path.resolve(this.outputDir, path.basename(filePath, ".zip"));
    await fs.ensureDir(extractDir);
    await extract(filePath, { dir: extractDir });
    return extractDir;
  }

  /**
   * Process a CSV file
   * @private
   * @param {string} filePath - Path to the CSV file
   * @returns {Promise<string>} Path to the processed file
   */
  async processCsvFile(filePath) {
    const targetPath = path.resolve(this.outputDir, path.basename(filePath));
    await fs.copy(filePath, targetPath);
    return targetPath;
  }

  /**
   * Process a GZIP file
   * @private
   * @param {string} filePath - Path to the GZIP file
   * @returns {Promise<string>} Path to the decompressed file
   */
  async processGzipFile(filePath) {
    const targetPath = path.resolve(this.outputDir, path.basename(filePath).replace(/\.gz$|\.gzip$/, ""));

    const readStream = fs.createReadStream(filePath);
    const writeStream = fs.createWriteStream(targetPath);
    const gunzipStream = zlib.createGunzip();

    return new Promise((resolve, reject) => {
      pipeline(readStream, gunzipStream, writeStream, (error) => {
        if (error) {
          reject(error);
        } else {
          resolve(targetPath);
        }
      });
    });
  }

  /**
   * Archive a processed file
   * @private
   * @param {string} filePath - Path to the file to archive
   * @returns {Promise<string>} Path to the archived file
   */
  async archiveFile(filePath) {
    try {
      const archivePath = path.resolve(this.archiveDir, path.basename(filePath));

      await fs.move(filePath, archivePath, { overwrite: true });

      logger.debug("File archived", {
        original: filePath,
        archived: archivePath,
      });

      return archivePath;
    } catch (error) {
      logger.error("Error archiving file", {
        path: filePath,
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Clean up temporary files
   * @returns {Promise<void>}
   */
  async cleanup() {
    try {
      // Remove processed files older than 24 hours
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);

      const files = glob.sync("*", { cwd: this.outputDir }).map((match) => path.resolve(this.outputDir, match));

      for (const file of files) {
        const stats = await fs.stat(file);
        if (stats.mtime < yesterday) {
          await fs.remove(file);
          logger.debug("Removed old processed file", { path: file });
        }
      }

      logger.info("Cleanup completed");
    } catch (error) {
      logger.error("Error during cleanup", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Copy file to output directory (Step 1 of new architecture)
   * @param {string} filePath - Path to the source file
   * @returns {Promise<string>} Path to the copied file
   */
  async copyFileToOutput(filePath) {
    try {
      const stats = await fs.stat(filePath);
      if (!stats.isFile()) {
        logger.warn("Skipping non-file", { path: filePath });
        return null;
      }

      const fileName = path.basename(filePath);
      let outputPath = path.resolve(this.outputDir, fileName);

      // Handle duplicate filenames by adding a counter
      let counter = 1;
      const fileExt = path.extname(fileName);
      const baseName = path.basename(fileName, fileExt);

      while (await fs.pathExists(outputPath)) {
        const newFileName = `${baseName}_${counter}${fileExt}`;
        outputPath = path.resolve(this.outputDir, newFileName);
        counter++;
      }

      // Copy file to output directory
      await fs.copy(filePath, outputPath);

      logger.info("File copied successfully", {
        source: filePath,
        destination: outputPath,
        size: stats.size,
        isDuplicate: counter > 1,
      });

      return outputPath;
    } catch (error) {
      logger.error("Error copying file", {
        path: filePath,
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Extract ZIP file to subdirectory in output directory (Step 2 of new architecture)
   * @param {string} zipFilePath - Path to the ZIP file in output directory
   * @returns {Promise<string>} Path to the extracted directory
   */
  async extractZipToOutput(zipFilePath) {
    try {
      // Create subdirectory for extraction - handle both .zip and .gzip extensions
      const fileName = path.basename(zipFilePath);
      const zipName = fileName.replace(/\.(zip|gzip)$/i, "");
      const extractDir = path.resolve(this.outputDir, zipName);
      await fs.ensureDir(extractDir);

      // Extract ZIP to subdirectory
      await extract(zipFilePath, { dir: extractDir });

      // Delete original ZIP file after successful extraction
      await fs.remove(zipFilePath);

      logger.info("ZIP extraction completed", {
        zipFile: zipFilePath,
        extractedTo: extractDir,
        originalZipDeleted: true,
      });

      return extractDir;
    } catch (error) {
      logger.error("Error extracting ZIP file", {
        path: zipFilePath,
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  /**
   * Create a ZIP archive of the output directory
   * @param {string} fromDate - Start date for archive name
   * @param {string} toDate - End date for archive name
   * @returns {Promise<string>} Path to created archive
   */
  async createArchive(fromDate, toDate) {
    try {
      const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, -5);
      const archiveName = `data-recovery_${fromDate}_to_${toDate}_${timestamp}.zip`;
      const archivePath = path.resolve(this.archiveDir, archiveName);

      logger.info("Starting archive creation", {
        outputDir: this.outputDir,
        archivePath,
        fromDate,
        toDate,
      });

      // Ensure archive directory exists
      await fs.ensureDir(this.archiveDir);

      // Create archive
      const archive = archiver("zip", {
        zlib: { level: 9 }, // Maximum compression
      });

      const output = fs.createWriteStream(archivePath);

      return new Promise((resolve, reject) => {
        output.on("close", () => {
          logger.info("Archive created successfully", {
            archivePath,
            totalBytes: archive.pointer(),
            finalSize: `${(archive.pointer() / 1024 / 1024).toFixed(2)} MB`,
          });
          resolve(archivePath);
        });

        archive.on("warning", (err) => {
          if (err.code === "ENOENT") {
            logger.warn("Archive warning", { error: err.message });
          } else {
            reject(err);
          }
        });

        archive.on("error", (err) => {
          reject(err);
        });

        // Pipe archive data to the file
        archive.pipe(output);

        // Add entire output directory to archive
        archive.directory(this.outputDir, false);

        // Finalize the archive
        archive.finalize();
      });
    } catch (error) {
      logger.error("Error creating archive", {
        error: error.message,
        stack: error.stack,
        outputDir: this.outputDir,
        archiveDir: this.archiveDir,
      });
      throw error;
    }
  }

  /**
   * Clean output directory after archiving
   * @returns {Promise<void>}
   */
  async cleanOutputDirectory() {
    try {
      logger.info("Cleaning output directory", {
        outputDir: this.outputDir,
      });

      // Get all files and directories in output directory
      const items = await fs.readdir(this.outputDir);

      for (const item of items) {
        const itemPath = path.resolve(this.outputDir, item);
        await fs.remove(itemPath);
        logger.debug("Removed output item", { path: itemPath });
      }

      logger.info("Output directory cleaned successfully", {
        outputDir: this.outputDir,
        itemsRemoved: items.length,
      });
    } catch (error) {
      logger.error("Error cleaning output directory", {
        error: error.message,
        stack: error.stack,
        outputDir: this.outputDir,
      });
      throw error;
    }
  }

  /**
   * Find all files with specific extensions in a directory recursively
   * @param {string} dir - Directory to search
   * @param {string[]} extensions - File extensions to find
   * @returns {Promise<string[]>} Array of file paths
   */
  async findAllFiles(dir, extensions) {
    const pattern =
      extensions.length === 1 ? `**/*${extensions[0]}` : `**/*.{${extensions.map((ext) => ext.slice(1)).join(",")}}`;

    return glob.sync(pattern, { cwd: dir }).map((match) => path.join(dir, match));
  }
}

module.exports = FileProcessor;
