// Test service file to simulate FileProcessor calling logger
const logger = require("../config/logger");

class TestFileProcessor {
  static testLogging() {
    logger.info("FileProcessor is processing files", {
      batchSize: 100,
      filesFound: 25,
    });

    logger.warn("Processing delay detected", {
      expectedTime: "2s",
      actualTime: "5s",
    });
  }
}

// Export and call the test
module.exports = TestFileProcessor;
TestFileProcessor.testLogging();
