const winston = require("winston");
const path = require("path");
const os = require("os");

/**
 * Get service name from call stack
 */
const getServiceName = () => {
  const stack = new Error().stack;
  const stackLines = stack.split("\n");

  for (let i = 1; i < stackLines.length; i++) {
    const line = stackLines[i];

    // Skip logger-related files and winston
    if (line.includes("logger.js") || line.includes("winston") || line.includes("node_modules")) {
      continue;
    }

    // Look for service files first (highest priority)
    if (line.includes("/services/")) {
      const serviceMatch = line.match(/\/services\/([^/]+)\.js/);
      if (serviceMatch) {
        return serviceMatch[1];
      }
    }

    // Look for other source files that might contain service logic
    if (line.includes("/src/")) {
      // Try to match common service patterns in the path or class name
      const patterns = [
        /\/([A-Z][a-zA-Z]*(?:Service|Processor|Adapter|Manager|Handler|Controller))\.js/,
        /\/([A-Z][a-zA-Z]*(?:Service|Processor|Adapter))\.js/,
        /at ([A-Z][a-zA-Z]*(?:Service|Processor|Adapter))\./,
        /\/src\/[^/]*\/([A-Z][^/\s.]+)\.js/,
      ];

      for (const pattern of patterns) {
        const match = line.match(pattern);
        if (match && !["logger", "index", "Logger"].includes(match[1])) {
          return match[1];
        }
      }
    }

    // Check for specific file patterns in any directory
    const filePatterns = [
      /\/([A-Z][a-zA-Z]*(?:Processor|Service|Adapter|Manager|Handler))\.js/,
      /at Object\.([a-zA-Z]+Processor|[a-zA-Z]+Service|[a-zA-Z]+Adapter)/,
      /at ([A-Z][a-zA-Z]*)\.[a-zA-Z]/,
    ];

    for (const pattern of filePatterns) {
      const match = line.match(pattern);
      if (match && !["Object", "logger", "index", "Logger", "Error"].includes(match[1])) {
        return match[1];
      }
    }
  }
  return "Application";
};

/**
 * Environment information format
 */
const environmentInfo = winston.format((info) => {
  info.serviceName = info.serviceName || getServiceName();
  info.tenantId = process.env.TENANT_ID || "default";
  info.mappingName = process.env.MAPPING_NAME || "default-mapping";
  info.batchDate = new Date().toISOString().split("T")[0];
  return info;
})();

/**
 * Custom structured format with brackets and colors
 */
const structuredFormat = winston.format.combine(
  environmentInfo,
  winston.format.timestamp({
    format: "YYYY-MM-DD HH:mm:ss.SSS",
  }),
  winston.format.errors({ stack: true }),
  winston.format.printf(({ level, message, timestamp, serviceName, tenantId, mappingName, batchDate, ...meta }) => {
    const metaString = Object.keys(meta).length > 0 ? ` ${JSON.stringify(meta)}` : "";
    return `[${timestamp}] [${serviceName}] : [${tenantId}] : [${mappingName}] : [${batchDate}] : [${level.toUpperCase()}] : ${message}${metaString}`;
  })
);

/**
 * Console format with colors
 */
const colorizedConsoleFormat = winston.format.combine(
  environmentInfo,
  winston.format.timestamp({
    format: "YYYY-MM-DD HH:mm:ss.SSS",
  }),
  winston.format.errors({ stack: true }),
  winston.format.printf(({ level, message, timestamp, serviceName, tenantId, mappingName, batchDate, ...meta }) => {
    // Color codes
    const colors = {
      timestamp: "\x1b[90m", // Gray
      serviceName: "\x1b[35m", // Magenta
      tenantId: "\x1b[36m", // Cyan
      mappingName: "\x1b[33m", // Yellow
      batchDate: "\x1b[32m", // Green
      info: "\x1b[34m", // Blue
      warn: "\x1b[93m", // Bright Yellow
      error: "\x1b[91m", // Bright Red
      debug: "\x1b[94m", // Bright Blue
      reset: "\x1b[0m", // Reset
    };

    const levelColor = colors[level.toLowerCase()] || colors.info;
    const metaString = Object.keys(meta).length > 0 ? ` ${JSON.stringify(meta)}` : "";

    return `${colors.timestamp}[${timestamp}]${colors.reset} ${colors.serviceName}[${serviceName}]${colors.reset} : ${
      colors.tenantId
    }[${tenantId}]${colors.reset} : ${colors.mappingName}[${mappingName}]${colors.reset} : ${
      colors.batchDate
    }[${batchDate}]${colors.reset} : ${levelColor}[${level.toUpperCase()}]${colors.reset} : ${message}${metaString}`;
  })
);

/**
 * Custom format for log messages (JSON for files)
 */
const customFormat = winston.format.combine(
  environmentInfo,
  winston.format.timestamp({
    format: "YYYY-MM-DD HH:mm:ss.SSS",
  }),
  winston.format.errors({ stack: true }),
  winston.format.metadata({
    fillExcept: [
      "message",
      "level",
      "timestamp",
      "serviceName",
      "tenantId",
      "mappingName",
      "batchDate",
      "hostname",
      "env",
      "version",
      "traceId",
      "spanId",
      "requestId",
    ],
  }),
  winston.format.json()
);

/**
 * Custom format for console output
 */
const consoleFormat = winston.format.combine(colorizedConsoleFormat);

/**
 * Logger configuration
 */
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || "info",
  format: customFormat,
  defaultMeta: {
    service: process.env.SERVICE_NAME || "das-data-recovery",
    environment: process.env.NODE_ENV || "development",
  },
  transports: [
    // Error logs
    new winston.transports.File({
      filename: path.join("logs", "error.log"),
      level: "error",
      format: structuredFormat, // Use bracket format for files too
      maxsize: 10485760, // 10MB
      maxFiles: 5,
      tailable: true,
    }),
    // Combined logs
    new winston.transports.File({
      filename: path.join("logs", "combined.log"),
      format: structuredFormat, // Use bracket format for files too
      maxsize: 10485760, // 10MB
      maxFiles: 5,
      tailable: true,
    }),
    // Debug logs
    new winston.transports.File({
      filename: path.join("logs", "debug.log"),
      level: "debug",
      format: structuredFormat, // Use bracket format for files too
      maxsize: 10485760, // 10MB
      maxFiles: 3,
      tailable: true,
    }),
  ],
});

// Add console transport in development
if (process.env.NODE_ENV !== "production") {
  logger.add(
    new winston.transports.Console({
      format: consoleFormat,
      level: "debug",
    })
  );
}

// Add error handling for uncaught exceptions and unhandled rejections
logger.exceptions.handle(
  new winston.transports.File({
    filename: path.join("logs", "exceptions.log"),
    maxsize: 10485760, // 10MB
    maxFiles: 5,
  })
);

process.on("unhandledRejection", (error) => {
  logger.error("Unhandled Rejection", {
    error: error.message,
    stack: error.stack,
  });
});

// Add service context method
logger.withService = function (serviceName) {
  return this.child({ serviceName });
};

module.exports = logger;
