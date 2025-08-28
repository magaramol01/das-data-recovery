const logger = require('../utils/logger');
const FROM = process.env.FROM;
const TO = process.env.TO;

function getDateRange(from, to) {
  const start = new Date(from);
  const end = new Date(to);
  const result = [];

  let current = new Date(start);
  while (current <= end) {
    result.push(current.toISOString().slice(0, 10)); // format YYYY-MM-DD
    current.setDate(current.getDate() + 1);
  }

  return result;
}

const TOTAL_TIME_PERIOD = getDateRange(FROM, TO);
logger.info('Time period calculated', { TOTAL_TIME_PERIOD });
