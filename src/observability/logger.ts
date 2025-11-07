import pino from 'pino';
import { appConfig } from '../config/index.js';

const isDevelopment = appConfig.server.environment === 'development';

export const logger = pino({
  level: appConfig.observability.logLevel,
  transport: isDevelopment ? {
    target: 'pino-pretty',
    options: {
      colorize: true,
      translateTime: 'HH:MM:ss Z',
      ignore: 'pid,hostname',
    },
  } : undefined,
  formatters: {
    level: (label) => {
      return { level: label.toUpperCase() };
    },
  },
  timestamp: pino.stdTimeFunctions.isoTime,
});

export interface LogContext {
  correlationId?: string;
  userId?: string;
  operation?: string;
  duration?: number;
  [key: string]: any;
}

export const createContextLogger = (context: LogContext) => {
  return logger.child(context);
};

export default logger;