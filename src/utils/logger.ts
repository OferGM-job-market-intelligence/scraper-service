/**
 * Structured Logger
 *
 * Outputs JSON lines with consistent fields for easy parsing by
 * Elasticsearch / Kibana / any log aggregator.
 *
 * Usage:
 *   import { logger } from "@/utils/logger";
 *   logger.info("Job scraped", { jobId: "linkedin_123", source: "linkedin" });
 *
 * Output:
 *   {"timestamp":"2026-02-13T10:30:00.000Z","level":"info","service":"scraper-service","message":"Job scraped","jobId":"linkedin_123","source":"linkedin"}
 */

import { config } from "../config/env.js";

type LogLevel = "debug" | "info" | "warn" | "error";

// Numeric severity for filtering — only log at or above the configured level
const LOG_LEVELS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

interface LogEntry {
  timestamp: string;
  level: LogLevel;
  service: string;
  message: string;
  [key: string]: unknown;
}

class Logger {
  private readonly service: string;
  private readonly minLevel: number;

  constructor(service: string, level: LogLevel) {
    this.service = service;
    this.minLevel = LOG_LEVELS[level];
  }

  /**
   * Creates a child logger that always includes the given context fields.
   * Useful for request-scoped or scraper-scoped logging.
   */
  child(context: Record<string, unknown>): ChildLogger {
    return new ChildLogger(this, context);
  }

  debug(message: string, meta?: Record<string, unknown>): void {
    this.log("debug", message, meta);
  }

  info(message: string, meta?: Record<string, unknown>): void {
    this.log("info", message, meta);
  }

  warn(message: string, meta?: Record<string, unknown>): void {
    this.log("warn", message, meta);
  }

  error(message: string, meta?: Record<string, unknown>): void {
    this.log("error", message, meta);
  }

  /** Core log method — serialises to JSON and writes to stdout/stderr */
  log(level: LogLevel, message: string, meta?: Record<string, unknown>): void {
    if (LOG_LEVELS[level] < this.minLevel) return;

    const entry: LogEntry = {
      timestamp: new Date().toISOString(),
      level,
      service: this.service,
      message,
      ...meta,
    };

    const line = JSON.stringify(entry);

    if (level === "error") {
      process.stderr.write(line + "\n");
    } else {
      process.stdout.write(line + "\n");
    }
  }
}

/**
 * Child logger — wraps a parent logger and merges context into every call.
 */
class ChildLogger {
  constructor(
    private readonly parent: Logger,
    private readonly context: Record<string, unknown>,
  ) {}

  debug(message: string, meta?: Record<string, unknown>): void {
    this.parent.debug(message, { ...this.context, ...meta });
  }

  info(message: string, meta?: Record<string, unknown>): void {
    this.parent.info(message, { ...this.context, ...meta });
  }

  warn(message: string, meta?: Record<string, unknown>): void {
    this.parent.warn(message, { ...this.context, ...meta });
  }

  error(message: string, meta?: Record<string, unknown>): void {
    this.parent.error(message, { ...this.context, ...meta });
  }
}

/** Singleton logger for the scraper service */
export const logger = new Logger(config.SERVICE_NAME, config.LOG_LEVEL);

export { Logger, ChildLogger };
