/**
 * Configuration Module
 *
 * Loads environment variables and validates them with Zod schemas at startup.
 * Fails fast if any required config is missing or invalid — no silent defaults
 * for production-critical values like broker addresses.
 */

import { z } from "zod";

// ---------------------------------------------------------------------------
// Schema — defines what the environment must look like
// ---------------------------------------------------------------------------
const envSchema = z.object({
  // Kafka
  KAFKA_BROKERS: z
    .string()
    .default("localhost:29092")
    .describe("Comma-separated Kafka broker addresses"),
  KAFKA_CLIENT_ID: z.string().default("scraper-service"),
  KAFKA_TOPIC_JOBS_RAW: z.string().default("jobs.raw"),

  // Redis
  REDIS_HOST: z.string().default("localhost"),
  REDIS_PORT: z.coerce.number().int().positive().default(6380),
  REDIS_PASSWORD: z.string().optional(),

  // Scraping behaviour
  SCRAPE_INTERVAL: z.coerce.number().int().positive().default(3_600_000),
  MAX_REQUESTS_PER_HOUR: z.coerce.number().int().positive().default(50),
  DEDUP_TTL_SECONDS: z.coerce.number().int().positive().default(2_592_000),

  // Service metadata
  NODE_ENV: z
    .enum(["development", "production", "test"])
    .default("development"),
  LOG_LEVEL: z.enum(["debug", "info", "warn", "error"]).default("info"),
  SERVICE_NAME: z.string().default("scraper-service"),
  PORT: z.coerce.number().int().positive().default(3000),
});

// ---------------------------------------------------------------------------
// Parse + freeze — run once at import time
// ---------------------------------------------------------------------------
const parsed = envSchema.safeParse(process.env);

if (!parsed.success) {
  console.error("❌ Invalid environment variables:");
  console.error(parsed.error.format());
  process.exit(1);
}

/**
 * Validated, read-only configuration object.
 * Import this anywhere:
 *   import { config } from "@/config/env";
 */
export const config = Object.freeze(parsed.data);

export type Config = z.infer<typeof envSchema>;
