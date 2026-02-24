/**
 * Redis Client
 *
 * Singleton Redis connection used for:
 *   1. Rate limiting  — per-source request counters (see RateLimiter)
 *   2. Deduplication  — URL hashes to skip already-scraped jobs
 *
 * Connection is lazy — created on first access and reused thereafter.
 * Includes automatic reconnection and health-check support.
 */

import Redis from "ioredis";
import { config } from "../config/env.js";
import { logger } from "../utils/logger.js";

// ---------------------------------------------------------------------------
// Singleton client
// ---------------------------------------------------------------------------

let client: Redis.Redis | null = null;

/**
 * Returns the shared Redis client, creating it on first call.
 * Safe to call from anywhere — always returns the same instance.
 */
export function getRedisClient(): Redis.Redis {
  if (client) return client;

  client = new Redis.Redis({
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    password: config.REDIS_PASSWORD || undefined,
    maxRetriesPerRequest: 3,
    retryStrategy(times: number) {
      // Exponential backoff: 200ms, 400ms, 800ms, … capped at 10s
      const delay = Math.min(times * 200, 10_000);
      logger.warn("Redis reconnecting", { attempt: times, delayMs: delay });
      return delay;
    },
    // Don't throw if initial connection fails — let retries handle it
    lazyConnect: false,
  });

  client.on("connect", () => {
    logger.info("Redis connected", {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
    });
  });

  client.on("error", (error: Error) => {
    logger.error("Redis error", { error: error.message });
  });

  client.on("close", () => {
    logger.warn("Redis connection closed");
  });

  return client;
}

// ---------------------------------------------------------------------------
// Deduplication helpers
// ---------------------------------------------------------------------------

/**
 * Generate a deterministic hash for a job URL.
 * Used as the Redis key for dedup lookups.
 */
function hashJobUrl(url: string): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(url);
  return `dedup:${hasher.digest("hex")}`;
}

/**
 * Check if a job URL has already been scraped (exists in Redis).
 *
 * @returns true if the job is a duplicate, false if it's new
 */
export async function isDuplicate(url: string): Promise<boolean> {
  const redis = getRedisClient();
  const key = hashJobUrl(url);

  try {
    const exists = await redis.exists(key);
    return exists === 1;
  } catch (error) {
    // If Redis is down, assume not a duplicate — better to re-process
    // than to silently drop a job
    logger.error("Dedup check failed — assuming not duplicate", {
      url,
      error: error instanceof Error ? error.message : String(error),
    });
    return false;
  }
}

/**
 * Mark a job URL as scraped. Sets a key with the configured TTL
 * so entries auto-expire after DEDUP_TTL_SECONDS (default 30 days).
 */
export async function markAsScraped(url: string): Promise<void> {
  const redis = getRedisClient();
  const key = hashJobUrl(url);

  try {
    await redis.set(key, "1", "EX", config.DEDUP_TTL_SECONDS);
  } catch (error) {
    // Non-fatal — worst case we'll scrape the job again next cycle
    logger.error("Failed to mark job as scraped", {
      url,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/**
 * Gracefully close the Redis connection.
 * Call during service shutdown.
 */
export async function disconnectRedis(): Promise<void> {
  if (client) {
    await client.quit();
    client = null;
    logger.info("Redis disconnected gracefully");
  }
}

/**
 * Health check — returns true if Redis responds to PING.
 */
export async function isRedisHealthy(): Promise<boolean> {
  try {
    const redis = getRedisClient();
    const pong = await redis.ping();
    return pong === "PONG";
  } catch {
    return false;
  }
}
