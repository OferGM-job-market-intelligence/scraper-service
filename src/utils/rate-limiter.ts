/**
 * Rate Limiter
 *
 * Sliding-window rate limiting backed by Redis INCR + TTL.
 * Each scraping source (linkedin, indeed, etc.) gets its own counter key.
 *
 * Algorithm:
 *   1. INCR the key `ratelimit:{source}:{hourBucket}`
 *   2. If the key is new (count === 1), set TTL to 1 hour
 *   3. If count exceeds MAX_REQUESTS_PER_HOUR, reject the request
 *
 * This gives us per-source, per-hour rate limiting with automatic cleanup.
 */

import type Redis from "ioredis";
import { config } from "../config/env.js";
import { logger } from "./logger.js";

export class RateLimiter {
  private readonly redis: Redis.Redis;
  private readonly maxRequests: number;
  private readonly windowSeconds: number;

  constructor(redis: Redis.Redis) {
    this.redis = redis;
    this.maxRequests = config.MAX_REQUESTS_PER_HOUR;
    this.windowSeconds = 3600; // 1 hour window
  }

  /**
   * Check whether a request from the given source is allowed.
   * Returns true if under the limit, false if rate-limited.
   */
  async isAllowed(source: string): Promise<boolean> {
    const hourBucket = Math.floor(Date.now() / (this.windowSeconds * 1000));
    const key = `ratelimit:${source}:${hourBucket}`;

    try {
      const count = await this.redis.incr(key);

      // First request in this window — set expiry so the key auto-cleans
      if (count === 1) {
        await this.redis.expire(key, this.windowSeconds);
      }

      if (count > this.maxRequests) {
        logger.warn("Rate limit exceeded", {
          source,
          count,
          maxRequests: this.maxRequests,
        });
        return false;
      }

      return true;
    } catch (error) {
      // If Redis is down, fail open — let the request through.
      // Logging the failure is sufficient; blocking scraping on a cache
      // outage would be worse than temporarily exceeding rate limits.
      logger.error("Rate limit check failed — failing open", {
        source,
        error: error instanceof Error ? error.message : String(error),
      });
      return true;
    }
  }

  /**
   * Returns the current count and remaining quota for a source.
   * Useful for logging / health checks.
   */
  async getStatus(
    source: string,
  ): Promise<{ count: number; remaining: number }> {
    const hourBucket = Math.floor(Date.now() / (this.windowSeconds * 1000));
    const key = `ratelimit:${source}:${hourBucket}`;

    try {
      const countStr = await this.redis.get(key);
      const count = countStr ? parseInt(countStr, 10) : 0;
      return {
        count,
        remaining: Math.max(0, this.maxRequests - count),
      };
    } catch {
      return { count: 0, remaining: this.maxRequests };
    }
  }
}
