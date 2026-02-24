/**
 * Scraper Service — Entry Point
 *
 * Orchestrates the scraping pipeline:
 *   1. Validate config (Zod — fails fast on bad env vars)
 *   2. Connect to Redis (dedup + rate limiting)
 *   3. Connect to Kafka producer (job publishing)
 *   4. Register all scrapers (LinkedIn, Indeed, …)
 *   5. Run an initial scrape cycle
 *   6. Schedule recurring cycles at SCRAPE_INTERVAL
 *   7. Expose /health endpoint for Docker + Kubernetes readiness probes
 *   8. Handle SIGINT/SIGTERM for graceful shutdown
 *
 * @module scraper-service
 * @version 1.0.0
 */

import { config } from "./config/env.js";
import { logger } from "./utils/logger.js";
import {
  getRedisClient,
  disconnectRedis,
  isRedisHealthy,
} from "./redis/client.js";
import { disconnectProducer } from "./kafka/producer.js";
import {
  createAllScrapers,
  type BaseScraper,
  type ScrapeResult,
} from "./scrapers/index.js";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

let scrapers: BaseScraper[] = [];
let schedulerTimer: ReturnType<typeof setInterval> | null = null;
let isShuttingDown = false;

// ---------------------------------------------------------------------------
// Scrape Cycle
// ---------------------------------------------------------------------------

/**
 * Runs all registered scrapers sequentially.
 * Sequential (not parallel) to respect aggregate rate limits.
 */
async function runScrapeCycle(): Promise<void> {
  if (isShuttingDown) return;

  logger.info("=== Scrape cycle starting ===", {
    scraperCount: scrapers.length,
  });

  const results: Record<string, ScrapeResult> = {};

  for (const scraper of scrapers) {
    if (isShuttingDown) break;

    try {
      const result = await scraper.run();
      results[scraper.source] = result;
    } catch (error) {
      logger.error("Scraper failed unexpectedly", {
        scraper: scraper.name,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  // Aggregate stats for the cycle
  const totals = Object.values(results).reduce(
    (acc, r) => ({
      found: acc.found + r.found,
      published: acc.published + r.published,
      duplicates: acc.duplicates + r.duplicates,
    }),
    { found: 0, published: 0, duplicates: 0 },
  );

  logger.info("=== Scrape cycle complete ===", {
    ...totals,
    nextCycleIn: `${config.SCRAPE_INTERVAL / 1000}s`,
  });
}

// ---------------------------------------------------------------------------
// Health Server
// ---------------------------------------------------------------------------

/**
 * Minimal HTTP server for health checks.
 * Kubernetes liveness/readiness probes hit GET /health.
 */
function startHealthServer(): void {
  Bun.serve({
    port: config.PORT,
    async fetch(req: Request): Promise<Response> {
      const url = new URL(req.url);

      if (url.pathname === "/health") {
        const redisOk = await isRedisHealthy();

        const status = redisOk ? 200 : 503;
        return new Response(
          JSON.stringify({
            status: redisOk ? "healthy" : "degraded",
            service: config.SERVICE_NAME,
            uptime: process.uptime(),
            redis: redisOk ? "connected" : "disconnected",
            scrapers: scrapers.map((s) => s.source),
          }),
          {
            status,
            headers: { "Content-Type": "application/json" },
          },
        );
      }

      return new Response("Not Found", { status: 404 });
    },
  });

  logger.info("Health server listening", { port: config.PORT });
}

// ---------------------------------------------------------------------------
// Graceful Shutdown
// ---------------------------------------------------------------------------

async function shutdown(signal: string): Promise<void> {
  if (isShuttingDown) return;
  isShuttingDown = true;

  logger.info("Shutting down", { signal });

  // Stop the scheduler
  if (schedulerTimer) {
    clearInterval(schedulerTimer);
    schedulerTimer = null;
  }

  // Close connections
  await disconnectProducer();
  await disconnectRedis();

  logger.info("Shutdown complete");
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  logger.info("🕷️  Scraper Service starting", {
    env: config.NODE_ENV,
    scrapeInterval: `${config.SCRAPE_INTERVAL / 1000}s`,
    maxRequestsPerHour: config.MAX_REQUESTS_PER_HOUR,
  });

  // 1. Verify Redis connection
  const redis = getRedisClient();
  try {
    await redis.ping();
    logger.info("Redis connection verified");
  } catch (error) {
    logger.error("Redis connection failed — service cannot start", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exit(1);
  }

  // 2. Register scrapers
  scrapers = createAllScrapers();
  logger.info("Scrapers registered", {
    scrapers: scrapers.map((s) => s.name),
  });

  // 3. Start health server
  startHealthServer();

  // 4. Run initial scrape cycle
  await runScrapeCycle();

  // 5. Schedule recurring cycles
  schedulerTimer = setInterval(runScrapeCycle, config.SCRAPE_INTERVAL);
  logger.info("Scheduler started", {
    intervalMs: config.SCRAPE_INTERVAL,
  });
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

main().catch((error) => {
  logger.error("Fatal startup error", {
    error: error instanceof Error ? error.message : String(error),
    stack: error instanceof Error ? error.stack : undefined,
  });
  process.exit(1);
});
