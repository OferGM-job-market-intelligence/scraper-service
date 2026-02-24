/**
 * Base Scraper (Abstract)
 *
 * Defines the scraping contract that all source-specific scrapers must implement.
 * Provides shared pipeline logic: rate limiting → scrape → dedup → publish.
 *
 * Concrete implementations (Days 10-13):
 *   - LinkedInScraper  (src/scrapers/linkedin-scraper.ts)
 *   - IndeedScraper    (src/scrapers/indeed-scraper.ts)
 */

import type { RawJobPosting } from "../kafka/producer.js";
import { publishJob } from "../kafka/producer.js";
import { isDuplicate, markAsScraped } from "../redis/client.js";
import { RateLimiter } from "../utils/rate-limiter.js";
import { logger, type ChildLogger } from "../utils/logger.js";
import { getRedisClient } from "../redis/client.js";

export type ScraperSource = "linkedin" | "indeed" | "glassdoor";

export interface ScrapeResult {
  /** Total jobs found on the page */
  found: number;
  /** Jobs that passed dedup and were published */
  published: number;
  /** Jobs skipped because they were already scraped */
  duplicates: number;
  /** Jobs skipped due to rate limiting */
  rateLimited: boolean;
}

export abstract class BaseScraper {
  /** Which source this scraper targets */
  abstract readonly source: ScraperSource;
  /** Human-readable name for logging */
  abstract readonly name: string;

  protected readonly log: ChildLogger;
  private rateLimiter: RateLimiter | null = null;

  constructor() {
    // Child logger is initialised after construction since `source` is abstract.
    // We use a getter pattern instead.
    this.log = logger.child({ scraper: this.constructor.name });
  }

  /**
   * Scrape job listings from the source.
   * Each concrete scraper implements the HTTP + parsing logic here.
   *
   * @returns Array of raw job postings extracted from the source
   */
  protected abstract scrape(): Promise<RawJobPosting[]>;

  /**
   * Run the full scraping pipeline:
   *   1. Check rate limit
   *   2. Call scrape()
   *   3. Dedup each job against Redis
   *   4. Publish new jobs to Kafka
   *   5. Mark published jobs as scraped
   */
  async run(): Promise<ScrapeResult> {
    const result: ScrapeResult = {
      found: 0,
      published: 0,
      duplicates: 0,
      rateLimited: false,
    };

    // Step 1: Rate limit check
    const limiter = this.getRateLimiter();
    const allowed = await limiter.isAllowed(this.source);

    if (!allowed) {
      this.log.warn("Skipping scrape cycle — rate limited");
      result.rateLimited = true;
      return result;
    }

    // Step 2: Scrape
    this.log.info("Starting scrape cycle");
    const startTime = Date.now();

    let jobs: RawJobPosting[];
    try {
      jobs = await this.scrape();
    } catch (error) {
      this.log.error("Scrape failed", {
        error: error instanceof Error ? error.message : String(error),
      });
      return result;
    }

    result.found = jobs.length;
    this.log.info("Scrape completed", { found: jobs.length });

    // Steps 3-5: Dedup → Publish → Mark
    for (const job of jobs) {
      try {
        const duplicate = await isDuplicate(job.url);
        if (duplicate) {
          result.duplicates++;
          continue;
        }

        await publishJob(job);
        await markAsScraped(job.url);
        result.published++;
      } catch (error) {
        this.log.error("Failed to process job", {
          jobId: job.job_id,
          url: job.url,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }

    const elapsed = Date.now() - startTime;
    this.log.info("Scrape cycle finished", {
      ...result,
      elapsedMs: elapsed,
    });

    return result;
  }

  private getRateLimiter(): RateLimiter {
    if (!this.rateLimiter) {
      this.rateLimiter = new RateLimiter(getRedisClient());
    }
    return this.rateLimiter;
  }
}