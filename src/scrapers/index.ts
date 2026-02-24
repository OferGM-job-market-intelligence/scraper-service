/**
 * Scraper Registry
 *
 * Central export point for all scraper implementations.
 * The scheduler uses this to iterate over all registered scrapers.
 */

export { BaseScraper, type ScraperSource, type ScrapeResult } from "./base-scraper.js";
export { LinkedInScraper } from "./linkedin-scraper.js";
export { IndeedScraper } from "./indeed-scraper.js";

import { BaseScraper } from "./base-scraper.js";
import { LinkedInScraper } from "./linkedin-scraper.js";
import { IndeedScraper } from "./indeed-scraper.js";

/**
 * Factory that creates all registered scrapers.
 * Add new scrapers here as they are implemented.
 */
export function createAllScrapers(): BaseScraper[] {
  return [
    new LinkedInScraper(),
    new IndeedScraper(),
  ];
}