/**
 * LinkedIn Scraper
 *
 * Scrapes job postings from LinkedIn's public job search pages.
 * Uses Cheerio for HTML parsing (no headless browser needed).
 *
 * Implementation: Day 11
 * Integration testing: Day 12
 */

import { BaseScraper, type ScraperSource } from "./base-scraper.js";
import type { RawJobPosting } from "../kafka/producer.js";

export class LinkedInScraper extends BaseScraper {
  readonly source: ScraperSource = "linkedin";
  readonly name = "LinkedIn Scraper";

  protected async scrape(): Promise<RawJobPosting[]> {
    // TODO (Day 11): Implement LinkedIn scraping
    //   - Build search URL with query params
    //   - Fetch HTML with Axios (respect User-Agent)
    //   - Parse with Cheerio: title, company, location, description, URL, posted_at
    //   - Handle pagination
    //   - Parse location string into structured format
    this.log.info("LinkedIn scraper not yet implemented — returning empty");
    return [];
  }
}