/**
 * Indeed Scraper
 *
 * Scrapes job postings from Indeed's public job search pages.
 * Staggered 2 hours from LinkedIn to distribute load.
 *
 * Implementation: Day 13
 */

import { BaseScraper, type ScraperSource } from "./base-scraper.js";
import type { RawJobPosting } from "../kafka/producer.js";

export class IndeedScraper extends BaseScraper {
  readonly source: ScraperSource = "indeed";
  readonly name = "Indeed Scraper";

  protected async scrape(): Promise<RawJobPosting[]> {
    // TODO (Day 13): Implement Indeed scraping
    //   - Build search URL with query params
    //   - Fetch HTML with Axios
    //   - Parse with Cheerio: title, company, location, description, URL, posted_at
    //   - Handle Indeed-specific HTML structure
    //   - Handle pagination
    this.log.info("Indeed scraper not yet implemented — returning empty");
    return [];
  }
}