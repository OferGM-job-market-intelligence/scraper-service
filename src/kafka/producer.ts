/**
 * Kafka Producer
 *
 * Publishes scraped job postings to the `jobs.raw` topic for downstream
 * processing by the NLP service.
 *
 * Message format:
 *   Key:   job_id (ensures all updates for the same job land on the same partition)
 *   Value: JSON-serialised RawJobPosting
 *
 * Retry behaviour is handled by KafkaJS internally (3 retries with backoff).
 */

import {
  Kafka,
  type Producer,
  type ProducerRecord,
  CompressionTypes,
} from "kafkajs";
import { config } from "../config/env.js";
import { logger } from "../utils/logger.js";

// ---------------------------------------------------------------------------
// Types — whats publish to Kafka
// ---------------------------------------------------------------------------

/**
 * Raw job data as scraped, before NLP enrichment.
 * Mirrors the structure the NLP service expects to consume.
 */
export interface RawJobPosting {
  job_id: string;
  title: string;
  company: string;
  location: {
    city?: string;
    state?: string;
    country?: string;
    raw: string; // Original location string before parsing
  };
  description: string;
  url: string;
  posted_at: string | null;
  salary_text: string | null; // Raw salary string; NLP will parse it
  employment_type: string | null;
  source: "linkedin" | "indeed" | "glassdoor";
  scraped_at: string; // ISO 8601
}

// ---------------------------------------------------------------------------
// Producer singleton
// ---------------------------------------------------------------------------

let producer: Producer | null = null;
let isConnected = false;

/**
 * Lazily initialises and connects the Kafka producer.
 * Safe to call multiple times — returns the same instance.
 */
async function getProducer(): Promise<Producer> {
  if (producer && isConnected) return producer;

  const kafka = new Kafka({
    clientId: config.KAFKA_CLIENT_ID,
    brokers: config.KAFKA_BROKERS.split(",").map((b) => b.trim()),
    retry: {
      initialRetryTime: 300,
      retries: 5,
    },
  });

  producer = kafka.producer({
    allowAutoTopicCreation: true,
    idempotent: true, // Exactly-once semantics within a session
  });

  producer.on("producer.connect", () => {
    isConnected = true;
    logger.info("Kafka producer connected", {
      brokers: config.KAFKA_BROKERS,
    });
  });

  producer.on("producer.disconnect", () => {
    isConnected = false;
    logger.warn("Kafka producer disconnected");
  });

  await producer.connect();
  return producer;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Publish a single job posting to Kafka.
 *
 * @param job - The scraped job data
 * @returns The Kafka record metadata on success
 * @throws If publishing fails after retries
 */
export async function publishJob(job: RawJobPosting): Promise<void> {
  const prod = await getProducer();

  const record: ProducerRecord = {
    topic: config.KAFKA_TOPIC_JOBS_RAW,
    compression: CompressionTypes.GZIP,
    messages: [
      {
        key: job.job_id,
        value: JSON.stringify(job),
        headers: {
          source: job.source,
          scraped_at: job.scraped_at,
        },
      },
    ],
  };

  try {
    await prod.send(record);
    logger.debug("Job published to Kafka", {
      jobId: job.job_id,
      topic: config.KAFKA_TOPIC_JOBS_RAW,
      source: job.source,
    });
  } catch (error) {
    logger.error("Failed to publish job to Kafka", {
      jobId: job.job_id,
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}

/**
 * Publish a batch of jobs to Kafka in a single request.
 * More efficient than individual publishes when scraping returns many results.
 */
export async function publishJobBatch(jobs: RawJobPosting[]): Promise<void> {
  if (jobs.length === 0) return;

  const prod = await getProducer();

  const record: ProducerRecord = {
    topic: config.KAFKA_TOPIC_JOBS_RAW,
    compression: CompressionTypes.GZIP,
    messages: jobs.map((job) => ({
      key: job.job_id,
      value: JSON.stringify(job),
      headers: {
        source: job.source,
        scraped_at: job.scraped_at,
      },
    })),
  };

  try {
    await prod.send(record);
    logger.info("Job batch published to Kafka", {
      count: jobs.length,
      topic: config.KAFKA_TOPIC_JOBS_RAW,
      source: jobs[0]?.source,
    });
  } catch (error) {
    logger.error("Failed to publish job batch to Kafka", {
      count: jobs.length,
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}

/**
 * Gracefully disconnect the Kafka producer.
 * Call during service shutdown.
 */
export async function disconnectProducer(): Promise<void> {
  if (producer) {
    await producer.disconnect();
    producer = null;
    isConnected = false;
    logger.info("Kafka producer disconnected gracefully");
  }
}
