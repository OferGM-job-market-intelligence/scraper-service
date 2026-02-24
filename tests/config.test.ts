/**
 * Tests for the config module.
 *
 * Verifies Zod schema validation catches bad env vars
 * and that defaults are applied correctly.
 */

import { describe, expect, it } from "bun:test";
import { z } from "zod";

// Can't directly test the singleton config (it runs at import time),
// so we test the schema logic independently with the same schema shape.
const envSchema = z.object({
  KAFKA_BROKERS: z.string().default("localhost:29092"),
  KAFKA_CLIENT_ID: z.string().default("scraper-service"),
  KAFKA_TOPIC_JOBS_RAW: z.string().default("jobs.raw"),
  REDIS_HOST: z.string().default("localhost"),
  REDIS_PORT: z.coerce.number().int().positive().default(6379),
  REDIS_PASSWORD: z.string().optional(),
  SCRAPE_INTERVAL: z.coerce.number().int().positive().default(3_600_000),
  MAX_REQUESTS_PER_HOUR: z.coerce.number().int().positive().default(50),
  DEDUP_TTL_SECONDS: z.coerce.number().int().positive().default(2_592_000),
  NODE_ENV: z
    .enum(["development", "production", "test"])
    .default("development"),
  LOG_LEVEL: z.enum(["debug", "info", "warn", "error"]).default("info"),
  SERVICE_NAME: z.string().default("scraper-service"),
  PORT: z.coerce.number().int().positive().default(3000),
});

describe("Config Schema", () => {
  it("should apply all defaults when no env vars are set", () => {
    const result = envSchema.parse({});

    expect(result.KAFKA_BROKERS).toBe("localhost:29092");
    expect(result.KAFKA_CLIENT_ID).toBe("scraper-service");
    expect(result.KAFKA_TOPIC_JOBS_RAW).toBe("jobs.raw");
    expect(result.REDIS_HOST).toBe("localhost");
    expect(result.REDIS_PORT).toBe(6379);
    expect(result.SCRAPE_INTERVAL).toBe(3_600_000);
    expect(result.MAX_REQUESTS_PER_HOUR).toBe(50);
    expect(result.NODE_ENV).toBe("development");
    expect(result.LOG_LEVEL).toBe("info");
    expect(result.PORT).toBe(3000);
  });

  it("should coerce string numbers to integers", () => {
    const result = envSchema.parse({
      REDIS_PORT: "6380",
      PORT: "4000",
      SCRAPE_INTERVAL: "1800000",
    });

    expect(result.REDIS_PORT).toBe(6380);
    expect(result.PORT).toBe(4000);
    expect(result.SCRAPE_INTERVAL).toBe(1_800_000);
  });

  it("should override defaults with provided values", () => {
    const result = envSchema.parse({
      KAFKA_BROKERS: "kafka-1:9092,kafka-2:9092",
      NODE_ENV: "production",
      LOG_LEVEL: "error",
    });

    expect(result.KAFKA_BROKERS).toBe("kafka-1:9092,kafka-2:9092");
    expect(result.NODE_ENV).toBe("production");
    expect(result.LOG_LEVEL).toBe("error");
  });

  it("should reject invalid NODE_ENV values", () => {
    const result = envSchema.safeParse({ NODE_ENV: "staging" });
    expect(result.success).toBe(false);
  });

  it("should reject invalid LOG_LEVEL values", () => {
    const result = envSchema.safeParse({ LOG_LEVEL: "trace" });
    expect(result.success).toBe(false);
  });

  it("should reject negative port numbers", () => {
    const result = envSchema.safeParse({ PORT: "-1" });
    expect(result.success).toBe(false);
  });

  it("should allow optional REDIS_PASSWORD", () => {
    const withPassword = envSchema.parse({ REDIS_PASSWORD: "secret123" });
    expect(withPassword.REDIS_PASSWORD).toBe("secret123");

    const withoutPassword = envSchema.parse({});
    expect(withoutPassword.REDIS_PASSWORD).toBeUndefined();
  });
});
