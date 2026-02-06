# Scraper Service

Microservice for scraping job postings from LinkedIn, Indeed, and Glassdoor.

## 🎯 Purpose

Fetches job postings hourly and publishes them to Kafka for processing by the NLP service.

## 🛠️ Tech Stack

- **Runtime**: Bun.js (3x faster than Node.js)
- **Language**: TypeScript
- **Libraries**: Cheerio (HTML parsing), Axios (HTTP), KafkaJS, ioredis

## 📊 Features

- ✅ Rate limiting (max 50 requests/hour per source)
- ✅ Deduplication via Redis
- ✅ Publishes to Kafka topic `jobs.raw`
- ✅ Runs every hour automatically
- ✅ Error handling and retry logic

## 🚀 Quick Start
```bash
# Install Bun (if not installed)
curl -fsSL https://bun.sh/install | bash

# Install dependencies
bun install

# Copy env template
cp .env.example .env

# Run in development
bun run dev

# Run in production
bun run start
```

## 🔧 Environment Variables
```bash
KAFKA_BROKERS=localhost:29092
REDIS_HOST=localhost
REDIS_PORT=6379
SCRAPE_INTERVAL=3600000  # 1 hour in ms
```

## 📁 Project Structure
```
src/
├── index.ts                 # Entry point
├── scrapers/
│   ├── base-scraper.ts     # Abstract base class
│   ├── linkedin-scraper.ts # LinkedIn implementation
│   └── indeed-scraper.ts   # Indeed implementation
├── kafka/
│   └── producer.ts         # Kafka producer
├── redis/
│   └── client.ts           # Redis client
└── utils/
    ├── rate-limiter.ts     # Rate limiting logic
    └── logger.ts           # Structured logging
```

## 📈 Performance

to be added

## 🧪 Testing
```bash
bun test
```

## 🐳 Docker
```bash
docker build -t job-market/scraper:v1 .
docker run -e KAFKA_BROKERS=kafka:9092 job-market/scraper:v1
```

## 📊 Status

- [x] Project initialized
- [ ] LinkedIn scraper (Week 2)
- [ ] Indeed scraper (Week 3)
- [ ] Rate limiting (Week 2)
- [ ] Kafka integration (Week 2)
- [ ] Docker deployment (Week 3)