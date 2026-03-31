# webcrawler

A distributed web crawler written in Rust. Seeds URLs into an SQS queue, crawls them concurrently across multiple EC2 workers, stores raw HTML in S3, and enqueues parsed links back into the frontier.

## Architecture

```
seed-loader
    │
    ▼
SQS: crawler-frontier
    │
    ├──► crawler-worker (×N EC2 instances)
    │         │  1. fetch robots.txt  →  DynamoDB: CrawlerDomains (24h cache)
    │         │  2. check is_allowed
    │         │  3. rate limit        →  Redis (sliding window, per-domain)
    │         │  4. HTTP fetch
    │         │  5. store raw HTML    →  S3: html/{blake3-hash}.html
    │         │  6. store metadata    →  DynamoDB: UrlMetadata
    │         │  7. enqueue ParseJob  →  SQS: crawler-parsing
    │
    └──► parsing-worker (×N EC2 instances)
              │  1. read raw HTML     ←  S3
              │  2. extract links + text
              │  3. store parsed JSON →  S3: parsed/{hash}.json
              │  4. enqueue new URLs  →  SQS: crawler-frontier
```

## Crate layout

```
crates/
  config/           — env-based Settings struct (envy + dotenvy)
  domain/           — shared types: CrawlJob, ParseJob, UrlMetaData, DomainRecord
  storage-client/   — S3Storage, DynamoStorage, DiskStorage
  queue-client/     — SqsClient (send/receive CrawlJob & ParseJob, delete)
  cache-client/     — RedisClient (sliding-window rate limiter via Lua)
  crawler-worker/   — HTTP fetcher, robots.txt compliance, worker loop
  parsing-worker/   — HTML link + text extractor, SQS polling binary
  seed-loader/      — one-shot binary to push seed URLs into the frontier
```

## AWS resources

| Resource | Name / Key |
|---|---|
| S3 bucket | `webcrawler-yash-test` |
| DynamoDB table | `UrlMetadata` (PK: `url`) |
| DynamoDB table | `CrawlerDomains` (PK: `domain`) |
| SQS queue | `crawler-frontier` |
| SQS queue | `crawler-parsing` |
| SQS queue | `crawler-frontier-dlq` |
| AWS region | `ap-south-1` |

## Configuration

All settings are read from environment variables (or a `.env` file in the working directory). Every variable has a built-in default shown below.

| Variable | Default | Description |
|---|---|---|
| `TIME_OUT_DURATION` | `15` | HTTP request timeout in seconds |
| `MAXIMUM_ALLOWED_SIZE` | `2000000` | Max response body in bytes (2 MB) |
| `USER_AGENT` | `my-crawler/0.1` | HTTP User-Agent header |
| `REQ_PER_SECOND` | `1` | Max requests per second per domain (overridden by robots Crawl-delay) |
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection string |
| `S3_BUCKET` | `webcrawler-yash-test` | S3 bucket for HTML + parsed JSON |
| `AWS_REGION` | `ap-south-1` | AWS region for all services |
| `FRONTIER_QUEUE_URL` | *(SQS URL)* | SQS frontier queue |
| `PARSING_QUEUE_URL` | *(SQS URL)* | SQS parsing queue |
| `FRONTIER_DLQ_URL` | *(SQS URL)* | Dead-letter queue URL |

## Local development

### Prerequisites

- Rust (stable, 1.78+)
- Docker (for Redis)
- AWS credentials configured (`~/.aws/credentials` or environment)

### Run Redis

```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
```

### Build

```bash
cargo build --release
```

### Seed the frontier

```bash
cargo run --bin seed-loader
```

Edit `crates/seed-loader/src/main.rs` to change the seed URLs.

### Start a crawler worker

```bash
cargo run --bin crawler-worker
```

### Start a parsing worker

```bash
cargo run --bin parsing-worker
```

## Rate limiting

Each domain gets its own sliding-window counter in Redis. The window is 1 second. A Lua script atomically checks and increments the counter so concurrent workers on the same host share a single rate limit per domain.

If `Crawl-delay` is present in `robots.txt` the robots value overrides `REQ_PER_SECOND`. A random jitter of 50–550 ms is added between rate-limit retries to prevent thundering herd across workers.

## robots.txt compliance

`crawler-worker` fetches `https://{domain}/robots.txt` before the first request to each domain and caches the parsed result in DynamoDB (`CrawlerDomains`). The cache is refreshed after 24 hours. If the fetch or DynamoDB lookup fails the crawler defaults to allowing all paths (permissive fallback).

## S3 layout

```
html/
  {blake3-hash}.html    ← raw HTML, stored during crawl
parsed/
  {blake3-hash}.json    ← extracted links + text, stored after parsing
```
