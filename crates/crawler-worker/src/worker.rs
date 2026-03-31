use std::sync::Arc;

use cache_client::RedisClient;
use chrono::Utc;
use domain::{
    error::CrawlerError,
    models::{ParseJob, UrlMetaData},
};
use queue_client::SqsClient;
use rand::Rng;
use reqwest::Client;
use storage_client::{DynamoStorage, S3Storage};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use url::Url;

pub fn spawn_worker(
    http: Arc<Client>,
    s3: Arc<S3Storage>,
    dynamo: Arc<DynamoStorage>,
    sqs: Arc<SqsClient>,
    frontier_queue_url: String,
    parsing_queue_url: String,
    redis: Arc<RedisClient>,
    req_per_second: u8,
) -> JoinHandle<Result<(), CrawlerError>> {
    tokio::task::spawn(async move {
        loop {
            // Long-poll SQS for the next CrawlJob (blocks up to 20s if empty)
            match sqs.receive_crawl_job(&frontier_queue_url).await {
                Err(e) => {
                    eprintln!("SQS receive error: {e}");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Ok(None) => {} // queue empty, loop and poll again
                Ok(Some((job, receipt))) => {
                    let http = Arc::clone(&http);
                    let s3 = Arc::clone(&s3);
                    let dynamo = Arc::clone(&dynamo);
                    let sqs = Arc::clone(&sqs);
                    let frontier_queue_url = frontier_queue_url.clone();
                    let parsing_queue_url = parsing_queue_url.clone();
                    let redis = Arc::clone(&redis);

                    tokio::task::spawn(async move {
                        let domain = extract_domain(&job.url);
                        loop {
                            match redis.check_rate_limit(&domain, req_per_second).await {
                                Ok(true) => break,
                                Ok(false) => {
                                    let jitter = rand::thread_rng().gen_range(0u64..=100);
                                    tokio::time::sleep(Duration::from_millis(200 + jitter)).await;
                                }
                                Err(e) => {
                                    eprintln!("rate limit check failed for {domain}: {e}, skipping");
                                    break;
                                }
                            }
                        }

                        match crate::fetcher::fetch(&http, &job.url, &s3).await {
                            Ok((_html, hash, storage_path)) => {
                                println!("{} depth={} -> {hash}", job.url, job.depth);

                                let metadata = UrlMetaData {
                                    url: job.url.clone(),
                                    storage_path: storage_path.clone(),
                                    last_crawled: Utc::now(),
                                    content_hash: hash,
                                    depth: job.depth,
                                };
                                if let Err(e) = dynamo.put_item(&metadata).await {
                                    eprintln!("metadata save failed for {}: {e}", job.url);
                                }

                                // delete from frontier only after successful fetch + metadata save
                                if let Err(e) = sqs.delete_message(&frontier_queue_url, &receipt).await {
                                    eprintln!("failed to delete SQS message for {}: {e}", job.url);
                                }

                                let parse_job = ParseJob {
                                    url: job.url.clone(),
                                    storage_path,
                                    depth: job.depth,
                                };
                                if let Err(e) = sqs.send_parse_job(&parsing_queue_url, parse_job).await {
                                    eprintln!("failed to enqueue ParseJob for {}: {e}", job.url);
                                }
                            }
                            Err(e) => eprintln!("fetch failed for {}: {e}", job.url),
                        }
                    });
                }
            }
        }
    })
}

fn extract_domain(url: &str) -> String {
    Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_string()))
        .unwrap_or_else(|| url.to_string())
}
