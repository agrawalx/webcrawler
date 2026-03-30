use std::sync::Arc;

use cache_client::RedisClient;
use chrono::Utc;
use domain::{
    error::CrawlerError,
    models::{CrawlJob, ParseJob, UrlMetaData},
};
use reqwest::Client;
use storage_client::DiskStorage;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use rand::Rng;
use tokio::time::Duration;
use url::Url;

pub fn spawn_worker(
    req_tx: mpsc::Sender<oneshot::Sender<Option<CrawlJob>>>,
    client: Arc<Client>,
    storage: Arc<DiskStorage>,
    parse_tx: mpsc::Sender<ParseJob>,
    redis: Arc<RedisClient>,
    req_per_second: u8,
) -> JoinHandle<Result<(), CrawlerError>> {
    tokio::task::spawn(async move {
        loop {
            let (resp_tx, resp_rx) = oneshot::channel();
            req_tx.send(resp_tx).await.expect("queue actor dropped");

            match resp_rx.await.expect("queue actor dropped") {
                Some(job) => {
                    let client = Arc::clone(&client);
                    let storage = Arc::clone(&storage);
                    let parse_tx = parse_tx.clone();
                    let redis = Arc::clone(&redis);

                    // Spawn the full pipeline — rate limit, fetch, write, parse.
                    // The dispatcher loop immediately continues to the next job.
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

                        match crate::fetcher::fetch(&client, &job.url, &storage).await {
                            Ok((_html, hash, storage_path)) => {
                                println!("{} depth={} -> {hash}", job.url, job.depth);
                                let metadata = UrlMetaData {
                                    url: job.url.clone(),
                                    storage_path: storage_path.clone(),
                                    last_crawled: Utc::now(),
                                    content_hash: hash,
                                    depth: job.depth,
                                };
                                if let Err(e) = storage.save_metadata(&metadata).await {
                                    eprintln!("metadata save failed for {}: {e}", job.url);
                                }
                                let _ = parse_tx
                                    .send(ParseJob {
                                        url: job.url,
                                        storage_path,
                                        depth: job.depth,
                                    })
                                    .await;
                            }
                            Err(e) => eprintln!("fetch failed for {}: {e}", job.url),
                        }
                    });
                }
                None => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
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
