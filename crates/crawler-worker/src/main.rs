use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use config::Settings;
use domain::error;
use domain::models::CrawlJob;
use reqwest::Client;
use storage_client::{DynamoStorage, S3Storage};
use tokio::sync::{mpsc, oneshot};
use cache_client::RedisClient; 
mod fetcher;
mod parser_bridge;
mod worker;

const MAX_DEPTH: u8 = 1;

#[tokio::main]
async fn main() -> Result<(), error::CrawlerError> {
    let env_variables = Settings::from_env().expect("failed to load config");
    let client = Arc::new(
        Client::builder()
            .connect_timeout(env_variables.duration())
            .user_agent(env_variables.user_agent)
            .build()
            .expect("failed to build reqwest client"),
    );
    let storage = Arc::new(
        S3Storage::new(&env_variables.s3_bucket, &env_variables.aws_region).await
    );

    let dynamo_storage = Arc::new(DynamoStorage::new("UrlMetadata", &env_variables.aws_region).await);

    let seed_urls = vec!["https://example.com", "https://rust-lang.org"];
    let redis = Arc::new(RedisClient::new(&env_variables.redis_url).await?);

    // crawlers → queue actor (push discovered URLs)
    let (push_tx, mut push_rx) = mpsc::channel::<CrawlJob>(1000);
    // workers → queue actor (request next job)
    let (req_tx, mut req_rx) = mpsc::channel::<oneshot::Sender<Option<CrawlJob>>>(100);
    // workers → parser bridge
    let (parse_tx, parse_rx) = mpsc::channel::<domain::models::ParseJob>(1000);

    // queue actor — owns the queue and seen set, no locks needed
    tokio::task::spawn(async move {
        let mut queue: VecDeque<CrawlJob> = VecDeque::new();
        let mut seen: HashSet<String> = HashSet::new();
        loop {
            tokio::select! {
                Some(job) = push_rx.recv() => {
                    if !seen.contains(&job.url) && job.depth <= MAX_DEPTH {
                        seen.insert(job.url.clone());
                        queue.push_back(job);
                    }
                }
                Some(resp) = req_rx.recv() => {
                    let _ = resp.send(queue.pop_front());
                }
            }
        }
    });

    // seed the queue
    for url in &seed_urls {
        let _ = push_tx.send(CrawlJob { url: url.to_string(), depth: 0 }).await;
    }

    parser_bridge::spawn_parser_actor(parse_rx, push_tx.clone(), Arc::clone(&storage));

    let handle = worker::spawn_worker(req_tx, client, Arc::clone(&storage),Arc::clone(&dynamo_storage),  parse_tx, redis, env_variables.req_per_second);

    match handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("task error: {e}"),
        Err(e) => eprintln!("task panicked: {e}"),
    }
    Ok(())
}
