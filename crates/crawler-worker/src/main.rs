use std::sync::Arc;

use cache_client::RedisClient;
use config::Settings;
use domain::error;
use queue_client::SqsClient;
use reqwest::Client;
use storage_client::{DynamoStorage, S3Storage};

mod fetcher;
mod worker;

#[tokio::main]
async fn main() -> Result<(), error::CrawlerError> {
    let cfg = Settings::from_env().expect("failed to load config");

    let http = Arc::new(
        Client::builder()
            .connect_timeout(cfg.duration())
            .user_agent(cfg.user_agent)
            .build()
            .expect("failed to build reqwest client"),
    );
    let s3 = Arc::new(S3Storage::new(&cfg.s3_bucket, &cfg.aws_region).await);
    let dynamo = Arc::new(DynamoStorage::new("UrlMetadata", &cfg.aws_region).await);
    let sqs = Arc::new(SqsClient::new(&cfg.aws_region).await);
    let redis = Arc::new(RedisClient::new(&cfg.redis_url).await?);

    let handle = worker::spawn_worker(
        http,
        s3,
        dynamo,
        sqs,
        cfg.frontier_queue_url,
        cfg.parsing_queue_url,
        redis,
        cfg.req_per_second,
    );

    match handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("task error: {e}"),
        Err(e) => eprintln!("task panicked: {e}"),
    }
    Ok(())
}
