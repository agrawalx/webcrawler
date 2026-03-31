use std::sync::Arc;

use config::Settings;
use domain::models::CrawlJob;
use queue_client::SqsClient;
use storage_client::S3Storage;

mod extractor;
mod parser;

#[tokio::main]
async fn main() {
    let cfg = Settings::from_env().expect("failed to load config");
    let sqs = Arc::new(SqsClient::new(&cfg.aws_region).await);
    let s3 = Arc::new(S3Storage::new(&cfg.s3_bucket, &cfg.aws_region).await);

    println!("parsing-worker started, polling {}", cfg.parsing_queue_url);

    loop {
        match sqs.receive_parse_job(&cfg.parsing_queue_url).await {
            Err(e) => {
                eprintln!("SQS receive error: {e}");
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
            Ok(None) => {} // empty, long-poll will have waited 20s already
            Ok(Some((job, receipt))) => {
                let sqs = Arc::clone(&sqs);
                let s3 = Arc::clone(&s3);
                let frontier_url = cfg.frontier_queue_url.clone();
                let parsing_url = cfg.parsing_queue_url.clone();

                tokio::task::spawn(async move {
                    let html = match s3.get_html(&job.storage_path).await {
                        Ok(h) => h,
                        Err(e) => {
                            eprintln!("failed to fetch {} from S3: {e}", job.storage_path);
                            return;
                        }
                    };

                    let parsed = extractor::parse(&html, &job.url);
                    println!("parsed: {} ({} words, {} links)", job.url, parsed.word_count, parsed.links.len());

                    // derive hash from the S3 key stem: "html/{hash}.html" → "{hash}"
                    let hash = std::path::Path::new(&job.storage_path)
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or(&job.url);

                    match serde_json::to_vec(&parsed) {
                        Ok(bytes) => {
                            if let Err(e) = s3.store_parsed(hash, &bytes).await {
                                eprintln!("store_parsed failed for {}: {e}", job.url);
                            }
                        }
                        Err(e) => eprintln!("serialize failed for {}: {e}", job.url),
                    }

                    // delete from parsing queue after successful processing
                    if let Err(e) = sqs.delete_message(&parsing_url, &receipt).await {
                        eprintln!("failed to delete parse SQS message for {}: {e}", job.url);
                    }

                    // send extracted URLs back to frontier
                    for url in parsed.links {
                        let crawl_job = CrawlJob { url: url.clone(), depth: job.depth + 1 };
                        if let Err(e) = sqs.send_crawl_job(&frontier_url, crawl_job).await {
                            eprintln!("failed to enqueue {url}: {e}");
                        }
                    }
                });
            }
        }
    }
}
