use config::Settings;
use domain::models::CrawlJob;
use queue_client::SqsClient;

#[tokio::main]
async fn main() {
    let cfg = Settings::from_env().expect("failed to load config");
    let sqs = SqsClient::new(&cfg.aws_region).await;

    let seeds = vec![
        "https://example.com",
        "https://rust-lang.org",
    ];

    for url in seeds {
        let job = CrawlJob { url: url.to_string(), depth: 0 };
        match sqs.send_crawl_job(&cfg.frontier_queue_url, job).await {
            Ok(()) => println!("seeded: {url}"),
            Err(e) => eprintln!("failed to seed {url}: {e}"),
        }
    }
}
