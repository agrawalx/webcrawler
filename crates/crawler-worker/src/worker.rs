use std::sync::Arc;

use chrono::Utc;
use domain::{
    error::CrawlerError,
    models::{CrawlJob, ParseJob, UrlMetaData},
};
use reqwest::Client;
use storage_client::DiskStorage;
use tokio::{sync::mpsc, task::JoinHandle, time::Duration};
use tokio::sync::oneshot;

pub fn spawn_workers(
    n: usize,
    req_tx: mpsc::Sender<oneshot::Sender<Option<CrawlJob>>>,
    client: Arc<Client>,
    storage: Arc<DiskStorage>,
    parse_tx: mpsc::Sender<ParseJob>,
) -> Vec<JoinHandle<Result<(), CrawlerError>>> {
    (0..n)
        .map(|_| {
            let req_tx = req_tx.clone();
            let client = Arc::clone(&client);
            let storage = Arc::clone(&storage);
            let parse_tx = parse_tx.clone();

            tokio::task::spawn(async move {
                loop {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    req_tx.send(resp_tx).await.expect("queue actor dropped");

                    match resp_rx.await.expect("queue actor dropped") {
                        Some(job) => {
                            match crate::fetcher::fetch(&client, &job.url, &storage).await {
                                Ok((_html, hash, storage_path)) => {
                                    let metadata = UrlMetaData {
                                        url: job.url.clone(),
                                        storage_path: storage_path.clone(),
                                        last_crawled: Utc::now(),
                                        content_hash: hash.clone(),
                                        depth: job.depth,
                                    };
                                    if let Err(e) = storage.save_metadata(&metadata).await {
                                        eprintln!("metadata save failed for {}: {e}", job.url);
                                    }
                                    println!("{} depth={}-> {hash}", job.url, job.depth);
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
                        }
                        None => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            })
        })
        .collect()
}
