use std::sync::Arc;

use domain::models::{CrawlJob, ParseJob};
use storage_client::DiskStorage;
use tokio::sync::mpsc;

pub fn spawn_parser_actor(
    mut parse_rx: mpsc::Receiver<ParseJob>,
    push_tx: mpsc::Sender<CrawlJob>,
    storage: Arc<DiskStorage>,
) {
    tokio::task::spawn(async move {
        while let Some(job) = parse_rx.recv().await {
            let push_tx = push_tx.clone();
            let storage = Arc::clone(&storage);
            tokio::task::spawn(async move {
                let html = match tokio::fs::read_to_string(&job.storage_path).await {
                    Ok(h) => h,
                    Err(e) => {
                        eprintln!("parser: failed to read {}: {e}", job.storage_path);
                        return;
                    }
                };

                let parsed = parsing_worker::extractor::parse(&html, &job.url);

                // derive hash from the storage_path filename stem (e.g. "./crawl-data/{hash}.html")
                let hash = std::path::Path::new(&job.storage_path)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or(&job.url);

                match serde_json::to_vec(&parsed) {
                    Ok(bytes) => {
                        if let Err(e) = storage.store_parsed(hash, &bytes).await {
                            eprintln!("parser: store_parsed failed for {}: {e}", job.url);
                        }
                    }
                    Err(e) => eprintln!("parser: serialize failed for {}: {e}", job.url),
                }

                for url in parsed.links {
                    let _ = push_tx.send(CrawlJob { url, depth: job.depth + 1 }).await;
                }
            });
        }
    });
}
