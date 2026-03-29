use domain::error::{self, CrawlerError};
use std::collections::HashSet;
use reqwest::{self, Client};
use config::Settings;
use domain::models::{CrawlJob, UrlMetaData};
use tokio::time::Duration;
use std::collections::VecDeque;
use std::sync::Arc;
use storage_client::DiskStorage;
mod fetcher;
use tokio::sync::{mpsc, oneshot};

const MAX_DEPTH: u8 = 1; 
#[tokio::main]
async fn main() -> Result< (), error::CrawlerError>{
    let env_variables = Settings::from_env().expect("failed to load config");
    let timeout = env_variables.duration();
    let value = env_variables.user_agent;
    let client = Arc::new(Client::builder().connect_timeout(timeout).user_agent(value).build().expect("should be able to build reqwest client"));
    let storage = Arc::new(DiskStorage::new(&env_variables.storage_base_dir).await.expect("failed to create storage dir"));
    let urls = vec![
        "https://example.com",
        "https://rust-lang.org",
    ];
    // crawlers → actor (pushing discovered URLs)
    let (push_tx, mut push_rx) = mpsc::channel::<CrawlJob>(1000);
    // crawler → actor (ask for work)
    let (req_tx, mut req_rx) = mpsc::channel::<oneshot::Sender<Option<CrawlJob>>>(100);

    tokio::task::spawn(async move {
        // owns everything, no locks
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
                let job = queue.pop_front(); // None if queue empty
                let _ = resp.send(job);      // crawler handles None
            }
            }
        }
    });
    
    let push_tx = push_tx.clone();
    for url in &urls {
        push_tx.send(CrawlJob { url: url.to_string(), depth: 0 }).await;
    };
    let mut handles = vec![]; 
    for i in 0..4 {
        let req_tx = req_tx.clone();
        let client = Arc::clone(&client);
        let push_tx = push_tx.clone();
        let storage = Arc::clone(&storage);
        let handle :tokio::task::JoinHandle<Result<(), CrawlerError>>= tokio::task::spawn(async move {
            loop {
                let (resp_tx, resp_rx) = oneshot::channel(); 
                req_tx.send(resp_tx).await.expect("sender dropped"); 

                match resp_rx.await.expect("actor dropped") {
                    Some(job) => {
                        match fetcher::fetch(&client, &job.url, &storage).await {
                            Ok((html, hash, storage_path)) => {
                            let metadata = UrlMetaData {
                                url: job.url.clone(),
                                storage_path,
                                last_crawled: chrono::Utc::now(),
                                content_hash: hash.clone(),
                                depth: job.depth,
                            };
                            if let Err(e) = storage.save_metadata(&metadata).await {
                                eprintln!("metadata save failed for {}: {e}", job.url);
                            }
                            println!("{} {}-> {hash}", job.url, job.depth);
                            if let Ok(urls) = fetcher::extract_url(&html, &job.url) {
                                for url in urls {
                                    push_tx.send(CrawlJob {
                                        url,
                                        depth: job.depth + 1
                                    }).await.expect("actor dropped");
                                }
                            }
                        }
                        Err(e) => eprintln!("fetch failed for {}: {e}", job.url),
                        }
                    }
                    None => {
                        // queue empty, wait a bit before asking again
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }); 
        handles.push(handle); 
    }
        
    // we don't use join_all because if a single task failed and panics, it will bring down the whole group. this way of handling separate tasks makes them independent of each other
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {},
            Ok(Err(e)) => eprintln!("task error: {e}"),
            Err(e) => eprintln!("task panicked: {e}"),
        }
    }
    Ok(())
}