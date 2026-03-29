use domain::error::{self, CrawlerError};
use std::collections::HashMap;
use reqwest::{self, Client};
use config::Settings; 
use tokio::time::Duration; 
use tokio::task; 
use std::sync::Arc; 
mod fetcher; 

#[tokio::main]
async fn main() -> Result< (), error::CrawlerError>{
    let env_variables = Settings::from_env().expect("failed to load config"); 
    let timeout = env_variables.duration();
    let value = env_variables.user_agent; 
    let client = Arc::new(Client::builder().connect_timeout(timeout).user_agent(value).build().expect("should be able to build reqwest client"));
    let urls = vec![
        "https://example.com",
        "https://rust-lang.org",
        "https://wikipedia.org",
    ];
    let mut handles = vec![]; 
    for i in urls {
        let client = Arc::clone(&client); 
        let handle = task::spawn(async move {
            let (html, hash) = fetcher::fetch(&client, i).await?; 
            println!("{hash}");
            if let Ok(urls) = fetcher::extract_url(&html, i) {
                for j in urls {
                    println!("{i} -> {hash} -> found {} urls", j);
                }
            }
            Ok::<(), CrawlerError>(())
        });
        handles.push(handle)
    };
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