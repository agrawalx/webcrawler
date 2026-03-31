use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
pub struct CrawlJob {
    pub url: String,
    pub depth: u8,
}

#[derive(Serialize)]
pub struct UrlMetaData {
    pub url: String,
    pub storage_path: String,
    pub last_crawled: DateTime<Utc>,
    pub content_hash: String,
    pub depth: u8,
}
#[derive(Serialize, Deserialize)]
pub struct ParseJob {
    pub url: String,
    pub storage_path: String, // where the HTML file lives
    pub depth: u8,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DomainRecord {
    pub domain: String,
    pub disallow: Vec<String>,
    pub crawl_delay: Option<u64>,
    pub fetched_at: DateTime<Utc>,
}
