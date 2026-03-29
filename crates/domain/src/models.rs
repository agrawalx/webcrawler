use chrono::{DateTime, Local}; 
pub struct CrawlJob {
    url: String,
    depth: u8 
}

pub struct UrlMetaData {
    url: String,
    location_on_s3: String,
    last_crawled: DateTime<Local>,
    content_hash: String,
    depth: u8
}

