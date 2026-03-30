#[derive(Debug, thiserror::Error)]
pub enum CrawlerError {
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),
    #[error("DNS resolution failed for: {0}")]
    DNSFailure(String),
    #[error("request timed out {0}")]
    Timeout(String),
    #[error("HTTP {status} for {url}")]
    HttpError { status: u16, url: String },
    #[error("Blocked by robots.txt: {0}")]
    RobotsBlocked(String),
    #[error("Content too large: {0} bytes")]
    TooLarge(usize),
    #[error("Unexpected content type: {0}")]
    WrongContentType(String),
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Storage error: {0}")]
    Cache(#[from] cache_client::error::CacheError)
}