use domain::error::CrawlerError;
use futures::StreamExt;
use reqwest::Client;
use scraper::{Html, Selector};
use storage_client::DiskStorage;
use url::{ParseError, Url};

pub async fn fetch(
    client: &Client,
    url: &str,
    storage: &DiskStorage,
) -> Result<(String, String, String), CrawlerError> {
    let response = client.get(url).send().await?;
    let mut stream = response.bytes_stream();
    let mut hasher = blake3::Hasher::new();
    let mut buf = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        hasher.update(&chunk);
        buf.extend_from_slice(&chunk);
    }

    let hash = hasher.finalize().to_hex().to_string();
    let storage_path = storage
        .store_html(&hash, &buf)
        .await
        .map_err(|e| CrawlerError::Storage(e.to_string()))?;

    let html = String::from_utf8_lossy(&buf).into_owned();
    Ok((html, hash, storage_path))
}

pub fn extract_url(html: &str, base_url: &str) -> Result<Vec<String>, ParseError> {
    let document = Html::parse_document(html);
    let base = Url::parse(base_url)?;
    let selector = Selector::parse("a").unwrap();
    let mut urls = vec![];
    for element in document.select(&selector) {
        if let Some(link) = element.value().attr("href") {
            urls.push(link.to_string());
        }
    }
    let extracted_urls = urls
        .into_iter()
        .filter_map(|href| {
            if href.starts_with("javascript:")
                || href.starts_with("mailto:")
                || href.starts_with('#')
            {
                return None;
            }
            let parsed = Url::parse(&href).or_else(|_| base.join(&href)).ok()?;
            match parsed.scheme() {
                "http" | "https" => Some(parsed.to_string()),
                _ => None,
            }
        })
        .collect();
    Ok(extracted_urls)
}
