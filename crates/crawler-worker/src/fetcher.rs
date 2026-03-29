use domain::error::{self, CrawlerError};
use reqwest::{Client}; 
use url::{Url, ParseError}; 
use scraper::{Html, Selector};
pub async fn fetch(client: &Client, url: &str) -> Result<(String, String), error::CrawlerError> {
    let response = client.get(url).send().await?;
    let html = response.text().await?;
    let hash = blake3::hash(html.as_bytes());
    Ok((html, hash.to_string()))
}

pub fn extract_url(html: &str, base_url: &str) -> Result<Vec<String>, ParseError> {
    // 1. parse the html string into a document
    let document = Html::parse_document(html); 
    let base = Url::parse(base_url)?; 
    // 2. create a selector for all <a> tags
    let selector = Selector::parse("a").unwrap(); 
    let mut urls = vec![]; 
    // 3. iterate over matches, grab the href attribute
    for element in document.select(&selector) {
        if let Some(link) = element.value().attr("href") {
            urls.push(link.to_string());
        }
    };
    // 4. resolve relative URLs against base_url
    let extracted_urls = urls.into_iter().filter_map(|href| {
        if href.starts_with("javascript:") || href.starts_with("mailto:") || href.starts_with('#') {
            return None
        }
        // trying to parse absolute path first, if not try relative and fail silently if both does not work. 
        let parsed = Url::parse(&href).or_else(|_| base.join(&href)).ok()?; 
        // only http & https
        match parsed.scheme() {
                "http" | "https" => Some(parsed.to_string()),
                _ => None,
            }
    }).collect(); 
    // 5. return only http/https URLs
    Ok(extracted_urls)
}