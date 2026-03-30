use domain::error::CrawlerError;
use futures::StreamExt;
use reqwest::Client;
use storage_client::DiskStorage;

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
