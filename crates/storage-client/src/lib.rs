use aws_config::meta::region::RegionProviderChain;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, Utc};
use domain::models::{DomainRecord, UrlMetaData};
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Failed to write file: {0}")]
    WriteError(#[from] std::io::Error),
    #[error("Failed to serialize metadata: {0}")]
    SerializeError(#[from] serde_json::Error),
    #[error("S3 error: {0}")]
    S3(String),
    #[error("DynamoDB error: {0}")]
    DynamoDB(String),
}

pub struct DiskStorage {
    base_dir: PathBuf,
    metadata_file: Mutex<tokio::fs::File>,
}

impl DiskStorage {
    pub async fn new(base_dir: &str) -> Result<Self, StorageError> {
        let base_dir = PathBuf::from(base_dir);
        tokio::fs::create_dir_all(&base_dir).await?;
        let metadata_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(base_dir.join("metadata.jsonl"))
            .await?;
        Ok(Self {
            base_dir,
            metadata_file: Mutex::new(metadata_file),
        })
    }

    pub async fn store_html(&self, hash: &str, content: &[u8]) -> Result<String, StorageError> {
        let path = self.base_dir.join(format!("{hash}.html"));
        tokio::fs::write(&path, content).await?;
        Ok(path.to_string_lossy().into_owned())
    }

    pub async fn store_parsed(&self, hash: &str, content: &[u8]) -> Result<(), StorageError> {
        let parsed_dir = self.base_dir.join("parsed");
        tokio::fs::create_dir_all(&parsed_dir).await?;
        let path = parsed_dir.join(format!("{hash}.json"));
        tokio::fs::write(&path, content).await?;
        Ok(())
    }

    pub async fn save_metadata(&self, metadata: &UrlMetaData) -> Result<(), StorageError> {
        let mut line = serde_json::to_string(metadata)?;
        line.push('\n');
        let mut file = self.metadata_file.lock().await;
        file.write_all(line.as_bytes()).await?;
        file.flush().await?;
        Ok(())
    }
}

pub struct S3Storage {
    client: S3Client,
    bucket: String,
}

impl S3Storage {
    pub async fn new(bucket: &str, region: &str) -> Self {
        let region_provider = RegionProviderChain::first_try(Region::new(region.to_string()))
            .or_default_provider()
            .or_else(Region::new("ap-south-1"));

        let config = aws_config::from_env().region(region_provider).load().await;

        Self {
            client: S3Client::new(&config),
            bucket: bucket.to_string(),
        }
    }

    pub async fn store_html(&self, hash: &str, content: &[u8]) -> Result<String, StorageError> {
        let key = format!("html/{hash}.html");
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(content.to_vec()))
            .content_type("text/html")
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;
        Ok(key)
    }

    pub async fn get_html(&self, key: &str) -> Result<String, StorageError> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?
            .into_bytes();
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    pub async fn store_parsed(&self, hash: &str, content: &[u8]) -> Result<(), StorageError> {
        let key = format!("parsed/{hash}.json");
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(content.to_vec()))
            .content_type("application/json")
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;
        Ok(())
    }
}

pub struct DynamoStorage {
    client: DynamoClient,
    table_name: String,
}

impl DynamoStorage {
    pub async fn new(table_name: &str, region: &str) -> Self {
        // same pattern as S3Storage::new()
        // look up aws_config::from_env().region(...).load().await
        let region_provider = RegionProviderChain::first_try(Region::new(region.to_string()))
            .or_default_provider()
            .or_else(Region::new("ap-south-1"));

        let config = aws_config::from_env().region(region_provider).load().await;

        Self {
            client: DynamoClient::new(&config),
            table_name: table_name.to_string(),
        }
    }

    pub async fn put_item(&self, metadata: &UrlMetaData) -> Result<(), StorageError> {
        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("url", AttributeValue::S(metadata.url.clone()))
            .item(
                "content_hash",
                AttributeValue::S(metadata.content_hash.clone()),
            )
            .item(
                "storage_path",
                AttributeValue::S(metadata.storage_path.clone()),
            )
            .item(
                "last_crawled",
                AttributeValue::S(metadata.last_crawled.to_rfc3339()),
            )
            .item("depth", AttributeValue::N(metadata.depth.to_string()))
            .send()
            .await
            .map_err(|e| StorageError::DynamoDB(e.to_string()))?;

        Ok(())
    }

    pub async fn hash_exists(&self, hash: &str) -> Result<bool, StorageError> {
        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .index_name("hash-index") // GSI you created
            .key_condition_expression("#h = :hash")
            .expression_attribute_names("#h", "content_hash")
            .expression_attribute_values(":hash", AttributeValue::S(hash.to_string()))
            .limit(1) // only need to know if one exists
            .send()
            .await
            .map_err(|e| StorageError::DynamoDB(e.to_string()))?;

        Ok(result.count() > 0)
    }
    pub async fn put_domain(&self, record: &DomainRecord) -> Result<(), StorageError> {
        let disallow_json = serde_json::to_string(&record.disallow)
            .map_err(|e| StorageError::DynamoDB(e.to_string()))?;

        self.client
            .put_item()
            .table_name("CrawlerDomains")
            .item("domain", AttributeValue::S(record.domain.clone()))
            .item("disallow", AttributeValue::S(disallow_json))
            .item(
                "crawl_delay",
                AttributeValue::N(record.crawl_delay.unwrap_or(0).to_string()),
            )
            .item(
                "fetched_at",
                AttributeValue::S(record.fetched_at.to_rfc3339()),
            )
            .send()
            .await
            .map_err(|e| StorageError::DynamoDB(e.to_string()))?;

        Ok(())
    }

    pub async fn get_domain(&self, domain: &str) -> Result<Option<DomainRecord>, StorageError> {
        let result = self
            .client
            .get_item()
            .table_name("CrawlerDomains")
            .key("domain", AttributeValue::S(domain.to_string()))
            .send()
            .await
            .map_err(|e| StorageError::DynamoDB(e.to_string()))?;

        let item = match result.item() {
            Some(i) => i,
            None => return Ok(None),
        };

        let domain = item["domain"].as_s().unwrap().clone();
        let disallow: Vec<String> =
            serde_json::from_str(item["disallow"].as_s().unwrap()).unwrap_or_default();
        let crawl_delay = item
            .get("crawl_delay")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .filter(|&d| d > 0);
        let fetched_at = item["fetched_at"]
            .as_s()
            .unwrap()
            .parse::<DateTime<Utc>>()
            .unwrap();

        Ok(Some(DomainRecord {
            domain,
            disallow,
            crawl_delay,
            fetched_at,
        }))
    }
}

#[tokio::test]
async fn test_s3_upload() {
    let storage = S3Storage::new("webcrawler-yash-test", "ap-south-1").await;
    let result = storage
        .store_html("testhash123", b"<html>test</html>")
        .await;
    println!("{:?}", result);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_dynamo() {
    use chrono::Utc;

    let storage = DynamoStorage::new("UrlMetadata", "ap-south-1").await;

    let metadata = UrlMetaData {
        url: String::from("https://example.com"),
        content_hash: String::from("testhash123"),
        storage_path: String::from("html/testhash123.html"),
        last_crawled: Utc::now(),
        depth: 0,
    };

    // test put
    let put = storage.put_item(&metadata).await;
    println!("put: {:?}", put);
    assert!(put.is_ok());

    // test hash exists
    let exists = storage.hash_exists("testhash123").await;
    println!("exists: {:?}", exists);
    assert!(exists.unwrap() == true);

    // test hash not exists
    let not_exists = storage.hash_exists("doesnotexist").await;
    assert!(not_exists.unwrap() == false);
}
