use std::path::PathBuf;

use domain::models::UrlMetaData;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Failed to write file: {0}")]
    WriteError(#[from] std::io::Error),
    #[error("Failed to serialize metadata: {0}")]
    SerializeError(#[from] serde_json::Error),
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
