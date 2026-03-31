#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("SQS error: {0}")]
    SqsError(String),

    #[error("Serialize error: {0}")]
    SerializeError(#[from] serde_json::Error),

    #[error("Empty message body")]
    EmptyBody,
}
