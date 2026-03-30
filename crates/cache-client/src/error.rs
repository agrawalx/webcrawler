use deadpool_redis::redis;

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("create pool error")]
    CreationError(#[from] deadpool_redis::CreatePoolError), 
    #[error("connection pool error")]
    ConnectionError(#[from] deadpool_redis::PoolError),
    #[error("Script error")]
    CommandError(#[from] redis::RedisError)
}