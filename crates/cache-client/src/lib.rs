pub mod error;
use error::CacheError;
#[derive(Clone)]
pub struct RedisClient {
    pool: deadpool_redis::Pool,
}

impl RedisClient {
    pub async fn new(redis_url: &str) -> Result<Self, CacheError> {
        let cfg = deadpool_redis::Config::from_url(redis_url);
        let pool = cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
        Ok(Self { pool })
    }

    pub async fn check_rate_limit(&self, domain: &str, limit: u8) -> Result<bool, CacheError> {
        let mut conn = self.pool.get().await?;
        let script = redis::Script::new(
            r#"
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local limit = tonumber(ARGV[3])
            local id = ARGV[4]
            
            redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
            local count = redis.call('ZCARD', key)
            
            if count < limit then
                redis.call('ZADD', key, now, id)
                redis.call('EXPIRE', key, math.ceil(window / 1000))
                return 1
            else
                return 0
            end
        "#,
        );
        let key = format!("rate_limit:{domain}");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let id = uuid::Uuid::new_v4().to_string();

        let result: i32 = script
            .key(&key)
            .arg(now) // ARGV[1]: current timestamp ms
            .arg(1000u64) // ARGV[2]: window size (1 second)
            .arg(limit) // ARGV[3]: max requests
            .arg(&id) // ARGV[4]: unique request id
            .invoke_async(&mut *conn)
            .await?;

        Ok(result == 1)
    }
}
