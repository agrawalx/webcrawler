use aws_config::meta::region::RegionProviderChain;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::config::Region;
use domain::models::{CrawlJob, ParseJob};

use crate::error::QueueError;
pub mod error;

pub struct SqsClient {
    client: Client,
}

impl SqsClient {
    pub async fn new(region: &str) -> Self {
        let region_provider = RegionProviderChain::first_try(Region::new(region.to_string()))
            .or_default_provider()
            .or_else(Region::new("ap-south-1"));

        let config = aws_config::from_env().region(region_provider).load().await;

        Self {
            client: Client::new(&config),
        }
    }

    pub async fn send_crawl_job(&self, queue_url: &str, job: CrawlJob) -> Result<(), QueueError> {
        let body = serde_json::to_string(&job)?;
        self.client
            .send_message()
            .queue_url(queue_url)
            .message_body(body)
            .send()
            .await
            .map_err(|e| QueueError::SqsError(e.to_string()))?;
        Ok(())
    }

    pub async fn receive_crawl_job(
        &self,
        queue_url: &str,
    ) -> Result<Option<(CrawlJob, String)>, QueueError> {
        let recv_msg = self
            .client
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(1)
            .wait_time_seconds(20)
            .send()
            .await
            .map_err(|e| QueueError::SqsError(e.to_string()))?;

        let message = match recv_msg.messages().first() {
            Some(m) => m,
            None => return Ok(None),
        };

        let body = message.body().ok_or(QueueError::EmptyBody)?;
        let job: CrawlJob = serde_json::from_str(body)?;
        let receipt = message
            .receipt_handle()
            .ok_or(QueueError::EmptyBody)?
            .to_string();

        Ok(Some((job, receipt)))
    }

    pub async fn send_parse_job(&self, queue_url: &str, job: ParseJob) -> Result<(), QueueError> {
        let body = serde_json::to_string(&job)?;
        self.client
            .send_message()
            .queue_url(queue_url)
            .message_body(body)
            .send()
            .await
            .map_err(|e| QueueError::SqsError(e.to_string()))?;
        Ok(())
    }

    pub async fn receive_parse_job(
        &self,
        queue_url: &str,
    ) -> Result<Option<(ParseJob, String)>, QueueError> {
        let recv_msg = self
            .client
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(1)
            .wait_time_seconds(20)
            .send()
            .await
            .map_err(|e| QueueError::SqsError(e.to_string()))?;

        let message = match recv_msg.messages().first() {
            Some(m) => m,
            None => return Ok(None),
        };

        let body = message.body().ok_or(QueueError::EmptyBody)?;
        let job: ParseJob = serde_json::from_str(body)?;
        let receipt = message
            .receipt_handle()
            .ok_or(QueueError::EmptyBody)?
            .to_string();

        Ok(Some((job, receipt)))
    }

    pub async fn delete_message(
        &self,
        queue_url: &str,
        receipt_handle: &str,
    ) -> Result<(), QueueError> {
        self.client
            .delete_message()
            .queue_url(queue_url)
            .receipt_handle(receipt_handle)
            .send()
            .await
            .map_err(|e| QueueError::SqsError(e.to_string()))?;
        Ok(())
    }
}
