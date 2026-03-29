use std::time::Duration;

use serde::Deserialize; 
#[derive(Debug, Deserialize)]

pub struct Settings {
    #[serde(default = "default_timeout_secs")]
    pub time_out_duration: u64,
    #[serde(default = "default_allowed_size")]
    pub maximum_allowed_size: u64, // represents size in bytes
    #[serde(default = "default_user_agent")]
    pub user_agent: String
}
fn default_timeout_secs() -> u64 {
    15
}
fn default_allowed_size() -> u64 {
    2000000
}
fn default_user_agent() -> String {
    String::from("my-crawler/0.1")
}

impl Settings {
    pub fn from_env() -> Result<Self, envy::Error> {
        dotenvy::dotenv().ok(); // loads .env file if present
        envy::from_env::<Settings>()
    }

    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.time_out_duration)
    }
}
