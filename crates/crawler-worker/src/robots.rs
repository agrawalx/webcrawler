use chrono::Utc;
use domain::models::DomainRecord;
use reqwest::Client;

pub async fn fetch_robots(client: &Client, domain: &str) -> DomainRecord {
    let url = format!("https://{domain}/robots.txt");

    // if fetch fails → return permissive record (allow everything)
    // robots.txt not existing = no restrictions
    let text = match client.get(&url).send().await {
        Ok(resp) => resp.text().await.unwrap_or_default(),
        Err(_) => {
            return DomainRecord {
                domain: domain.to_string(),
                disallow: vec![],
                crawl_delay: None,
                fetched_at: Utc::now(),
            };
        }
    };

    parse_robots(&text, domain)
}

fn parse_robots(text: &str, domain: &str) -> DomainRecord {
    let mut disallow = vec![];
    let mut crawl_delay = None;
    let mut applicable = false; // are current rules for our agent?

    for line in text.lines() {
        let line = line.trim();

        if line.starts_with("User-agent:") {
            let agent = line.trim_start_matches("User-agent:").trim();
            // rules apply if agent is * or matches our crawler name
            applicable = agent == "*" || agent == "my-crawler";
        }

        if !applicable {
            continue;
        }

        if line.starts_with("Disallow:") {
            let path = line.trim_start_matches("Disallow:").trim();
            if !path.is_empty() {
                disallow.push(path.to_string());
            }
        }

        if line.starts_with("Crawl-delay:") {
            let delay = line.trim_start_matches("Crawl-delay:").trim();
            crawl_delay = delay.parse::<u64>().ok();
        }
    }

    DomainRecord {
        domain: domain.to_string(),
        disallow,
        crawl_delay,
        fetched_at: Utc::now(),
    }
}

pub fn permissive_record(domain: &str) -> DomainRecord {
    DomainRecord {
        domain: domain.to_string(),
        disallow: vec![],
        crawl_delay: None,
        fetched_at: Utc::now(),
    }
}

pub fn is_allowed(record: &DomainRecord, url: &str) -> bool {
    // extract path from url
    let path = url::Url::parse(url)
        .map(|u| u.path().to_string())
        .unwrap_or_default();

    // check if path starts with any disallow rule
    !record
        .disallow
        .iter()
        .any(|rule| path.starts_with(rule.as_str()))
}
