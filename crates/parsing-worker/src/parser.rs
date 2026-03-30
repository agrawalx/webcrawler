use scraper::{Html, Selector};
use url::{ParseError, Url};

pub fn extract_url(html: &str, base_url: &str) -> Result<Vec<String>, ParseError> {
    let document = Html::parse_document(html);
    let base = Url::parse(base_url)?;
    let selector = Selector::parse("a").unwrap();

    let urls = document
        .select(&selector)
        .filter_map(|el| el.value().attr("href"))
        .filter(|href| {
            !href.starts_with("javascript:")
                && !href.starts_with("mailto:")
                && !href.starts_with('#')
        })
        .filter_map(|href| {
            let parsed = Url::parse(href).or_else(|_| base.join(href)).ok()?;
            match parsed.scheme() {
                "http" | "https" => Some(parsed.to_string()),
                _ => None,
            }
        })
        .collect();

    Ok(urls)
}

/// Strip HTML tags and return plain text. Placeholder until a proper text
/// extraction pass is implemented.
pub fn extract_text(html: &str) -> String {
    let document = Html::parse_document(html);
    document.root_element().text().collect::<Vec<_>>().join(" ")
}
