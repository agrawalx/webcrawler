use scraper::{Html, Selector};
use serde::Serialize;

use crate::parser::extract_url;

#[derive(Serialize)]
pub struct ParsedPage {
    pub url: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub body_text: String,
    pub links: Vec<String>,
    pub word_count: usize,
}

pub fn parse(html: &str, url: &str) -> ParsedPage {
    let document = Html::parse_document(html);

    let title = extract_title(&document);
    let description = extract_description(&document);
    let body_text = extract_body_text(&document);
    let links = extract_url(html, url).unwrap_or_default();
    let word_count = body_text.split_whitespace().count();

    ParsedPage { url: url.to_string(), title, description, body_text, links, word_count }
}

fn extract_title(document: &Html) -> Option<String> {
    let selector = Selector::parse("title").unwrap();
    document
        .select(&selector)
        .next()
        .map(|el| el.text().collect::<String>().trim().to_string())
        .filter(|s| !s.is_empty())
}

fn extract_description(document: &Html) -> Option<String> {
    let selector = Selector::parse(r#"meta[name="description"]"#).unwrap();
    document
        .select(&selector)
        .next()
        .and_then(|el| el.value().attr("content"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn extract_body_text(document: &Html) -> String {
    // tags whose subtrees we skip entirely
    let noise = Selector::parse("script, style, nav, footer, header").unwrap();
    let body_sel = Selector::parse("body").unwrap();

    let body = match document.select(&body_sel).next() {
        Some(b) => b,
        None => return String::new(),
    };

    let noisy_ids: std::collections::HashSet<_> = body
        .select(&noise)
        .map(|el| el.id())
        .collect();

    body.descendants()
        .filter_map(|node| {
            // skip nodes that are inside a noisy element
            if node.ancestors().any(|a| noisy_ids.contains(&a.id())) {
                return None;
            }
            node.value().as_text().map(|t| t.trim().to_string())
        })
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}
