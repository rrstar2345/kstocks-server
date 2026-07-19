use std::sync::OnceLock;

/// Shared User-Agent header used for all NSE API requests.
pub const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";

/// App-lifetime, shared `reqwest::Client`. Cheap to clone (Arc-backed), reused
/// across concurrent tasks so TCP/TLS connections to www.nseindia.com get pooled.
static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

pub fn get_client() -> reqwest::Client {
    HTTP_CLIENT.get_or_init(reqwest::Client::new).clone()
}

pub fn get(url: &str) -> reqwest::RequestBuilder {
    get_client().get(url).header("User-Agent", USER_AGENT)
}
