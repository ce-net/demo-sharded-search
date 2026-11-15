//! The demo's public cell runtime. Each cell instance is an [`ident::Identity`] (its own Ed25519 key), holds a
//! [`cap::Chain`] that says what it may call, serves a typed api with [`rpc::Server`] and keeps its state in a
//! [`store::Store`].
pub mod cap;
pub mod ident;
pub mod rpc;
pub mod store;

pub use rpc::{Caller, Client, Creds, Op, Pool, RpcError, Server};

/// Common command-line bits every cell takes, read from `--flag value` pairs and `SS_*` env vars.
pub struct Args {
    map: std::collections::HashMap<String, String>,
    pub rest: Vec<String>,
}

impl Args {
    pub fn parse() -> Args {
        let mut map = std::collections::HashMap::new();
        let mut rest = Vec::new();
        let mut it = std::env::args().skip(1).peekable();
        while let Some(a) = it.next() {
            if let Some(k) = a.strip_prefix("--") {
                if let Some((k, v)) = k.split_once('=') {
                    map.insert(k.to_string(), v.to_string());
                } else if it.peek().is_some_and(|n| !n.starts_with("--")) {
                    map.insert(k.to_string(), it.next().unwrap());
                } else {
                    map.insert(k.to_string(), "true".into());
                }
            } else {
                rest.push(a);
            }
        }
        Args { map, rest }
    }

    /// `--name`, else env `SS_NAME` (dashes become underscores), else `default`.
    pub fn get(&self, name: &str, default: &str) -> String {
        self.map.get(name).cloned().or_else(|| std::env::var(format!("SS_{}", name.to_uppercase().replace('-', "_"))).ok()).unwrap_or_else(|| default.to_string())
    }

    pub fn num(&self, name: &str, default: u64) -> u64 {
        self.get(name, &default.to_string()).parse().unwrap_or(default)
    }

    pub fn has(&self, name: &str) -> bool {
        self.map.contains_key(name)
    }
}

/// A cell's identity and chain from `--seed`/`--seed-file` and `--cap`/`--cap-file` (or `SS_SEED`, `SS_CAP`), and the
/// trusted roots from `--roots` (comma-separated public keys) or `--roots-file`.
pub fn creds_from(a: &Args) -> Result<(Creds, Vec<String>), String> {
    let me = if !a.get("seed", "").is_empty() {
        ident::Identity::from_seed_hex(&a.get("seed", ""))?
    } else if !a.get("seed-file", "").is_empty() {
        ident::Identity::load_or_create(std::path::Path::new(&a.get("seed-file", "")))?
    } else {
        ident::Identity::generate()
    };
    let cap_s = if !a.get("cap", "").is_empty() { a.get("cap", "") } else { std::fs::read_to_string(a.get("cap-file", "")).unwrap_or_default() };
    let chain = if cap_s.trim().is_empty() { cap::Chain(vec![]) } else { cap::Chain::decode(&cap_s)? };
    let roots_s = if !a.get("roots", "").is_empty() { a.get("roots", "") } else { std::fs::read_to_string(a.get("roots-file", "")).unwrap_or_default() };
    let roots: Vec<String> = roots_s.split([',', '\n', ' ']).map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
    Ok((Creds { me, chain }, roots))
}
