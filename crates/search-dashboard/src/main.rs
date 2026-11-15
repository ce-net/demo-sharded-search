//! `search-dashboard`: the live map. It follows the store's change feed (shards, hosts, metrics, rebalancer events)
//! and pushes one compact state to every browser twice a second over Server-Sent Events. It serves the same page that
//! replays a recorded run, so the live view and the recorded proof look the same.
//!
//! GET /             the page (from --web)
//! GET /api/state    one state, JSON
//! GET /api/events   the state every 500 ms (text/event-stream)
use cellnet::store::Store;
use search_api::*;
use sharded_core::lease::View;
use sharded_core::mono_ms;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Default)]
struct State {
    place: BTreeMap<u32, PlaceRec>,
    inst: HashMap<String, InstRec>,
    hosts: BTreeMap<String, HostRec>,
    view: View,
    hview: View,
    metrics: VecDeque<serde_json::Value>,
    events: VecDeque<serde_json::Value>,
    manifest: Option<Manifest>,
}

impl State {
    fn apply(&mut self, d: &cellnet::store::Doc) {
        let now = mono_ms();
        if d.key == keys::MANIFEST {
            self.manifest = serde_json::from_str(&d.value).ok();
        } else if d.key == keys::METRICS {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&d.value) {
                self.metrics.push_back(v);
                while self.metrics.len() > 300 {
                    self.metrics.pop_front();
                }
            }
        } else if d.key.starts_with(keys::EVENTS) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&d.value) {
                self.events.push_back(v);
                while self.events.len() > 50 {
                    self.events.pop_front();
                }
            }
        } else if d.key.starts_with(keys::HOSTS) {
            if let Ok(h) = serde_json::from_str::<HostRec>(&d.value) {
                self.hview.observe(&h.machine, h.beat, now);
                self.hosts.insert(h.machine.clone(), h);
            }
        } else if d.key.ends_with("/place") {
            if let Ok(p) = serde_json::from_str::<PlaceRec>(&d.value) {
                self.place.insert(p.shard, p);
            }
        } else if d.key.starts_with(keys::SHARDS) {
            if let Ok(r) = serde_json::from_str::<InstRec>(&d.value) {
                self.view.observe(&r.id, r.beat, now);
                self.inst.insert(r.id.clone(), r);
            }
        }
    }

    fn snapshot(&self, ttl: u64) -> serde_json::Value {
        let now = mono_ms();
        let machines: Vec<serde_json::Value> = self.hosts.values().map(|h| serde_json::json!({"machine": h.machine, "up": self.hview.live(&h.machine, now, ttl), "instances": h.instances.len(), "cores": h.cores})).collect();
        let shards: Vec<serde_json::Value> = self
            .place
            .values()
            .map(|p| {
                let ms: Vec<serde_json::Value> = p
                    .members
                    .iter()
                    .map(|id| match self.inst.get(id) {
                        Some(r) => serde_json::json!({"id": id, "machine": r.machine, "role": r.role, "ready": r.ready, "live": !r.gone && self.view.live(id, now, ttl)}),
                        None => serde_json::json!({"id": id, "machine": "", "role": "joining", "ready": false, "live": false}),
                    })
                    .collect();
                serde_json::json!({"shard": p.shard, "members": ms})
            })
            .collect();
        serde_json::json!({
            "corpus": self.manifest.as_ref().map(|m| serde_json::json!({"name": m.name, "docs": m.docs, "shards": m.shards, "replicas": m.replicas, "hash": m.hash})),
            "machines": machines,
            "shards": shards,
            "metrics": self.metrics.iter().rev().take(120).rev().collect::<Vec<_>>(),
            "events": self.events.iter().rev().take(12).collect::<Vec<_>>(),
        })
    }
}

async fn follow(st: Arc<Mutex<State>>, store: Store) {
    let mut since = 0;
    loop {
        match store.changes("ss/", since, 1000).await {
            Ok(c) => {
                let mut s = st.lock().unwrap();
                for d in &c.docs {
                    s.apply(d);
                }
                since = c.head;
            }
            Err(e) => {
                eprintln!("store: {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

fn content_type(p: &str) -> &'static str {
    match p.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript",
        "css" => "text/css",
        "json" => "application/json",
        "jsonl" => "application/x-ndjson",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}

async fn serve(mut s: tokio::net::TcpStream, st: Arc<Mutex<State>>, web: PathBuf, ttl: u64) -> std::io::Result<()> {
    let mut buf = vec![0u8; 4096];
    let n = s.read(&mut buf).await?;
    let req = String::from_utf8_lossy(&buf[..n]);
    let path = req.split_whitespace().nth(1).unwrap_or("/").split('?').next().unwrap_or("/").to_string();
    if path == "/api/state" {
        let body = st.lock().unwrap().snapshot(ttl).to_string();
        let h = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncache-control: no-store\r\ncontent-length: {}\r\n\r\n", body.len());
        s.write_all(h.as_bytes()).await?;
        return s.write_all(body.as_bytes()).await;
    }
    if path == "/api/events" {
        s.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-store\r\n\r\n").await?;
        loop {
            let body = st.lock().unwrap().snapshot(ttl).to_string();
            s.write_all(format!("data: {body}\n\n").as_bytes()).await?;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    let rel = if path == "/" { "index.html".to_string() } else { path.trim_start_matches('/').to_string() };
    if rel.contains("..") {
        return s.write_all(b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n").await;
    }
    match tokio::fs::read(web.join(&rel)).await {
        Ok(b) => {
            let h = format!("HTTP/1.1 200 OK\r\ncontent-type: {}\r\ncontent-length: {}\r\n\r\n", content_type(&rel), b.len());
            s.write_all(h.as_bytes()).await?;
            s.write_all(&b).await
        }
        Err(_) => s.write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n").await,
    }
}

#[tokio::main]
async fn main() {
    let a = cellnet::Args::parse();
    let (creds, _) = match cellnet::creds_from(&a) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("search-dashboard: {e}");
            std::process::exit(1);
        }
    };
    let store = Store::new(&a.get("store", "127.0.0.1:7811"), creds);
    let st = Arc::new(Mutex::new(State::default()));
    tokio::spawn(follow(st.clone(), store));
    let web = PathBuf::from(a.get("web", "."));
    let ttl = a.num("ttl-ms", 3000);
    let listen = a.get("listen", "127.0.0.1:7830");
    let l = tokio::net::TcpListener::bind(&listen).await.expect("bind");
    eprintln!("search.dashboard at http://{listen}/");
    loop {
        if let Ok((s, _)) = l.accept().await {
            let (st, web) = (st.clone(), web.clone());
            tokio::spawn(async move {
                let _ = serve(s, st, web, ttl).await;
            });
        }
    }
}
