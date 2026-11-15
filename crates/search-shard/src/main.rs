//! `search-shard`: one replica of one shard. It serves the search.shard api on its own port, writes its own record to
//! the store every tick, takes part in the shard's lease (one leader per shard) and installs its segment from this
//! machine's segment cache or, when the cache does not have it, from a ready replica of the same shard.
use cellnet::rpc::{bind, Pool, RpcError, Server};
use cellnet::store::Store;
use search_api::*;
use sharded_core::lease::{self, Emitted, Seen, View};
use sharded_core::segment::{self, Hit, Segment};
use sharded_core::{mono_ms, now_ms, text};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

struct DeltaDoc {
    doc: IngestDoc,
    tf: HashMap<String, u32>,
    dl: u32,
}

struct Shard {
    id: String,
    pubkey: String,
    machine: String,
    shard: u32,
    boot: u64,
    started: std::time::Instant,
    cache: PathBuf,
    seg: RwLock<Option<Arc<Segment>>>,
    delta: RwLock<Vec<DeltaDoc>>,
    role: Mutex<String>,
    members: Mutex<Vec<String>>,
    peers: Mutex<Vec<InstRec>>,
    installing: AtomicBool,
    beat: AtomicU64,
    pool: Pool,
}

impl Shard {
    fn ready(&self) -> bool {
        self.seg.read().unwrap().is_some()
    }

    fn role(&self) -> String {
        self.role.lock().unwrap().clone()
    }

    fn query(&self, r: QueryReq) -> Result<QueryResp, RpcError> {
        let t0 = std::time::Instant::now();
        let seg = self.seg.read().unwrap().clone().ok_or_else(|| RpcError::new("unavailable", "segment not installed yet"))?;
        let terms = text::query_terms(&r.q);
        let k = (r.k.clamp(1, 100)) as usize;
        let mut hits = seg.search(&terms, k);
        let delta = self.delta.read().unwrap();
        if !delta.is_empty() {
            let mut dh = Vec::new();
            for d in delta.iter() {
                let mut s = 0f32;
                for t in &terms {
                    if let Some(tf) = d.tf.get(t) {
                        let df = seg.df(t) as u64 + 1;
                        s += text::bm25(text::idf(seg.n_docs as u64 + delta.len() as u64, df), *tf as f32, d.dl as f32, seg.avgdl);
                    }
                }
                if s > 0.0 {
                    dh.push(Hit { doc: d.doc.doc.clone(), title: d.doc.title.clone(), score: s });
                }
            }
            hits = segment::merge(vec![hits, dh], k);
        }
        Ok(QueryResp { hits, inst: self.id.clone(), machine: self.machine.clone(), shard: self.shard, role: self.role(), seg: seg.hash.clone(), docs: seg.n_docs as u64 + delta.len() as u64, took_us: t0.elapsed().as_micros() as u64 })
    }

    fn add_delta(&self, docs: &[IngestDoc]) -> u32 {
        let mut d = self.delta.write().unwrap();
        let mut n = 0;
        for doc in docs {
            if d.iter().any(|x| x.doc.doc == doc.doc) {
                continue;
            }
            let toks = text::tokenize(&text::doc_text(&doc.title, &doc.text));
            let mut tf = HashMap::new();
            for t in &toks {
                *tf.entry(t.clone()).or_insert(0) += 1;
            }
            d.push(DeltaDoc { doc: doc.clone(), tf, dl: toks.len() as u32 });
            n += 1;
        }
        n
    }

    async fn ingest(self: Arc<Self>, r: IngestReq) -> Result<IngestResp, RpcError> {
        if !r.replicated && self.role() != "leader" {
            let leader = self.peers.lock().unwrap().iter().find(|p| p.role == "leader").map(|p| p.addr.clone()).unwrap_or_default();
            return Err(RpcError::new("not_leader", format!("leader: {leader}")));
        }
        for d in &r.docs {
            if d.doc.len() != 32 || hex::decode(&d.doc).is_err() || d.doc != text::doc_hash(&d.id) {
                return Err(RpcError::new("invalid", format!("{}: not a doc key", d.doc)));
            }
        }
        let accepted = self.add_delta(&r.docs);
        let mut acked = 0;
        if !r.replicated {
            let followers: Vec<String> = self.peers.lock().unwrap().iter().filter(|p| p.id != self.id && !p.gone).map(|p| p.addr.clone()).collect();
            let mut tasks = Vec::new();
            for a in followers {
                let me = self.clone();
                let req = IngestReq { docs: r.docs.clone(), replicated: true };
                tasks.push(tokio::spawn(async move { me.pool.call::<ShardIngest>(&a, &req, Duration::from_secs(3)).await.is_ok() }));
            }
            for t in tasks {
                if t.await.unwrap_or(false) {
                    acked += 1;
                }
            }
        }
        Ok(IngestResp { accepted, delta: self.delta.read().unwrap().len() as u64, followers_acked: acked })
    }

    fn snapshot(&self, r: SnapshotReq) -> Result<SnapshotResp, RpcError> {
        let seg = self.seg.read().unwrap().clone().ok_or_else(|| RpcError::new("unavailable", "segment not installed"))?;
        let b = seg.bytes();
        let off = (r.offset as usize).min(b.len());
        let len = (r.len.clamp(1, 8 << 20) as usize).min(b.len() - off);
        let delta = if r.offset == 0 { self.delta.read().unwrap().iter().map(|d| d.doc.clone()).collect() } else { vec![] };
        Ok(SnapshotResp { hash: seg.hash.clone(), total: b.len() as u64, offset: off as u64, data: hex::encode(&b[off..off + len]), delta })
    }

    /// Install segment `hash`: from this machine's cache when it is there, else copied from `from`.
    async fn install(self: &Arc<Self>, from: &str, hash: &str) -> Result<InstallResp, RpcError> {
        let t0 = std::time::Instant::now();
        let path = segment::seg_path(&self.cache, hash);
        let mut copied = false;
        let mut delta = vec![];
        if !path.exists() {
            if from.is_empty() {
                return Err(RpcError::new("unavailable", "no ready replica to copy from"));
            }
            let mut buf: Vec<u8> = Vec::new();
            let mut total = u64::MAX;
            while (buf.len() as u64) < total {
                let r = self.pool.call::<ShardSnapshot>(from, &SnapshotReq { offset: buf.len() as u64, len: 4 << 20 }, Duration::from_secs(30)).await?;
                if r.hash != hash {
                    return Err(RpcError::failed(format!("{from} holds {}, not {hash}", &r.hash[..12.min(r.hash.len())])));
                }
                if r.offset == 0 {
                    delta = r.delta;
                }
                total = r.total;
                buf.extend_from_slice(&hex::decode(&r.data).map_err(|e| RpcError::failed(e.to_string()))?);
                if r.data.is_empty() {
                    break;
                }
            }
            let got = segment::store_bytes(&self.cache, &buf).map_err(|e| RpcError::failed(e.to_string()))?;
            if got != hash {
                return Err(RpcError::failed(format!("copied bytes hash to {got}, expected {hash}")));
            }
            copied = true;
        }
        let seg = tokio::task::spawn_blocking({
            let path = path.clone();
            move || Segment::open(&path, true)
        })
        .await
        .map_err(|e| RpcError::failed(e.to_string()))?
        .map_err(RpcError::failed)?;
        if seg.hash != hash {
            return Err(RpcError::failed(format!("cache file hashes to {}", seg.hash)));
        }
        let bytes = seg.bytes().len() as u64;
        self.add_delta(&delta);
        *self.seg.write().unwrap() = Some(Arc::new(seg));
        Ok(InstallResp { hash: hash.to_string(), bytes, ms: t0.elapsed().as_millis() as u64, copied })
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("search-shard: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let a = cellnet::Args::parse();
    let (creds, roots) = cellnet::creds_from(&a)?;
    let shard = a.num("shard", 0) as u32;
    let machine = a.get("machine", "local");
    let tick = a.num("tick-ms", 500);
    let ttl = a.num("ttl-ms", 3000);
    let data = PathBuf::from(a.get("data", "run/data"));
    let cache = PathBuf::from(a.get("cache", &data.join("cache").to_string_lossy()));
    let (l, addr) = bind(&a.get("listen", "0.0.0.0:0"), &a.get("advertise", "127.0.0.1")).await.map_err(|e| e.to_string())?;
    let store = Store::new(&a.get("store", "127.0.0.1:7811"), creds.clone());
    let me = creds.me.clone();
    let s = Arc::new(Shard {
        id: me.id(),
        pubkey: me.public_hex(),
        machine: machine.clone(),
        shard,
        boot: now_ms(),
        started: std::time::Instant::now(),
        cache,
        seg: RwLock::new(None),
        delta: RwLock::new(vec![]),
        role: Mutex::new("joining".into()),
        members: Mutex::new(vec![]),
        peers: Mutex::new(vec![]),
        installing: AtomicBool::new(false),
        beat: AtomicU64::new(0),
        pool: Pool::new(creds.clone()),
    });
    eprintln!("search.shard {} shard {} on {} at {}", s.id, shard_label(shard), machine, addr);
    let audit_store = store.clone();
    let audit_id = s.id.clone();
    let file_sink = cellnet::rpc::audit_file(data.join("audit.jsonl"));
    let (s1, s2, s3, s4, s5) = (s.clone(), s.clone(), s.clone(), s.clone(), s.clone());
    let srv = Server::new(SHARD, me.clone(), roots)
        .audit(Arc::new(move |row| {
            file_sink(row.clone());
            let st = audit_store.clone();
            let id = audit_id.clone();
            tokio::spawn(async move {
                let _ = st.put_json(&keys::audit(&id, row.t_ms), &row).await;
            });
        }))
        .on::<ShardHealth, _, _>(move |_: (), _| {
            let s = s1.clone();
            async move { Ok(Health { ok: s.ready(), version: env!("CARGO_PKG_VERSION").into(), uptime_s: s.started.elapsed().as_secs(), id: s.id.clone(), machine: s.machine.clone() }) }
        })
        .on::<ShardQuery, _, _>(move |r: QueryReq, _| {
            let s = s2.clone();
            async move { s.query(r) }
        })
        .on::<ShardIngest, _, _>(move |r: IngestReq, _| {
            let s = s3.clone();
            async move { s.ingest(r).await }
        })
        .on::<ShardSnapshot, _, _>(move |r: SnapshotReq, _| {
            let s = s4.clone();
            async move { s.snapshot(r) }
        })
        .on::<ShardInstall, _, _>(move |r: InstallReq, _| {
            let s = s5.clone();
            async move { s.install(&r.from, &r.hash).await }
        });
    tokio::spawn(async move {
        if let Err(e) = srv.serve(l).await {
            eprintln!("serve: {e}");
            std::process::exit(1);
        }
    });

    let mut view = View::default();
    let mut emitted = Emitted::default();
    let mut was_member = false;
    let mut out_since: Option<u64> = None;
    let mut last_role = String::new();
    loop {
        let beat = s.beat.fetch_add(1, Ordering::SeqCst) + 1;
        let now = mono_ms();
        emitted.record(beat, now);
        let docs = match store.list(&keys::shard_prefix(shard)).await {
            Ok(d) => d,
            Err(e) => {
                eprintln!("store: {e}");
                tokio::time::sleep(Duration::from_millis(tick)).await;
                continue;
            }
        };
        let mut place = PlaceRec::default();
        let mut recs: Vec<InstRec> = vec![];
        for d in &docs {
            if d.key.ends_with("/place") {
                place = serde_json::from_str(&d.value).unwrap_or_default();
            } else if let Ok(r) = serde_json::from_str::<InstRec>(&d.value) {
                if r.id != s.id {
                    recs.push(r);
                }
            }
        }
        let members = place.members.clone();
        let ready = s.ready();
        let mut seen: HashMap<String, Seen> = HashMap::new();
        for r in &recs {
            if r.gone {
                continue;
            }
            view.observe(&r.id, r.beat, now);
            let vote = if r.vote_for.is_empty() { None } else { Some(lease::Vote { for_id: r.vote_for.clone(), beat: r.vote_beat }) };
            seen.insert(r.id.clone(), Seen { id: r.id.clone(), beat: r.beat, ready: r.ready, vote });
        }
        view.observe(&s.id, beat, now);
        seen.insert(s.id.clone(), Seen { id: s.id.clone(), beat, ready, vote: None });
        let my_vote = if members.contains(&s.id) { lease::vote(&members, &seen, &view, now, ttl) } else { None };
        seen.get_mut(&s.id).unwrap().vote = my_vote.clone();
        let member = members.contains(&s.id);
        let leader = member && lease::is_leader(&s.id, &members, &seen, &view, &emitted, now, ttl);
        let role = if leader { "leader" } else if member && ready { "follower" } else { "joining" };
        if role != last_role {
            eprintln!("{} {} -> {role} (beat {beat})", now_ms(), s.id);
            last_role = role.to_string();
        }
        *s.role.lock().unwrap() = role.to_string();
        *s.members.lock().unwrap() = members.clone();
        *s.peers.lock().unwrap() = recs.iter().filter(|r| members.contains(&r.id) && view.live(&r.id, now, ttl)).cloned().collect();
        let (seg, ndocs) = s.seg.read().unwrap().as_ref().map(|g| (g.hash.clone(), g.n_docs as u64)).unwrap_or_default();
        let rec = InstRec {
            id: s.id.clone(),
            pubkey: s.pubkey.clone(),
            machine: machine.clone(),
            addr: addr.clone(),
            shard,
            pid: std::process::id(),
            boot: s.boot,
            beat,
            ready,
            seg,
            docs: ndocs,
            delta: s.delta.read().unwrap().len() as u64,
            role: role.to_string(),
            vote_for: my_vote.as_ref().map(|v| v.for_id.clone()).unwrap_or_default(),
            vote_beat: my_vote.as_ref().map(|v| v.beat).unwrap_or(0),
            t_ms: now_ms(),
            gone: false,
        };
        if let Err(e) = store.put_json(&keys::inst(shard, &s.id), &rec).await {
            eprintln!("store put: {e}");
        }
        // install the segment once the placement names its hash
        if !ready && !place.seg.is_empty() && !s.installing.swap(true, Ordering::SeqCst) {
            let from = recs
                .iter()
                .filter(|r| r.ready && r.seg == place.seg && view.live(&r.id, now, ttl))
                .min_by_key(|r| (r.machine != machine, r.role != "leader"))
                .map(|r| r.addr.clone())
                .unwrap_or_default();
            let (s2, hash) = (s.clone(), place.seg.clone());
            tokio::spawn(async move {
                match s2.install(&from, &hash).await {
                    Ok(r) => eprintln!("{} installed {} ({} bytes, {} ms, copied {})", s2.id, &r.hash[..12], r.bytes, r.ms, r.copied),
                    Err(e) => eprintln!("{} install: {e}", s2.id),
                }
                s2.installing.store(false, Ordering::SeqCst);
            });
        }
        // retired: the rebalancer took this instance out of the shard
        if member {
            was_member = true;
            out_since = None;
        } else if was_member {
            let t = *out_since.get_or_insert(now);
            if now - t > 2 * ttl {
                let mut gone = rec.clone();
                gone.gone = true;
                gone.role = "retired".into();
                let _ = store.put_json(&keys::inst(shard, &s.id), &gone).await;
                eprintln!("{} retired from {}", s.id, shard_label(shard));
                std::process::exit(0);
            }
        }
        view.forget_except(&members);
        let spent = mono_ms() - now;
        tokio::time::sleep(Duration::from_millis(tick.saturating_sub(spent).max(20))).await;
    }
}
