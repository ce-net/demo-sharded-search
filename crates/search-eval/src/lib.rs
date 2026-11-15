//! The eval cell's library: the exact brute-force baseline, recall@k of the router's answers against it, and a
//! closed-loop query load that reports p50/p99 every second.
use cellnet::rpc::{Creds, Pool, RpcError};
use search_api::{RouterQuery, RouterQueryReq, RouterQueryResp};
use serde::{Deserialize, Serialize};
use sharded_core::{stats, text};
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Query {
    pub id: String,
    pub text: String,
}

/// BEIR queries (`{"_id","text"}` lines). With a qrels file (`query-id\tcorpus-id\tscore` TSV) only queries named in
/// it are kept: the dataset's test split. Sorted by id, so the set and its order are fixed.
pub fn load_queries(path: &Path, qrels: Option<&Path>) -> Result<Vec<Query>, String> {
    let keep: Option<HashSet<String>> = match qrels {
        Some(p) => Some(std::fs::read_to_string(p).map_err(|e| format!("{}: {e}", p.display()))?.lines().skip(1).filter_map(|l| l.split('\t').next().map(|s| s.to_string())).collect()),
        None => None,
    };
    let f = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut out = vec![];
    for l in BufReader::new(f).lines() {
        let l = l.map_err(|e| e.to_string())?;
        let v: serde_json::Value = serde_json::from_str(&l).map_err(|e| e.to_string())?;
        let id = v["_id"].as_str().unwrap_or("").to_string();
        if keep.as_ref().is_some_and(|k| !k.contains(&id)) {
            continue;
        }
        out.push(Query { id, text: v["text"].as_str().unwrap_or("").to_string() });
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(out)
}

pub fn queries_hash(q: &[Query]) -> String {
    sharded_core::segment::sha256_hex(serde_json::to_string(q).unwrap_or_default().as_bytes())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Baseline {
    pub k: usize,
    pub docs: u64,
    pub queries: usize,
    pub queries_hash: String,
    pub corpus: String,
    pub results: BTreeMap<String, Vec<String>>,
    pub build_s: f64,
}

#[derive(PartialEq)]
struct S(f32, String);
impl Eq for S {}
impl PartialOrd for S {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for S {
    // the worst kept hit on top: lowest score, then the larger doc key
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        o.0.partial_cmp(&self.0).unwrap_or(std::cmp::Ordering::Equal).then(self.1.cmp(&o.1))
    }
}

fn for_each_doc(corpus: &Path, limit: u64, mut f: impl FnMut(String, Vec<String>)) -> Result<u64, String> {
    let file = std::fs::File::open(corpus).map_err(|e| format!("{}: {e}", corpus.display()))?;
    let mut n = 0;
    let mut seen = HashSet::new();
    for l in BufReader::with_capacity(1 << 20, file).lines() {
        if n >= limit {
            break;
        }
        let l = l.map_err(|e| e.to_string())?;
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&l) else { continue };
        let Some(id) = v["_id"].as_str() else { continue };
        let h = text::doc_hash(id);
        if !seen.insert(h.clone()) {
            continue;
        }
        let toks = text::tokenize(&text::doc_text(v["title"].as_str().unwrap_or(""), v["text"].as_str().unwrap_or("")));
        f(h, toks);
        n += 1;
    }
    Ok(n)
}

/// Exact BM25 top-k of every query over the whole corpus with global statistics, by scoring every document (no
/// index). Two passes over the file: document frequencies of the query terms, then scores. The same tokenizer, the
/// same doc keys and the same document set (the first `limit`) as the loader.
pub fn baseline(corpus: &Path, queries: &[Query], k: usize, limit: u64) -> Result<Baseline, String> {
    let t0 = Instant::now();
    let qterms: Vec<Vec<String>> = queries.iter().map(|q| text::query_terms(&q.text)).collect();
    let mut term_q: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, ts) in qterms.iter().enumerate() {
        for t in ts {
            term_q.entry(t.clone()).or_default().push(i);
        }
    }
    let mut df: HashMap<String, u64> = HashMap::new();
    let mut total_dl = 0u64;
    let n = for_each_doc(corpus, limit, |_, toks| {
        total_dl += toks.len() as u64;
        let mut u = HashSet::new();
        for t in &toks {
            if term_q.contains_key(t) && u.insert(t.as_str()) {
                *df.entry(t.clone()).or_default() += 1;
            }
        }
    })?;
    let avgdl = total_dl as f32 / n.max(1) as f32;
    let idf: HashMap<&String, f32> = term_q.keys().map(|t| (t, text::idf(n, *df.get(t).unwrap_or(&0)))).collect();
    let mut heaps: Vec<BinaryHeap<S>> = (0..queries.len()).map(|_| BinaryHeap::new()).collect();
    for_each_doc(corpus, limit, |h, toks| {
        let mut tf: HashMap<&str, u32> = HashMap::new();
        for t in &toks {
            if term_q.contains_key(t) {
                *tf.entry(t.as_str()).or_default() += 1;
            }
        }
        if tf.is_empty() {
            return;
        }
        let mut score: HashMap<usize, f32> = HashMap::new();
        for (t, c) in &tf {
            let key = t.to_string();
            let w = idf[&key];
            let s = text::bm25(w, *c as f32, toks.len() as f32, avgdl);
            for q in &term_q[&key] {
                *score.entry(*q).or_default() += s;
            }
        }
        for (q, s) in score {
            let hp = &mut heaps[q];
            if hp.len() < k {
                hp.push(S(s, h.clone()));
            } else if let Some(top) = hp.peek() {
                if s > top.0 || (s == top.0 && h < top.1) {
                    hp.pop();
                    hp.push(S(s, h.clone()));
                }
            }
        }
    })?;
    let mut results = BTreeMap::new();
    for (i, hp) in heaps.into_iter().enumerate() {
        let mut v: Vec<S> = hp.into_vec();
        v.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(&b.1)));
        results.insert(queries[i].id.clone(), v.into_iter().map(|s| s.1).collect());
    }
    Ok(Baseline { k, docs: n, queries: queries.len(), queries_hash: queries_hash(queries), corpus: corpus.display().to_string(), results, build_s: t0.elapsed().as_secs_f64() })
}

/// A caller of search.router.
#[derive(Clone)]
pub struct RouterClient {
    pool: Arc<Pool>,
    pub addr: String,
}

impl RouterClient {
    pub fn new(addr: &str, creds: Creds) -> RouterClient {
        RouterClient { pool: Arc::new(Pool::new(creds)), addr: addr.to_string() }
    }
    pub async fn query(&self, q: &str, k: u32) -> Result<RouterQueryResp, RpcError> {
        self.pool.call::<RouterQuery>(&self.addr, &RouterQueryReq { q: q.to_string(), k, hedge_ms: 0 }, Duration::from_secs(5)).await
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecallPass {
    pub queries: usize,
    pub recall_at_10: f64,
    pub missing_shard_answers: u64,
    pub errors: u64,
    pub secs: f64,
    /// sha256 over every query's top-10 doc keys in query order: identical answers give an identical hash
    pub answers_hash: String,
}

pub async fn recall_pass(r: &RouterClient, queries: &[Query], base: &Baseline) -> RecallPass {
    let t0 = Instant::now();
    let mut sum = 0.0;
    let mut missing = 0;
    let mut errors = 0;
    let mut all = String::new();
    for q in queries {
        match r.query(&q.text, 10).await {
            Ok(a) => {
                let got: Vec<String> = a.hits.iter().map(|h| h.doc.clone()).collect();
                sum += stats::recall_at(&got, base.results.get(&q.id).map(|v| v.as_slice()).unwrap_or(&[]), 10);
                missing += a.missing.len() as u64;
                all.push_str(&format!("{}:{}\n", q.id, got.join(",")));
            }
            Err(_) => errors += 1,
        }
    }
    RecallPass { queries: queries.len(), recall_at_10: sum / queries.len().max(1) as f64, missing_shard_answers: missing, errors, secs: t0.elapsed().as_secs_f64(), answers_hash: sharded_core::segment::sha256_hex(all.as_bytes()) }
}

/// One second of the query loop.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Second {
    pub t_ms: u64,
    pub n: u64,
    pub errors: u64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub hedged: u64,
    pub missing: u64,
    pub recall: f64,
    pub hist: Vec<u32>,
}

/// Run `conc` closed-loop workers over the query set until `stop` is set; `each` gets every finished second.
pub async fn load(r: RouterClient, queries: Arc<Vec<Query>>, base: Arc<Baseline>, conc: usize, stop: Arc<std::sync::atomic::AtomicBool>, each: impl Fn(Second) + Send + Sync + 'static) {
    load_fn(move |q: String| { let r = r.clone(); async move { r.query(&q, 10).await } }, queries, base, conc, stop, each).await
}

/// [`load`] with any query function (the kill test wraps the router call to keep every sample).
pub async fn load_fn<F, Fut>(query: F, queries: Arc<Vec<Query>>, base: Arc<Baseline>, conc: usize, stop: Arc<std::sync::atomic::AtomicBool>, each: impl Fn(Second) + Send + Sync + 'static)
where
    F: Fn(String) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<RouterQueryResp, RpcError>> + Send,
{
    #[derive(Default)]
    struct Acc {
        lat: Vec<f64>,
        errors: u64,
        hedged: u64,
        missing: u64,
        recall: f64,
    }
    let acc = Arc::new(Mutex::new(Acc::default()));
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut workers = vec![];
    for _ in 0..conc {
        let (r, queries, base, acc, next, stop) = (query.clone(), queries.clone(), base.clone(), acc.clone(), next.clone(), stop.clone());
        workers.push(tokio::spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst) % queries.len();
                let q = &queries[i];
                let t = Instant::now();
                let res = r(q.text.clone()).await;
                let ms = t.elapsed().as_secs_f64() * 1e3;
                let failed = {
                    let mut a = acc.lock().unwrap();
                    match res {
                        Ok(ans) => {
                            a.lat.push(ms);
                            a.hedged += ans.hedged as u64;
                            a.missing += ans.missing.len() as u64;
                            let got: Vec<String> = ans.hits.iter().map(|h| h.doc.clone()).collect();
                            a.recall += stats::recall_at(&got, base.results.get(&q.id).map(|v| v.as_slice()).unwrap_or(&[]), 10);
                            false
                        }
                        Err(_) => {
                            a.errors += 1;
                            true
                        }
                    }
                };
                if failed {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }));
    }
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.tick().await;
    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        tick.tick().await;
        let a = std::mem::take(&mut *acc.lock().unwrap());
        let mut lat = a.lat;
        lat.sort_by(|x, y| x.partial_cmp(y).unwrap());
        let n = lat.len() as u64;
        each(Second {
            t_ms: sharded_core::now_ms(),
            n,
            errors: a.errors,
            p50_ms: stats::percentile(&lat, 50.0),
            p99_ms: stats::percentile(&lat, 99.0),
            max_ms: lat.last().copied().unwrap_or(0.0),
            hedged: a.hedged,
            missing: a.missing,
            recall: if n > 0 { a.recall / n as f64 } else { 0.0 },
            hist: stats::histogram(&lat),
        });
    }
    for w in workers {
        let _ = w.await;
    }
}
