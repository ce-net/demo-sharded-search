//! `search-router`: the search.router cell. It follows every shard's records through the store's change feed (so it
//! always knows each shard's leader by name, not by a configured address), sends a query to the leader of every
//! shard at once, sends the same query to a follower when the leader has not answered within the hedge delay, takes
//! whichever answers first, and merges the top k.
use cellnet::rpc::{bind, Pool, RpcError, Server};
use cellnet::store::Store;
use search_api::*;
use sharded_core::lease::View;
use sharded_core::{mono_ms, now_ms, segment};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
struct Map {
    place: BTreeMap<u32, PlaceRec>,
    inst: HashMap<String, InstRec>,
    view: View,
}

struct Router {
    map: Mutex<Map>,
    /// recent shard-call latencies (ms); the hedge delay is their p95, never below `hedge_ms`
    recent: Mutex<std::collections::VecDeque<f64>>,
    hedge_now: Mutex<f64>,
    pool: Pool,
    ttl: u64,
    hedge_ms: u32,
    deadline: Duration,
}

#[derive(Clone)]
struct Target {
    id: String,
    addr: String,
    machine: String,
}

impl Router {
    /// Live, ready members of a shard: the leader first, then followers.
    fn targets(&self, shard: u32) -> Vec<Target> {
        let m = self.map.lock().unwrap();
        let now = mono_ms();
        let Some(p) = m.place.get(&shard) else { return vec![] };
        let mut t: Vec<(u8, Target)> = p
            .members
            .iter()
            .filter_map(|id| m.inst.get(id))
            .filter(|r| r.ready && !r.gone && m.view.live(&r.id, now, self.ttl))
            .map(|r| (if r.role == "leader" { 0 } else { 1 }, Target { id: r.id.clone(), addr: r.addr.clone(), machine: r.machine.clone() }))
            .collect();
        t.sort_by_key(|(o, _)| *o);
        t.into_iter().map(|(_, t)| t).collect()
    }

    fn shards(&self) -> Vec<u32> {
        self.map.lock().unwrap().place.keys().copied().collect()
    }

    async fn ask(self: &Arc<Self>, t: &Target, q: &QueryReq) -> Result<QueryResp, RpcError> {
        let t0 = Instant::now();
        let r = self.pool.call::<ShardQuery>(&t.addr, q, self.deadline).await;
        if r.is_ok() {
            self.observe(t0.elapsed().as_secs_f64() * 1e3);
        }
        r
    }

    /// Tail-at-scale hedging: send the backup request once the primary is slower than 95% of recent calls.
    fn observe(&self, ms: f64) {
        let mut r = self.recent.lock().unwrap();
        r.push_back(ms);
        if r.len() > 2048 {
            r.pop_front();
        }
        if r.len() % 128 == 0 {
            let mut v: Vec<f64> = r.iter().copied().collect();
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            *self.hedge_now.lock().unwrap() = sharded_core::stats::percentile(&v, 95.0);
        }
    }

    fn hedge_delay(&self, asked: u32) -> Duration {
        let floor = if asked == 0 { self.hedge_ms } else { asked } as f64;
        Duration::from_micros((self.hedge_now.lock().unwrap().max(floor) * 1000.0) as u64)
    }

    async fn one_shard(self: Arc<Self>, shard: u32, q: QueryReq, hedge: Duration) -> (ShardAnswer, Vec<segment::Hit>) {
        let t0 = Instant::now();
        let targets = self.targets(shard);
        let fail = |err: String, t0: Instant| (ShardAnswer { shard, inst: String::new(), machine: String::new(), role: String::new(), ms: t0.elapsed().as_secs_f64() * 1e3, hedged: false, ok: false, err }, vec![]);
        if targets.is_empty() {
            return fail("no live replica".into(), t0);
        }
        let primary = targets[0].clone();
        let backup = targets.get(1).cloned();
        let ok = |r: QueryResp, t: &Target, hedged: bool, t0: Instant| (ShardAnswer { shard, inst: t.id.clone(), machine: t.machine.clone(), role: r.role.clone(), ms: t0.elapsed().as_secs_f64() * 1e3, hedged, ok: true, err: String::new() }, r.hits);
        let me = self.clone();
        let q1 = q.clone();
        let p2 = primary.clone();
        let mut first = tokio::spawn(async move { me.ask(&p2, &q1).await });
        tokio::select! {
            r = &mut first => {
                match r {
                    Ok(Ok(resp)) => return ok(resp, &primary, false, t0),
                    _ => {
                        // the leader failed fast (closed, refused): go straight to a follower
                        let Some(b) = backup else { return fail(format!("{} failed, no follower", primary.id), t0) };
                        return match self.ask(&b, &q).await {
                            Ok(resp) => ok(resp, &b, true, t0),
                            Err(e) => fail(e.to_string(), t0),
                        };
                    }
                }
            }
            _ = tokio::time::sleep(hedge) => {}
        }
        let Some(b) = backup else {
            return match first.await {
                Ok(Ok(resp)) => ok(resp, &primary, false, t0),
                Ok(Err(e)) => fail(e.to_string(), t0),
                Err(e) => fail(e.to_string(), t0),
            };
        };
        let me = self.clone();
        let b2 = b.clone();
        let mut second = tokio::spawn(async move { me.ask(&b2, &q).await });
        let mut first_done = false;
        let mut second_done = false;
        let mut last_err = String::new();
        while !(first_done && second_done) {
            tokio::select! {
                r = &mut first, if !first_done => {
                    first_done = true;
                    match r { Ok(Ok(resp)) => { second.abort(); return ok(resp, &primary, false, t0) } Ok(Err(e)) => last_err = e.to_string(), Err(e) => last_err = e.to_string() }
                }
                r = &mut second, if !second_done => {
                    second_done = true;
                    match r { Ok(Ok(resp)) => { first.abort(); return ok(resp, &b, true, t0) } Ok(Err(e)) => last_err = e.to_string(), Err(e) => last_err = e.to_string() }
                }
            }
        }
        fail(last_err, t0)
    }

    async fn query(self: Arc<Self>, r: RouterQueryReq) -> Result<RouterQueryResp, RpcError> {
        let t0 = Instant::now();
        let k = r.k.clamp(1, 100);
        let hedge = self.hedge_delay(r.hedge_ms);
        let q = QueryReq { q: r.q.clone(), k };
        let mut tasks = Vec::new();
        for s in self.shards() {
            tasks.push(tokio::spawn(self.clone().one_shard(s, q.clone(), hedge)));
        }
        let mut shards = Vec::new();
        let mut lists = Vec::new();
        for t in tasks {
            if let Ok((a, h)) = t.await {
                shards.push(a);
                lists.push(h);
            }
        }
        let missing: Vec<u32> = shards.iter().filter(|a| !a.ok).map(|a| a.shard).collect();
        let hedged = shards.iter().filter(|a| a.hedged).count() as u32;
        Ok(RouterQueryResp { hits: segment::merge(lists, k as usize), shards, missing, hedged, ms: t0.elapsed().as_secs_f64() * 1e3 })
    }

    fn apply(&self, d: &cellnet::store::Doc) {
        let mut m = self.map.lock().unwrap();
        if d.key.ends_with("/place") {
            if let Ok(p) = serde_json::from_str::<PlaceRec>(&d.value) {
                m.place.insert(p.shard, p);
            }
        } else if let Ok(r) = serde_json::from_str::<InstRec>(&d.value) {
            let now = mono_ms();
            m.view.observe(&r.id, r.beat, now);
            m.inst.insert(r.id.clone(), r);
        }
    }
}

/// Follow `ss/s/` through the change feed.
async fn follow(r: Arc<Router>, store: Store) {
    let mut since = 0;
    loop {
        match store.changes(keys::SHARDS, since, 1000).await {
            Ok(c) => {
                for d in &c.docs {
                    r.apply(d);
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

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("search-router: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let a = cellnet::Args::parse();
    let (creds, roots) = cellnet::creds_from(&a)?;
    let machine = a.get("machine", "local");
    let store = Store::new(&a.get("store", "127.0.0.1:7811"), creds.clone());
    let (l, addr) = bind(&a.get("listen", "0.0.0.0:7820"), &a.get("advertise", "127.0.0.1")).await.map_err(|e| e.to_string())?;
    let r = Arc::new(Router { map: Mutex::new(Map::default()), recent: Mutex::new(Default::default()), hedge_now: Mutex::new(0.0), pool: Pool::new(creds.clone()), ttl: a.num("ttl-ms", 3000), hedge_ms: a.num("hedge-ms", 20) as u32, deadline: Duration::from_millis(a.num("deadline-ms", 2000)) });
    tokio::spawn(follow(r.clone(), store.clone()));
    let started = Instant::now();
    let id = creds.me.id();
    let (r1, id1, m1) = (r.clone(), id.clone(), machine.clone());
    let srv = Server::new(ROUTER, creds.me.clone(), roots)
        .audit(cellnet::rpc::audit_file(std::path::PathBuf::from(a.get("data", "run/router")).join("audit.jsonl")))
        .on::<RouterHealth, _, _>(move |_: (), _| {
            let (id, m) = (id1.clone(), m1.clone());
            async move { Ok(Health { ok: true, version: env!("CARGO_PKG_VERSION").into(), uptime_s: started.elapsed().as_secs(), id, machine: m }) }
        })
        .on::<RouterQuery, _, _>(move |q: RouterQueryReq, _| {
            let r = r1.clone();
            async move { r.query(q).await }
        });
    // found by name: ss/n/search.router/<id>
    let st = store.clone();
    let rec = serde_json::json!({"api": ROUTER, "id": id, "addr": addr, "machine": machine});
    tokio::spawn(async move {
        loop {
            let mut v = rec.clone();
            v["t_ms"] = serde_json::json!(now_ms());
            let _ = st.put_json(&format!("ss/n/{ROUTER}/{}", v["id"].as_str().unwrap_or("")), &v).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
    eprintln!("search.router {id} at {addr}");
    srv.serve(l).await.map_err(|e| e.to_string())
}
