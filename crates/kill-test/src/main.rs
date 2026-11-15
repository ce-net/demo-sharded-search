//! `kill-test`: replay the kill test and record every number the page shows.
//!
//! 1. Start the dev store, one search.host per machine, the rebalancer and the router. The rebalancer places
//!    `shards x replicas` instances; wait until every shard has all its replicas ready and one leader.
//! 2. Record every instance identity with its machine, a recall@10 pass, and a query refused for a key without the
//!    `search.shard:query` right (with the audit row in CAL).
//! 3. Start a closed-loop query load that never stops. After `--before-s`, kill every process of one machine at once.
//! 4. Watch every shard: the time until a live replica on another machine leads each shard that lost its leader, and
//!    until every shard is back to full strength. Keep the load going `--after-s` more.
//! 5. A second recall pass on the same query set against the same baseline. Optionally add a machine and record how
//!    the rebalancer spreads shards onto it.
//!
//! Everything goes to `<out>/run.json` (and `seconds.jsonl`, `frames.json`). Only processes this run started are
//! ever stopped.
use cellnet::cap::Chain;
use cellnet::ident::Identity;
use cellnet::rpc::{Creds, Pool};
use cellnet::store::Store;
use search_api::*;
use search_eval::{load_queries, queries_hash, recall_pass, Baseline, RouterClient, Second};
use sharded_core::lease::View;
use sharded_core::{mono_ms, now_ms, stats};
use std::collections::{BTreeMap, HashMap};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct Procs(Mutex<Vec<(String, Child)>>);

impl Procs {
    fn start(&self, name: &str, bin: &Path, args: &[String], log: &Path) -> Result<u32, String> {
        let f = std::fs::File::create(log).map_err(|e| format!("{}: {e}", log.display()))?;
        let c = Command::new(bin)
            .args(args)
            .stdin(Stdio::null())
            .stdout(f.try_clone().map_err(|e| e.to_string())?)
            .stderr(f)
            .process_group(0)
            .spawn()
            .map_err(|e| format!("{}: {e}", bin.display()))?;
        let pid = c.id();
        self.0.lock().unwrap().push((name.to_string(), c));
        Ok(pid)
    }

    /// SIGKILL a whole process group this run started (a host and every instance it spawned).
    fn kill_group(&self, name: &str) -> Option<u32> {
        let mut v = self.0.lock().unwrap();
        let i = v.iter().position(|(n, _)| n == name)?;
        let (_, mut c) = v.remove(i);
        let pid = c.id();
        unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
        let _ = c.wait();
        Some(pid)
    }

    fn stop_all(&self) {
        let mut v = self.0.lock().unwrap();
        for (_, c) in v.iter() {
            unsafe { libc::kill(-(c.id() as i32), libc::SIGTERM) };
        }
        std::thread::sleep(Duration::from_millis(500));
        for (_, c) in v.iter_mut() {
            unsafe { libc::kill(-(c.id() as i32), libc::SIGKILL) };
            let _ = c.wait();
        }
        v.clear();
    }
}

/// One look at every shard.
#[derive(Clone, Debug, Default)]
struct Look {
    /// shard -> members (placement order) with their record
    shards: BTreeMap<u32, Vec<(String, Option<InstRec>, bool)>>, // (id, rec, live)
}

impl Look {
    fn leader(&self, s: u32) -> Option<&InstRec> {
        self.shards.get(&s)?.iter().find(|(_, r, live)| *live && r.as_ref().is_some_and(|r| r.role == "leader" && r.ready)).and_then(|(_, r, _)| r.as_ref())
    }
    fn whole(&self, s: u32, replicas: usize, not_on: &str) -> bool {
        let Some(m) = self.shards.get(&s) else { return false };
        let ok = m.iter().filter(|(_, r, live)| *live && r.as_ref().is_some_and(|r| r.ready && r.machine != not_on)).count();
        ok >= replicas && self.leader(s).is_some()
    }
}

async fn look(store: &Store, view: &mut View, ttl: u64) -> Result<Look, String> {
    let docs = store.list(keys::SHARDS).await.map_err(|e| e.to_string())?;
    let now = mono_ms();
    let mut place: BTreeMap<u32, PlaceRec> = BTreeMap::new();
    let mut inst: HashMap<String, InstRec> = HashMap::new();
    for d in docs {
        if d.key.ends_with("/place") {
            if let Ok(p) = serde_json::from_str::<PlaceRec>(&d.value) {
                place.insert(p.shard, p);
            }
        } else if let Ok(r) = serde_json::from_str::<InstRec>(&d.value) {
            view.observe(&r.id, r.beat, now);
            inst.insert(r.id.clone(), r);
        }
    }
    let mut l = Look::default();
    for (s, p) in place {
        let v = p.members.iter().map(|id| (id.clone(), inst.get(id).cloned(), view.live(id, now, ttl) && inst.get(id).is_some_and(|r| !r.gone))).collect();
        l.shards.insert(s, v);
    }
    Ok(l)
}

fn arg_list(pairs: &[(&str, String)]) -> Vec<String> {
    pairs.iter().flat_map(|(k, v)| [format!("--{k}"), v.clone()]).collect()
}

fn link_cache(from: &Path, to: &Path, hashes: &[String]) -> Result<u32, String> {
    std::fs::create_dir_all(to).map_err(|e| e.to_string())?;
    let mut n = 0;
    for h in hashes {
        let (a, b) = (from.join(format!("{h}.seg")), to.join(format!("{h}.seg")));
        if !b.exists() {
            std::fs::hard_link(&a, &b).or_else(|_| std::fs::copy(&a, &b).map(|_| ())).map_err(|e| format!("{}: {e}", a.display()))?;
            n += 1;
        }
    }
    Ok(n)
}

fn pct_of(samples: &[(u64, f64)], from: u64, to: u64) -> serde_json::Value {
    let mut v: Vec<f64> = samples.iter().filter(|(t, _)| *t >= from && *t < to).map(|x| x.1).collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let secs = (to.saturating_sub(from)) as f64 / 1000.0;
    serde_json::json!({"queries": v.len(), "secs": (secs * 10.0).round() / 10.0, "qps": if secs > 0.0 { ((v.len() as f64 / secs) * 10.0).round() / 10.0 } else { 0.0 }, "p50_ms": (stats::percentile(&v, 50.0) * 100.0).round() / 100.0, "p99_ms": (stats::percentile(&v, 99.0) * 100.0).round() / 100.0, "max_ms": (v.last().copied().unwrap_or(0.0) * 100.0).round() / 100.0})
}

#[tokio::main]
async fn main() {
    let procs = Arc::new(Procs(Mutex::new(vec![])));
    let p2 = procs.clone();
    let r = run(procs.clone()).await;
    p2.stop_all();
    if let Err(e) = r {
        eprintln!("kill-test: {e}");
        std::process::exit(1);
    }
}

async fn run(procs: Arc<Procs>) -> Result<(), String> {
    let a = cellnet::Args::parse();
    let bin = PathBuf::from(a.get("bin", &std::env::current_exe().map(|p| p.parent().unwrap().to_string_lossy().to_string()).unwrap_or_default()));
    let work = PathBuf::from(a.get("work", "run/kill-test"));
    let out = PathBuf::from(a.get("out", "proof"));
    let corpus_dir = PathBuf::from(a.get("corpus-dir", "run/corpus"));
    let manifest: Manifest = serde_json::from_slice(&std::fs::read(corpus_dir.join("manifest.json")).map_err(|e| format!("manifest: {e}"))?).map_err(|e| e.to_string())?;
    if manifest.compute_hash() != manifest.hash {
        return Err("manifest hash does not match its content".into());
    }
    let src_cache = PathBuf::from(a.get("cache", &corpus_dir.join("cache").to_string_lossy()));
    let machines: Vec<String> = a.get("machines", "machine-a,machine-b,machine-c").split(',').map(|s| s.to_string()).collect();
    let kill = a.get("kill", machines.last().unwrap());
    let add = a.get("add", "");
    let replicas = manifest.replicas as usize;
    let ttl = a.num("ttl-ms", 3000);
    let tick = a.num("tick-ms", 500);
    let before_s = a.num("before-s", 20);
    let after_s = a.num("after-s", 20);
    let conc = a.num("conc", 8) as usize;
    let store_port = a.num("store-port", 7911);
    let router_port = a.num("router-port", 7920);
    let seed_caches = a.get("seed-caches", "first");
    let qrels = a.get("qrels", "");
    let queries = load_queries(&PathBuf::from(a.get("queries", "run/corpus/queries.jsonl")), if qrels.is_empty() { None } else { Some(Path::new(&qrels)) })?;
    let base_path = a.get("baseline", "run/baseline.json");
    let base_bytes = std::fs::read(&base_path).map_err(|e| format!("{base_path}: {e}"))?;
    let base: Baseline = serde_json::from_slice(&base_bytes).map_err(|e| e.to_string())?;
    if base.queries_hash != queries_hash(&queries) || base.docs != manifest.docs {
        return Err(format!("baseline ({} docs) does not match this corpus ({} docs) or query set", base.docs, manifest.docs));
    }
    std::fs::create_dir_all(&work).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(&out).map_err(|e| e.to_string())?;
    let logs = work.join("logs");
    std::fs::create_dir_all(&logs).map_err(|e| e.to_string())?;

    // keys: one root for this run; every cell gets a chain from it
    let root = Identity::load_or_create(&work.join("root.key"))?;
    let roots = root.public_hex();
    let exp = now_ms() / 1000 + 86400;
    let issue = |name: &str, rights: &[&str]| -> Result<(PathBuf, PathBuf), String> {
        let id = Identity::load_or_create(&work.join("keys").join(format!("{name}.key")))?;
        let c = Chain::root(&root, &id.public_hex(), rights, exp);
        let cp = work.join("keys").join(format!("{name}.cap"));
        std::fs::write(&cp, c.encode()).map_err(|e| e.to_string())?;
        Ok((work.join("keys").join(format!("{name}.key")), cp))
    };
    let op_id = Identity::load_or_create(&work.join("keys/operator.key"))?;
    let op = Creds { me: op_id.clone(), chain: Chain::root(&root, &op_id.public_hex(), rights::OPERATOR, exp) };

    let store_addr = format!("127.0.0.1:{store_port}");
    let (k, c) = issue("store", &["state.store:*"])?;
    procs.start("store", &bin.join("dev-store"), &arg_list(&[("listen", store_addr.clone()), ("roots", roots.clone()), ("seed-file", k.display().to_string()), ("cap-file", c.display().to_string()), ("data", work.join("store").display().to_string())]), &logs.join("store.log"))?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let store = Store::new(&store_addr, op.clone());
    store.put_json(keys::MANIFEST, &manifest).await.map_err(|e| format!("store: {e}"))?;

    let hashes: Vec<String> = manifest.segments.iter().map(|s| s.hash.clone()).collect();
    let start_host = |m: &str| -> Result<u32, String> {
        let host_rights: Vec<&str> = rights::HOST.iter().chain(rights::SHARD.iter()).copied().collect();
        let (k, c) = issue(&format!("host-{m}"), &host_rights)?;
        let dir = work.join(m);
        procs.start(
            &format!("host:{m}"),
            &bin.join("search-host"),
            &arg_list(&[
                ("machine", m.to_string()),
                ("store", store_addr.clone()),
                ("roots", roots.clone()),
                ("seed-file", k.display().to_string()),
                ("cap-file", c.display().to_string()),
                ("data", dir.display().to_string()),
                ("cache", dir.join("cache").display().to_string()),
                ("bin", bin.join("search-shard").display().to_string()),
                ("tick-ms", tick.to_string()),
                ("ttl-ms", ttl.to_string()),
            ]),
            &logs.join(format!("host-{m}.log")),
        )
    };
    let mut seeded = BTreeMap::new();
    for (i, m) in machines.iter().enumerate() {
        if seed_caches == "all" || i == 0 {
            seeded.insert(m.clone(), link_cache(&src_cache, &work.join(m).join("cache"), &hashes)?);
        }
        start_host(m)?;
    }
    let (k, c) = issue("rebalancer", rights::REBALANCER)?;
    procs.start("rebalancer", &bin.join("search-rebalancer"), &arg_list(&[("store", store_addr.clone()), ("roots", roots.clone()), ("seed-file", k.display().to_string()), ("cap-file", c.display().to_string()), ("replicas", replicas.to_string()), ("ttl-ms", ttl.to_string())]), &logs.join("rebalancer.log"))?;
    let router_addr = format!("127.0.0.1:{router_port}");
    let (k, c) = issue("router", rights::ROUTER)?;
    procs.start("router", &bin.join("search-router"), &arg_list(&[("store", store_addr.clone()), ("roots", roots.clone()), ("seed-file", k.display().to_string()), ("cap-file", c.display().to_string()), ("listen", router_addr.clone()), ("ttl-ms", ttl.to_string()), ("hedge-ms", a.get("hedge-ms", "20")), ("data", work.join("router").display().to_string())]), &logs.join("router.log"))?;
    if a.has("dashboard") {
        let (k, c) = issue("dashboard", &["state.store:read"])?;
        procs.start("dashboard", &bin.join("search-dashboard"), &arg_list(&[("store", store_addr.clone()), ("roots", roots.clone()), ("seed-file", k.display().to_string()), ("cap-file", c.display().to_string()), ("listen", a.get("dashboard", "127.0.0.1:7830")), ("web", a.get("web", "web"))]), &logs.join("dashboard.log"))?;
    }

    // 1. wait until every shard is whole
    let t_start = mono_ms();
    let mut view = View::default();
    let boot_timeout = a.num("boot-timeout-s", 600) * 1000;
    let mut frames: Vec<serde_json::Value> = vec![];
    let midx = |m: &str, ms: &[String]| ms.iter().position(|x| x == m).map(|i| i as i64).unwrap_or(-1);
    let mut all_machines = machines.clone();
    if !add.is_empty() {
        all_machines.push(add.clone());
    }
    let frame = |l: &Look, t: u64, all_machines: &[String]| -> serde_json::Value {
        let s: Vec<String> = l
            .shards
            .values()
            .map(|ms| {
                ms.iter()
                    .map(|(_, r, live)| {
                        let st = match r {
                            None => 'J',
                            Some(_) if !*live => 'X',
                            Some(r) if r.role == "leader" => 'L',
                            Some(r) if r.ready => 'F',
                            Some(_) => 'J',
                        };
                        format!("{}{}", r.as_ref().map(|r| midx(&r.machine, all_machines)).unwrap_or(-1), st)
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect();
        serde_json::json!({"t": t, "s": s})
    };
    loop {
        let l = look(&store, &mut view, ttl).await?;
        let n_whole = (0..manifest.shards).filter(|s| l.whole(*s, replicas, "")).count();
        if frames.len() % 4 == 0 {
            eprintln!("boot: {n_whole}/{} shards whole after {:.1} s", manifest.shards, (mono_ms() - t_start) as f64 / 1000.0);
        }
        frames.push(frame(&l, mono_ms() - t_start, &all_machines));
        if n_whole as u32 == manifest.shards {
            break;
        }
        if mono_ms() - t_start > boot_timeout {
            return Err("timed out waiting for every shard to be whole".into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let boot_ms = mono_ms() - t_start;
    tokio::time::sleep(Duration::from_millis(2 * tick)).await;
    let l0 = look(&store, &mut view, ttl).await?;
    let mut identities = vec![];
    for (s, ms) in &l0.shards {
        for (id, r, _) in ms {
            if let Some(r) = r {
                identities.push(serde_json::json!({"id": id, "pubkey": r.pubkey, "machine": r.machine, "shard": s, "role": r.role, "seg": &r.seg[..16.min(r.seg.len())], "docs": r.docs}));
            }
        }
    }
    let leaders0: BTreeMap<u32, (String, String)> = (0..manifest.shards).filter_map(|s| l0.leader(s).map(|r| (s, (r.id.clone(), r.machine.clone())))).collect();
    eprintln!("whole: {} instances, {} leaders after {:.1} s", identities.len(), leaders0.len(), boot_ms as f64 / 1000.0);

    // 2. recall before, and a refused query
    let rc = RouterClient::new(&router_addr, op.clone());
    let warm = recall_pass(&rc, &queries, &base).await;
    let recall_before = recall_pass(&rc, &queries, &base).await;
    eprintln!("recall@10 before: {:.4} ({} queries, {} errors; warm-up pass {:.4})", recall_before.recall_at_10, recall_before.queries, recall_before.errors, warm.recall_at_10);
    let stranger = Identity::generate();
    let s_creds = Creds { me: stranger.clone(), chain: Chain::root(&root, &stranger.public_hex(), &["search.shard:health"], exp) };
    let spool = Pool::new(s_creds.clone());
    let target = l0.leader(0).ok_or("no leader for shard 0")?.clone();
    let health = spool.call::<ShardHealth>(&target.addr, &(), Duration::from_secs(3)).await;
    let refused = spool.call::<ShardQuery>(&target.addr, &QueryReq { q: queries[0].text.clone(), k: 10 }, Duration::from_secs(3)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let audit: Vec<serde_json::Value> = store
        .list(&format!("{}{}/", keys::AUDIT, target.id))
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter_map(|d| serde_json::from_str::<serde_json::Value>(&d.value).ok())
        .filter(|v| v["caller"].as_str() == Some(&stranger.id()))
        .collect();
    let cap_test = serde_json::json!({
        "caller": stranger.id(),
        "grant_cal": cellnet::cap::cal(&s_creds.chain),
        "target": {"id": target.id, "shard": target.shard, "machine": target.machine},
        "health": match &health { Ok(h) => format!("ok (instance {})", h.id), Err(e) => format!("error: {e}") },
        "query": match &refused { Ok(_) => "ANSWERED (this is a failure)".to_string(), Err(e) => format!("{}: {}", e.kind, e.msg) },
        "audit_rows": audit,
    });
    eprintln!("cap test: query -> {}", cap_test["query"]);

    // 3. the query loop that never stops
    let seconds: Arc<Mutex<Vec<(Second, String)>>> = Arc::new(Mutex::new(vec![]));
    let samples: Arc<Mutex<Vec<(u64, f64)>>> = Arc::new(Mutex::new(vec![]));
    let phase = Arc::new(Mutex::new("warm-up".to_string()));
    let stop = Arc::new(AtomicBool::new(false));
    let (sec2, ph2, st2) = (seconds.clone(), phase.clone(), store.clone());
    let lt = {
        let (rc, q, b, stop, samples) = (rc.clone(), Arc::new(queries.clone()), Arc::new(base.clone()), stop.clone(), samples.clone());
        tokio::spawn(async move {
            load_with_samples(rc, q, b, conc, stop, samples, move |s| {
                let p = ph2.lock().unwrap().clone();
                let st = st2.clone();
                let (s2, p2) = (s.clone(), p.clone());
                tokio::spawn(async move {
                    let _ = st.put_json(keys::METRICS, &serde_json::json!({"second": s2, "phase": p2})).await;
                });
                sec2.lock().unwrap().push((s, p));
            })
            .await
        })
    };
    // warm-up: the same load, not counted (page cache, connections, the router's hedge estimate)
    let warm_s = a.num("warm-s", 15);
    tokio::time::sleep(Duration::from_secs(warm_s)).await;
    let t_load = mono_ms();
    *phase.lock().unwrap() = "before".into();
    let mut last_frame = 0;
    while mono_ms() - t_load < before_s * 1000 {
        let l = look(&store, &mut view, ttl).await?;
        if mono_ms() - last_frame >= 500 {
            frames.push(frame(&l, mono_ms() - t_start, &all_machines));
            last_frame = mono_ms();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 4. lose a machine
    let lost_leaders: BTreeMap<u32, String> = leaders0.iter().filter(|(_, (_, m))| *m == kill).map(|(s, (id, _))| (*s, id.clone())).collect();
    let lost_instances: Vec<String> = identities.iter().filter(|v| v["machine"] == kill.as_str()).map(|v| v["id"].as_str().unwrap_or("").to_string()).collect();
    *phase.lock().unwrap() = "during".into();
    let t_kill = mono_ms();
    let kill_wall = now_ms();
    let killed_pid = procs.kill_group(&format!("host:{kill}")).ok_or("that machine's host was not started by this run")?;
    eprintln!("killed {kill} (process group {killed_pid}): {} instances, {} leaders", lost_instances.len(), lost_leaders.len());
    let mut elected: BTreeMap<u32, (u64, String, String)> = BTreeMap::new();
    let mut amber_max = 0usize;
    let t_whole;
    let whole_wall;
    loop {
        let l = look(&store, &mut view, ttl).await?;
        let now = mono_ms();
        for s in lost_leaders.keys() {
            if !elected.contains_key(s) {
                if let Some(r) = l.leader(*s) {
                    if r.machine != kill {
                        elected.insert(*s, (now - t_kill, r.id.clone(), r.machine.clone()));
                    }
                }
            }
        }
        let amber = (0..manifest.shards).filter(|s| l.leader(*s).is_none_or(|r| r.machine == kill)).count();
        amber_max = amber_max.max(amber);
        if now - last_frame >= 500 {
            frames.push(frame(&l, now - t_start, &all_machines));
            last_frame = now;
        }
        let whole = (0..manifest.shards).filter(|s| l.whole(*s, replicas, &kill)).count();
        if whole as u32 == manifest.shards && elected.len() == lost_leaders.len() {
            t_whole = now;
            whole_wall = now_ms();
            frames.push(frame(&l, now - t_start, &all_machines));
            break;
        }
        if now - t_kill > 300_000 {
            return Err("shards not whole 300 s after the kill".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    *phase.lock().unwrap() = "after".into();
    eprintln!("whole again {:.1} s after the kill", (t_whole - t_kill) as f64 / 1000.0);
    while mono_ms() - t_whole < after_s * 1000 {
        let l = look(&store, &mut view, ttl).await?;
        if mono_ms() - last_frame >= 500 {
            frames.push(frame(&l, mono_ms() - t_start, &all_machines));
            last_frame = mono_ms();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    stop.store(true, Ordering::SeqCst);
    let _ = lt.await;
    let t_end = mono_ms();
    let recall_after = recall_pass(&rc, &queries, &base).await;
    eprintln!("recall@10 after: {:.4}", recall_after.recall_at_10);
    let l1 = look(&store, &mut view, ttl).await?;
    let mut per_machine_after: BTreeMap<String, usize> = BTreeMap::new();
    for ms in l1.shards.values() {
        for (_, r, live) in ms {
            if let (Some(r), true) = (r, live) {
                *per_machine_after.entry(r.machine.clone()).or_default() += 1;
            }
        }
    }

    // 5. optionally add a machine: no config change, just a host that heartbeats
    let mut added = serde_json::Value::Null;
    if !add.is_empty() {
        let t_add = mono_ms();
        if seed_caches == "all" {
            link_cache(&src_cache, &work.join(&add).join("cache"), &hashes)?;
        }
        start_host(&add)?;
        let mut t_first = None;
        let mut settled = None;
        let mut stable_since: Option<u64> = None;
        let mut last_counts = BTreeMap::new();
        while mono_ms() - t_add < a.num("add-timeout-s", 240) * 1000 {
            let l = look(&store, &mut view, ttl).await?;
            let mut counts: BTreeMap<String, usize> = BTreeMap::new();
            for ms in l.shards.values() {
                for (_, r, live) in ms {
                    if let (Some(r), true) = (r, live) {
                        if r.ready {
                            *counts.entry(r.machine.clone()).or_default() += 1;
                        }
                    }
                }
            }
            if t_first.is_none() && counts.get(&add).copied().unwrap_or(0) > 0 {
                t_first = Some(mono_ms() - t_add);
            }
            let all_whole = (0..manifest.shards).all(|s| l.whole(s, replicas, ""));
            if all_whole && counts == last_counts && counts.get(&add).copied().unwrap_or(0) > 0 {
                let t = *stable_since.get_or_insert(mono_ms());
                if mono_ms() - t > 3 * ttl {
                    settled = Some(t - t_add);
                    break;
                }
            } else {
                stable_since = None;
            }
            last_counts = counts;
            if mono_ms() - last_frame >= 500 {
                frames.push(frame(&l, mono_ms() - t_start, &all_machines));
                last_frame = mono_ms();
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let recall_added = recall_pass(&rc, &queries, &base).await;
        added = serde_json::json!({"machine": add, "first_replica_ready_ms": t_first, "settled_ms": settled, "ready_instances_per_machine": last_counts, "recall_at_10": recall_added.recall_at_10, "answers_hash": recall_added.answers_hash, "config_changed": false});
        eprintln!("added {add}: {added}");
    }

    // events the rebalancer logged
    let events: Vec<serde_json::Value> = store.list(keys::EVENTS).await.map_err(|e| e.to_string())?.into_iter().filter_map(|d| serde_json::from_str(&d.value).ok()).collect();
    let after_kill: Vec<&serde_json::Value> = events.iter().filter(|e| (kill_wall..=whole_wall).contains(&e["t_ms"].as_u64().unwrap_or(0))).collect();
    let samples = samples.lock().unwrap().clone();
    let secs = seconds.lock().unwrap().clone();
    let mut sj = String::new();
    for (s, p) in &secs {
        let mut v = serde_json::to_value(s).unwrap();
        v["phase"] = serde_json::json!(p);
        sj.push_str(&format!("{v}\n"));
    }
    std::fs::write(out.join("seconds.jsonl"), sj).map_err(|e| e.to_string())?;
    let mut elect_ms: Vec<u64> = elected.values().map(|e| e.0).collect();
    elect_ms.sort();
    let run = serde_json::json!({
        "recorded_at": cellnet::cap::iso_minute(kill_wall / 1000),
        "what": "kill every instance on one machine under a query loop that never stops",
        "corpus": {"name": manifest.name, "source": manifest.source, "docs": manifest.docs, "manifest_hash": manifest.hash, "source_sha256": manifest.source_sha256, "shards": manifest.shards, "replicas": manifest.replicas, "segment_bytes": manifest.segments.iter().map(|s| s.bytes).sum::<u64>()},
        "queries": {"count": queries.len(), "hash": base.queries_hash, "baseline_sha256": sharded_core::segment::sha256_hex(&base_bytes), "baseline_build_s": base.build_s},
        "setup": {"machines": machines, "physical_hosts": a.get("physical", "one machine"), "tick_ms": tick, "lease_ttl_ms": ttl, "hedge_ms_floor": a.num("hedge-ms", 20), "hedge": "p95 of recent shard calls, never below the floor", "warm_up_s": warm_s, "concurrency": conc, "seeded_caches": seeded},
        "boot": {"instances": identities.len(), "leaders": leaders0.len(), "whole_after_ms": boot_ms},
        "identities": identities,
        "cap_test": cap_test,
        "recall_before": recall_before,
        "recall_after": recall_after,
        "recall_identical": recall_before.recall_at_10 == recall_after.recall_at_10,
        "answers_identical": recall_before.answers_hash == recall_after.answers_hash,
        "kill": {
            "machine": kill,
            "instances_lost": lost_instances.len(),
            "leaders_lost": lost_leaders.len(),
            "amber_max": amber_max,
            "elect_ms": elected.iter().map(|(s, (ms, id, m))| serde_json::json!({"shard": s, "old": lost_leaders.get(s), "new": id, "machine": m, "ms": ms})).collect::<Vec<_>>(),
            "elect_ms_max": elect_ms.last(),
            "elect_ms_median": if elect_ms.is_empty() { 0 } else { elect_ms[elect_ms.len() / 2] },
            "elect_under_two_leases": elect_ms.last().is_some_and(|m| *m < 2 * ttl),
            "replicas_whole_ms": t_whole - t_kill,
            "rebalancer_actions": after_kill.len(),
            "instances_per_machine_after": per_machine_after,
        },
        "latency": {
            "before": pct_of(&samples, t_load, t_kill),
            "during": pct_of(&samples, t_kill, t_whole),
            "after": pct_of(&samples, t_whole, t_end),
            "errors": secs.iter().map(|s| s.0.errors).sum::<u64>(),
            "hedged": secs.iter().map(|s| s.0.hedged).sum::<u64>(),
            "missing_shard_answers": secs.iter().map(|s| s.0.missing).sum::<u64>(),
        },
        "add_machine": added,
        "events": after_kill,
    });
    std::fs::write(out.join("run.json"), serde_json::to_string_pretty(&run).unwrap()).map_err(|e| e.to_string())?;
    std::fs::write(out.join("frames.json"), serde_json::to_string(&serde_json::json!({"machines": all_machines, "t_kill": t_kill - t_start, "t_whole": t_whole - t_start, "frames": frames})).unwrap()).map_err(|e| e.to_string())?;
    println!("{}", serde_json::to_string_pretty(&serde_json::json!({"kill": run["kill"]["elect_ms_max"], "whole_ms": run["kill"]["replicas_whole_ms"], "latency": run["latency"], "recall_before": recall_before.recall_at_10, "recall_after": recall_after.recall_at_10})).unwrap());
    Ok(())
}

/// The eval loop, also keeping every sample with its time on this clock (for exact percentiles per phase).
async fn load_with_samples(rc: RouterClient, q: Arc<Vec<search_eval::Query>>, b: Arc<Baseline>, conc: usize, stop: Arc<AtomicBool>, samples: Arc<Mutex<Vec<(u64, f64)>>>, each: impl Fn(Second) + Send + Sync + 'static) {
    let timed = TimedRouter { rc, samples };
    search_eval::load_fn(move |text: String| { let t = timed.clone(); async move { t.query(&text).await } }, q, b, conc, stop, each).await
}

#[derive(Clone)]
struct TimedRouter {
    rc: RouterClient,
    samples: Arc<Mutex<Vec<(u64, f64)>>>,
}

impl TimedRouter {
    async fn query(&self, text: &str) -> Result<RouterQueryResp, cellnet::RpcError> {
        let t0 = mono_ms();
        let i = std::time::Instant::now();
        let r = self.rc.query(text, 10).await;
        if r.is_ok() {
            self.samples.lock().unwrap().push((t0, i.elapsed().as_secs_f64() * 1e3));
        }
        r
    }
}

