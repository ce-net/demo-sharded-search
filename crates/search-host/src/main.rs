//! `search-host`: one per machine. It is the machine's half of placement: `spawn` starts shard instances here (each
//! with a fresh Ed25519 key and a chain the host narrows from its own), `instances` lists them, `stop` stops one and
//! `kill_all` drops every instance on this machine at once (the kill test). It heartbeats `ss/h/<machine>`, which is
//! how the rebalancer learns that a machine exists: adding a machine is starting a host on it.
use cellnet::cap::Chain;
use cellnet::ident::Identity;
use cellnet::rpc::{bind, RpcError, Server};
use cellnet::store::Store;
use search_api::*;
use sharded_core::now_ms;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct Host {
    me: Identity,
    chain: Chain,
    machine: String,
    bin: PathBuf,
    data: PathBuf,
    cache: PathBuf,
    store_addr: String,
    roots: String,
    advertise: String,
    extra: Vec<String>,
    kids: Mutex<BTreeMap<String, Child>>,
}

impl Host {
    fn spawn_one(&self, shard: u32) -> Result<String, String> {
        let inst = Identity::generate();
        let id = inst.id();
        let exp = now_ms() / 1000 + 7 * 86400;
        let chain = self.chain.delegate(&self.me, &inst.public_hex(), rights::SHARD, exp)?;
        let dir = self.data.join("inst").join(&id);
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let seed_file = dir.join("seed.key");
        std::fs::write(&seed_file, inst.seed_hex()).map_err(|e| e.to_string())?;
        let cap_file = dir.join("cap.hex");
        std::fs::write(&cap_file, chain.encode()).map_err(|e| e.to_string())?;
        let log = std::fs::File::create(dir.join("log.txt")).map_err(|e| e.to_string())?;
        let mut cmd = Command::new(&self.bin);
        cmd.arg("--shard")
            .arg(shard.to_string())
            .arg("--machine")
            .arg(&self.machine)
            .arg("--store")
            .arg(&self.store_addr)
            .arg("--seed-file")
            .arg(&seed_file)
            .arg("--cap-file")
            .arg(&cap_file)
            .arg("--roots")
            .arg(&self.roots)
            .arg("--data")
            .arg(&dir)
            .arg("--cache")
            .arg(&self.cache)
            .arg("--advertise")
            .arg(&self.advertise)
            .args(&self.extra)
            .stdin(Stdio::null())
            .stdout(log.try_clone().map_err(|e| e.to_string())?)
            .stderr(log);
        let child = cmd.spawn().map_err(|e| format!("{}: {e}", self.bin.display()))?;
        self.kids.lock().unwrap().insert(id.clone(), child);
        Ok(id)
    }

    fn reap(&self) -> Vec<String> {
        let mut k = self.kids.lock().unwrap();
        k.retain(|_, c| matches!(c.try_wait(), Ok(None)));
        k.keys().cloned().collect()
    }

    fn stop(&self, id: &str) -> bool {
        let mut k = self.kids.lock().unwrap();
        if let Some(mut c) = k.remove(id) {
            unsafe { libc::kill(c.id() as i32, libc::SIGTERM) };
            let _ = c.wait();
            true
        } else {
            false
        }
    }

    fn kill_all(&self) -> u32 {
        let mut k = self.kids.lock().unwrap();
        let n = k.len() as u32;
        for c in k.values() {
            unsafe { libc::kill(c.id() as i32, libc::SIGKILL) };
        }
        for (_, mut c) in std::mem::take(&mut *k) {
            let _ = c.wait();
        }
        n
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("search-host: {e}");
        std::process::exit(1);
    }
}

fn default_machine() -> String {
    std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string()).ok().filter(|s| !s.is_empty()).unwrap_or_else(|| "local".into())
}

async fn run() -> Result<(), String> {
    let a = cellnet::Args::parse();
    let (creds, roots) = cellnet::creds_from(&a)?;
    let machine = a.get("machine", &default_machine());
    let data = PathBuf::from(a.get("data", &format!("run/{machine}")));
    let bin = PathBuf::from(a.get("bin", &std::env::current_exe().map(|p| p.with_file_name("search-shard").to_string_lossy().to_string()).unwrap_or_default()));
    let store_addr = a.get("store", "127.0.0.1:7811");
    let store = Store::new(&store_addr, creds.clone());
    let (l, addr) = bind(&a.get("listen", "0.0.0.0:0"), &a.get("advertise", "127.0.0.1")).await.map_err(|e| e.to_string())?;
    let mut extra = vec![];
    for f in ["tick-ms", "ttl-ms"] {
        if a.has(f) {
            extra.push(format!("--{f}"));
            extra.push(a.get(f, ""));
        }
    }
    let h = Arc::new(Host {
        me: creds.me.clone(),
        chain: creds.chain.clone(),
        machine: machine.clone(),
        bin,
        cache: PathBuf::from(a.get("cache", &data.join("cache").to_string_lossy())),
        data: data.clone(),
        store_addr,
        roots: roots.join(","),
        advertise: a.get("advertise", "127.0.0.1"),
        extra,
        kids: Mutex::new(BTreeMap::new()),
    });
    let started = std::time::Instant::now();
    let (h1, h2, h3, h4, h5) = (h.clone(), h.clone(), h.clone(), h.clone(), h.clone());
    let srv = Server::new(HOST, creds.me.clone(), roots.clone())
        .audit(cellnet::rpc::audit_file(data.join("audit.jsonl")))
        .on::<HostHealth, _, _>(move |_: (), _| {
            let h = h1.clone();
            async move { Ok(Health { ok: true, version: env!("CARGO_PKG_VERSION").into(), uptime_s: started.elapsed().as_secs(), id: h.me.id(), machine: h.machine.clone() }) }
        })
        .on::<HostSpawn, _, _>(move |r: SpawnReq, _| {
            let h = h2.clone();
            async move {
                let mut ids = vec![];
                for _ in 0..r.n.clamp(1, 16) {
                    ids.push(h.spawn_one(r.shard).map_err(RpcError::failed)?);
                }
                Ok(SpawnResp { ids })
            }
        })
        .on::<HostInstances, _, _>(move |_: (), _| {
            let h = h3.clone();
            async move { Ok(h.reap()) }
        })
        .on::<HostStop, _, _>(move |r: StopReq, _| {
            let h = h4.clone();
            async move { Ok(tokio::task::spawn_blocking(move || h.stop(&r.id)).await.unwrap_or(false)) }
        })
        .on::<HostKillAll, _, _>(move |_: (), _| {
            let h = h5.clone();
            async move {
                let n = h.kill_all();
                // the host goes down with its machine's instances, a moment after it answers
                tokio::spawn(async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    std::process::exit(137);
                });
                Ok(n)
            }
        });
    let hb = h.clone();
    let id = creds.me.id();
    let boot = now_ms();
    tokio::spawn(async move {
        let mut beat = 0;
        loop {
            beat += 1;
            let rec = HostRec { machine: hb.machine.clone(), id: id.clone(), addr: addr.clone(), boot, beat, cores: std::thread::available_parallelism().map(|n| n.get() as u32).unwrap_or(1), instances: hb.reap(), t_ms: now_ms() };
            if let Err(e) = store.put_json(&keys::host(&hb.machine), &rec).await {
                eprintln!("store: {e}");
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    });
    eprintln!("search.host {} for machine {machine}", creds.me.id());
    srv.serve(l).await.map_err(|e| e.to_string())
}
