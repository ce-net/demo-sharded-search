//! `search-rebalancer`: the search.rebalancer cell. It watches the shard records and the host heartbeats in the store,
//! runs the planner (`sharded_core::plan`) every tick and carries out what it says: drop a dead member from its
//! shard's placement, ask a machine's search.host to spawn a replacement, retire an extra copy. The machines come only
//! from host heartbeats, so a new machine is used as soon as its host starts, with no change to any config.
use cellnet::rpc::{bind, Pool, Server};
use cellnet::store::Store;
use search_api::*;
use sharded_core::lease::View;
use sharded_core::plan::{self, Action, Member};
use sharded_core::{mono_ms, now_ms};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("search-rebalancer: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let a = cellnet::Args::parse();
    let (creds, roots) = cellnet::creds_from(&a)?;
    let store = Store::new(&a.get("store", "127.0.0.1:7811"), creds.clone());
    let pool = Arc::new(Pool::new(creds.clone()));
    let replicas = a.num("replicas", 3) as usize;
    let ttl = a.num("ttl-ms", 3000);
    let dead_ms = a.num("dead-ms", ttl);
    let grace_ms = a.num("boot-grace-ms", 20_000);
    let host_ttl = a.num("host-ttl-ms", 3000);
    let tick = a.num("tick-ms", 500);
    let max_moves = a.num("max-moves", 4) as usize;
    let status = Arc::new(Mutex::new(RebalancerStatus { replicas: replicas as u32, ..Default::default() }));
    let st2 = status.clone();
    let (l, addr) = bind(&a.get("listen", "0.0.0.0:0"), &a.get("advertise", "127.0.0.1")).await.map_err(|e| e.to_string())?;
    let srv = Server::new(REBALANCER, creds.me.clone(), roots).on::<RebalancerStatusOp, _, _>(move |_: (), _| {
        let s = st2.clone();
        async move { Ok(s.lock().unwrap().clone()) }
    });
    tokio::spawn(async move {
        let _ = srv.serve(l).await;
    });
    eprintln!("search.rebalancer {} at {addr}, replicas {replicas}", creds.me.id());

    let mut hview = View::default();
    let mut iview = View::default();
    let mut spawned: HashMap<String, (u64, String)> = HashMap::new(); // id -> (when, machine)
    let mut ev_n = 0u64;
    loop {
        tokio::time::sleep(Duration::from_millis(tick)).await;
        let now = mono_ms();
        let Ok(Some(manifest)) = store.get_json::<Manifest>(keys::MANIFEST).await else {
            continue;
        };
        let Ok(hosts) = store.list(keys::HOSTS).await else { continue };
        let Ok(sdocs) = store.list(keys::SHARDS).await else { continue };
        let mut host_addr: BTreeMap<String, String> = BTreeMap::new();
        for d in &hosts {
            if let Ok(h) = serde_json::from_str::<HostRec>(&d.value) {
                hview.observe(&format!("{}#{}", h.machine, h.boot), h.beat, now);
                if hview.live(&format!("{}#{}", h.machine, h.boot), now, host_ttl) {
                    host_addr.insert(h.machine.clone(), h.addr.clone());
                }
            }
        }
        let machines: Vec<String> = host_addr.keys().cloned().collect();
        let mut places: BTreeMap<u32, (PlaceRec, u64)> = BTreeMap::new();
        let mut insts: HashMap<String, InstRec> = HashMap::new();
        for d in &sdocs {
            if d.key.ends_with("/place") {
                if let Ok(p) = serde_json::from_str::<PlaceRec>(&d.value) {
                    places.insert(p.shard, (p, d.version));
                }
            } else if let Ok(r) = serde_json::from_str::<InstRec>(&d.value) {
                iview.observe(&r.id, r.beat, now);
                insts.insert(r.id.clone(), r);
            }
        }
        // a shard without a placement yet gets an empty one, naming its segment
        for s in 0..manifest.shards {
            if !places.contains_key(&s) {
                let seg = manifest.segments.iter().find(|g| g.shard == s).map(|g| g.hash.clone()).unwrap_or_default();
                let p = PlaceRec { shard: s, members: vec![], epoch: 0, seg };
                if let Ok(d) = store.put_json(&keys::place(s), &p).await {
                    places.insert(s, (p, d.version));
                }
            }
        }
        let mut input: BTreeMap<u32, Vec<Member>> = BTreeMap::new();
        for (s, (p, _)) in &places {
            let ms = p
                .members
                .iter()
                .map(|id| {
                    let rec = insts.get(id);
                    let machine = rec.map(|r| r.machine.clone()).or_else(|| spawned.get(id).map(|x| x.1.clone())).unwrap_or_default();
                    let live = rec.is_some_and(|r| !r.gone) && iview.live(id, now, ttl);
                    let dead = !live
                        && match iview.still_for(id, now) {
                            Some(t) => t >= dead_ms || rec.is_some_and(|r| r.gone),
                            None => spawned.get(id).map(|x| now.saturating_sub(x.0) >= grace_ms).unwrap_or(true),
                        };
                    Member { id: id.clone(), machine, live, ready: rec.is_some_and(|r| r.ready), leader: rec.is_some_and(|r| r.role == "leader"), still_ms: if dead { dead_ms } else { 0 } }
                })
                .collect();
            input.insert(*s, ms);
        }
        let pending = HashMap::new();
        let actions = plan::plan(&plan::Input { machines: &machines, shards: &input, replicas, dead_ms, pending: &pending, max_moves });
        if actions.is_empty() {
            let mut s = status.lock().unwrap();
            s.machines = machines.clone();
            s.shards = manifest.shards;
            continue;
        }
        // carry out: spawns at once (one call per machine per shard), then one placement write per shard
        let mut changes: BTreeMap<u32, (Vec<String>, Vec<String>)> = BTreeMap::new(); // shard -> (add, remove)
        let mut calls = Vec::new();
        for act in &actions {
            match act {
                Action::Drop { shard, id } | Action::Retire { shard, id } => changes.entry(*shard).or_default().1.push(id.clone()),
                Action::Spawn { shard, machine } => {
                    let Some(addr) = host_addr.get(machine).cloned() else { continue };
                    let (pool, shard, machine) = (pool.clone(), *shard, machine.clone());
                    calls.push(tokio::spawn(async move { (shard, machine.clone(), pool.call::<HostSpawn>(&addr, &SpawnReq { shard, n: 1 }, Duration::from_secs(10)).await) }));
                }
            }
        }
        for c in calls {
            if let Ok((shard, machine, r)) = c.await {
                match r {
                    Ok(resp) => {
                        for id in resp.ids {
                            spawned.insert(id.clone(), (mono_ms(), machine.clone()));
                            changes.entry(shard).or_default().0.push(id);
                        }
                    }
                    Err(e) => eprintln!("spawn {} on {machine}: {e}", shard_label(shard)),
                }
            }
        }
        let mut log = vec![];
        for (shard, (add, remove)) in changes {
            let Some((mut p, ver)) = places.get(&shard).cloned() else { continue };
            p.members.retain(|m| !remove.contains(m));
            p.members.extend(add.iter().cloned());
            p.epoch += 1;
            if let Err(e) = store.put_if(&keys::place(shard), &serde_json::to_string(&p).unwrap_or_default(), ver).await {
                eprintln!("place {}: {e}", shard_label(shard));
                continue;
            }
            for id in &remove {
                let why = if actions.iter().any(|a| matches!(a, Action::Retire { id: x, .. } if x == id)) { "retire" } else { "drop" };
                log.push(serde_json::json!({"t_ms": now_ms(), "kind": why, "shard": shard, "id": id, "machine": input.get(&shard).and_then(|v| v.iter().find(|m| &m.id == id)).map(|m| m.machine.clone()).unwrap_or_default()}));
            }
            for id in &add {
                log.push(serde_json::json!({"t_ms": now_ms(), "kind": "spawn", "shard": shard, "id": id, "machine": spawned.get(id).map(|x| x.1.clone()).unwrap_or_default()}));
            }
        }
        for e in &log {
            ev_n += 1;
            eprintln!("{e}");
            let _ = store.put_json(&keys::event(now_ms(), ev_n), e).await;
        }
        let mut s = status.lock().unwrap();
        s.machines = machines;
        s.shards = manifest.shards;
        s.actions += log.len() as u64;
        s.last = log.iter().rev().take(10).map(|e| e.to_string()).collect();
    }
}
