//! The rebalancer's planner: given the machines that are up, each shard's members and which of them are live and
//! ready, decide what to spawn and what to retire. Pure, so every rule below has a test.
//!
//! 1. A member whose beat stood still for `dead_ms` (on the rebalancer's clock) is dropped from its shard.
//! 2. A shard with fewer than `replicas` members gets one more, on the machine that holds the fewest members of that
//!    shard (anti-affinity), then the fewest instances overall, then by a stable hash of (shard, machine).
//! 3. When every shard is fully ready and the machines are uneven (a machine was added, or one came back), it moves
//!    one replica at a time: spawn on the lightest machine for a shard that sits on the heaviest, and retire the extra
//!    copy from the heaviest machine once the new one is ready. No config names the machines, so a new machine gets
//!    shards the moment its host heartbeats.
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Debug)]
pub struct Member {
    pub id: String,
    pub machine: String,
    pub live: bool,
    pub ready: bool,
    pub leader: bool,
    /// how long its beat has stood still on the rebalancer's clock
    pub still_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Drop { shard: u32, id: String },
    Spawn { shard: u32, machine: String },
    Retire { shard: u32, id: String },
}

pub struct Input<'a> {
    pub machines: &'a [String],
    pub shards: &'a BTreeMap<u32, Vec<Member>>,
    pub replicas: usize,
    pub dead_ms: u64,
    /// spawns already asked for and not yet visible, per (shard, machine)
    pub pending: &'a HashMap<(u32, String), usize>,
    pub max_moves: usize,
}

fn h(shard: u32, m: &str) -> [u8; 32] {
    Sha256::digest(format!("{shard}/{m}").as_bytes()).into()
}

pub fn plan(inp: &Input) -> Vec<Action> {
    let mut out = Vec::new();
    if inp.machines.is_empty() {
        return out;
    }
    // members that count, after dropping the dead
    let mut kept: BTreeMap<u32, Vec<Member>> = BTreeMap::new();
    for (s, ms) in inp.shards {
        let mut k = Vec::new();
        for m in ms {
            if !m.live && m.still_ms >= inp.dead_ms {
                out.push(Action::Drop { shard: *s, id: m.id.clone() });
            } else {
                k.push(m.clone());
            }
        }
        kept.insert(*s, k);
    }
    // load per machine (members + pending spawns)
    let mut load: HashMap<String, usize> = inp.machines.iter().map(|m| (m.clone(), 0)).collect();
    for ms in kept.values() {
        for m in ms {
            if let Some(l) = load.get_mut(&m.machine) {
                *l += 1;
            }
        }
    }
    for ((_, m), n) in inp.pending {
        if let Some(l) = load.get_mut(m) {
            *l += n;
        }
    }
    let per_shard = |s: u32, kept: &BTreeMap<u32, Vec<Member>>, mach: &str| -> usize {
        kept.get(&s).map(|v| v.iter().filter(|m| m.machine == mach).count()).unwrap_or(0) + inp.pending.get(&(s, mach.to_string())).copied().unwrap_or(0)
    };
    let pick = |s: u32, kept: &BTreeMap<u32, Vec<Member>>, load: &HashMap<String, usize>| -> Option<String> {
        inp.machines.iter().min_by(|a, b| per_shard(s, kept, a).cmp(&per_shard(s, kept, b)).then(load[*a].cmp(&load[*b])).then(h(s, a).cmp(&h(s, b)))).cloned()
    };
    // 2. refill
    let mut deficit = false;
    for (s, ms) in &kept {
        let pend: usize = inp.machines.iter().map(|m| inp.pending.get(&(*s, m.clone())).copied().unwrap_or(0)).sum();
        let mut have = ms.len() + pend;
        while have < inp.replicas {
            deficit = true;
            let Some(m) = pick(*s, &kept, &load) else { break };
            *load.get_mut(&m).unwrap() += 1;
            out.push(Action::Spawn { shard: *s, machine: m });
            have += 1;
        }
    }
    if deficit || !inp.pending.is_empty() {
        return out;
    }
    let all_ready = kept.values().all(|ms| ms.len() >= inp.replicas && ms.iter().all(|m| m.ready && m.live));
    if !all_ready {
        return out;
    }
    // 3a. a shard that has an extra ready copy: retire one from the machine that holds the most of that shard, then
    //     the heaviest machine; never the leader when another choice exists
    for (s, ms) in &kept {
        if ms.len() > inp.replicas {
            let victim = ms
                .iter()
                .filter(|m| !m.leader)
                .max_by(|a, b| per_shard(*s, &kept, &a.machine).cmp(&per_shard(*s, &kept, &b.machine)).then(load[&a.machine].cmp(&load[&b.machine])).then(b.id.cmp(&a.id)))
                .or_else(|| ms.first());
            if let Some(v) = victim {
                out.push(Action::Retire { shard: *s, id: v.id.clone() });
                return out;
            }
        }
    }
    // 3b. even out: heaviest vs lightest machine
    let mut moves = 0;
    let mut load2 = load.clone();
    while moves < inp.max_moves {
        let heavy = inp.machines.iter().max_by(|a, b| load2[*a].cmp(&load2[*b]).then(b.cmp(a))).unwrap().clone();
        let light = inp.machines.iter().min_by(|a, b| load2[*a].cmp(&load2[*b]).then(a.cmp(b))).unwrap().clone();
        if load2[&heavy] <= load2[&light] + 1 {
            break;
        }
        // a shard on the heavy machine with no copy on the light one; prefer shards with two copies on the heavy one
        let cand = kept
            .iter()
            .filter(|(s, _)| per_shard(**s, &kept, &heavy) > 0 && per_shard(**s, &kept, &light) == 0)
            .filter(|(s, _)| !out.iter().any(|a| matches!(a, Action::Spawn { shard, .. } if shard == *s)))
            .max_by(|a, b| per_shard(*a.0, &kept, &heavy).cmp(&per_shard(*b.0, &kept, &heavy)).then(b.0.cmp(a.0)));
        let Some((s, _)) = cand else { break };
        out.push(Action::Spawn { shard: *s, machine: light.clone() });
        *load2.get_mut(&light).unwrap() += 1;
        *load2.get_mut(&heavy).unwrap() -= 1;
        moves += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem(id: &str, machine: &str) -> Member {
        Member { id: id.into(), machine: machine.into(), live: true, ready: true, leader: false, still_ms: 0 }
    }

    fn fill(machines: &[&str], shards: u32, r: usize) -> BTreeMap<u32, Vec<Member>> {
        let ms: Vec<String> = machines.iter().map(|s| s.to_string()).collect();
        let mut sh: BTreeMap<u32, Vec<Member>> = (0..shards).map(|s| (s, vec![])).collect();
        let mut n = 0;
        loop {
            let pend = HashMap::new();
            let acts = plan(&Input { machines: &ms, shards: &sh, replicas: r, dead_ms: 3000, pending: &pend, max_moves: 4 });
            let spawns: Vec<_> = acts.iter().filter_map(|a| if let Action::Spawn { shard, machine } = a { Some((*shard, machine.clone())) } else { None }).collect();
            if spawns.is_empty() {
                break;
            }
            for (s, m) in spawns {
                n += 1;
                sh.get_mut(&s).unwrap().push(mem(&format!("i{n}"), &m));
            }
        }
        sh
    }

    #[test]
    fn anti_affinity_with_three_machines() {
        let sh = fill(&["a", "b", "c"], 32, 3);
        for ms in sh.values() {
            assert_eq!(ms.len(), 3);
            let mut m: Vec<_> = ms.iter().map(|x| x.machine.clone()).collect();
            m.sort();
            m.dedup();
            assert_eq!(m.len(), 3, "a shard has two copies on one machine");
        }
    }

    #[test]
    fn dead_machine_is_refilled_elsewhere() {
        let mut sh = fill(&["a", "b", "c"], 8, 3);
        for ms in sh.values_mut() {
            for m in ms.iter_mut() {
                if m.machine == "c" {
                    m.live = false;
                    m.still_ms = 5000;
                }
            }
        }
        let machines = vec!["a".to_string(), "b".to_string()];
        let pend = HashMap::new();
        let acts = plan(&Input { machines: &machines, shards: &sh, replicas: 3, dead_ms: 3000, pending: &pend, max_moves: 4 });
        assert_eq!(acts.iter().filter(|a| matches!(a, Action::Drop { .. })).count(), 8);
        assert_eq!(acts.iter().filter(|a| matches!(a, Action::Spawn { .. })).count(), 8);
    }

    #[test]
    fn new_machine_gets_shards() {
        let sh = fill(&["a", "b"], 8, 3);
        let machines = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let pend = HashMap::new();
        let acts = plan(&Input { machines: &machines, shards: &sh, replicas: 3, dead_ms: 3000, pending: &pend, max_moves: 4 });
        assert!(acts.iter().any(|a| matches!(a, Action::Spawn { machine, .. } if machine == "c")), "{acts:?}");
    }
}
