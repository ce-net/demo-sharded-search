//! The per-shard single-leader rule. It is ce-replica's lease-with-acks rule (repo ce-net/replica, `plan.rs`),
//! applied to the members of one shard instead of the nodes of one cell:
//!
//! - Every member writes its own record each tick with a beat counter that only grows. Nobody writes anyone else's
//!   record, so the store needs no compare-and-set and no clock agreement.
//! - Liveness is measured on the reader's own clock: a member is live while its beat keeps changing within `ttl`.
//! - The candidate is the first member in placement order (the order the rebalancer added them) that is live and has
//!   its segment installed. A member that joins later sorts last, so a returning or new replica never displaces a
//!   working leader (no flapping).
//! - Every member votes for the candidate in its view and acks the candidate's last beat it saw.
//! - The candidate is leader only while a majority of the placement's members vote for it with a beat the candidate
//!   emitted less than `ttl` ago on its own clock. A voter moves its vote only after the old leader's beat stood still
//!   for `ttl`, by which time the old leader's count of that vote has expired, so two leaders never overlap.
//! - Without a majority nobody leads (CP, not AP); reads still go to any ready replica.
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Vote {
    pub for_id: String,
    pub beat: u64,
}

/// What a member published in its last record.
#[derive(Clone, Debug)]
pub struct Seen {
    pub id: String,
    pub beat: u64,
    pub ready: bool,
    pub vote: Option<Vote>,
}

/// One reader's view of beats over time, on its own clock.
#[derive(Default, Debug)]
pub struct View {
    last: HashMap<String, (u64, u64)>, // id -> (beat, when it last changed, ms on my clock)
}

impl View {
    pub fn observe(&mut self, id: &str, beat: u64, now_ms: u64) {
        match self.last.get_mut(id) {
            Some((b, t)) if *b == beat => {
                let _ = t;
            }
            Some(e) => *e = (beat, now_ms),
            None => {
                self.last.insert(id.to_string(), (beat, now_ms));
            }
        }
    }

    pub fn live(&self, id: &str, now_ms: u64, ttl_ms: u64) -> bool {
        self.last.get(id).is_some_and(|(_, t)| now_ms.saturating_sub(*t) < ttl_ms)
    }

    /// How long the beat of `id` has stood still, if it was ever seen.
    pub fn still_for(&self, id: &str, now_ms: u64) -> Option<u64> {
        self.last.get(id).map(|(_, t)| now_ms.saturating_sub(*t))
    }

    pub fn forget_except(&mut self, keep: &[String]) {
        self.last.retain(|k, _| keep.contains(k));
    }
}

/// The candidate in this view: first member (placement order) that is live and ready.
pub fn candidate<'a>(members: &'a [String], seen: &HashMap<String, Seen>, view: &View, now_ms: u64, ttl_ms: u64) -> Option<&'a String> {
    members.iter().find(|m| seen.get(*m).is_some_and(|s| s.ready) && view.live(m, now_ms, ttl_ms))
}

/// The vote this member should publish.
pub fn vote(members: &[String], seen: &HashMap<String, Seen>, view: &View, now_ms: u64, ttl_ms: u64) -> Option<Vote> {
    candidate(members, seen, view, now_ms, ttl_ms).map(|c| Vote { for_id: c.clone(), beat: seen.get(c).map(|s| s.beat).unwrap_or(0) })
}

/// Beats a member emitted, with the time it emitted each (its own clock), to count acks against.
#[derive(Default, Debug)]
pub struct Emitted {
    at: BTreeMap<u64, u64>,
}

impl Emitted {
    pub fn record(&mut self, beat: u64, now_ms: u64) {
        self.at.insert(beat, now_ms);
        while self.at.len() > 256 {
            let first = *self.at.keys().next().unwrap();
            self.at.remove(&first);
        }
    }
    pub fn age(&self, beat: u64, now_ms: u64) -> Option<u64> {
        self.at.get(&beat).map(|t| now_ms.saturating_sub(*t))
    }
}

/// (votes for `me` that are still fresh, voters). Voters are the placement's members.
pub fn quorum(me: &str, members: &[String], seen: &HashMap<String, Seen>, emitted: &Emitted, now_ms: u64, ttl_ms: u64) -> (usize, usize) {
    let yes = members
        .iter()
        .filter_map(|m| seen.get(m))
        .filter_map(|s| s.vote.as_ref())
        .filter(|v| v.for_id == me && emitted.age(v.beat, now_ms).is_some_and(|a| a < ttl_ms))
        .count();
    (yes, members.len())
}

pub fn has_quorum((yes, voters): (usize, usize)) -> bool {
    voters > 0 && yes * 2 > voters
}

/// This member leads iff it is the candidate in its own view and holds a fresh majority.
pub fn is_leader(me: &str, members: &[String], seen: &HashMap<String, Seen>, view: &View, emitted: &Emitted, now_ms: u64, ttl_ms: u64) -> bool {
    candidate(members, seen, view, now_ms, ttl_ms).is_some_and(|c| c == me) && has_quorum(quorum(me, members, seen, emitted, now_ms, ttl_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tiny simulation: members tick on a shared store; `dead` stop writing.
    struct Sim {
        members: Vec<String>,
        recs: HashMap<String, Seen>,
        views: HashMap<String, View>,
        emitted: HashMap<String, Emitted>,
        beats: HashMap<String, u64>,
    }

    impl Sim {
        fn new(n: usize) -> Sim {
            let members: Vec<String> = (0..n).map(|i| format!("m{i}")).collect();
            Sim { recs: HashMap::new(), views: members.iter().map(|m| (m.clone(), View::default())).collect(), emitted: members.iter().map(|m| (m.clone(), Emitted::default())).collect(), beats: HashMap::new(), members }
        }
        fn tick(&mut self, now: u64, dead: &[&str], ttl: u64) -> Vec<String> {
            let mut leaders = vec![];
            for m in self.members.clone() {
                if dead.contains(&m.as_str()) {
                    continue;
                }
                let b = self.beats.entry(m.clone()).or_default();
                *b += 1;
                let beat = *b;
                self.emitted.get_mut(&m).unwrap().record(beat, now);
                let view = self.views.get_mut(&m).unwrap();
                for (id, s) in &self.recs {
                    view.observe(id, s.beat, now);
                }
                view.observe(&m, beat, now);
                let mut seen = self.recs.clone();
                seen.insert(m.clone(), Seen { id: m.clone(), beat, ready: true, vote: None });
                let v = vote(&self.members, &seen, view, now, ttl);
                self.recs.insert(m.clone(), Seen { id: m.clone(), beat, ready: true, vote: v });
                if is_leader(&m, &self.members, &self.recs, view, &self.emitted[&m], now, ttl) {
                    leaders.push(m.clone());
                }
            }
            leaders
        }
    }

    #[test]
    fn one_leader_then_failover_within_two_ttl() {
        let ttl = 3000;
        let mut s = Sim::new(3);
        let mut t = 0;
        let mut first = vec![];
        for _ in 0..10 {
            t += 500;
            first = s.tick(t, &[], ttl);
            assert!(first.len() <= 1);
        }
        assert_eq!(first, vec!["m0".to_string()]);
        let killed = t;
        let mut elected = None;
        for _ in 0..40 {
            t += 500;
            let l = s.tick(t, &["m0"], ttl);
            assert!(l.len() <= 1, "two leaders at {t}: {l:?}");
            if l == vec!["m1".to_string()] && elected.is_none() {
                elected = Some(t - killed);
            }
        }
        let e = elected.expect("no new leader");
        assert!(e < 2 * ttl, "took {e} ms");
    }

    #[test]
    fn minority_never_leads() {
        let ttl = 3000;
        let mut s = Sim::new(3);
        let mut t = 0;
        for _ in 0..10 {
            t += 500;
            s.tick(t, &[], ttl);
        }
        for _ in 0..40 {
            t += 500;
            let l = s.tick(t, &["m0", "m1"], ttl);
            assert!(l.is_empty(), "a minority of one elected {l:?}");
        }
    }

    #[test]
    fn returning_member_does_not_displace() {
        let ttl = 3000;
        let mut s = Sim::new(3);
        let mut t = 0;
        for _ in 0..10 {
            t += 500;
            s.tick(t, &[], ttl);
        }
        for _ in 0..20 {
            t += 500;
            s.tick(t, &["m0"], ttl);
        }
        // the rebalancer drops the dead member and appends a fresh one at the end
        s.members = vec!["m1".into(), "m2".into(), "m0".into()];
        for _ in 0..20 {
            t += 500;
            let l = s.tick(t, &[], ttl);
            assert_eq!(l, vec!["m1".to_string()]);
        }
    }
}
