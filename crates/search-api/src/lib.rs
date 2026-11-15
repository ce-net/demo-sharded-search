//! The typed contracts. Every op names the right a caller's chain must hold.
//!
//! | api | op | right |
//! |---|---|---|
//! | search.shard | health, query, ingest, snapshot, install_snapshot | search.shard:<op> |
//! | search.router | health, query | search.router:<op> |
//! | search.host | health, spawn, instances, stop, kill_all | search.host:<op> |
//! | search.rebalancer | status | search.rebalancer:status |
use cellnet::op;
use serde::{Deserialize, Serialize};
pub use sharded_core::segment::Hit;

pub const SHARD: &str = "search.shard";
pub const ROUTER: &str = "search.router";
pub const HOST: &str = "search.host";
pub const REBALANCER: &str = "search.rebalancer";

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Health {
    pub ok: bool,
    pub version: String,
    pub uptime_s: u64,
    pub id: String,
    pub machine: String,
}

// ---- search.shard -------------------------------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryReq {
    pub q: String,
    pub k: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryResp {
    pub hits: Vec<Hit>,
    pub inst: String,
    pub machine: String,
    pub shard: u32,
    pub role: String,
    pub seg: String,
    pub docs: u64,
    pub took_us: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IngestDoc {
    pub doc: String,
    pub id: String,
    pub title: String,
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestReq {
    pub docs: Vec<IngestDoc>,
    /// set by a leader forwarding to its followers
    pub replicated: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestResp {
    pub accepted: u32,
    pub delta: u64,
    pub followers_acked: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotReq {
    pub offset: u64,
    pub len: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotResp {
    pub hash: String,
    pub total: u64,
    pub offset: u64,
    /// hex
    pub data: String,
    /// documents ingested since the segment was built (sent with offset 0)
    pub delta: Vec<IngestDoc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstallReq {
    /// address of a ready replica of the same shard
    pub from: String,
    pub hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstallResp {
    pub hash: String,
    pub bytes: u64,
    pub ms: u64,
    /// false: the file was already in this machine's segment cache
    pub copied: bool,
}

op!(ShardHealth, SHARD, "health", "search.shard:health", () => Health);
op!(ShardQuery, SHARD, "query", "search.shard:query", QueryReq => QueryResp);
op!(ShardIngest, SHARD, "ingest", "search.shard:ingest", IngestReq => IngestResp);
op!(ShardSnapshot, SHARD, "snapshot", "search.shard:snapshot", SnapshotReq => SnapshotResp);
op!(ShardInstall, SHARD, "install_snapshot", "search.shard:install_snapshot", InstallReq => InstallResp);

// ---- search.router ------------------------------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RouterQueryReq {
    pub q: String,
    pub k: u32,
    /// 0 = the router's default
    pub hedge_ms: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardAnswer {
    pub shard: u32,
    pub inst: String,
    pub machine: String,
    pub role: String,
    pub ms: f64,
    pub hedged: bool,
    pub ok: bool,
    pub err: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RouterQueryResp {
    pub hits: Vec<Hit>,
    pub shards: Vec<ShardAnswer>,
    pub missing: Vec<u32>,
    pub hedged: u32,
    pub ms: f64,
}

op!(RouterHealth, ROUTER, "health", "search.router:health", () => Health);
op!(RouterQuery, ROUTER, "query", "search.router:query", RouterQueryReq => RouterQueryResp);

// ---- search.host --------------------------------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpawnReq {
    pub shard: u32,
    pub n: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpawnResp {
    pub ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopReq {
    pub id: String,
}

op!(HostHealth, HOST, "health", "search.host:health", () => Health);
op!(HostSpawn, HOST, "spawn", "search.host:spawn", SpawnReq => SpawnResp);
op!(HostInstances, HOST, "instances", "search.host:instances", () => Vec<String>);
op!(HostStop, HOST, "stop", "search.host:stop", StopReq => bool);
op!(HostKillAll, HOST, "kill_all", "search.host:kill_all", () => u32);

// ---- search.rebalancer --------------------------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct RebalancerStatus {
    pub machines: Vec<String>,
    pub shards: u32,
    pub replicas: u32,
    pub actions: u64,
    pub last: Vec<String>,
}

op!(RebalancerStatusOp, REBALANCER, "status", "search.rebalancer:status", () => RebalancerStatus);

// ---- records in the store (app prefix `ss/`) ----------------------------------------------------------------------

/// What one shard instance publishes about itself every tick. Only that instance writes it.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct InstRec {
    pub id: String,
    pub pubkey: String,
    pub machine: String,
    pub addr: String,
    pub shard: u32,
    pub pid: u32,
    pub boot: u64,
    pub beat: u64,
    pub ready: bool,
    pub seg: String,
    pub docs: u64,
    pub delta: u64,
    pub role: String,
    pub vote_for: String,
    pub vote_beat: u64,
    /// the writer's wall clock (informational; the lease never compares clocks)
    pub t_ms: u64,
    /// set when the instance leaves its shard on its own (retired)
    pub gone: bool,
}

/// A shard's members in placement order (the rebalancer appends new ones at the end).
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PlaceRec {
    pub shard: u32,
    pub members: Vec<String>,
    pub epoch: u64,
    pub seg: String,
}

/// One host per machine; it spawns and stops shard instances there.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct HostRec {
    pub machine: String,
    pub id: String,
    pub addr: String,
    pub boot: u64,
    pub beat: u64,
    pub cores: u32,
    pub instances: Vec<String>,
    pub t_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct SegmentInfo {
    pub shard: u32,
    pub hash: String,
    pub docs: u64,
    pub bytes: u64,
}

/// The corpus as loaded: source, per-shard segment hashes, and the hash of this manifest itself.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Manifest {
    pub name: String,
    pub source: String,
    pub source_sha256: String,
    pub docs: u64,
    pub shards: u32,
    pub replicas: u32,
    pub segments: Vec<SegmentInfo>,
    pub collections: Vec<SegmentInfo>,
    /// sha256 of this manifest serialized with this field empty
    pub hash: String,
}

impl Manifest {
    pub fn compute_hash(&self) -> String {
        let mut m = self.clone();
        m.hash.clear();
        sharded_core::segment::sha256_hex(serde_json::to_string(&m).unwrap_or_default().as_bytes())
    }
}

pub mod keys {
    pub const MANIFEST: &str = "ss/manifest";
    pub const METRICS: &str = "ss/m/now";
    pub fn shard_prefix(s: u32) -> String {
        format!("ss/s/{s:03}/")
    }
    pub fn place(s: u32) -> String {
        format!("ss/s/{s:03}/place")
    }
    pub fn inst(s: u32, id: &str) -> String {
        format!("ss/s/{s:03}/i/{id}")
    }
    pub const SHARDS: &str = "ss/s/";
    pub const HOSTS: &str = "ss/h/";
    pub fn host(m: &str) -> String {
        format!("ss/h/{m}")
    }
    pub fn audit(server: &str, t: u64) -> String {
        format!("ss/a/{server}/{t}")
    }
    pub const AUDIT: &str = "ss/a/";
    pub fn event(t: u64, n: u64) -> String {
        format!("ss/e/{t:015}-{n:06}")
    }
    pub const EVENTS: &str = "ss/e/";
}

/// The rights each kind of cell is granted (by its host, from the host's own chain).
pub mod rights {
    pub const SHARD: &[&str] = &["state.store:read", "state.store:write", "search.shard:snapshot", "search.shard:ingest", "search.shard:health"];
    pub const ROUTER: &[&str] = &["state.store:read", "search.shard:query", "search.shard:health"];
    pub const REBALANCER: &[&str] = &["state.store:read", "state.store:write", "search.host:spawn", "search.host:stop", "search.host:instances", "search.host:health", "search.shard:install_snapshot"];
    pub const HOST: &[&str] = &["state.store:read", "state.store:write", "search.shard:snapshot", "search.shard:ingest", "search.shard:health"];
    pub const OPERATOR: &[&str] = &["*"];
}

/// Two-digit shard label for logs and the map.
pub fn shard_label(s: u32) -> String {
    format!("s{s:02}")
}
