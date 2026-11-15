//! A key/value store with the shape of ce-net's state.store: Get, Put (with `if_version`), List by prefix, and a
//! change feed by sequence number. The cells only ever use [`Store`], so the same code runs against this in-process
//! dev store (public, for local runs and CI) or against state.store on a ce-net node.
use crate::rpc::{Creds, Pool, RpcError, Server};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Doc {
    pub key: String,
    pub value: String,
    pub seq: u64,
    pub version: u64,
}

#[derive(Serialize, Deserialize)]
pub struct GetReq {
    pub key: String,
}
#[derive(Serialize, Deserialize)]
pub struct PutReq {
    pub key: String,
    pub value: String,
    pub if_version: Option<u64>,
}
#[derive(Serialize, Deserialize)]
pub struct ListReq {
    pub prefix: String,
}
#[derive(Serialize, Deserialize)]
pub struct ChangesReq {
    pub prefix: String,
    pub since: u64,
    pub wait_ms: u32,
}
#[derive(Serialize, Deserialize)]
pub struct ChangesResp {
    pub docs: Vec<Doc>,
    pub head: u64,
}
#[derive(Serialize, Deserialize, Default)]
pub struct StoreHealth {
    pub records: u64,
    pub head: u64,
    pub ops: u64,
}

pub const API: &str = "state.store";
crate::op!(Get, "state.store", "get", "state.store:read", GetReq => Option<Doc>);
crate::op!(Put, "state.store", "put", "state.store:write", PutReq => Doc);
crate::op!(List, "state.store", "list", "state.store:read", ListReq => Vec<Doc>);
crate::op!(Changes, "state.store", "changes", "state.store:read", ChangesReq => ChangesResp);
crate::op!(Health, "state.store", "health", "state.store:read", () => StoreHealth);

#[derive(Default)]
struct Inner {
    docs: BTreeMap<String, Doc>,
    log: std::collections::VecDeque<(u64, String)>,
    seq: u64,
    ops: u64,
}

/// The dev store itself.
#[derive(Clone, Default)]
pub struct MemStore {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<tokio::sync::Notify>,
}

const LOG_KEEP: usize = 200_000;

impl MemStore {
    pub fn get(&self, key: &str) -> Option<Doc> {
        let mut i = self.inner.lock().unwrap();
        i.ops += 1;
        i.docs.get(key).cloned()
    }

    pub fn put(&self, key: &str, value: &str, if_version: Option<u64>) -> Result<Doc, RpcError> {
        let doc = {
            let mut i = self.inner.lock().unwrap();
            i.ops += 1;
            let cur = i.docs.get(key).map(|d| d.version).unwrap_or(0);
            if let Some(v) = if_version {
                if v != cur {
                    return Err(RpcError::new("conflict", format!("{key} is at version {cur}, not {v}")));
                }
            }
            i.seq += 1;
            let d = Doc { key: key.to_string(), value: value.to_string(), seq: i.seq, version: cur + 1 };
            i.docs.insert(key.to_string(), d.clone());
            let s = i.seq;
            i.log.push_back((s, key.to_string()));
            while i.log.len() > LOG_KEEP {
                i.log.pop_front();
            }
            d
        };
        self.notify.notify_waiters();
        Ok(doc)
    }

    pub fn list(&self, prefix: &str) -> Vec<Doc> {
        let mut i = self.inner.lock().unwrap();
        i.ops += 1;
        i.docs.range(prefix.to_string()..).take_while(|(k, _)| k.starts_with(prefix)).map(|(_, d)| d.clone()).collect()
    }

    fn changes_now(&self, prefix: &str, since: u64) -> ChangesResp {
        let i = self.inner.lock().unwrap();
        let oldest = i.log.front().map(|x| x.0).unwrap_or(i.seq + 1);
        let docs: Vec<Doc> = if since + 1 < oldest {
            // the feed was trimmed past `since`: send the whole prefix
            i.docs.range(prefix.to_string()..).take_while(|(k, _)| k.starts_with(prefix)).map(|(_, d)| d.clone()).collect()
        } else {
            let start = i.log.partition_point(|(s, _)| *s <= since);
            let mut seen = std::collections::HashSet::new();
            i.log.range(start..).filter(|(_, k)| k.starts_with(prefix) && seen.insert(k.clone())).filter_map(|(_, k)| i.docs.get(k).cloned()).collect()
        };
        ChangesResp { docs, head: i.seq }
    }

    pub async fn changes(&self, prefix: &str, since: u64, wait_ms: u32) -> ChangesResp {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms as u64);
        loop {
            let notified = self.notify.notified();
            let r = self.changes_now(prefix, since);
            if !r.docs.is_empty() || wait_ms == 0 {
                return r;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.changes_now(prefix, since);
            }
        }
    }

    pub fn health(&self) -> StoreHealth {
        let i = self.inner.lock().unwrap();
        StoreHealth { records: i.docs.len() as u64, head: i.seq, ops: i.ops }
    }

    /// The store as a cell.
    pub fn server(&self, me: crate::ident::Identity, roots: Vec<String>) -> Server {
        let (a, b, c, d, e) = (self.clone(), self.clone(), self.clone(), self.clone(), self.clone());
        Server::new(API, me, roots)
            .on::<Get, _, _>(move |r: GetReq, _| { let s = a.clone(); async move { Ok(s.get(&r.key)) } })
            .on::<Put, _, _>(move |r: PutReq, _| { let s = b.clone(); async move { s.put(&r.key, &r.value, r.if_version) } })
            .on::<List, _, _>(move |r: ListReq, _| { let s = c.clone(); async move { Ok(s.list(&r.prefix)) } })
            .on::<Changes, _, _>(move |r: ChangesReq, _| { let s = d.clone(); async move { Ok(s.changes(&r.prefix, r.since, r.wait_ms.min(30_000)).await) } })
            .on::<Health, _, _>(move |_: (), _| { let s = e.clone(); async move { Ok(s.health()) } })
    }
}

/// A store client: the dev store's address, or (in a mesh build) state.store.
#[derive(Clone)]
pub struct Store {
    pool: Arc<Pool>,
    addr: String,
}

const D: Duration = Duration::from_secs(5);

impl Store {
    pub fn new(addr: &str, creds: Creds) -> Store {
        Store { pool: Arc::new(Pool::new(creds)), addr: addr.to_string() }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub async fn get(&self, key: &str) -> Result<Option<Doc>, RpcError> {
        self.pool.call::<Get>(&self.addr, &GetReq { key: key.into() }, D).await
    }

    pub async fn put(&self, key: &str, value: &str) -> Result<Doc, RpcError> {
        self.pool.call::<Put>(&self.addr, &PutReq { key: key.into(), value: value.into(), if_version: None }, D).await
    }

    pub async fn put_if(&self, key: &str, value: &str, if_version: u64) -> Result<Doc, RpcError> {
        self.pool.call::<Put>(&self.addr, &PutReq { key: key.into(), value: value.into(), if_version: Some(if_version) }, D).await
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<Doc>, RpcError> {
        self.pool.call::<List>(&self.addr, &ListReq { prefix: prefix.into() }, D).await
    }

    pub async fn changes(&self, prefix: &str, since: u64, wait_ms: u32) -> Result<ChangesResp, RpcError> {
        self.pool.call::<Changes>(&self.addr, &ChangesReq { prefix: prefix.into(), since, wait_ms }, Duration::from_millis(wait_ms as u64) + D).await
    }

    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>, RpcError> {
        match self.get(key).await? {
            Some(d) => serde_json::from_str(&d.value).map(Some).map_err(|e| RpcError::failed(format!("{key}: {e}"))),
            None => Ok(None),
        }
    }

    pub async fn put_json<T: Serialize>(&self, key: &str, v: &T) -> Result<Doc, RpcError> {
        self.put(key, &serde_json::to_string(v).unwrap_or_default()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn versions_and_feed() {
        let s = MemStore::default();
        let d = s.put("a/1", "x", None).unwrap();
        assert_eq!(d.version, 1);
        assert!(s.put("a/1", "y", Some(0)).is_err());
        s.put("a/1", "y", Some(1)).unwrap();
        s.put("b/1", "z", None).unwrap();
        assert_eq!(s.list("a/").len(), 1);
        let c = s.changes("a/", 0, 0).await;
        assert_eq!(c.docs.len(), 1);
        assert_eq!(c.docs[0].value, "y");
        let s2 = s.clone();
        let w = tokio::spawn(async move { s2.changes("c/", c.head, 2000).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        s.put("c/9", "w", None).unwrap();
        assert_eq!(w.await.unwrap().docs[0].key, "c/9");
    }
}
