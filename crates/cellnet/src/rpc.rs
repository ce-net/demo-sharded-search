//! Typed calls between cells over TCP. Each frame is a u32 length and a JSON body. A connection opens with a Hello
//! that carries the caller's capability chain and a signature by the chain's holder over the api name and the time
//! (proof that the caller holds the key, not just a copy of the chain). Each request names an op; the server checks
//! the op's right against the chain before the handler runs, and writes every refusal to the audit log.
use crate::cap::{self, Chain};
use crate::ident::{self, Identity};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

/// One typed op of a cell's api.
pub trait Op {
    const API: &'static str;
    const NAME: &'static str;
    const RIGHT: &'static str;
    type Req: Serialize + DeserializeOwned + Send + 'static;
    type Resp: Serialize + DeserializeOwned + Send + 'static;
}

/// `op!(Query, "search.shard", "query", "search.shard:query", QueryReq => QueryResp);`
#[macro_export]
macro_rules! op {
    ($name:ident, $api:expr, $op:expr, $right:expr, $req:ty => $resp:ty) => {
        pub struct $name;
        impl $crate::rpc::Op for $name {
            const API: &'static str = $api;
            const NAME: &'static str = $op;
            const RIGHT: &'static str = $right;
            type Req = $req;
            type Resp = $resp;
        }
    };
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RpcError {
    /// refused | invalid | failed | unavailable | timeout | not_leader
    pub kind: String,
    pub msg: String,
}

impl RpcError {
    pub fn new(kind: &str, msg: impl Into<String>) -> RpcError {
        RpcError { kind: kind.into(), msg: msg.into() }
    }
    pub fn failed(msg: impl Into<String>) -> RpcError {
        RpcError::new("failed", msg)
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.msg)
    }
}

#[derive(Serialize, Deserialize)]
struct Hello {
    api: String,
    cap: String,
    ts_ms: u64,
    sig: String,
}

#[derive(Serialize, Deserialize)]
struct HelloAck {
    ok: bool,
    server: String,
    err: String,
}

#[derive(Serialize, Deserialize)]
struct Request {
    id: u64,
    op: String,
    body: Value,
}

#[derive(Serialize, Deserialize)]
struct Response {
    id: u64,
    #[serde(default)]
    ok: Option<Value>,
    #[serde(default)]
    err: Option<RpcError>,
}

fn hello_msg(api: &str, ts: u64) -> Vec<u8> {
    format!("ss-hello-v1\n{api}\n{ts}").into_bytes()
}

const MAX_FRAME: usize = 64 << 20;

async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<Vec<u8>> {
    let n = r.read_u32_le().await? as usize;
    if n > MAX_FRAME {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large"));
    }
    let mut b = vec![0u8; n];
    r.read_exact(&mut b).await?;
    Ok(b)
}

async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, b: &[u8]) -> std::io::Result<()> {
    let mut f = Vec::with_capacity(4 + b.len());
    f.extend_from_slice(&(b.len() as u32).to_le_bytes());
    f.extend_from_slice(b);
    w.write_all(&f).await
}

/// Who is calling, as the gate verified it.
#[derive(Clone, Debug)]
pub struct Caller {
    pub key: String,
    pub rights: Vec<String>,
    pub chain: Chain,
}

impl Caller {
    pub fn id(&self) -> &str {
        ident::short(&self.key)
    }
}

/// One audit row: every refusal, with the rule that refused it written in CAL.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditRow {
    pub t_ms: u64,
    pub server: String,
    pub api: String,
    pub op: String,
    pub need: String,
    pub caller: String,
    pub verdict: String,
    pub reason: String,
    pub cal: String,
}

pub type AuditSink = Arc<dyn Fn(AuditRow) + Send + Sync>;

/// An audit sink that appends JSON lines to a file.
pub fn audit_file(path: std::path::PathBuf) -> AuditSink {
    let lock = Arc::new(Mutex::new(()));
    Arc::new(move |row: AuditRow| {
        let _g = lock.lock();
        if let Some(d) = path.parent() {
            let _ = std::fs::create_dir_all(d);
        }
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
            use std::io::Write;
            let _ = writeln!(f, "{}", serde_json::to_string(&row).unwrap_or_default());
        }
    })
}

type BoxFut = Pin<Box<dyn Future<Output = Result<Value, RpcError>> + Send>>;
type Handler = Arc<dyn Fn(Value, Caller) -> BoxFut + Send + Sync>;

pub struct Server {
    api: &'static str,
    me: Identity,
    roots: Vec<String>,
    ops: HashMap<String, (&'static str, Handler)>,
    audit: Option<AuditSink>,
}

impl Server {
    pub fn new(api: &'static str, me: Identity, roots: Vec<String>) -> Server {
        Server { api, me, roots, ops: HashMap::new(), audit: None }
    }

    pub fn audit(mut self, sink: AuditSink) -> Server {
        self.audit = Some(sink);
        self
    }

    pub fn on<O, F, Fut>(mut self, f: F) -> Server
    where
        O: Op,
        F: Fn(O::Req, Caller) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O::Resp, RpcError>> + Send + 'static,
    {
        assert_eq!(O::API, self.api, "op {} belongs to {}", O::NAME, O::API);
        let f = Arc::new(f);
        let h: Handler = Arc::new(move |v: Value, c: Caller| {
            let f = f.clone();
            Box::pin(async move {
                let req: O::Req = serde_json::from_value(v).map_err(|e| RpcError::new("invalid", format!("{}: {e}", O::NAME)))?;
                let resp = f(req, c).await?;
                serde_json::to_value(resp).map_err(|e| RpcError::failed(e.to_string()))
            }) as BoxFut
        });
        self.ops.insert(O::NAME.to_string(), (O::RIGHT, h));
        self
    }

    fn refuse(&self, op: &str, need: &str, caller: &str, reason: &str, chain: Option<&Chain>) {
        if let Some(a) = &self.audit {
            a(AuditRow {
                t_ms: sharded_core::now_ms(),
                server: self.me.id(),
                api: self.api.to_string(),
                op: op.to_string(),
                need: need.to_string(),
                caller: caller.to_string(),
                verdict: "refused".into(),
                reason: reason.to_string(),
                cal: chain.map(cap::cal).unwrap_or_default(),
            });
        }
    }

    /// Serve forever on `l`.
    pub async fn serve(self, l: TcpListener) -> std::io::Result<()> {
        let me = Arc::new(self);
        loop {
            let (s, _) = l.accept().await?;
            let me = me.clone();
            tokio::spawn(async move {
                let _ = me.conn(s).await;
            });
        }
    }

    async fn conn(self: Arc<Self>, s: TcpStream) -> std::io::Result<()> {
        let _ = s.set_nodelay(true);
        let (mut r, mut w) = s.into_split();
        let hb = tokio::time::timeout(Duration::from_secs(10), read_frame(&mut r)).await.map_err(|_| std::io::Error::other("hello timeout"))??;
        let hello: Hello = serde_json::from_slice(&hb).map_err(std::io::Error::other)?;
        let check = || -> Result<Caller, String> {
            if hello.api != self.api {
                return Err(format!("this is {}, not {}", self.api, hello.api));
            }
            let chain = Chain::decode(&hello.cap)?;
            chain.verify(&self.roots, sharded_core::now_ms() / 1000)?;
            let skew = sharded_core::now_ms().abs_diff(hello.ts_ms);
            if skew > 30_000 {
                return Err(format!("hello is {skew} ms off this clock"));
            }
            ident::verify(chain.holder(), &hello_msg(&hello.api, hello.ts_ms), &hello.sig).map_err(|e| format!("holder proof: {e}"))?;
            Ok(Caller { key: chain.holder().to_string(), rights: chain.rights().to_vec(), chain })
        };
        let caller = match check() {
            Ok(c) => c,
            Err(e) => {
                let ch = Chain::decode(&hello.cap).ok();
                self.refuse("hello", "", ch.as_ref().map(|c| ident::short(c.holder()).to_string()).unwrap_or_default().as_str(), &e, ch.as_ref());
                let ack = HelloAck { ok: false, server: self.me.id(), err: e };
                write_frame(&mut w, &serde_json::to_vec(&ack)?).await?;
                return Ok(());
            }
        };
        write_frame(&mut w, &serde_json::to_vec(&HelloAck { ok: true, server: self.me.id(), err: String::new() })?).await?;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256);
        let writer = tokio::spawn(async move {
            while let Some(b) = rx.recv().await {
                if write_frame(&mut w, &b).await.is_err() {
                    break;
                }
            }
        });
        loop {
            let b = match read_frame(&mut r).await {
                Ok(b) => b,
                Err(_) => break,
            };
            let req: Request = match serde_json::from_slice(&b) {
                Ok(r) => r,
                Err(_) => break,
            };
            let tx = tx.clone();
            let me = self.clone();
            let caller = caller.clone();
            tokio::spawn(async move {
                let resp = match me.ops.get(&req.op) {
                    None => Response { id: req.id, ok: None, err: Some(RpcError::new("invalid", format!("{} has no op {}", me.api, req.op))) },
                    Some((right, _)) if !cap::allows(&caller.rights, right) => {
                        me.refuse(&req.op, right, caller.id(), &format!("the chain does not hold {right}"), Some(&caller.chain));
                        Response { id: req.id, ok: None, err: Some(RpcError::new("refused", format!("{} needs {right}", req.op))) }
                    }
                    Some((_, h)) => match h(req.body, caller).await {
                        Ok(v) => Response { id: req.id, ok: Some(v), err: None },
                        Err(e) => Response { id: req.id, ok: None, err: Some(e) },
                    },
                };
                let _ = tx.send(serde_json::to_vec(&resp).unwrap_or_default()).await;
            });
        }
        drop(tx);
        let _ = writer.await;
        Ok(())
    }
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Response>>>>;

/// A connection to one cell instance; calls are pipelined over it.
pub struct Client {
    w: tokio::sync::Mutex<OwnedWriteHalf>,
    pending: Pending,
    next: AtomicU64,
    alive: Arc<AtomicBool>,
    pub server: String,
    pub addr: String,
}

/// The key and chain a cell calls with.
#[derive(Clone)]
pub struct Creds {
    pub me: Identity,
    pub chain: Chain,
}

impl Client {
    pub async fn connect(addr: &str, api: &str, creds: &Creds, timeout: Duration) -> Result<Client, RpcError> {
        let s = tokio::time::timeout(timeout, TcpStream::connect(addr)).await.map_err(|_| RpcError::new("timeout", format!("connect {addr}")))?.map_err(|e| RpcError::new("unavailable", format!("connect {addr}: {e}")))?;
        let _ = s.set_nodelay(true);
        let (mut r, mut w) = s.into_split();
        let ts = sharded_core::now_ms();
        let hello = Hello { api: api.to_string(), cap: creds.chain.encode(), ts_ms: ts, sig: creds.me.sign(&hello_msg(api, ts)) };
        let io = |e: std::io::Error| RpcError::new("unavailable", format!("{addr}: {e}"));
        write_frame(&mut w, &serde_json::to_vec(&hello).unwrap_or_default()).await.map_err(io)?;
        let ab = tokio::time::timeout(timeout, read_frame(&mut r)).await.map_err(|_| RpcError::new("timeout", format!("hello {addr}")))?.map_err(io)?;
        let ack: HelloAck = serde_json::from_slice(&ab).map_err(|e| RpcError::failed(e.to_string()))?;
        if !ack.ok {
            return Err(RpcError::new("refused", ack.err));
        }
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let (p2, a2) = (pending.clone(), alive.clone());
        tokio::spawn(async move {
            loop {
                let b = match read_frame(&mut r).await {
                    Ok(b) => b,
                    Err(_) => break,
                };
                if let Ok(resp) = serde_json::from_slice::<Response>(&b) {
                    if let Some(tx) = p2.lock().unwrap().remove(&resp.id) {
                        let _ = tx.send(resp);
                    }
                }
            }
            a2.store(false, Ordering::SeqCst);
            p2.lock().unwrap().clear();
        });
        Ok(Client { w: tokio::sync::Mutex::new(w), pending, next: AtomicU64::new(1), alive, server: ack.server, addr: addr.to_string() })
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    pub async fn call<O: Op>(&self, req: &O::Req, deadline: Duration) -> Result<O::Resp, RpcError> {
        if !self.is_alive() {
            return Err(RpcError::new("unavailable", format!("{} closed", self.addr)));
        }
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let body = serde_json::to_value(req).map_err(|e| RpcError::new("invalid", e.to_string()))?;
        let frame = serde_json::to_vec(&Request { id, op: O::NAME.to_string(), body }).unwrap_or_default();
        {
            let mut w = self.w.lock().await;
            if let Err(e) = write_frame(&mut *w, &frame).await {
                self.alive.store(false, Ordering::SeqCst);
                self.pending.lock().unwrap().remove(&id);
                return Err(RpcError::new("unavailable", format!("{}: {e}", self.addr)));
            }
        }
        let resp = match tokio::time::timeout(deadline, rx).await {
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                return Err(RpcError::new("timeout", format!("{} {} after {:?}", self.addr, O::NAME, deadline)));
            }
            Ok(Err(_)) => return Err(RpcError::new("unavailable", format!("{} closed during {}", self.addr, O::NAME))),
            Ok(Ok(r)) => r,
        };
        if let Some(e) = resp.err {
            return Err(e);
        }
        serde_json::from_value(resp.ok.unwrap_or(Value::Null)).map_err(|e| RpcError::failed(format!("{}: {e}", O::NAME)))
    }
}

/// Connections by (address, api), reconnected when one dies.
pub struct Pool {
    creds: Creds,
    map: tokio::sync::Mutex<HashMap<(String, &'static str), Arc<Client>>>,
    pub connect_timeout: Duration,
}

impl Pool {
    pub fn new(creds: Creds) -> Pool {
        Pool { creds, map: tokio::sync::Mutex::new(HashMap::new()), connect_timeout: Duration::from_millis(1500) }
    }

    pub async fn get(&self, addr: &str, api: &'static str) -> Result<Arc<Client>, RpcError> {
        let key = (addr.to_string(), api);
        {
            let m = self.map.lock().await;
            if let Some(c) = m.get(&key) {
                if c.is_alive() {
                    return Ok(c.clone());
                }
            }
        }
        let c = Arc::new(Client::connect(addr, api, &self.creds, self.connect_timeout).await?);
        self.map.lock().await.insert(key, c.clone());
        Ok(c)
    }

    pub async fn call<O: Op>(&self, addr: &str, req: &O::Req, deadline: Duration) -> Result<O::Resp, RpcError> {
        let c = self.get(addr, O::API).await?;
        let r = c.call::<O>(req, deadline).await;
        if let Err(e) = &r {
            if e.kind == "unavailable" {
                self.map.lock().await.remove(&(addr.to_string(), O::API));
            }
        }
        r
    }

    pub fn creds(&self) -> &Creds {
        &self.creds
    }
}

/// Bind `0.0.0.0:<port>` (0 = any) and return the listener and the address to advertise.
pub async fn bind(listen: &str, advertise_host: &str) -> std::io::Result<(TcpListener, String)> {
    let l = TcpListener::bind(listen).await?;
    let port = l.local_addr()?.port();
    Ok((l, format!("{advertise_host}:{port}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Serialize, Deserialize)]
    pub struct Ping {
        pub n: u32,
    }
    crate::op!(PingOp, "test.cell", "ping", "test.cell:ping", Ping => u32);
    crate::op!(AdminOp, "test.cell", "admin", "test.cell:admin", () => String);

    #[tokio::test]
    async fn gate_checks_rights_and_audits() {
        let root = Identity::generate();
        let server_id = Identity::generate();
        let rows = Arc::new(Mutex::new(Vec::<AuditRow>::new()));
        let r2 = rows.clone();
        let srv = Server::new("test.cell", server_id, vec![root.public_hex()])
            .audit(Arc::new(move |r| r2.lock().unwrap().push(r)))
            .on::<PingOp, _, _>(|p: Ping, _c| async move { Ok(p.n + 1) })
            .on::<AdminOp, _, _>(|_: (), _c| async move { Ok("admin".to_string()) });
        let (l, addr) = bind("127.0.0.1:0", "127.0.0.1").await.unwrap();
        tokio::spawn(srv.serve(l));
        let me = Identity::generate();
        let chain = Chain::root(&root, &me.public_hex(), &["test.cell:ping"], sharded_core::now_ms() / 1000 + 60);
        let c = Client::connect(&addr, "test.cell", &Creds { me: me.clone(), chain: chain.clone() }, Duration::from_secs(2)).await.unwrap();
        assert_eq!(c.call::<PingOp>(&Ping { n: 41 }, Duration::from_secs(2)).await.unwrap(), 42);
        let e = c.call::<AdminOp>(&(), Duration::from_secs(2)).await.unwrap_err();
        assert_eq!(e.kind, "refused");
        assert_eq!(rows.lock().unwrap().len(), 1);
        assert!(rows.lock().unwrap()[0].cal.contains("may test.cell:ping"));
        // a chain presented by a key that does not hold it is refused at hello
        let thief = Identity::generate();
        let r = Client::connect(&addr, "test.cell", &Creds { me: thief, chain }, Duration::from_secs(2)).await;
        assert!(r.is_err());
    }
}
