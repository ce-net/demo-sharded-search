//! `dev-store`: the state.store-shaped dev store as a cell (in memory). Local runs and CI use it; on a ce-net node the
//! cells point `--store` at an adapter for state.store instead (see the README).
use cellnet::rpc::bind;
use cellnet::store::MemStore;

#[tokio::main]
async fn main() {
    let a = cellnet::Args::parse();
    let (creds, roots) = match cellnet::creds_from(&a) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("dev-store: {e}");
            std::process::exit(1);
        }
    };
    let (l, addr) = bind(&a.get("listen", "0.0.0.0:7811"), &a.get("advertise", "127.0.0.1")).await.expect("bind");
    eprintln!("state.store (dev) {} at {addr}", creds.me.id());
    let s = MemStore::default();
    let srv = s.server(creds.me.clone(), roots).audit(cellnet::rpc::audit_file(std::path::PathBuf::from(a.get("data", "run/store")).join("audit.jsonl")));
    srv.serve(l).await.expect("serve");
}
