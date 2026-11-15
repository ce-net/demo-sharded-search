//! `search-eval baseline --corpus <jsonl> --queries <jsonl> [--qrels <tsv>] [--limit N] --out baseline.json`
//! `search-eval recall --router <addr> --queries .. --baseline baseline.json`   (one pass, recall@10)
//! `search-eval load --router <addr> --queries .. --baseline .. [--secs 60] [--conc 8] [--store addr]`
use search_eval::*;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("search-eval: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let a = cellnet::Args::parse();
    let qrels = a.get("qrels", "");
    let queries = load_queries(&PathBuf::from(a.get("queries", "run/corpus/queries.jsonl")), if qrels.is_empty() { None } else { Some(std::path::Path::new(&qrels)) })?;
    match a.rest.first().map(|s| s.as_str()) {
        Some("baseline") => {
            let b = tokio::task::spawn_blocking({
                let corpus = PathBuf::from(a.get("corpus", ""));
                let limit = a.num("limit", u64::MAX);
                let q = queries.clone();
                move || baseline(&corpus, &q, 10, limit)
            })
            .await
            .map_err(|e| e.to_string())??;
            let out = a.get("out", "run/baseline.json");
            let s = serde_json::to_string(&b).unwrap();
            std::fs::write(&out, &s).map_err(|e| e.to_string())?;
            eprintln!("baseline: {} queries over {} docs in {:.1} s -> {out}", b.queries, b.docs, b.build_s);
            println!("{}", sharded_core::segment::sha256_hex(s.as_bytes()));
            Ok(())
        }
        Some(cmd @ ("recall" | "load")) => {
            let (creds, _) = cellnet::creds_from(&a)?;
            let base: Baseline = serde_json::from_slice(&std::fs::read(a.get("baseline", "run/baseline.json")).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
            if base.queries_hash != queries_hash(&queries) {
                return Err("the baseline was built for a different query set".into());
            }
            let r = RouterClient::new(&a.get("router", "127.0.0.1:7820"), creds.clone());
            if cmd == "recall" {
                let p = recall_pass(&r, &queries, &base).await;
                println!("{}", serde_json::to_string_pretty(&p).unwrap());
                return Ok(());
            }
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let st = if a.has("store") { Some(cellnet::store::Store::new(&a.get("store", ""), creds)) } else { None };
            let secs = a.num("secs", 60);
            let s2 = stop.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
                s2.store(true, std::sync::atomic::Ordering::SeqCst);
            });
            load(r, Arc::new(queries), Arc::new(base), a.num("conc", 8) as usize, stop, move |s| {
                println!("{}", serde_json::to_string(&s).unwrap());
                if let Some(st) = &st {
                    let (st, s) = (st.clone(), s.clone());
                    tokio::spawn(async move {
                        let _ = st.put_json(search_api::keys::METRICS, &s).await;
                    });
                }
            })
            .await;
            Ok(())
        }
        _ => Err("usage: search-eval baseline|recall|load ...".into()),
    }
}
