//! `corpus-loader`: stream a corpus dump (BEIR JSON lines: `{"_id","title","text"}`) into one collection per shard,
//! each record keyed by its doc hash, then build each shard's segment into the segment cache and write the manifest
//! (source hash, per-shard collection and segment hashes, and the manifest's own hash).
//!
//! corpus-loader --corpus <file|-> --out <dir> --cache <dir> [--shards 32] [--replicas 3] [--limit N]
//!               [--source <url>] [--store <addr> --cap-file <f> --seed-file <f>]   (also writes ss/manifest)
use search_api::{Manifest, SegmentInfo};
use sha2::{Digest, Sha256};
use sharded_core::segment::{self, Builder};
use sharded_core::text;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

#[derive(serde::Deserialize)]
struct Line {
    #[serde(rename = "_id")]
    id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    text: String,
}

struct HashingReader<R> {
    r: R,
    h: Sha256,
}
impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.r.read(buf)?;
        self.h.update(&buf[..n]);
        Ok(n)
    }
}

fn file_sha(p: &Path) -> std::io::Result<String> {
    let mut f = std::fs::File::open(p)?;
    let mut h = Sha256::new();
    let mut b = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut b)?;
        if n == 0 {
            break;
        }
        h.update(&b[..n]);
    }
    Ok(hex::encode(h.finalize()))
}

fn build_shard(coll: &Path, cache: &Path, shard: u32) -> Result<(SegmentInfo, SegmentInfo), String> {
    let f = std::fs::File::open(coll).map_err(|e| format!("{}: {e}", coll.display()))?;
    let mut b = Builder::default();
    for line in BufReader::new(f).lines() {
        let line = line.map_err(|e| e.to_string())?;
        let v: serde_json::Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
        let (doc, title, body) = (v["doc"].as_str().unwrap_or(""), v["title"].as_str().unwrap_or(""), v["text"].as_str().unwrap_or(""));
        b.add(doc, title, &text::tokenize(&text::doc_text(title, body)));
    }
    let docs = b.len() as u64;
    let bytes = b.bytes();
    let n = bytes.len() as u64;
    let hash = segment::store_bytes(cache, &bytes).map_err(|e| e.to_string())?;
    let csha = file_sha(coll).map_err(|e| e.to_string())?;
    let csize = std::fs::metadata(coll).map(|m| m.len()).unwrap_or(0);
    Ok((SegmentInfo { shard, hash, docs, bytes: n }, SegmentInfo { shard, hash: csha, docs, bytes: csize }))
}

fn main() {
    if let Err(e) = run() {
        eprintln!("corpus-loader: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let a = cellnet::Args::parse();
    let shards = a.num("shards", 32) as u32;
    let replicas = a.num("replicas", 3) as u32;
    let limit = a.num("limit", u64::MAX);
    let out = PathBuf::from(a.get("out", "run/corpus"));
    let cache = PathBuf::from(a.get("cache", "run/cache"));
    let corpus = a.get("corpus", "-");
    let colls = out.join("collections");
    std::fs::create_dir_all(&colls).map_err(|e| e.to_string())?;
    let t0 = std::time::Instant::now();
    let input: Box<dyn Read> = if corpus == "-" { Box::new(std::io::stdin()) } else { Box::new(std::fs::File::open(&corpus).map_err(|e| format!("{corpus}: {e}"))?) };
    let mut hr = HashingReader { r: input, h: Sha256::new() };
    let mut writers: Vec<BufWriter<std::fs::File>> = (0..shards)
        .map(|s| std::fs::File::create(colls.join(format!("shard-{s:03}.jsonl"))).map(|f| BufWriter::with_capacity(1 << 20, f)))
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())?;
    let mut n = 0u64;
    let mut bad = 0u64;
    let mut seen = std::collections::HashSet::new();
    {
        let mut r = BufReader::with_capacity(1 << 20, &mut hr);
        let mut line = String::new();
        while n < limit {
            line.clear();
            if r.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
                break;
            }
            let Ok(l) = serde_json::from_str::<Line>(line.trim_end()) else {
                bad += 1;
                continue;
            };
            let h = text::doc_hash(&l.id);
            if !seen.insert(h.clone()) {
                bad += 1;
                continue;
            }
            let s = text::shard_of(&h, shards);
            let rec = serde_json::json!({"doc": h, "id": l.id, "title": l.title, "text": l.text});
            writeln!(writers[s as usize], "{rec}").map_err(|e| e.to_string())?;
            n += 1;
            if n % 500_000 == 0 {
                eprintln!("{n} docs, {:.0} s", t0.elapsed().as_secs_f64());
            }
        }
    }
    for w in &mut writers {
        w.flush().map_err(|e| e.to_string())?;
    }
    drop(writers);
    // the source hash covers the bytes read; with --limit that is the prefix up to the last document taken
    let source_sha = hex::encode(hr.h.clone().finalize());
    eprintln!("{n} docs into {shards} collections ({bad} skipped) in {:.1} s; building segments", t0.elapsed().as_secs_f64());
    let threads = a.num("threads", 4).max(1) as usize;
    let next = std::sync::atomic::AtomicU32::new(0);
    let results = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|sc| {
        for _ in 0..threads {
            sc.spawn(|| loop {
                let s = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if s >= shards {
                    break;
                }
                let r = build_shard(&colls.join(format!("shard-{s:03}.jsonl")), &cache, s);
                results.lock().unwrap().push(r);
            });
        }
    });
    let mut segs = vec![];
    let mut cols = vec![];
    for r in results.into_inner().unwrap() {
        let (s, c) = r?;
        segs.push(s);
        cols.push(c);
    }
    segs.sort_by_key(|s| s.shard);
    cols.sort_by_key(|s| s.shard);
    let mut m = Manifest {
        name: a.get("name", "corpus"),
        source: a.get("source", &corpus),
        source_sha256: source_sha,
        docs: n,
        shards,
        replicas,
        segments: segs,
        collections: cols,
        hash: String::new(),
    };
    m.hash = m.compute_hash();
    let mp = out.join("manifest.json");
    std::fs::write(&mp, serde_json::to_string_pretty(&m).unwrap()).map_err(|e| e.to_string())?;
    eprintln!("manifest {} ({} docs, {} shards) in {:.1} s -> {}", m.hash, m.docs, m.shards, t0.elapsed().as_secs_f64(), mp.display());
    println!("{}", m.hash);
    if a.has("store") {
        let (creds, _) = cellnet::creds_from(&a)?;
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        rt.block_on(async {
            let st = cellnet::store::Store::new(&a.get("store", ""), creds);
            st.put_json(search_api::keys::MANIFEST, &m).await.map(|_| ()).map_err(|e| e.to_string())
        })?;
    }
    Ok(())
}
