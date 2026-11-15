//! One shard's immutable index segment: a single file, named by its sha256, read through mmap. Replicas of a shard on
//! the same machine map the same file, so the page cache holds one copy per machine, not one per instance.
//!
//! Layout (little-endian):
//!   0   magic "SSEG0001"
//!   8   n_docs u32, n_terms u32
//!   16  avgdl f64
//!   24  off_terms, off_strs, off_post, off_docs, off_meta, total  (u64 each)
//!   terms: n_terms x { str_off u32, str_len u32, post_idx u64, df u32, pad u32 }   (24 bytes, sorted by term)
//!   strs:  term bytes
//!   post:  { doc u32, tf u16 } (6 bytes), grouped by term
//!   docs:  n_docs x { dl u32, meta_off u32, meta_len u32 }
//!   meta:  "<doc hash>\t<title>"
use crate::text::{bm25, idf};
use sha2::{Digest, Sha256};
use std::collections::{BinaryHeap, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"SSEG0001";
const HDR: usize = 72;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Hit {
    pub doc: String,
    pub title: String,
    pub score: f32,
}

/// Builds a segment from documents given as (doc hash, title, tokens).
#[derive(Default)]
pub struct Builder {
    post: HashMap<String, Vec<(u32, u16)>>,
    docs: Vec<(u32, String)>,
    total_dl: u64,
}

impl Builder {
    pub fn add(&mut self, hash: &str, title: &str, tokens: &[String]) {
        let id = self.docs.len() as u32;
        let mut tf: HashMap<&str, u32> = HashMap::new();
        for t in tokens {
            *tf.entry(t.as_str()).or_default() += 1;
        }
        for (t, n) in tf {
            self.post.entry(t.to_string()).or_default().push((id, n.min(u16::MAX as u32) as u16));
        }
        let title: String = title.chars().filter(|c| *c != '\t' && *c != '\n').take(200).collect();
        self.docs.push((tokens.len() as u32, format!("{hash}\t{title}")));
        self.total_dl += tokens.len() as u64;
    }

    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// Serialize; the bytes are deterministic for the same documents in the same order.
    pub fn bytes(self) -> Vec<u8> {
        let mut terms: Vec<(String, Vec<(u32, u16)>)> = self.post.into_iter().collect();
        terms.sort_by(|a, b| a.0.cmp(&b.0));
        let n_docs = self.docs.len() as u32;
        let avgdl = if n_docs == 0 { 0.0 } else { self.total_dl as f64 / n_docs as f64 };
        let mut strs = Vec::new();
        let mut table = Vec::with_capacity(terms.len() * 24);
        let mut post = Vec::new();
        let mut idx = 0u64;
        for (t, p) in &terms {
            table.extend_from_slice(&(strs.len() as u32).to_le_bytes());
            table.extend_from_slice(&(t.len() as u32).to_le_bytes());
            table.extend_from_slice(&idx.to_le_bytes());
            table.extend_from_slice(&(p.len() as u32).to_le_bytes());
            table.extend_from_slice(&0u32.to_le_bytes());
            strs.extend_from_slice(t.as_bytes());
            for (d, tf) in p {
                post.extend_from_slice(&d.to_le_bytes());
                post.extend_from_slice(&tf.to_le_bytes());
            }
            idx += p.len() as u64;
        }
        let mut docs = Vec::with_capacity(self.docs.len() * 12);
        let mut meta = Vec::new();
        for (dl, m) in &self.docs {
            docs.extend_from_slice(&dl.to_le_bytes());
            docs.extend_from_slice(&(meta.len() as u32).to_le_bytes());
            docs.extend_from_slice(&(m.len() as u32).to_le_bytes());
            meta.extend_from_slice(m.as_bytes());
        }
        let off_terms = HDR as u64;
        let off_strs = off_terms + table.len() as u64;
        let off_post = off_strs + strs.len() as u64;
        let off_docs = off_post + post.len() as u64;
        let off_meta = off_docs + docs.len() as u64;
        let total = off_meta + meta.len() as u64;
        let mut out = Vec::with_capacity(total as usize);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&n_docs.to_le_bytes());
        out.extend_from_slice(&(terms.len() as u32).to_le_bytes());
        out.extend_from_slice(&avgdl.to_le_bytes());
        for o in [off_terms, off_strs, off_post, off_docs, off_meta, total] {
            out.extend_from_slice(&o.to_le_bytes());
        }
        out.extend_from_slice(&table);
        out.extend_from_slice(&strs);
        out.extend_from_slice(&post);
        out.extend_from_slice(&docs);
        out.extend_from_slice(&meta);
        out
    }
}

pub fn sha256_hex(b: &[u8]) -> String {
    hex::encode(Sha256::digest(b))
}

/// Where a segment of `hash` lives in a machine's segment cache.
pub fn seg_path(cache: &Path, hash: &str) -> PathBuf {
    cache.join(format!("{hash}.seg"))
}

/// Write bytes into the cache under their own hash (temp file + rename, so a reader never sees half a file).
pub fn store_bytes(cache: &Path, bytes: &[u8]) -> std::io::Result<String> {
    std::fs::create_dir_all(cache)?;
    let hash = sha256_hex(bytes);
    let dest = seg_path(cache, &hash);
    if dest.exists() {
        return Ok(hash);
    }
    let tmp = cache.join(format!(".{hash}.{}.tmp", std::process::id()));
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    std::fs::rename(&tmp, &dest)?;
    Ok(hash)
}

/// A read-only, mmapped segment.
pub struct Segment {
    map: memmap2::Mmap,
    pub hash: String,
    pub n_docs: u32,
    pub n_terms: u32,
    pub avgdl: f32,
    off: [usize; 6],
}

fn u32_at(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64_at(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

/// Ordered by score, then by doc key (the deterministic tie-break every ranker here uses).
#[derive(PartialEq)]
struct Scored(f32, u32);
impl Eq for Scored {}
impl PartialOrd for Scored {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Scored {
    // min-heap on score: the worst kept hit is at the top
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        o.0.partial_cmp(&self.0).unwrap_or(std::cmp::Ordering::Equal).then(self.1.cmp(&o.1))
    }
}

impl Segment {
    /// Open and check a segment file. `verify` re-hashes the whole file (done once on install).
    pub fn open(path: &Path, verify: bool) -> Result<Segment, String> {
        let f = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let map = unsafe { memmap2::Mmap::map(&f) }.map_err(|e| format!("mmap {}: {e}", path.display()))?;
        if map.len() < HDR || &map[..8] != MAGIC {
            return Err(format!("{}: not a segment", path.display()));
        }
        let name = path.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let hash = if verify { sha256_hex(&map) } else { name.clone() };
        if verify && !name.is_empty() && name != hash && name.len() == 64 {
            return Err(format!("{}: content hash is {hash}", path.display()));
        }
        let mut off = [0usize; 6];
        for (i, o) in off.iter_mut().enumerate() {
            *o = u64_at(&map, 24 + i * 8) as usize;
        }
        if off[5] != map.len() {
            return Err(format!("{}: length {} but header says {}", path.display(), map.len(), off[5]));
        }
        Ok(Segment { n_docs: u32_at(&map, 8), n_terms: u32_at(&map, 12), avgdl: f64::from_le_bytes(map[16..24].try_into().unwrap()) as f32, hash, off, map })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.map
    }

    fn term(&self, i: usize) -> &[u8] {
        let e = self.off[0] + i * 24;
        let so = u32_at(&self.map, e) as usize;
        let sl = u32_at(&self.map, e + 4) as usize;
        &self.map[self.off[1] + so..self.off[1] + so + sl]
    }

    /// (first posting index, df) of a term.
    pub fn lookup(&self, t: &str) -> Option<(u64, u32)> {
        let (mut lo, mut hi) = (0usize, self.n_terms as usize);
        let tb = t.as_bytes();
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.term(mid).cmp(tb) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let e = self.off[0] + mid * 24;
                    return Some((u64_at(&self.map, e + 8), u32_at(&self.map, e + 16)));
                }
            }
        }
        None
    }

    pub fn df(&self, t: &str) -> u32 {
        self.lookup(t).map(|x| x.1).unwrap_or(0)
    }

    pub fn dl(&self, doc: u32) -> u32 {
        u32_at(&self.map, self.off[3] + doc as usize * 12)
    }

    pub fn meta(&self, doc: u32) -> (&str, &str) {
        let e = self.off[3] + doc as usize * 12;
        let mo = u32_at(&self.map, e + 4) as usize;
        let ml = u32_at(&self.map, e + 8) as usize;
        let s = std::str::from_utf8(&self.map[self.off[4] + mo..self.off[4] + mo + ml]).unwrap_or("");
        s.split_once('\t').unwrap_or((s, ""))
    }

    /// Top `k` by BM25 with this segment's own statistics (term-at-a-time into one score array).
    pub fn search(&self, terms: &[String], k: usize) -> Vec<Hit> {
        let n = self.n_docs as usize;
        if n == 0 || k == 0 {
            return vec![];
        }
        let mut acc = vec![0f32; n];
        let mut touched: Vec<u32> = Vec::new();
        for t in terms {
            let Some((start, df)) = self.lookup(t) else { continue };
            let w = idf(self.n_docs as u64, df as u64);
            let base = self.off[2] + start as usize * 6;
            for j in 0..df as usize {
                let o = base + j * 6;
                let d = u32_at(&self.map, o);
                let tf = u16::from_le_bytes([self.map[o + 4], self.map[o + 5]]) as f32;
                let s = bm25(w, tf, self.dl(d) as f32, self.avgdl);
                if acc[d as usize] == 0.0 {
                    touched.push(d);
                }
                acc[d as usize] += s;
            }
        }
        let mut heap: BinaryHeap<Scored> = BinaryHeap::with_capacity(k + 1);
        for d in touched {
            let s = acc[d as usize];
            if heap.len() < k {
                heap.push(Scored(s, d));
            } else if let Some(top) = heap.peek() {
                let better = s > top.0 || (s == top.0 && self.meta(d).0 < self.meta(top.1).0);
                if better {
                    heap.pop();
                    heap.push(Scored(s, d));
                }
            }
        }
        let mut hits: Vec<Hit> = heap.into_iter().map(|Scored(s, d)| { let (h, t) = self.meta(d); Hit { doc: h.to_string(), title: t.to_string(), score: s } }).collect();
        sort_hits(&mut hits);
        hits
    }
}

/// Score descending, doc key ascending.
pub fn sort_hits(h: &mut [Hit]) {
    h.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal).then(a.doc.cmp(&b.doc)));
}

/// Merge per-shard top lists into one top `k` (documents are disjoint across shards; a duplicate keeps its best).
pub fn merge(lists: Vec<Vec<Hit>>, k: usize) -> Vec<Hit> {
    let mut best: HashMap<String, Hit> = HashMap::new();
    for l in lists {
        for h in l {
            match best.get(&h.doc) {
                Some(b) if b.score >= h.score => {}
                _ => {
                    best.insert(h.doc.clone(), h);
                }
            }
        }
    }
    let mut v: Vec<Hit> = best.into_values().collect();
    sort_hits(&mut v);
    v.truncate(k);
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text::tokenize;

    fn seg(docs: &[(&str, &str)]) -> (tempdir::Dir, Segment) {
        let mut b = Builder::default();
        for (id, t) in docs {
            b.add(&crate::text::doc_hash(id), id, &tokenize(t));
        }
        let d = tempdir::Dir::new();
        let h = store_bytes(&d.0, &b.bytes()).unwrap();
        let s = Segment::open(&seg_path(&d.0, &h), true).unwrap();
        (d, s)
    }

    mod tempdir {
        pub struct Dir(pub std::path::PathBuf);
        impl Dir {
            pub fn new() -> Dir {
                let p = std::env::temp_dir().join(format!("sseg-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
                std::fs::create_dir_all(&p).unwrap();
                Dir(p)
            }
        }
        impl Drop for Dir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }

    #[test]
    fn finds_and_ranks() {
        let (_d, s) = seg(&[("a", "gearbox torque spec"), ("b", "gearbox gearbox oil"), ("c", "pizza dough")]);
        assert_eq!(s.n_docs, 3);
        let h = s.search(&["gearbox".into()], 10);
        assert_eq!(h.len(), 2);
        assert_eq!(h[0].title, "b");
        assert!(s.search(&["nothing".into()], 10).is_empty());
        assert_eq!(s.df("pizza"), 1);
    }

    #[test]
    fn deterministic_bytes() {
        let mk = || {
            let mut b = Builder::default();
            for i in 0..50 {
                b.add(&crate::text::doc_hash(&i.to_string()), "t", &tokenize(&format!("word{} common w{}", i % 7, i % 3)));
            }
            b.bytes()
        };
        assert_eq!(sha256_hex(&mk()), sha256_hex(&mk()));
    }

    #[test]
    fn merge_keeps_top() {
        let a = vec![Hit { doc: "1".into(), title: "".into(), score: 3.0 }, Hit { doc: "2".into(), title: "".into(), score: 1.0 }];
        let b = vec![Hit { doc: "3".into(), title: "".into(), score: 2.0 }];
        let m = merge(vec![a, b], 2);
        assert_eq!(m.iter().map(|h| h.doc.as_str()).collect::<Vec<_>>(), vec!["1", "3"]);
    }
}
