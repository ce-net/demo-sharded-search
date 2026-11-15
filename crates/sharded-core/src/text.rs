//! Tokenizer, document identity and shard assignment. The shards, the brute-force baseline and the router all use
//! these exact functions, so a difference in recall can only come from the index statistics, never from parsing.
use sha2::{Digest, Sha256};

const STOP: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of",
    "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with",
    "he", "she", "his", "her", "its", "from", "which", "who", "has", "have", "had", "were", "been", "also", "than",
];

/// Lowercased alphanumeric runs of 2..=40 characters, stopwords dropped. Unicode letters are kept as they are.
pub fn tokenize(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let flush = |cur: &mut String, out: &mut Vec<String>| {
        let n = cur.chars().count();
        if (2..=40).contains(&n) && !STOP.contains(&cur.as_str()) {
            out.push(std::mem::take(cur));
        } else {
            cur.clear();
        }
    };
    for ch in s.chars() {
        if ch.is_alphanumeric() {
            for l in ch.to_lowercase() {
                cur.push(l);
            }
        } else {
            flush(&mut cur, &mut out);
        }
    }
    flush(&mut cur, &mut out);
    out
}

/// The text a document is indexed by: title and body.
pub fn doc_text(title: &str, text: &str) -> String {
    format!("{title} {text}")
}

/// A document's key everywhere: sha256 of its source id, first 16 bytes, hex.
pub fn doc_hash(source_id: &str) -> String {
    let d = Sha256::digest(source_id.as_bytes());
    hex::encode(&d[..16])
}

/// The shard a document lives in: the first 8 bytes of its key, modulo the shard count.
pub fn shard_of(doc_hash: &str, shards: u32) -> u32 {
    let b = hex::decode(&doc_hash[..16]).unwrap_or_else(|_| vec![0; 8]);
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8]);
    (u64::from_be_bytes(a) % shards as u64) as u32
}

/// BM25 with Anserini's defaults (k1 0.9, b 0.4).
pub const K1: f32 = 0.9;
pub const B: f32 = 0.4;

pub fn idf(n_docs: u64, df: u64) -> f32 {
    (1.0 + (n_docs as f64 - df as f64 + 0.5) / (df as f64 + 0.5)).ln() as f32
}

pub fn bm25(idf: f32, tf: f32, dl: f32, avgdl: f32) -> f32 {
    idf * tf * (K1 + 1.0) / (tf + K1 * (1.0 - B + B * dl / avgdl.max(1e-6)))
}

/// Query terms: tokenized, deduplicated, order kept.
pub fn query_terms(q: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    tokenize(q).into_iter().filter(|t| seen.insert(t.clone())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tokens() {
        assert_eq!(tokenize("The Szechwan dish, food & cuisine!"), vec!["szechwan", "dish", "food", "cuisine"]);
        assert_eq!(tokenize("Ä Øresund x"), vec!["øresund"]);
    }
    #[test]
    fn shards_are_stable() {
        let h = doc_hash("<dbpedia:Umeå>");
        assert_eq!(h.len(), 32);
        assert_eq!(shard_of(&h, 32), shard_of(&h, 32));
        assert!(shard_of(&h, 32) < 32);
    }
}
