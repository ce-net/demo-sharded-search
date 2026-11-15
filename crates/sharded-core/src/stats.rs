//! Latency percentiles, a fixed log-scale histogram, and recall@k.

/// Exact percentile (nearest rank) of a sample set, in the sample's unit.
pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

/// Upper bounds (ms) of the histogram buckets; the last bucket is everything above.
pub const BUCKETS_MS: &[f64] = &[1.0, 2.0, 3.0, 5.0, 7.5, 10.0, 15.0, 20.0, 30.0, 50.0, 75.0, 100.0, 150.0, 250.0, 500.0, 1000.0, 2000.0];

pub fn histogram(samples_ms: &[f64]) -> Vec<u32> {
    let mut h = vec![0u32; BUCKETS_MS.len() + 1];
    for &s in samples_ms {
        let i = BUCKETS_MS.iter().position(|b| s <= *b).unwrap_or(BUCKETS_MS.len());
        h[i] += 1;
    }
    h
}

/// recall@k of `got` against the exact list `truth` (|got ∩ truth| / min(k, |truth|)); 1.0 when the truth is empty.
pub fn recall_at(got: &[String], truth: &[String], k: usize) -> f64 {
    let t: Vec<&String> = truth.iter().take(k).collect();
    if t.is_empty() {
        return 1.0;
    }
    let hit = got.iter().take(k).filter(|g| t.contains(g)).count();
    hit as f64 / t.len() as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pct() {
        let v: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        assert_eq!(percentile(&v, 50.0), 50.0);
        assert_eq!(percentile(&v, 99.0), 99.0);
        assert_eq!(histogram(&[0.5, 4.0, 9999.0]).iter().sum::<u32>(), 3);
    }
    #[test]
    fn recall() {
        let t: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let g: Vec<String> = ["a", "x", "c"].iter().map(|s| s.to_string()).collect();
        assert!((recall_at(&g, &t, 10) - 2.0 / 3.0).abs() < 1e-9);
    }
}
