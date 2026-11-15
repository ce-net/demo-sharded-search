//! The pure parts of the sharded search demo. No network, no store: everything here is a function of its inputs, so
//! the cells stay thin and every rule has a unit test.
pub mod lease;
pub mod plan;
pub mod segment;
pub mod stats;
pub mod text;

/// Milliseconds since the Unix epoch (for records and logs only; the lease never compares two machines' clocks).
pub fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Milliseconds on this process's monotonic clock.
pub fn mono_ms() -> u64 {
    use std::sync::OnceLock;
    static T0: OnceLock<std::time::Instant> = OnceLock::new();
    T0.get_or_init(std::time::Instant::now).elapsed().as_millis() as u64
}
