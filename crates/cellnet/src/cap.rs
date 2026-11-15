//! Attenuating capability chains, checked offline. The same model as ce-cap: each link is signed by the key the
//! previous link was granted to, can only narrow the rights and the expiry, and the first link is signed by a root
//! the verifier trusts. Rights are ce-cap ability strings (`search.shard:query`); `api:*` and `*` are wildcards.
//!
//! This is a small public stand-in so the demo builds from public crates. On the ce-net mesh the same rights are
//! carried by ce-cap v3 chains and checked by ce-gate; [`cal`] prints a chain in CAL, the cell auth language, so a
//! refusal in the audit log reads the same way in both.
use crate::ident::{self, Identity};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Link {
    pub iss: String,
    pub sub: String,
    pub rights: Vec<String>,
    pub exp: u64,
    pub sig: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Chain(pub Vec<Link>);

fn body(iss: &str, sub: &str, rights: &[String], exp: u64) -> Vec<u8> {
    format!("ss-cap-v1\n{iss}\n{sub}\n{}\n{exp}", rights.join(",")).into_bytes()
}

/// Does a set of rights allow `need`?
pub fn allows(rights: &[String], need: &str) -> bool {
    let api = need.split(':').next().unwrap_or("");
    rights.iter().any(|r| r == "*" || r == need || (r.ends_with(":*") && &r[..r.len() - 2] == api))
}

impl Chain {
    /// A root grant: `root` gives `sub` (a public key, hex) these rights until `exp` (unix seconds).
    pub fn root(root: &Identity, sub: &str, rights: &[&str], exp: u64) -> Chain {
        let rights: Vec<String> = rights.iter().map(|s| s.to_string()).collect();
        let sig = root.sign(&body(&root.public_hex(), sub, &rights, exp));
        Chain(vec![Link { iss: root.public_hex(), sub: sub.to_string(), rights, exp, sig }])
    }

    /// The holder `me` passes a narrower grant on to `sub`.
    pub fn delegate(&self, me: &Identity, sub: &str, rights: &[&str], exp: u64) -> Result<Chain, String> {
        let last = self.0.last().ok_or("empty chain")?;
        if last.sub != me.public_hex() {
            return Err("this chain was not granted to this key".into());
        }
        for r in rights {
            if !allows(&last.rights, r) {
                return Err(format!("cannot grant {r}: not held"));
            }
        }
        let exp = exp.min(last.exp);
        let rights: Vec<String> = rights.iter().map(|s| s.to_string()).collect();
        let sig = me.sign(&body(&me.public_hex(), sub, &rights, exp));
        let mut c = self.0.clone();
        c.push(Link { iss: me.public_hex(), sub: sub.to_string(), rights, exp, sig });
        Ok(Chain(c))
    }

    pub fn encode(&self) -> String {
        hex::encode(serde_json::to_vec(&self.0).unwrap_or_default())
    }

    pub fn decode(s: &str) -> Result<Chain, String> {
        let b = hex::decode(s.trim()).map_err(|e| format!("cap: {e}"))?;
        serde_json::from_slice(&b).map(Chain).map_err(|e| format!("cap: {e}"))
    }

    pub fn holder(&self) -> &str {
        self.0.last().map(|l| l.sub.as_str()).unwrap_or("")
    }

    pub fn rights(&self) -> &[String] {
        self.0.last().map(|l| l.rights.as_slice()).unwrap_or(&[])
    }

    /// Check every signature, the attenuation and the expiry; Ok means `holder()` holds `rights()`.
    pub fn verify(&self, roots: &[String], now_s: u64) -> Result<(), String> {
        let first = self.0.first().ok_or("empty chain")?;
        if !roots.iter().any(|r| r == &first.iss) {
            return Err(format!("not rooted at a trusted key (root {})", ident::short(&first.iss)));
        }
        for (i, l) in self.0.iter().enumerate() {
            ident::verify(&l.iss, &body(&l.iss, &l.sub, &l.rights, l.exp), &l.sig).map_err(|e| format!("link {i}: {e}"))?;
            if l.exp <= now_s {
                return Err(format!("link {i} expired"));
            }
            if i > 0 {
                let p = &self.0[i - 1];
                if l.iss != p.sub {
                    return Err(format!("link {i} is not signed by the holder of link {}", i - 1));
                }
                if l.exp > p.exp {
                    return Err(format!("link {i} outlives its parent"));
                }
                if let Some(r) = l.rights.iter().find(|r| !allows(&p.rights, r)) {
                    return Err(format!("link {i} widens the grant with {r}"));
                }
            }
        }
        Ok(())
    }
}

/// Unix seconds as `YYYY-MM-DDTHH:MMZ`.
pub fn iso_minute(s: u64) -> String {
    let days = (s / 86400) as i64;
    let rem = s % 86400;
    // civil from days (Howard Hinnant)
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}Z", rem / 3600, (rem % 3600) / 60)
}

/// The chain in CAL, one block per link.
pub fn cal(c: &Chain) -> String {
    let mut out = String::new();
    for l in &c.0 {
        out.push_str(&format!("key:{} grants key:{} {{\n", ident::short(&l.iss), ident::short(&l.sub)));
        for r in &l.rights {
            out.push_str(&format!("  may {r}\n"));
        }
        out.push_str(&format!("  until {}\n}}\n", iso_minute(l.exp)));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_attenuates() {
        let root = Identity::generate();
        let host = Identity::generate();
        let inst = Identity::generate();
        let c = Chain::root(&root, &host.public_hex(), &["search.shard:*", "state.store:read"], 4_000_000_000);
        let d = c.delegate(&host, &inst.public_hex(), &["search.shard:query"], 4_000_000_000).unwrap();
        d.verify(&[root.public_hex()], 1_700_000_000).unwrap();
        assert!(allows(d.rights(), "search.shard:query"));
        assert!(!allows(d.rights(), "search.shard:ingest"));
        assert!(c.delegate(&host, &inst.public_hex(), &["state.store:write"], 4_000_000_000).is_err());
        assert!(d.verify(&[host.public_hex()], 1_700_000_000).is_err());
        let mut forged = d.clone();
        forged.0[1].rights.push("search.shard:ingest".into());
        assert!(forged.verify(&[root.public_hex()], 1_700_000_000).is_err());
        assert!(cal(&d).contains("may search.shard:query"));
        assert_eq!(iso_minute(0), "1970-01-01T00:00Z");
        assert_eq!(iso_minute(1_763_200_000), "2025-11-15T09:46Z");
    }
}
