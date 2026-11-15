//! An instance identity: an Ed25519 key. The id is the first 16 hex characters of the public key, so every one of
//! the shard instances is a distinct principal that signs its own calls.
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};

#[derive(Clone)]
pub struct Identity {
    key: SigningKey,
}

impl Identity {
    pub fn generate() -> Identity {
        let mut seed = [0u8; 32];
        getrandom::getrandom(&mut seed).expect("os randomness");
        Identity { key: SigningKey::from_bytes(&seed) }
    }

    pub fn from_seed_hex(s: &str) -> Result<Identity, String> {
        let b = hex::decode(s.trim()).map_err(|e| format!("seed: {e}"))?;
        let a: [u8; 32] = b.try_into().map_err(|_| "seed: need 32 bytes".to_string())?;
        Ok(Identity { key: SigningKey::from_bytes(&a) })
    }

    /// Read a seed file, or create it (mode 600) when it does not exist.
    pub fn load_or_create(path: &std::path::Path) -> Result<Identity, String> {
        if let Ok(s) = std::fs::read_to_string(path) {
            return Identity::from_seed_hex(&s);
        }
        let id = Identity::generate();
        if let Some(d) = path.parent() {
            let _ = std::fs::create_dir_all(d);
        }
        std::fs::write(path, id.seed_hex()).map_err(|e| format!("{}: {e}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(id)
    }

    pub fn seed_hex(&self) -> String {
        hex::encode(self.key.to_bytes())
    }

    pub fn public_hex(&self) -> String {
        hex::encode(self.key.verifying_key().to_bytes())
    }

    pub fn id(&self) -> String {
        self.public_hex()[..16].to_string()
    }

    pub fn sign(&self, msg: &[u8]) -> String {
        hex::encode(self.key.sign(msg).to_bytes())
    }
}

pub fn verify(public_hex: &str, msg: &[u8], sig_hex: &str) -> Result<(), String> {
    let pk: [u8; 32] = hex::decode(public_hex).map_err(|e| e.to_string())?.try_into().map_err(|_| "public key: 32 bytes".to_string())?;
    let vk = VerifyingKey::from_bytes(&pk).map_err(|e| e.to_string())?;
    let sb: [u8; 64] = hex::decode(sig_hex).map_err(|e| e.to_string())?.try_into().map_err(|_| "signature: 64 bytes".to_string())?;
    vk.verify(msg, &ed25519_dalek::Signature::from_bytes(&sb)).map_err(|_| "bad signature".to_string())
}

pub fn short(public_hex: &str) -> &str {
    &public_hex[..public_hex.len().min(16)]
}
