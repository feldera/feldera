//! Fingerprinter for circuits.
//!
//! Based on the FNV-1a hash function.
//! `std::hash::DefaultHasher` is not used because it is not stable beyond the
//! current rust release.

pub struct Fingerprinter {
    // The fingerprint of the circuit.
    hash: u64,
}

impl Default for Fingerprinter {
    fn default() -> Self {
        Self {
            hash: Self::FNV_OFFSET_BASIS,
        }
    }
}

impl Fingerprinter {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    pub fn hash(&mut self, key: &str) -> u64 {
        for byte in key.bytes() {
            self.hash ^= byte as u64;
            self.hash = self.hash.wrapping_mul(Fingerprinter::FNV_PRIME);
        }
        self.hash
    }

    pub fn finish(self) -> u64 {
        self.hash
    }
}
