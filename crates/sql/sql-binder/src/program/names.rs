//! Calcite assigns repeated result names numeric suffixes in projection order.

use std::collections::HashMap;

#[derive(Default)]
pub(super) struct OutputNames {
    next_suffix: HashMap<String, usize>,
}

impl OutputNames {
    /// Explicit SELECT items keep their spelling during wildcard expansion,
    /// even when an earlier item has the same name.
    pub(super) fn record(&mut self, name: &str) {
        self.next_suffix.entry(name.to_owned()).or_default();
    }

    /// Reserve the name and return its replacement if it was already used.
    pub(super) fn disambiguate(&mut self, name: &str) -> Option<String> {
        let Some(mut suffix) = self.next_suffix.get(name).copied() else {
            self.record(name);
            return None;
        };
        loop {
            let candidate = format!("{name}{suffix}");
            suffix += 1;
            if !self.next_suffix.contains_key(&candidate) {
                self.next_suffix.insert(name.to_owned(), suffix);
                self.record(&candidate);
                return Some(candidate);
            }
        }
    }
}
