use std::{
    panic::{set_hook, take_hook},
    sync::{
        Once,
        atomic::{AtomicU64, Ordering},
    },
};

/// Number of times the process has panicked.
pub static N_PANICS: AtomicU64 = AtomicU64::new(0);

pub fn enable_counting_panics() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let prev = take_hook();
        set_hook(Box::new(move |info| {
            N_PANICS.fetch_add(1, Ordering::Relaxed);
            prev(info);
        }));
    });
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate::panic::{N_PANICS, enable_counting_panics};

    /// Test that [enable_counting_panics] actually increments [N_PANICS].
    ///
    /// This is set to be ignored because it races with proptest initialization:
    /// both [enable_counting_panics] and proptests set a panic hook, which is
    /// unavoidably racy.  See https://github.com/rust-lang/rust/issues/92649
    /// for an explanation of the race and a proposed way to avoid it (currently
    /// available only in nightly).
    #[test]
    #[ignore = "races with proptests"]
    fn test_counting_panics() {
        enable_counting_panics();
        let panics_before = N_PANICS.load(Ordering::Relaxed);
        std::thread::spawn(|| panic!()).join().unwrap_err();
        let panics_after = N_PANICS.load(Ordering::Relaxed);
        assert!(
            panics_after > panics_before,
            "{panics_after} should be greater than {panics_before}"
        );
    }
}
