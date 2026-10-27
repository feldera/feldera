//! Starting a DBSP runtime must keep the panic hook that the process installed
//! before it.
//!
//! The pipeline server installs a hook that counts panics for the
//! `process_panics_total` metric and then starts a runtime.  The runtime used
//! to drop that hook, so the metric was always 0.
//!
//! This file has only one test, so that no other test in the process can start
//! a runtime first.

use dbsp::Runtime;
use std::{
    panic,
    sync::atomic::{AtomicU64, Ordering},
};

static N_PANICS: AtomicU64 = AtomicU64::new(0);

#[test]
fn runtime_keeps_earlier_panic_hook() {
    let prev = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        N_PANICS.fetch_add(1, Ordering::Relaxed);
        prev(info);
    }));

    let (handle, ()) = Runtime::init_circuit(1, |_circuit| Ok(())).unwrap();

    std::thread::spawn(|| panic!("expected panic"))
        .join()
        .unwrap_err();
    assert_eq!(N_PANICS.load(Ordering::Relaxed), 1);

    handle.kill().unwrap();
}
