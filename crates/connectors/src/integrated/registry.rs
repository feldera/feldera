//! Objects that the integrated input connectors of one pipeline share.
//!
//! A connector that must share state with its peers, such as one replication
//! pipeline for every connector that reads the same Postgres publication,
//! gets the object from [`ConnectorRegistry::get_or_create`].  The registry
//! holds only weak references, so an object lives as long as some connector
//! holds it.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use feldera_adapterlib::errors::controller::ControllerError;

/// An object that connectors share through a [`ConnectorRegistry`].
pub trait SharedConnectorState: Any + Send + Sync {
    /// Runs once the controller has opened every connector of the pipeline
    /// configuration.  A connector that waits for its peers in `open` would
    /// deadlock when the connector-init pool has fewer threads than there are
    /// peers, so work that needs all of them waits for this call instead.
    ///
    /// An error fails the pipeline start.
    fn initial_connect_done(&self) -> Result<(), ControllerError> {
        Ok(())
    }

    /// Runs when the pipeline stops, before the controller disconnects its
    /// inputs one by one.
    fn shutdown(&self) {}

    /// Whether a connector may still join this object.  An object that is
    /// winding down says no, and [`ConnectorRegistry::get_or_create`] then
    /// makes a new one under the same key.
    fn accepts_members(&self) -> bool {
        true
    }
}

struct Entry {
    any: Weak<dyn Any + Send + Sync>,
    shared: Weak<dyn SharedConnectorState>,
}

/// Registry of the objects the connectors of one pipeline share.  One per
/// controller.
#[derive(Default)]
pub struct ConnectorRegistry {
    entries: Mutex<HashMap<String, Entry>>,
    initial_connect_done: AtomicBool,
    shutdown: AtomicBool,
}

impl ConnectorRegistry {
    /// Returns the live object registered under `key`, or registers the one
    /// `make` returns.  `key` must identify the type as well as the instance,
    /// because a live object of another type under the same key is replaced.
    pub fn get_or_create<T: SharedConnectorState>(
        &self,
        key: &str,
        make: impl FnOnce() -> Arc<T>,
    ) -> Arc<T> {
        let mut entries = self.entries.lock().unwrap();
        if let Some(entry) = entries.get(key)
            && let (Some(any), Some(shared)) = (entry.any.upgrade(), entry.shared.upgrade())
            && shared.accepts_members()
            && let Ok(live) = any.downcast::<T>()
        {
            return live;
        }
        let object = make();
        let shared: Arc<dyn SharedConnectorState> = object.clone();
        let any: Arc<dyn Any + Send + Sync> = object.clone();
        entries.insert(
            key.to_string(),
            Entry {
                any: Arc::downgrade(&any),
                shared: Arc::downgrade(&shared),
            },
        );
        object
    }

    /// Whether the controller has opened the initial connectors.  An object
    /// created afterwards gets no [`SharedConnectorState::initial_connect_done`]
    /// call and starts its work itself.
    pub fn is_initial_connect_done(&self) -> bool {
        self.initial_connect_done.load(Ordering::Acquire)
    }

    /// Whether the pipeline is stopping.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Tells every live object that the initial connectors are open.  Returns
    /// the first error.
    pub fn initial_connect_done(&self) -> Result<(), ControllerError> {
        self.initial_connect_done.store(true, Ordering::Release);
        for object in self.live() {
            object.initial_connect_done()?;
        }
        Ok(())
    }

    /// Tells every live object that the pipeline stops.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        for object in self.live() {
            object.shutdown();
        }
    }

    /// The live objects, in key order so that the calls are deterministic.
    fn live(&self) -> Vec<Arc<dyn SharedConnectorState>> {
        let entries = self.entries.lock().unwrap();
        let mut keys = entries.keys().collect::<Vec<_>>();
        keys.sort();
        keys.into_iter()
            .filter_map(|key| entries[key].shared.upgrade())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[derive(Default)]
    struct Counter {
        started: AtomicUsize,
        stopped: AtomicUsize,
        fail_start: bool,
        retired: AtomicBool,
    }

    impl SharedConnectorState for Counter {
        fn accepts_members(&self) -> bool {
            !self.retired.load(Ordering::SeqCst)
        }

        fn initial_connect_done(&self) -> Result<(), ControllerError> {
            self.started.fetch_add(1, Ordering::SeqCst);
            if self.fail_start {
                Err(ControllerError::ControllerPanic)
            } else {
                Ok(())
            }
        }

        fn shutdown(&self) {
            self.stopped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn same_key_shares_while_alive() {
        let registry = ConnectorRegistry::default();
        let a = registry.get_or_create("k", || Arc::new(Counter::default()));
        let b = registry.get_or_create("k", || Arc::new(Counter::default()));
        assert!(Arc::ptr_eq(&a, &b));
        let other = registry.get_or_create("other", || Arc::new(Counter::default()));
        assert!(!Arc::ptr_eq(&a, &other));

        // The last holder gone, the key makes a new object.
        drop((a, b));
        let c = registry.get_or_create("k", || Arc::new(Counter::default()));
        assert_eq!(c.started.load(Ordering::SeqCst), 0);
    }

    /// An object that winds down is still alive, held by its last connector,
    /// but a new connector must not join it.
    #[test]
    fn a_retired_object_is_replaced() {
        let registry = ConnectorRegistry::default();
        let old = registry.get_or_create("k", || Arc::new(Counter::default()));
        old.retired.store(true, Ordering::SeqCst);
        let new = registry.get_or_create("k", || Arc::new(Counter::default()));
        assert!(!Arc::ptr_eq(&old, &new));
        assert!(Arc::ptr_eq(
            &new,
            &registry.get_or_create("k", || Arc::new(Counter::default()))
        ));
    }

    #[test]
    fn signals_reach_each_live_object_once() {
        let registry = ConnectorRegistry::default();
        let a = registry.get_or_create("a", || Arc::new(Counter::default()));
        let dead = registry.get_or_create("dead", || Arc::new(Counter::default()));
        drop(dead);

        assert!(!registry.is_initial_connect_done());
        registry.initial_connect_done().unwrap();
        assert!(registry.is_initial_connect_done());
        assert_eq!(a.started.load(Ordering::SeqCst), 1);

        assert!(!registry.is_shutdown());
        registry.shutdown();
        assert!(registry.is_shutdown());
        assert_eq!(a.stopped.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn start_error_fails_the_signal() {
        let registry = ConnectorRegistry::default();
        let _failing = registry.get_or_create("a", || {
            Arc::new(Counter {
                fail_start: true,
                ..Counter::default()
            })
        });
        assert!(registry.initial_connect_done().is_err());
        assert!(registry.is_initial_connect_done());
    }
}
