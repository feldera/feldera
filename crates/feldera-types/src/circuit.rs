//! What a pipeline's controller and connectors share with the engine that runs its circuit:
//! how the circuit is laid out across hosts, how parsed input reaches the circuit, and whether
//! an output accumulates its updates.

use std::{
    collections::{BTreeSet, HashSet},
    error::Error as StdError,
    fmt::{self, Debug, Display, Error as FmtError, Formatter},
    iter::empty,
    net::SocketAddr,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use itertools::Either;

use crate::program_schema::SqlIdentifier;

/// A host for some workers in the [`Layout`] for a multi-host circuit.
#[allow(clippy::manual_non_exhaustive)]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Host {
    /// The IP address and TCP port on which the host listens and to which the
    /// other hosts connect.
    pub address: SocketAddr,

    /// The worker thread IDs implemented on this host.  Worker thread IDs start
    /// with 0 in the first host and increase sequentially from there.  A host
    /// has `workers.len()` workers.
    pub workers: Range<usize>,

    /// Prevents `Host` and `Layout::Multihost` from being instantiated without
    /// using the constructor (which checks the invariants).
    _private: (),
}

impl Debug for Host {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Host")
            .field("address", &self.address)
            .field("workers", &self.workers)
            .finish()
    }
}

/// How a circuit is laid out across one or more machines.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Layout {
    /// A layout whose workers run on a single host.
    Solo {
        /// The number of worker threads.
        n_workers: usize,
    },

    /// A layout across multiple machines.
    Multihost {
        /// Each host in the layout.  There should be two or more, each with a
        /// unique network address.
        hosts: Vec<Host>,

        /// The index within `hosts` of the current host.
        local_host_idx: usize,
    },
}

impl Layout {
    /// Returns a new solo layout with `n_workers` worker threads.
    pub fn new_solo(n_workers: usize) -> Layout {
        assert_ne!(n_workers, 0);
        Layout::Solo { n_workers }
    }

    /// Returns a new multihost layout with as many hosts as specified in
    /// `params`.  Each tuple in `params` specifies a host's unique network
    /// address and the number of workers to run on that host.  `local_address`
    /// must be one of the addresses in `params`.
    ///
    /// To execute such a multihost circuit, one must create a `Runtime` for it
    /// on every host in `params`, passing the same `params` in each case.  Each
    /// host must pass its own `local_address`.  The `Runtime` on each host
    /// listens on its own address and connects to all the other addresses.
    pub fn new_multihost(
        params: &[(SocketAddr, usize)],
        local_address: SocketAddr,
    ) -> Result<Layout, LayoutError> {
        // Check that the addresses are unique.
        let mut uniq = HashSet::new();
        if let Some((duplicate, _)) = params.iter().find(|(address, _)| !uniq.insert(address)) {
            return Err(LayoutError::DuplicateAddress(*duplicate));
        }

        // Find `local_address` in `params`.
        let local_host_idx = params
            .iter()
            .position(|(address, _)| *address == local_address)
            .ok_or(LayoutError::NoSuchAddress(local_address))?;

        if params.len() == 1 {
            Ok(Self::new_solo(params[0].1))
        } else {
            let mut hosts = Vec::with_capacity(params.len());
            let mut total_workers = 0;
            for (address, n_workers) in params {
                assert_ne!(*n_workers, 0);
                hosts.push(Host {
                    address: *address,
                    workers: total_workers..total_workers + *n_workers,
                    _private: (),
                });
                total_workers += *n_workers;
            }
            Ok(Layout::Multihost {
                hosts,
                local_host_idx,
            })
        }
    }

    /// Returns the range of IDs for the workers on the local machine.
    pub fn local_workers(&self) -> Range<usize> {
        match self {
            Self::Solo { n_workers } => 0..*n_workers,
            Self::Multihost {
                hosts,
                local_host_idx,
                ..
            } => hosts[*local_host_idx].workers.clone(),
        }
    }

    /// Returns an iterator over `Host`s in this layout other than this one.  If
    /// this is a single-host layout, this will be an empty iterator.
    pub fn other_hosts(&self) -> impl Iterator<Item = &Host> {
        match self {
            Self::Solo { .. } => Either::Left(empty()),
            Self::Multihost {
                hosts,
                local_host_idx,
            } => Either::Right(
                hosts
                    .iter()
                    .enumerate()
                    .filter_map(|(i, host)| (i != *local_host_idx).then_some(host)),
            ),
        }
    }

    /// Returns an iterator across the ranges of workers on all the hosts in
    /// this layout.  A single-host layout will have a single range with all the
    /// workers, a two-host layout will have two ranges, and so on.
    pub fn all_hosts(&self) -> impl Iterator<Item = Range<usize>> {
        match self {
            Self::Solo { n_workers } => Either::Left(std::iter::once(0..*n_workers)),
            Self::Multihost { hosts, .. } => {
                Either::Right(hosts.iter().map(|host| host.workers.clone()))
            }
        }
    }

    /// Returns the network address for the current machine, or `None` if this
    /// is a solo layout.
    pub fn local_address(&self) -> Option<SocketAddr> {
        match self {
            Self::Solo { .. } => None,
            Self::Multihost {
                hosts,
                local_host_idx,
                ..
            } => Some(hosts[*local_host_idx].address),
        }
    }

    /// Returns the total number of worker threads in this layout.
    pub fn n_workers(&self) -> usize {
        match self {
            Self::Solo { n_workers } => *n_workers,
            Self::Multihost { hosts, .. } => hosts.iter().map(|host| host.workers.len()).sum(),
        }
    }

    pub fn is_multihost(&self) -> bool {
        matches!(self, Self::Multihost { .. })
    }

    pub fn is_solo(&self) -> bool {
        matches!(self, Self::Solo { .. })
    }

    pub fn n_hosts(&self) -> usize {
        match self {
            Layout::Solo { .. } => 1,
            Layout::Multihost { hosts, .. } => hosts.len(),
        }
    }

    pub fn local_host_idx(&self) -> usize {
        match self {
            Layout::Solo { .. } => 0,
            Layout::Multihost { local_host_idx, .. } => *local_host_idx,
        }
    }

    /// Returns the host index corresponding to the given `worker_idx`.
    pub fn worker_idx_to_host_idx(&self, worker_idx: usize) -> Option<usize> {
        self.all_hosts()
            .position(|workers| workers.contains(&worker_idx))
    }

    /// Returns [`HostInfo`] for this host when running in multihost mode,
    /// or `None` for solo pipelines.
    ///
    /// [`HostInfo`]: crate::checkpoint::HostInfo
    pub fn host_info(&self) -> Option<crate::checkpoint::HostInfo> {
        match self {
            Layout::Solo { .. } => None,
            Layout::Multihost {
                hosts,
                local_host_idx,
            } => Some(crate::checkpoint::HostInfo {
                host_idx: *local_host_idx,
                n_hosts: hosts.len(),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub enum LayoutError {
    /// The socket address passed to `new_multihost()` isn't in the list of
    /// hosts.
    NoSuchAddress(SocketAddr),
    /// The list of socket addresses passed to `new_multihost()` contains a
    /// duplicate.
    DuplicateAddress(SocketAddr),
}

impl Display for LayoutError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::NoSuchAddress(address) => write!(f, "address {address} not in list of hosts"),
            Self::DuplicateAddress(address) => {
                write!(f, "duplicate address {address} in list of hosts")
            }
        }
    }
}

impl StdError for LayoutError {}

/// Input prepared for flushing into a circuit.
///
/// An input handle offers two ways to push data into a circuit:
///
/// - Immediately, one record or one vector of records at a time.
///
/// - Preparing data in advance into [StagedBuffers] with the handle's `stage` method. Then,
///   later, calling [StagedBuffers::flush] pushes the input buffers into the circuit.
///
/// Both approaches are equally correct.  They can differ in performance, because pushing has a
/// significant cost for a large number of records.  Using [StagedBuffers] has a similar cost,
/// but it incurs it in the call to `stage` rather than in [StagedBuffers::flush].  This means
/// that, if the code driving the circuit can buffer data ahead of the circuit's demand for it,
/// the cost can be hidden and data processing as a whole runs faster.
pub trait StagedBuffers: Send + Sync {
    /// Flushes the data gathered into this buffer to the circuit.
    ///
    /// Fails if the circuit cannot take the data, for example because the engine converts the
    /// buffered records only now and a record does not convert.
    fn flush(&mut self) -> anyhow::Result<()>;
}

/// Most accumulators (created with dyn_accumulate()) are always enabled.
/// One special case is when the accumulator is used as part of an output handle
/// to collect updates to the output stream within a transaction. In this case,
/// if there is no output connector attached to the stream, there is no need to
/// store the updates (which during backfill can amount to storing a complete copy
/// of the table or view).
///
/// This flag enables this optimization by keeping track of the number of consumers
/// of the accumulator's output. It is equal to the number of attached output connectors
/// plus the number of times the same accumulator was instantiated as part of a regular
/// (non-output) operator with dyn_accumulate().
#[derive(Clone, Debug, Default)]
pub struct EnableCount(Arc<AtomicUsize>);

impl EnableCount {
    /// Creates a new `EnableCount` that is initially disabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if this `EnableCount` is enabled.
    ///
    /// This and [EnableCount::enable] use `SeqCst`, so that a caller that
    /// enables and then reads another `SeqCst` atomic, such as a transaction
    /// number, cannot miss a concurrent update of that atomic that precedes
    /// a call to this function.
    pub fn is_enabled(&self) -> bool {
        self.0.load(Ordering::SeqCst) > 0
    }

    /// Enable the accumulator for this output stream.
    ///
    /// This may be paired with a later call to [EnableCount::disable], if the
    /// stream should eventually be disabled.
    pub fn enable(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    /// Disable the accumulator for this output stream.
    ///
    /// This must be paired with a prior call to [EnableCount::enable].
    pub fn disable(&self) {
        let old = self.0.fetch_sub(1, Ordering::AcqRel);
        assert!(old > 0);
    }
}

/// What a circuit restored from a checkpoint must rebuild, because its program changed since
/// the checkpoint.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct BootstrapInfo {
    /// The input tables whose state the circuit rebuilds. Their connectors start over instead of
    /// resuming from their checkpointed offsets.
    pub backfill_tables: BTreeSet<SqlIdentifier>,

    /// The persistent ids of all the operators that the circuit rebuilds.
    pub backfill_persistent_ids: BTreeSet<String>,
}

/// How a concurrent bootstrap started.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConcurrentRestoreOutcome {
    /// The checkpoint matches the circuit: there is nothing to bootstrap, and the circuit is
    /// fully active.
    UpToDate,
    /// The circuit serves its unchanged part while a separate bootstrap circuit rebuilds the
    /// rest.
    Concurrent(BootstrapInfo),
    /// The rebuilt part cannot run concurrently, so the circuit fell back to a bootstrap that
    /// stops it until the bootstrap finishes.
    FellBack {
        /// Why the concurrent bootstrap was refused.
        reason: String,
        /// What the fallback bootstrap rebuilds.
        info: Option<BootstrapInfo>,
    },
}
