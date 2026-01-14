use crate::{
    Error, Runtime,
    algebra::F64,
    circuit::{
        GlobalNodeId, NodeId, balancer_balance_tax, balancer_key_distribution_refresh_threshold,
        balancer_min_absolute_improvement_threshold, balancer_min_relative_improvement_threshold,
        circuit_builder::{CircuitBase, MetadataExchange},
    },
    operator::dynamic::balance::{
        FlushState, MaxSat,
        accumulate_trace_balanced::{KeyDistribution, RebalancingExchangeSenderExchangeMetadata},
        maxsat::{Cost, HardConstraint, VariableIndex},
    },
    utils::{components, indent},
};
use itertools::Itertools as _;
use num::abs;
use petgraph::{
    algo::kosaraju_scc,
    graph::{DiGraph, UnGraph},
};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    fmt::{Display, Error as FmtError, Formatter},
    rc::Rc,
};
use tracing::{debug, info};

pub const MIN_RELATIVE_IMPROVEMENT_THRESHOLD: f64 = 1.2;
pub const MIN_ABSOLUTE_IMPROVEMENT_THRESHOLD: u64 = 10_000;
pub const BALANCE_TAX: f64 = 1.1;
pub const KEY_DISTRIBUTION_REFRESH_THRESHOLD: f64 = 0.1;

#[derive(Debug, Clone, Serialize)]
pub enum BalancerError {
    /// We currently only support balancing top-level nodes.
    NonTopLevelNode(GlobalNodeId),
    /// Trying to set a hint for a stream that is not registered with the balancer.
    NotRegisteredWithBalancer(NodeId),
    /// Hint cannot be enforced in the current state.
    InvalidPolicyHint(PartitioningPolicy, String),
    /// No solution found for .
    NoSolution,
}

impl Display for BalancerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::NonTopLevelNode(node) => write!(f, "node {node} is not a top level node"),
            Self::NotRegisteredWithBalancer(node) => {
                write!(f, "node {node} is not registered with the balancer")
            }
            // TODO: this is the only error that should normally propagate to the user.
            // (others are Feldera bugs). We may need a better error description here.
            Self::InvalidPolicyHint(policy, message) => {
                write!(
                    f,
                    "policy hint {policy:?} cannot be enforced in the current state: {message}"
                )
            }
            Self::NoSolution => f.write_str("no solution found for balancing constraints"),
        }
    }
}

/// A MaxSat constraint that enforces that the two streams are partitioned
/// using one of the following compatible policies (if `is_left_join` is false):
///
/// ```text
/// ┌─────────---──┬────────---------───┐
/// │    Left      │      Right         │
/// ├────────---───┼─────---------──────┤
/// │ Shard        │ Shard or Broadcast │
/// ├────────---───┼────---------───────┤
/// │ Broadcast    │ Shard or Balance   │
/// ├────────---───┼────---------───────┤
/// │ Balance      │ Broadcast          │
/// └────────---───┴────---------───────┘
/// ```
///
/// or, if `is_left_join` is true:
///
/// ```text
/// ┌─────────---──┬────────---------───┐
/// │    Left      │      Right         │
/// ├────────---───┼─────---------──────┤
/// │ Shard        │ Shard or Broadcast │
/// ├────────---───┼────---------───────┤
/// │ Balance      │ Broadcast          │
/// └────────---───┴────---------───────┘
/// ```

#[derive(Debug, Clone)]
pub struct JoinConstraint {
    v1: VariableIndex,
    v2: VariableIndex,
    is_left_join: bool,
}

impl JoinConstraint {
    pub fn create_join_constraint(
        maxsat: &mut MaxSat,
        v1: VariableIndex,
        v2: VariableIndex,
        is_left_join: bool,
    ) {
        let constraint = Self::new(v1, v2, is_left_join);
        maxsat.add_constraint(constraint);
    }

    fn new(v1: VariableIndex, v2: VariableIndex, is_left_join: bool) -> Self {
        Self {
            v1,
            v2,
            is_left_join,
        }
    }

    // Prune the domain of i2 given the current domain of i1.
    fn propagate_inner(
        &self,
        maxsat: &mut MaxSat,
        i1: VariableIndex,
        i2: VariableIndex,
        affected_variables: &mut BTreeSet<VariableIndex>,
    ) {
        let v1 = &maxsat.variables()[i1.0].domain;
        //println!("current variables: {:?}", maxsat.variables());

        // If the left domain doesn't contain Shard or Broadcast, then the right domain
        // cannot contain Shard.
        let remove_shard = !v1.contains_key(&PartitioningPolicy::Shard.into())
            && !v1.contains_key(&PartitioningPolicy::Broadcast.into());

        // If the left domain doesn't contain Shard or Balance, then the right domain
        // cannot contain Broadcast.
        let remove_broadcast = !v1.contains_key(&PartitioningPolicy::Shard.into())
            && !v1.contains_key(&PartitioningPolicy::Balance.into());

        // If the left domain doesn't contain Broadcast, then the right domain
        // cannot contain Balance.
        let remove_balance = !v1.contains_key(&PartitioningPolicy::Broadcast.into());

        let mut v2_modified = false;

        if remove_shard {
            // println!("removing SHARD from {}", maxsat.variables()[i2.0].name);
            v2_modified |= maxsat.remove_variable_assignment(i2, PartitioningPolicy::Shard.into());
        }
        if remove_broadcast {
            // println!("removing BROADCAST from {}", maxsat.variables()[i2.0].name);
            v2_modified |=
                maxsat.remove_variable_assignment(i2, PartitioningPolicy::Broadcast.into());
        }
        if remove_balance {
            // println!("removing BALANCE from {}", maxsat.variables()[i2.0].name);
            v2_modified |=
                maxsat.remove_variable_assignment(i2, PartitioningPolicy::Balance.into());
        }

        if v2_modified {
            affected_variables.insert(i2);
        }
    }
}

impl HardConstraint for JoinConstraint {
    fn propagate(&self, maxsat: &mut MaxSat, affected_variables: &mut BTreeSet<VariableIndex>) {
        if self.is_left_join {
            //let v1 = &mut all_variables[self.v1.0].domain;
            //if v1.remove(&PartitioningPolicy::Broadcast.into()).is_some() {
            if maxsat.remove_variable_assignment(self.v1, PartitioningPolicy::Broadcast.into()) {
                affected_variables.insert(self.v1);
            }
        }
        self.propagate_inner(maxsat, self.v1, self.v2, affected_variables);
        self.propagate_inner(maxsat, self.v2, self.v1, affected_variables);
    }

    fn variables(&self) -> Vec<VariableIndex> {
        vec![self.v1, self.v2]
    }
}

/// Partitioning policies supported by the balancer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum PartitioningPolicy {
    /// Shard the stream based on the hash of the key.
    Shard = 0,
    /// Broadcast the stream to all workers.
    Broadcast = 1,
    /// Split the stream into near-equal-sized partitions across all workers
    /// using the hash of the key-value pair.
    Balance = 2,
}

impl Display for PartitioningPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::Shard => write!(f, "shard"),
            Self::Broadcast => write!(f, "broadcast"),
            Self::Balance => write!(f, "balance"),
        }
    }
}

impl TryFrom<u8> for PartitioningPolicy {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(PartitioningPolicy::Shard),
            1 => Ok(PartitioningPolicy::Broadcast),
            2 => Ok(PartitioningPolicy::Balance),
            _ => Err(()),
        }
    }
}
impl From<PartitioningPolicy> for u8 {
    fn from(value: PartitioningPolicy) -> Self {
        value as u8
    }
}

/// A strongly connected component of the join graph.
///
/// There is a path consisting of joins between any two streams in the cluster.
/// Streams in the cluster must be assigned partitioning policies in coordination.
#[derive(Default, Debug)]
struct Cluster {
    /// Map from input stream to the layer index it belongs to.
    streams: BTreeMap<NodeId, usize>,

    /// Maps the output node id of a join operator, to its input streams.
    /// The bool indicates whether the join is a left join.
    // TODO: I don't thin the key in this map serves any purpose. This could just be a vector of values.
    joins: BTreeMap<NodeId, (NodeId, NodeId, bool)>,

    /// Partitions the cluster into layers such that nodes within a layer can be flushed together.
    ///
    /// This is used to postpone flushing a stream until all other streams in the same layer have
    /// received all inputs for the current transaction and the balancer is able to establish an
    /// optimal partitioning policy for the layer.
    ///
    /// Multiple layers are needed if there are dependencies between streams in the cluster, e.g.,:
    /// ```text
    /// s3 = join(s1, s2)
    /// s4 = join(s1, s3)
    /// ```
    ///
    /// Here s1 and s2 don't have any dependencies within the cluster and
    /// belong to layer 0. s3 depends on s1 and belongs to layer 1.
    layers: BTreeMap<usize, Vec<NodeId>>,

    /// Partitioning policies for each stream selected at the start of the current step.
    solution: BTreeMap<NodeId, PartitioningPolicy>,

    /// The key distribution of the cluster at the time `solution` was computed.
    ///
    /// Used to decide whether it's time to check for a better solution.
    last_distribution: BTreeMap<NodeId, KeyDistribution>,

    /// Compute a new solution for the cluster before the next step. Set either after a hint has changed
    /// or when the circuit was just started or restored from a checkpoint.
    refresh_requested: bool,
}

/// Hints allow the user to alter the behavior of the balancer.
#[derive(Debug, Clone)]
pub enum BalancerHint {
    /// Apply the specified policy to the stream instead of selecting one automatically.
    /// `None` clears any previously set hint.
    Policy(Option<PartitioningPolicy>),

    /// Choose a policy for the stream as if its total size, i.e., the number of records
    /// in the stream, is equal to the specified value.
    /// Setting size hint to `None` tells the balancer to use the actual size of the stream
    /// measured at runtime.
    Size(Option<usize>),

    /// Choose a policy for the stream as if its skew, i.e., the ratio of the largest
    /// to the average partition size, is equal to the specified value.
    /// Setting skew hint to `None` tells the balancer to use the actual skew of the stream
    /// measured at runtime.
    Skew(Option<F64>),
}

/// Currently configured hints for a stream.
#[derive(Debug, Default, Clone)]
pub struct BalancerHints {
    pub policy_hint: Option<PartitioningPolicy>,
    pub size_hint: Option<usize>,
    pub skew_hint: Option<F64>,
}

#[derive(Default, Debug)]
struct BalancerInner {
    /// All integrals managed by the balancer:
    /// origin node id -> (exchange sender node id, hints)
    integrals: BTreeMap<NodeId, (NodeId, Option<String>, BalancerHints)>,

    /// The most recent metadata for each stream.
    metadata: BTreeMap<NodeId, Vec<Option<RebalancingExchangeSenderExchangeMetadata>>>,

    /// The most recent key distribution for each stream extracted from `self.metadata`.
    key_distribution: BTreeMap<NodeId, KeyDistribution>,

    /// All joins managed by the balancer:
    /// join node id -> (left input node id, right input node id, is left join)
    joins: BTreeMap<NodeId, (NodeId, NodeId, bool)>,

    /// Connected components of the join graph:
    clusters: Vec<Cluster>,

    /// Map from input stream to the cluster index it belongs to.
    stream_to_cluster: BTreeMap<NodeId, usize>,

    /// Circuit's metadata exchange used to collect metadata from RebalancingExchangeSender instances.
    metadata_exchange: MetadataExchange,

    /// True between start_transaction and transaction_committed.
    transaction_in_progress: bool,
}

impl BalancerInner {
    fn new(metadata_exchange: &MetadataExchange) -> Self {
        Self {
            integrals: BTreeMap::new(),
            metadata: BTreeMap::new(),
            key_distribution: BTreeMap::new(),
            joins: BTreeMap::new(),
            clusters: Vec::new(),
            stream_to_cluster: BTreeMap::new(),
            metadata_exchange: metadata_exchange.clone(),
            transaction_in_progress: false,
        }
    }

    fn display_graph(&self) -> String {
        let integrals = self
            .integrals
            .iter()
            .map(|(stream, (exchange_sender, persistent_id, _))| {
                format!(
                    "{stream} ({}balancer:{exchange_sender})",
                    persistent_id
                        .as_ref()
                        .map(|id| format!("pid:{id}, "))
                        .unwrap_or_default()
                )
            })
            .join(", ");
        let joins = self
            .joins
            .values()
            .map(|(left, right, is_left_join)| {
                format!(
                    "{}join({left},{right})",
                    if *is_left_join { "left_" } else { "" }
                )
            })
            .join(", ");

        format!("Streams: {integrals}\nJoins: {joins}")
    }

    fn display_clusters(&self) -> String {
        (0..self.clusters.len())
            .map(|index| {
                format!(
                    "Cluster {index}:\n{}",
                    indent(&self.display_cluster(index), 2)
                )
            })
            .join("\n")
    }

    fn display_cluster(&self, cluster_index: usize) -> String {
        let cluster = &self.clusters[cluster_index];
        let integrals = cluster
            .streams
            .keys()
            .map(|stream| {
                let policy = cluster
                    .solution
                    .get(stream)
                    .map(|policy| format!(": <{policy:?}>"))
                    .unwrap_or_default();
                let distribution = self
                    .key_distribution
                    .get(stream)
                    .map(|distribution| format!(" distribution: {distribution}"))
                    .unwrap_or_default();
                //let flushed = self.flushed_status_for_stream(*stream);
                format!("{stream}{policy}{distribution}")
            })
            .join("\n");
        let joins = cluster
            .joins
            .values()
            .map(|(left, right, is_left_join)| {
                format!(
                    "{}join({left},{right})",
                    if *is_left_join { "left_" } else { "" }
                )
            })
            .join(", ");
        let layers = cluster
            .layers
            .iter()
            .map(|(layer, streams)| {
                format!(
                    "Layer {layer}: [{}]",
                    streams.iter().map(|stream| stream.to_string()).join(", ")
                )
            })
            .join(", ");
        format!(
            "Streams:\n{}\nJoins: {joins}\nLayers: {layers}",
            indent(&integrals, 2)
        )
    }

    fn display(&self) -> String {
        format!(
            "Join graph:\n{}\nClusters:\n{}",
            indent(&self.display_graph(), 2),
            indent(&self.display_clusters(), 2)
        )
    }

    /// Cache metadata for each stream at the end of each step, so that it can be accessed without re-parsing it,
    /// e.g., in ready_to_commit.
    fn parse_metadata(&mut self) {
        self.metadata.clear();

        for (stream, (exchange_sender, _persistent_id, _)) in self.integrals.iter() {
            let exchange_sender_metadata = self
                .metadata_exchange
                .get_global_operator_metadata_typed::<RebalancingExchangeSenderExchangeMetadata>(
                *exchange_sender,
            );
            self.key_distribution
                .insert(*stream, Self::key_distribution(&exchange_sender_metadata));
            self.metadata.insert(*stream, exchange_sender_metadata);
        }
    }

    /// Check if it's time to recompute the solution for a cluster.
    ///
    /// Returns true if either
    /// 1. cluster.refresh_requested is true, or
    /// 2. the key distribution of at least one stream in the cluster has changed by more than
    ///    `balancer_key_distribution_refresh_threshold`.
    fn cluster_needs_refresh(&self, cluster_index: usize) -> bool {
        let cluster = &self.clusters[cluster_index];
        if cluster.refresh_requested {
            return true;
        }
        cluster.streams.keys().any(|stream| {
            let Some(current_distribution) = self.key_distribution.get(stream) else {
                return true;
            };
            let Some(last_distribution) = cluster.last_distribution.get(stream) else {
                return true;
            };

            std::iter::zip(&current_distribution.sizes, &last_distribution.sizes).any(
                |(current, last)| {
                    if *last == 0 {
                        // Avoid division by zero: if last is 0, check if current is non-zero
                        *current != 0
                    } else {
                        // Check if current is more than 10% different from last
                        let diff = (*current as f64 - *last as f64).abs();
                        let percent_diff = diff / (*last as f64);
                        percent_diff > balancer_key_distribution_refresh_threshold()
                    }
                },
            )
        })
    }

    /// Compute an "optimal" policy for each stream in the cluster.
    ///
    /// This function does not update cluster.solution. It can therefore be used to validate that
    /// the cluster is in a consistent state without actually changing the solution.
    fn solve_cluster(
        &self,
        cluster_index: usize,
        ignore_fixed_policies: bool,
    ) -> Result<BTreeMap<NodeId, PartitioningPolicy>, BalancerError> {
        let mut domains = BTreeMap::new();
        let mut current_effective_policy = BTreeMap::new();

        // Create a MaxSat variable for each stream in the cluster.
        for stream in self.clusters[cluster_index].streams.clone().keys() {
            let (_exchange_sender, _persistent_id, hints) = self.integrals.get(stream).unwrap();

            let exchange_sender_metadata: Vec<Option<RebalancingExchangeSenderExchangeMetadata>> =
                self.metadata.get(stream).cloned().unwrap_or_default();

            if let Some(metadata) = exchange_sender_metadata
                .first()
                .and_then(|metadata| metadata.as_ref())
            {
                current_effective_policy.insert(*stream, metadata.current_policy);
            }

            let fixed_policy = self.get_fixed_policy(&exchange_sender_metadata, hints)?;

            // println!(
            //     "{} fixed_policy for {stream} (hints: {hints:?}): {:?}",
            //     Runtime::worker_index(),
            //     fixed_policy,
            // );

            let mut domain = self.compute_domain(&exchange_sender_metadata, hints);
            if let Some(fixed_policy) = &fixed_policy
                && !ignore_fixed_policies
            {
                domain.retain(|policy, _| *policy == *fixed_policy);
            }

            domains.insert(*stream, domain);
        }

        let solution = self.solve_cluster_with_domains(cluster_index, &domains)?;

        // Solution didn't change -- return it.
        if self.clusters[cluster_index].solution == solution {
            return Ok(solution);
        };

        // if Runtime::worker_index() == 0 {
        //     println!(
        //         "found new solution: {:?}, current solution: {:?}",
        //         solution, self.clusters[cluster_index].solution
        //     );
        // }

        // Check that the new solution is significantly better than the current solution.

        // Is the current solution still valid?
        if let Some(current_solution_cost) =
            self.validate_solution(&current_effective_policy, &domains)
        {
            let new_solution_cost = self.validate_solution(&solution, &domains).unwrap();

            if new_solution_cost.0 as f64 * balancer_min_relative_improvement_threshold()
                < current_solution_cost.0 as f64
                && new_solution_cost + Cost(balancer_min_absolute_improvement_threshold())
                    < current_solution_cost
            {
                return Ok(solution);
            } else {
                return Ok(current_effective_policy);
            }
        }

        // println!(
        //     "{} no current solution, returning new solution: {:?}",
        //     Runtime::worker_index(),
        //     solution
        // );
        Ok(solution)
    }

    /// Solve the cluster with pre-computed variable domains.
    fn solve_cluster_with_domains(
        &self,
        cluster_index: usize,
        domains: &BTreeMap<NodeId, BTreeMap<PartitioningPolicy, Cost>>,
    ) -> Result<BTreeMap<NodeId, PartitioningPolicy>, BalancerError> {
        //let cluster = &mut self.clusters[cluster_index];

        let mut maxsat = MaxSat::new();
        let mut variable_indexes = BTreeMap::new();

        // Create a MaxSat variable for each stream in the cluster.
        for stream in self.clusters[cluster_index].streams.clone().keys() {
            let domain = domains.get(stream).unwrap().clone();

            let variable_index = maxsat.add_variable(&stream.to_string(), &domain);
            variable_indexes.insert(*stream, variable_index);
        }

        for (_join, (left, right, is_left_join)) in self.clusters[cluster_index].joins.iter() {
            maxsat.add_constraint(JoinConstraint::new(
                variable_indexes[left],
                variable_indexes[right],
                *is_left_join,
            ));
        }

        let solution = maxsat.solve()?;
        let solution: BTreeMap<NodeId, PartitioningPolicy> = variable_indexes
            .iter()
            .map(|(stream, variable_index)| {
                (
                    *stream,
                    PartitioningPolicy::try_from(*solution.get(variable_index).unwrap()).unwrap(),
                )
            })
            .collect();

        // if Runtime::worker_index() == 0 {
        //     println!(
        //         "found new solution: {:?}, current solution: {:?}",
        //         solution, self.clusters[cluster_index].solution
        //     );
        // }

        Ok(solution)
    }

    fn validate_solution(
        &self,
        solution: &BTreeMap<NodeId, PartitioningPolicy>,
        domains: &BTreeMap<NodeId, BTreeMap<PartitioningPolicy, Cost>>,
    ) -> Option<Cost> {
        let mut total_cost = Cost(0);
        for (variable, domain) in domains.iter() {
            let policy = solution.get(variable)?;
            let cost = domain.get(policy)?;
            total_cost += *cost;
        }
        Some(total_cost)
    }

    /// Solve all clusters and update the solution for each cluster.
    fn solve_all_clusters(
        &mut self,
    ) -> Result<BTreeMap<NodeId, PartitioningPolicy>, BalancerError> {
        let num_clusters = self.clusters.len();
        let mut solutions = BTreeMap::new();

        for i in 0..num_clusters {
            // println!("{} solving cluster {i}", Runtime::worker_index());
            //let cluster = &mut self.clusters[cluster_index];
            if !self.cluster_needs_refresh(i) {
                solutions.extend(self.clusters[i].solution.clone());
                continue;
            }

            let cluster_solution = self.solve_cluster(i, false)?;
            if Runtime::worker_index() == 0 {
                if cluster_solution != self.clusters[i].solution {
                    info!(
                        "Cluster {i} solution changed: {:?} -> {:?} (cluster: {})",
                        &self.clusters[i].solution,
                        cluster_solution,
                        self.display_cluster(i)
                    );
                } else {
                    debug!(
                        "Cluster {i} solution UNchanged: {:?} (cluster: {})",
                        cluster_solution,
                        self.display_cluster(i)
                    );
                }
            }
            solutions.extend(cluster_solution);
            // println!("cluster {i}; solution: {:?}", self.clusters[i].solution);
        }

        self.set_policy(&solutions);

        Ok(solutions)
    }

    /// Compute the estimated costs of each policy for a stream based on its metadata and user hints.
    ///
    /// The cost of a policy is the largest amount of work across all workers needed to maintain this
    /// policy (the idea being that the slowest worker will determine the overall speed), which
    /// is in turn proportional to the amount of state maintained by the worker.
    fn compute_domain(
        &self,
        metadata: &[Option<RebalancingExchangeSenderExchangeMetadata>],
        hints: &BalancerHints,
    ) -> BTreeMap<PartitioningPolicy, Cost> {
        // Estimate the total size of the integral of the stream and the maximum size of a partition
        // (assuming key-based partitioning).
        //
        // If a size hint is specified, ignore the actual key distribution and use the hint.
        // If a skew hint is additionally specified, use it to compute the maximum size; otherwise
        // assume even distribution.
        //
        // If no size hint is specified, compute max and total sizes from the actual key distribution.
        let (total_size, max_size) = if let Some(size_hint) = hints.size_hint {
            let max_size = if let Some(skew_hint) = hints.skew_hint {
                ((size_hint as u64 / Runtime::num_workers() as u64) as f64 * skew_hint.into_inner())
                    as u64
            } else {
                size_hint as u64 / Runtime::num_workers() as u64
            };
            (size_hint as u64, max_size)
        } else {
            let key_distribution = Self::key_distribution(metadata);

            let total_size = abs(key_distribution.total_records()) as u64;
            let max_size = abs(key_distribution.sizes.iter().cloned().max().unwrap()) as u64;
            (total_size, max_size)
        };

        BTreeMap::from([
            // Shard: the cost is proportional to the maximum size of a partition.
            (PartitioningPolicy::Shard, Cost(max_size)),
            // Broadcast: every worker maintains the entire integral.
            (PartitioningPolicy::Broadcast, Cost(total_size)),
            // Balance: every worker maintains an equal share of the integral.
            // Assuming a perfectly balanced key distribution, this policy is slightly less efficient than Shard,
            // since it requires computing a more expensive hash of the entire key/value pair.
            // The `balancer_balance_tax` factor is used to discourage the use of this policy over Shard
            // if the skew is <balancer_balance_tax.
            (
                PartitioningPolicy::Balance,
                Cost(
                    ((total_size / Runtime::num_workers() as u64) as f64 * balancer_balance_tax())
                        as u64,
                ),
            ),
        ])
    }

    /// Set a hint for a stream.
    ///
    /// Fails if the hint specifies a fixed policy that cannot be enforced in the current state, i.e.,
    /// it either contradicts the set of existing hints for other streams (e.g., we refuse to join
    /// two streams both of which have Broadcast policies) or the policy cannot be enforced during the
    /// current transaction because some of the streams can no longer be rebalanced.
    ///
    /// If the function succeeds, the stream will be successfully rebalanced using the specified policy
    /// during the current transaction.
    fn set_hint(&mut self, node_id: NodeId, hint: BalancerHint) -> Result<(), BalancerError> {
        // println!(
        //     "{} set_hint({node_id}): {:?}",
        //     Runtime::worker_index(),
        //     hint
        // );
        let cluster_index = *self.stream_to_cluster.get(&node_id).unwrap();

        let (_exchange_sender, _persistent_id, hints) = self
            .integrals
            .get_mut(&node_id)
            .ok_or(BalancerError::NotRegisteredWithBalancer(node_id))?;

        let mut hints = hints.clone();

        match hint {
            BalancerHint::Policy(policy) => {
                if let Some(policy) = policy {
                    hints.policy_hint = Some(policy);
                    self.solve_cluster(cluster_index, false)?;
                } else {
                    hints.policy_hint = None;
                }
            }
            BalancerHint::Size(size) => hints.size_hint = size,
            BalancerHint::Skew(skew) => hints.skew_hint = skew,
        }
        self.integrals.get_mut(&node_id).unwrap().2 = hints.clone();
        self.clusters[cluster_index].refresh_requested = true;

        Ok(())
    }

    fn get_policy(&self) -> BTreeMap<NodeId, PartitioningPolicy> {
        let mut current_policy = BTreeMap::new();
        for cluster in self.clusters.iter() {
            for (stream, policy) in cluster.solution.iter() {
                current_policy.insert(*stream, *policy);
            }
        }
        current_policy
    }

    /// Domain with a single policy and cost 0.
    fn fixed_policy_domain(policy: PartitioningPolicy) -> BTreeMap<PartitioningPolicy, Cost> {
        BTreeMap::from([(policy, Cost(0))])
    }

    /// Complete domain with all policies, each with cost 0.
    fn default_policy_domain() -> BTreeMap<PartitioningPolicy, Cost> {
        BTreeMap::from([
            (PartitioningPolicy::Shard, Cost(0)),
            (PartitioningPolicy::Broadcast, Cost(0)),
            (PartitioningPolicy::Balance, Cost(0)),
        ])
    }

    // Get current policy for a stream.
    fn get_policy_for_stream(&self, node_id: NodeId) -> Option<PartitioningPolicy> {
        //println!("stream_to_cluster({node_id}): {:?}", self.stream_to_cluster);
        let cluster_index = *self.stream_to_cluster.get(&node_id).unwrap();
        self.clusters[cluster_index].solution.get(&node_id).cloned()
    }

    fn set_policy(&mut self, solution: &BTreeMap<NodeId, PartitioningPolicy>) {
        for (stream, policy) in solution.iter() {
            self.set_policy_for_stream(*stream, *policy);
        }
        for cluster in self.clusters.iter_mut() {
            cluster.refresh_requested = false;
        }
    }

    fn set_policy_for_stream(&mut self, stream: NodeId, policy: PartitioningPolicy) {
        let cluster_index = *self.stream_to_cluster.get(&stream).unwrap();
        self.clusters[cluster_index].solution.insert(stream, policy);
        if let Some(key_distribution) = self.key_distribution.get(&stream) {
            self.clusters[cluster_index]
                .last_distribution
                .insert(stream, key_distribution.clone());
        }
    }

    fn ready_to_commit(&self, node_id: NodeId) -> bool {
        // Find cluster and layer for node_id
        let cluster_index = *self.stream_to_cluster.get(&node_id).unwrap();

        let cluster = &self.clusters[cluster_index];
        let layer = *cluster.streams.get(&node_id).unwrap();

        // Get all nodes in this layer
        let nodes_in_layer = cluster.layers.get(&layer).unwrap();

        // Check if all workers for all nodes in this layer are in state >= FlushRequested
        for stream in nodes_in_layer {
            let Some(exchange_sender_metadata) = self.metadata.get(stream) else {
                return false;
            };

            // Check that all workers have flush_state >= FlushInProgress
            for worker_metadata in exchange_sender_metadata.iter() {
                match worker_metadata {
                    Some(metadata) => {
                        if metadata.flush_state < FlushState::FlushRequested {
                            return false;
                        }
                    }
                    None => {
                        // If a worker doesn't have metadata, it's not ready
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Check if the stream has a unique fixed policy that cannot change during the current transaction.
    ///
    /// Given metadata collected from RebalancingExchangeSender instances in all workers
    /// and current user hints, returns:
    /// * Some(policy) if the stream has a unique fixed policy that cannot change at the current step, i.e.:
    ///   - At least one of the workers has flushed the current transaction, and therefore can no longer rebalance
    ///   - Or the hint specifies a fixed policy
    /// * None if the stream doesn't have a fixed policy and can be rebalanced.
    /// * Error if the policy can no longer change during the current transaction and it contradicts the hint.
    fn get_fixed_policy(
        &self,
        metadata: &[Option<RebalancingExchangeSenderExchangeMetadata>],
        hints: &BalancerHints,
    ) -> Result<Option<PartitioningPolicy>, BalancerError> {
        // println!(
        //     "{} get_fixed_policy (metadata: {:?}) hints: {:?})",
        //     Runtime::worker_index(),
        //     metadata,
        //     hints
        // );

        // If any worker has started rebalancing its state, the policy for this stream cannot change until the next transaction.
        let any_flushed = metadata.iter().any(|metadata| {
            if let Some(metadata) = metadata {
                metadata.flush_state >= FlushState::RebalanceInProgress
            } else {
                false
            }
        });

        // If at least one worker has flushed the current transaction, then the policy cannot change until the
        // next transaction. Special case if this is the first step of a new transaction or we are between transactions
        // (i.e., when set_hint is called) and workers have not had a chance to exchange metadata yet (an alternative
        // is to introduce another round of metadata exchange after transaction commit).
        let fixed_policy = if any_flushed && self.transaction_in_progress {
            // println!("{} metadata: {:?}", Runtime::worker_index(), metadata);
            // All workers must have the same policy.
            assert!(
                metadata
                    .iter()
                    .all(|worker_metadata| worker_metadata.is_some()
                        && worker_metadata.as_ref().unwrap().current_policy
                            == metadata[0].as_ref().unwrap().current_policy)
            );
            Some(metadata[0].as_ref().unwrap().current_policy)
        } else {
            None
        };

        // println!("fixed_policy: {:?}, hints: {:?}", fixed_policy, hints);

        if fixed_policy.is_some()
            && hints.policy_hint.is_some()
            && fixed_policy != hints.policy_hint
        {
            return Err(BalancerError::InvalidPolicyHint(
                hints.policy_hint.unwrap(),
                format!(
                    "the current policy {fixed_policy:?} can no longer be changed during the current transaction"
                ),
            ));
        }

        if hints.policy_hint.is_some() {
            Ok(hints.policy_hint)
        } else {
            Ok(fixed_policy)
        }
    }

    /// Compute combined key distribution from metadata collected from
    /// RebalancingExchangeSender instances in all workers.
    fn key_distribution(
        metadata: &[Option<RebalancingExchangeSenderExchangeMetadata>],
    ) -> KeyDistribution {
        metadata.iter().fold(
            KeyDistribution::new(Runtime::num_workers()),
            |mut key_distribution, metadata| {
                if let Some(metadata) = metadata {
                    key_distribution.merge(&metadata.key_distribution);
                };
                key_distribution
            },
        )
    }

    /// Detect clusters whose state is inconsistent after restoring from checkpoint and add their
    /// and return the exchange sender node ids that need to be backfilled.
    fn invalidate_clusters_for_bootstrapping(
        &self,
        need_backfill: &BTreeSet<NodeId>,
    ) -> BTreeSet<NodeId> {
        let mut additional_need_backfill = BTreeSet::new();

        for cluster_index in 0..self.clusters.len() {
            // Find exchange senders that don't need backfill. These senders have partitioning policies set by the balancer
            // prior to the checkpoint. Check if the cluster can be solved with the same partitioning policies.
            // If not, we need to backfill the entire cluster.

            let mut domains = BTreeMap::new();
            let mut exchange_senders = BTreeSet::new();

            for stream_id in self.clusters[cluster_index].streams.keys() {
                let exchange_sender_id = self.integrals.get(stream_id).unwrap().0;
                exchange_senders.insert(exchange_sender_id);
                if !need_backfill.contains(&exchange_sender_id) {
                    // The operator reported its policy as part of the restore operation.
                    let exchange_sender_metadata = self
                        .metadata_exchange
                        .get_local_operator_metadata_typed::<RebalancingExchangeSenderExchangeMetadata>(
                        exchange_sender_id,
                    ).unwrap();

                    let policy = exchange_sender_metadata.current_policy;
                    domains.insert(*stream_id, Self::fixed_policy_domain(policy));
                } else {
                    domains.insert(*stream_id, Self::default_policy_domain());
                }
            }

            if self
                .solve_cluster_with_domains(cluster_index, &domains)
                .is_err()
            {
                info!(
                    "Cluster {cluster_index} has inconsistent partitioning policies after restoring from checkpoint; the state of the cluster will be discarded and backfilled"
                );
                additional_need_backfill.extend(exchange_senders);
            }
        }

        // println!("additional_need_backfill: {:?}", additional_need_backfill);
        additional_need_backfill
    }
}

/// The Balancer dynamically picks partitioning policies for all adaptive joins
/// in the circuit based on key distributions and user hints.
///
/// * Adaptive join operators register themselves with the balancer.
/// * The balancer partitions the join graph (vertices are streams, edges are joins) into connected components
///   at circuit construction time. Each connected component is optimized independently.
/// * At the start of each step, the balancer solves the optimization problem for each connected component
///   to determine an optimal partitioning policy for each stream in the component.
/// * The balancer works in tandem with the RebalancingExchangeSender operator, which keeps track of the
///   key distribution of the stream and enforces the policy selected by the balancer by redistributing the
///   state of the integral according to the latest policy.
#[derive(Debug, Clone)]
pub struct Balancer {
    inner: Rc<RefCell<BalancerInner>>,
}

impl Balancer {
    pub fn new(metadata_exchange: &MetadataExchange) -> Self {
        Self {
            inner: Rc::new(RefCell::new(BalancerInner::new(metadata_exchange))),
        }
    }

    /// Invoked during circuit construction to register a stream whose partitioning policy
    /// will be managed by the balancer.
    ///
    /// # Arguments
    ///
    /// * `stream` - The origin node id of the stream.
    /// * `exchange_sender` - Node id of the exchange sender operator that produces the stream.
    pub fn register_integral(
        &self,
        stream: NodeId,
        exchange_sender: NodeId,
        persistent_id: Option<String>,
    ) {
        self.inner.borrow_mut().integrals.insert(
            stream,
            (exchange_sender, persistent_id, BalancerHints::default()),
        );
    }

    /// Invoked during circuit construction to register a join between `left` and `right` streams.
    ///
    /// The balancer enforces the hard constraint that the two sides of the join are partitioned
    /// using one of the following compatible policies:
    /// ┌─────────---──┬────────---------───┐
    /// │    Left      │      Right         │
    /// ├────────---───┼─────---------──────┤
    /// │ Shard        │ Shard or Broadcast │
    /// ├────────---───┼────---------───────┤
    /// │ Broadcast    │ Shard or Balance   │
    /// ├────────---───┼────---------───────┤
    /// │ Balance      │ Broadcast          │
    /// └────────---───┴────---------───────┘
    ///
    pub fn register_join(&self, join: NodeId, left: NodeId, right: NodeId) {
        self.inner
            .borrow_mut()
            .joins
            .insert(join, (left, right, false));
    }

    /// Invoked during circuit construction to register a left join between `left` and `right` streams.
    ///
    /// The balancer enforces the hard constraint that the two sides of the join are partitioned
    /// using one of the following compatible policies:
    /// ┌─────────---──┬────────---------───┐
    /// │    Left      │      Right         │
    /// ├────────---───┼─────---------──────┤
    /// │ Shard        │ Shard or Broadcast │
    /// ├────────---───┼────---------───────┤
    /// │ Balance      │ Broadcast          │
    /// └────────---───┴────---------───────┘
    ///
    pub fn register_left_join(&self, join: NodeId, left: NodeId, right: NodeId) {
        self.inner
            .borrow_mut()
            .joins
            .insert(join, (left, right, true));
    }

    pub fn num_clusters(&self) -> usize {
        self.inner.borrow().clusters.len()
    }

    pub fn invalidate_clusters_for_bootstrapping(
        &self,
        need_backfill: &BTreeSet<NodeId>,
    ) -> BTreeSet<NodeId> {
        self.inner
            .borrow()
            .invalidate_clusters_for_bootstrapping(need_backfill)
    }

    /// Invoked after the circuit has been constructed and before it starts running.
    pub fn prepare(&self, circuit: &dyn CircuitBase) {
        // Compute connected components of the join graph.
        self.compute_clusters(circuit);
        if Runtime::worker_index() == 0 {
            info!("Join balancer state:\n{}", indent(&self.display(), 2));
        }
    }

    // Invoked at the start of each transaction.
    pub fn start_transaction(&self) {
        // if Runtime::worker_index() == 0 {
        //     println!("Balancer::start_transaction");
        // }
    }

    pub fn transaction_committed(&self) {
        self.inner.borrow_mut().transaction_in_progress = false;
    }

    /// Check if the balancer has full information about key distribution of streams in the same layer as node_id
    /// for the current transaction.
    ///
    /// When true, the partitioning policy proposed by the balancer is the best it can do until the next transaction.
    ///
    /// This is just a hint: an individual operator may choose to start flushing its state for the current transaction
    /// without waiting for this to be true. In this case, it partitioning policy can no longer change until the next
    /// transaction, which also reduces the space of possible policies for other streams in the same cluster.
    pub fn ready_to_commit(&self, node_id: NodeId) -> bool {
        self.inner.borrow().ready_to_commit(node_id)
    }

    // Invoked at the start of each step. Updates costs and solves the optimization problem.
    pub fn start_step(&self) {
        // if Runtime::worker_index() == 0 {
        //     println!("Balancer::start_step");
        // }

        // This shouldn't fail since all hints are validated when they are installed.
        // and otherwise the cluster should always be in a state where at least the currently active policy is valid,
        // so the MaxSat instance should always have a solution.
        self.inner.borrow_mut().solve_all_clusters().unwrap();
        // println!("{} solutions: {:?}", Runtime::worker_index(), solutions);

        self.inner.borrow_mut().transaction_in_progress = true;
    }

    pub fn update_metadata(&self) {
        self.inner.borrow_mut().parse_metadata();
    }

    /// Return the current policy selected for the stream by the balancer.
    pub fn get_optimal_policy(&self, node_id: NodeId) -> Option<PartitioningPolicy> {
        self.inner.borrow().get_policy_for_stream(node_id)
    }

    /// Partition nodes in `cluster` into layers such that nodes within a layer only depend
    /// on nodes in earlier layers.
    ///
    /// Layer 0 consists of all nodes that don't depend on any other nodes in the cluster.
    /// Layer 1 consists of all nodes that depend on nodes in layer 0.
    /// Layer 2 consists of all nodes that depend on nodes in layers 0 and 1, etc.
    ///
    /// Returns a map from layer index to the nodes in that layer and a map from node id to the layer index it belongs to.
    fn compute_layers(
        &self,
        dependencies: &BTreeMap<NodeId, BTreeSet<NodeId>>,
        cluster: &[NodeId],
    ) -> (BTreeMap<usize, Vec<NodeId>>, BTreeMap<NodeId, usize>) {
        let mut layers: BTreeMap<usize, Vec<NodeId>> = BTreeMap::new();
        let mut layer_index = 0;
        let mut remaining_nodes = cluster.iter().cloned().collect::<BTreeSet<_>>();
        let mut node_to_layer = BTreeMap::new();

        while !remaining_nodes.is_empty() {
            let mut layer = Vec::new();
            let mut new_remaining_nodes = BTreeSet::new();

            // Find all cluster nodes without dependencies among remaining nodes; add them to layer_index and remove them from cluster.
            for node in remaining_nodes.iter() {
                if dependencies
                    .get(node)
                    .unwrap()
                    .iter()
                    .any(|dep| remaining_nodes.contains(dep))
                {
                    new_remaining_nodes.insert(*node);
                } else {
                    layer.push(*node);
                    node_to_layer.insert(*node, layer_index);
                }
            }

            remaining_nodes = new_remaining_nodes;

            layers.insert(layer_index, layer);
            layer_index += 1;
        }

        (layers, node_to_layer)
    }

    fn compute_clusters(&self, circuit: &dyn CircuitBase) {
        // println!(
        //     "{} compute_clusters: integrals: {:?}, joins: {:?}",
        //     Runtime::worker_index(),
        //     self.inner.borrow().integrals,
        //     self.inner.borrow().joins
        // );

        // Build an undirected graph with vertices being streams and edges being joins.
        let mut inner = self.inner.borrow_mut();

        // Map from NodeId to a vertex index in the graph.
        let mut node_to_index = BTreeMap::new();

        let mut join_graph = UnGraph::<NodeId, NodeId>::new_undirected();

        for node_id in inner.integrals.keys() {
            let node_index = join_graph.add_node(*node_id);
            node_to_index.insert(*node_id, node_index);
        }

        for (join, (left, right, _is_left_join)) in inner.joins.iter() {
            let left_index = *node_to_index.get(left).unwrap();
            let right_index = *node_to_index.get(right).unwrap();
            join_graph.add_edge(left_index, right_index, *join);
        }

        // Compute connected components of the join graph.
        let clusters = components(&join_graph);
        let clusters = clusters
            .into_iter()
            .map(|(node_indices, edge_indices)| {
                let nodes = node_indices
                    .iter()
                    .map(|node| *join_graph.node_weight(*node).unwrap())
                    .collect::<Vec<_>>();
                let edges = edge_indices
                    .into_iter()
                    .map(|edge| {
                        let join_id = *join_graph.edge_weight(edge).unwrap();
                        let join_descr = inner.joins.get(&join_id).unwrap();
                        (join_id, *join_descr)
                    })
                    .collect::<BTreeMap<_, _>>();
                (nodes, edges)
            })
            .collect::<Vec<_>>();

        let dependencies: BTreeMap<NodeId, BTreeSet<NodeId>> = circuit.transitive_ancestors();

        let cluster_nodes: Vec<BTreeSet<NodeId>> = clusters
            .iter()
            .map(|(nodes, _)| nodes.iter().cloned().collect())
            .collect();

        // For each cluster, compute the union of dependencies of its nodes.
        let cluster_dependencies: Vec<BTreeSet<NodeId>> = cluster_nodes
            .iter()
            .map(|cluster_nodes| {
                cluster_nodes
                    .iter()
                    .flat_map(|node| dependencies.get(node).into_iter().flatten())
                    .cloned()
                    .collect()
            })
            .collect();

        // Add an edge between two clusters in a DiGraph if there is a dependency between them.
        let mut cluster_graph = DiGraph::<usize, ()>::new();
        let cluster_indices: Vec<_> = (0..cluster_nodes.len())
            .map(|i| cluster_graph.add_node(i))
            .collect();

        for (i, deps) in cluster_dependencies.iter().enumerate() {
            for (j, other_nodes) in cluster_nodes.iter().enumerate() {
                if i != j && deps.intersection(other_nodes).next().is_some() {
                    cluster_graph.add_edge(cluster_indices[i], cluster_indices[j], ());
                }
            }
        }

        // Compute strongly connected components of the cluster dependency graph.
        let sccs = kosaraju_scc(&cluster_graph);

        // Merge all clusters in each component
        let mut merged_clusters: Vec<(Vec<NodeId>, BTreeMap<NodeId, (NodeId, NodeId, bool)>)> =
            Vec::new();

        for scc in sccs {
            if scc.is_empty() {
                continue;
            }

            let mut merged_nodes = Vec::new();
            let mut merged_edges: BTreeMap<NodeId, (NodeId, NodeId, bool)> = BTreeMap::new();

            for cluster_idx in scc {
                let original_cluster_idx = cluster_graph[cluster_idx];
                let (nodes, edges) = &clusters[original_cluster_idx];
                merged_nodes.extend(nodes.iter().cloned());
                merged_edges.extend(
                    edges
                        .iter()
                        .map(|(join_id, join_descr)| (*join_id, *join_descr)),
                );
            }

            merged_clusters.push((merged_nodes, merged_edges));
        }

        let clusters: Vec<Cluster> = merged_clusters
            .into_iter()
            .map(|(nodes, joins)| {
                let (layers, streams) = self.compute_layers(&dependencies, &nodes);
                Cluster {
                    streams,
                    joins,
                    layers,
                    solution: BTreeMap::new(),
                    last_distribution: BTreeMap::new(),
                    refresh_requested: true,
                }
            })
            .collect();

        for (index, cluster) in clusters.iter().enumerate() {
            for (stream, _layer) in cluster.streams.iter() {
                inner.stream_to_cluster.insert(*stream, index);
            }
        }

        // println!(
        //     "{} balancer clusters: {:?}",
        //     Runtime::worker_index(),
        //     clusters
        // );

        inner.clusters = clusters;

        // TODO:
        // Validation:
        // - Each stream is in exactly one cluster.
        // - Each join is in exactly one cluster.
        // - Each cluster has at least one stream.
        // - Each cluster has at least one join.
    }

    pub fn set_hint(&self, node_id: NodeId, hint: BalancerHint) -> Result<(), Error> {
        self.inner.borrow_mut().set_hint(node_id, hint)?;
        Ok(())
    }

    pub fn get_policy(&self) -> BTreeMap<NodeId, PartitioningPolicy> {
        self.inner.borrow().get_policy()
    }

    pub fn set_policy(&self, solution: &BTreeMap<NodeId, PartitioningPolicy>) {
        self.inner.borrow_mut().set_policy(solution);
    }

    pub fn set_policy_for_stream(&self, stream: NodeId, policy: PartitioningPolicy) {
        self.inner
            .borrow_mut()
            .set_policy_for_stream(stream, policy);
    }

    pub fn key_distribtion_for_stream_local_worker(&self, stream: NodeId) -> Option<i64> {
        self.inner
            .borrow()
            .key_distribution
            .get(&stream)
            .map(|distribution| distribution.sizes[Runtime::worker_index()])
    }

    pub fn display(&self) -> String {
        self.inner.borrow().display()
    }
}
