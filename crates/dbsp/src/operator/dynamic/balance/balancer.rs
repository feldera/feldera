use crate::{
    algebra::F64,
    circuit::{circuit_builder::MetadataExchange, GlobalNodeId, NodeId},
    operator::dynamic::balance::{
        accumulate_trace_balanced::{KeyDistribution, RebalancingExchangeSenderExchangeMetadata},
        maxsat::{Cost, HardConstraint, VariableIndex},
        MaxSat,
    },
    utils::components,
    Error, Runtime,
};
use num::abs;
use petgraph::graph::UnGraph;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    fmt::{Display, Error as FmtError, Formatter},
    rc::Rc,
};

const MIN_RELATIVE_IMPROVEMENT_THRESHOLD: f64 = 1.5;
const MIN_ABSOLUTE_IMPROVEMENT_THRESHOLD: Cost = Cost(0);

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
    streams: BTreeSet<NodeId>,
    joins: BTreeMap<NodeId, (NodeId, NodeId, bool)>,

    /// Partitioning policies for each stream selected at the start of the current step.
    solution: BTreeMap<NodeId, PartitioningPolicy>,
}

/// Hints allow the user to alter the behavior of the balancer.
#[derive(Debug, Clone)]
pub enum BalancerHint {
    /// Apply the specified policy to the stream instead of selecting one automatically.
    /// `None` clears any previously set hint.
    Policy(Option<PartitioningPolicy>),

    /// Choose a policy for the stream as if its total size, i.e., the number of records
    /// in the stream, is equal to the specified value.
    /// `None` clears any previously set hint.
    Size(Option<usize>),

    /// Choose a policy for the stream as if its skew, i.e., the ratio of the largest
    /// to the average partition size, is equal to the specified value.
    /// `None` clears any previously set hint.
    Skew(Option<F64>),
}

/// Currently configured hints for a stream.
#[derive(Debug, Default, Clone)]
pub struct BalancerHints {
    pub policy_hint: Option<PartitioningPolicy>,
    pub size_hint: Option<usize>,
    pub skew_hint: Option<F64>,
}

impl RebalancingExchangeSenderExchangeMetadata {
    /// Check if the stream has a unique fixed policy that cannot change during the current transaction.
    ///
    /// Given metadata collected from RebalancingExchangeSender instances in all workers
    /// and current user hints, returns:
    /// * Some(policy) if the stream has a unique fixed policy that cannot change at the current step, i.e.,:
    ///   - At least one of the workers has flushed the current transaction, and therefore can no longer rebalance
    ///   - Or the hint specifies a fixed policy and this policy doesn't contradict
    /// * None if the stream doesn't have a fixed policy and can be rebalanced.
    /// * Error if the policy can no longer change during the current transaction and it contradicts the hint.
    fn get_fixed_policy(
        metadata: &[Option<RebalancingExchangeSenderExchangeMetadata>],
        hints: &BalancerHints,
    ) -> Result<Option<PartitioningPolicy>, BalancerError> {
        // println!(
        //     "{} get_fixed_policy (metadata: {:?}) hints: {:?})",
        //     Runtime::worker_index(),
        //     metadata
        //         .iter()
        //         .map(|metadata| metadata.as_ref().map(|metadata| metadata.flushed))
        //         .collect::<Vec<_>>(),
        //     hints
        // );

        let any_flushed = metadata.iter().any(|metadata| {
            if let Some(metadata) = metadata {
                metadata.flushed
            } else {
                false
            }
        });

        let all_flushed = metadata.iter().all(|metadata| {
            if let Some(metadata) = metadata {
                metadata.flushed
            } else {
                false
            }
        });

        // If at least one worker has flushed the current transaction, but the transaction is not yet
        // committed, i.e., not all workers have flushed, then the policy cannot change until the
        // next transaction.
        let fixed_policy = if any_flushed && !all_flushed {
            // println!("{} metadata: {:?}", Runtime::worker_index(), metadata);
            // All workers must have the same policy.
            assert!(metadata
                .iter()
                .all(|worker_metadata| worker_metadata.is_some()
                    && worker_metadata.as_ref().unwrap().current_policy
                        == metadata[0].as_ref().unwrap().current_policy));
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
                format!("the current policy {fixed_policy:?} can no longer be changed during the current transaction"),
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
}

#[derive(Default, Debug)]
struct BalancerInner {
    /// All integrals managed by the balancer:
    /// origin node id -> (exchange sender node id, hints)
    integrals: BTreeMap<NodeId, (NodeId, BalancerHints)>,

    /// All joins managed by the balancer:
    /// join node id -> (left input node id, right input node id, is left join)
    joins: BTreeMap<NodeId, (NodeId, NodeId, bool)>,

    /// Connected components of the join graph:
    clusters: Vec<Cluster>,

    /// Map from input stream to the cluster index it belongs to.
    stream_to_cluster: BTreeMap<NodeId, usize>,

    /// Circuit's metadata exchange used to collect metadata from RebalancingExchangeSender instances.
    metadata_exchange: MetadataExchange,
}

impl BalancerInner {
    fn new(metadata_exchange: &MetadataExchange) -> Self {
        Self {
            integrals: BTreeMap::new(),
            joins: BTreeMap::new(),
            clusters: Vec::new(),
            stream_to_cluster: BTreeMap::new(),
            metadata_exchange: metadata_exchange.clone(),
        }
    }

    /// Compute an "optimal" policy for each stream in the cluster.
    fn solve_cluster(
        &self,
        cluster_index: usize,
    ) -> Result<BTreeMap<NodeId, PartitioningPolicy>, BalancerError> {
        //let cluster = &mut self.clusters[cluster_index];

        let mut maxsat = MaxSat::new();
        let mut variable_indexes = BTreeMap::new();

        let mut domains = BTreeMap::new();

        let mut current_effective_policy = BTreeMap::new();

        // Create a MaxSat variable for each stream in the cluster.
        for stream in self.clusters[cluster_index].streams.clone().iter() {
            let (exchange_sender, hints) = self.integrals.get(stream).unwrap();

            let exchange_sender_metadata: Vec<Option<RebalancingExchangeSenderExchangeMetadata>> = self
                .metadata_exchange
                .get_global_operator_metadata_typed::<RebalancingExchangeSenderExchangeMetadata>(
                    *exchange_sender,
                );

            if let Some(metadata) = &exchange_sender_metadata
                .first()
                .and_then(|metadata| metadata.as_ref())
            {
                current_effective_policy.insert(*stream, metadata.current_policy);
            }

            let fixed_policy = RebalancingExchangeSenderExchangeMetadata::get_fixed_policy(
                &exchange_sender_metadata,
                hints,
            )?;

            // println!(
            //     "{} fixed_policy for {stream} (hints: {hints:?}): {:?}",
            //     Runtime::worker_index(),
            //     fixed_policy,
            // );

            let mut domain = self.compute_domain(&exchange_sender_metadata, hints);
            if let Some(fixed_policy) = &fixed_policy {
                domain.retain(|policy, _| *policy == *fixed_policy);
            }

            let variable_index = maxsat.add_variable(&stream.to_string(), &domain);
            variable_indexes.insert(*stream, variable_index);
            domains.insert(*stream, domain);
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

            if new_solution_cost.0 as f64 * MIN_RELATIVE_IMPROVEMENT_THRESHOLD
                < current_solution_cost.0 as f64
                && new_solution_cost + MIN_ABSOLUTE_IMPROVEMENT_THRESHOLD < current_solution_cost
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

    fn solve_all_clusters(
        &mut self,
    ) -> Result<BTreeMap<NodeId, PartitioningPolicy>, BalancerError> {
        let num_clusters = self.clusters.len();
        let mut solutions = BTreeMap::new();

        for i in 0..num_clusters {
            // println!("{} solving cluster {i}", Runtime::worker_index());
            let cluster_solution = self.solve_cluster(i)?;
            solutions.extend(cluster_solution);
            // println!("cluster {i}; solution: {:?}", self.clusters[i].solution);
        }

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
            let key_distribution =
                RebalancingExchangeSenderExchangeMetadata::key_distribution(metadata);

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
            // the 1.5x factor discourages the use of this policy unless the skew is >1.5x.
            (
                PartitioningPolicy::Balance,
                Cost(((total_size / Runtime::num_workers() as u64) as f64 * 1.5) as u64),
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
    /// If the function succeeds, the stream will be successfully rebalanced using the specified policy.
    fn set_hint(&mut self, node_id: NodeId, hint: BalancerHint) -> Result<(), BalancerError> {
        // println!(
        //     "{} set_hint({node_id}): {:?}",
        //     Runtime::worker_index(),
        //     hint
        // );
        let (_exchange_sender, hints) = self
            .integrals
            .get_mut(&node_id)
            .ok_or(BalancerError::NotRegisteredWithBalancer(node_id))?;

        let mut hints = hints.clone();

        match hint {
            BalancerHint::Policy(policy) => {
                let cluster_index = *self.stream_to_cluster.get(&node_id).unwrap();

                if let Some(policy) = policy {
                    hints.policy_hint = Some(policy);
                    self.solve_cluster(cluster_index)?;
                } else {
                    hints.policy_hint = None;
                }
            }
            BalancerHint::Size(size) => hints.size_hint = size,
            BalancerHint::Skew(skew) => hints.skew_hint = skew,
        }
        self.integrals.get_mut(&node_id).unwrap().1 = hints.clone();

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
    }

    fn set_policy_for_stream(&mut self, stream: NodeId, policy: PartitioningPolicy) {
        let cluster_index = *self.stream_to_cluster.get(&stream).unwrap();
        self.clusters[cluster_index].solution.insert(stream, policy);
    }
}

/// The Balancer dynamically picks partitioning policies for all adaptive joins
/// in the circuit based on key distributions and user hints.
///
/// * Adaptive join operators register themselves with the balancer.
/// * The balancer partitions the join graph (vertices are streams, edges are joins) into connected components.
///   Each connected component is optimized independently.
/// * At the start of each step, the balancer solves the optimization problem for each connected component
///   to determine an optimal partitioning policy for each stream in the component. Both the problem
///   formulation and the solver are very simplistic, but can be refined in the future.
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
    pub fn register_integral(&self, stream: NodeId, exchange_sender: NodeId) {
        self.inner
            .borrow_mut()
            .integrals
            .insert(stream, (exchange_sender, BalancerHints::default()));
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

    /// Invoked after the circuit has been constructed and before it starts running.
    pub fn prepare(&self) {
        // Compute connected components of the join graph.
        self.compute_clusters();
    }

    // Invoked at the start of each transaction.
    pub fn start_transaction(&self) {}

    // Invoked at the start of each step. Updates costs and solves the optimization problem.
    pub fn start_step(&self) {
        // This shouldn't fail since all hints are validated when they are installed
        // and otherwise the balancer should not get stuck with invalid policies.

        // println!(
        //     "{} start_step: solving all clusters",
        //     Runtime::worker_index()
        // );

        let solutions = self.inner.borrow_mut().solve_all_clusters().unwrap();
        // println!("{} solutions: {:?}", Runtime::worker_index(), solutions);
        self.inner.borrow_mut().set_policy(&solutions);
    }

    /// Return the policy selected for the stream at the start of the step.
    pub fn get_optimal_policy(&self, node_id: NodeId) -> Option<PartitioningPolicy> {
        self.inner.borrow().get_policy_for_stream(node_id)
    }

    fn compute_clusters(&self) {
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

        let clusters: Vec<Cluster> = clusters
            .into_iter()
            .map(|(nodes, edges)| Cluster {
                streams: nodes
                    .into_iter()
                    .map(|node| *join_graph.node_weight(node).unwrap())
                    .collect(),
                joins: edges
                    .into_iter()
                    .map(|edge| {
                        let join_id = *join_graph.edge_weight(edge).unwrap();
                        let join_descr = inner.joins.get(&join_id).unwrap();
                        (join_id, *join_descr)
                    })
                    .collect(),
                solution: BTreeMap::new(),
            })
            .collect();

        for (index, cluster) in clusters.iter().enumerate() {
            for stream in cluster.streams.iter() {
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
}
