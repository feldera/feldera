use crate::{
    algebra::F64,
    circuit::{circuit_builder::MetadataExchange, GlobalNodeId, NodeId},
    operator::dynamic::balance::accumulate_trace_balanced::{
        KeyDistribution, RebalancingExchangeSenderExchangeMetadata,
    },
    utils::{
        components,
        maxsat::{Cost, HardConstraint, MaxSat, Variable, VariableIndex},
    },
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

#[derive(Debug, Clone, Serialize)]
pub enum BalancerError {
    NonTopLevelNode(GlobalNodeId),
    NotRegisteredWithBalancer(NodeId),
    InvalidPolicyHint(Policy, String),
}

impl Display for BalancerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Self::NonTopLevelNode(node) => write!(f, "node {node} is not a top level node"),
            Self::NotRegisteredWithBalancer(node) => {
                write!(f, "node {node} is not registered with the balancer")
            }
            Self::InvalidPolicyHint(policy, message) => {
                write!(f, "invalid policy hint {policy:?}: {message}")
            }
        }
    }
}

#[derive(Debug)]
pub struct JoinConstraint {
    v1: VariableIndex,
    v2: VariableIndex,
}

impl JoinConstraint {
    pub fn create_join_constraint(maxsat: &mut MaxSat, v1: VariableIndex, v2: VariableIndex) {
        let constraint = Self::new(v1, v2);
        maxsat.add_constraint(Box::new(constraint));
    }

    fn new(v1: VariableIndex, v2: VariableIndex) -> Self {
        Self { v1, v2 }
    }

    fn propagate_inner(
        &self,
        all_variables: &mut [Variable],
        i1: VariableIndex,
        i2: VariableIndex,
        affected_variables: &mut BTreeSet<VariableIndex>,
    ) {
        let v1 = &all_variables[i1.0].domain;
        //println!("{} = {:?}", all_variables[i1.0].name, v1);

        let remove_shard =
            !v1.contains_key(&Policy::Shard.into()) && !v1.contains_key(&Policy::Broadcast.into());
        let remove_broadcast = !v1.contains_key(&Policy::Broadcast.into())
            && !v1.contains_key(&Policy::Balance.into());
        let remove_balance = !v1.contains_key(&Policy::Balance.into());

        let v2 = &mut all_variables[i2.0].domain;

        let mut v2_modified = false;

        if remove_shard {
            //println!("removing SHARD from {}", all_variables[i2.0].name);
            v2_modified |= v2.remove(&Policy::Shard.into()).is_some();
        }
        if remove_broadcast {
            //println!("removing BROADCAST from {}", all_variables[i2.0].name);
            v2_modified |= v2.remove(&Policy::Broadcast.into()).is_some();
        }
        if remove_balance {
            //println!("removing BALANCE from {}", all_variables[i2.0].name);
            v2_modified |= v2.remove(&Policy::Balance.into()).is_some();
        }

        if v2_modified {
            affected_variables.insert(i2);
        }
    }
}

impl HardConstraint for JoinConstraint {
    fn propagate(
        &self,
        all_variables: &mut [Variable],
        affected_variables: &mut BTreeSet<VariableIndex>,
    ) {
        self.propagate_inner(all_variables, self.v1, self.v2, affected_variables);
        self.propagate_inner(all_variables, self.v2, self.v1, affected_variables);
    }

    fn variables(&self) -> Vec<VariableIndex> {
        vec![self.v1, self.v2, self.v2]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum Policy {
    Shard = 0,
    Broadcast = 1,
    Balance = 2,
}

impl TryFrom<u8> for Policy {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Policy::Shard),
            1 => Ok(Policy::Broadcast),
            2 => Ok(Policy::Balance),
            _ => Err(()),
        }
    }
}
impl From<Policy> for u8 {
    fn from(value: Policy) -> Self {
        value as u8
    }
}

#[derive(Default, Debug)]
struct Cluster {
    streams: BTreeSet<NodeId>,
    joins: BTreeMap<NodeId, (NodeId, NodeId)>,

    solution: Option<BTreeMap<NodeId, Policy>>,
}

#[derive(Debug, Clone)]
pub enum BalancerHint {
    Policy(Option<Policy>),
    Size(Option<usize>),
    Skew(Option<F64>),
}

#[derive(Debug, Default, Clone)]
pub struct BalancerHints {
    pub policy_hint: Option<Policy>,
    pub size_hint: Option<usize>,
    pub skew_hint: Option<F64>,
}

impl RebalancingExchangeSenderExchangeMetadata {
    fn get_fixed_policy(
        metadata: &[Option<RebalancingExchangeSenderExchangeMetadata>],
        hints: &BalancerHints,
    ) -> Result<Option<Policy>, BalancerError> {
        let fixed_policy = if metadata.iter().any(|metadata| {
            if let Some(metadata) = metadata {
                metadata.flushed
            } else {
                false
            }
        }) {
            assert!(metadata.iter().all(|metadata| metadata.is_some()));
            // TODO: assert: current policy is the same for all exchange senders.
            // assert!(exchange_sender_metadata
            //     .iter()
            //     .all(|metadata| metadata == &exchange_sender_metadata[0]));
            Some(metadata[0].as_ref().unwrap().current_policy)
        } else {
            None
        };

        if fixed_policy.is_some() && fixed_policy != hints.policy_hint {
            return Err(BalancerError::InvalidPolicyHint(
                hints.policy_hint.unwrap(),
                format!("the current policy {fixed_policy:?} cannot no longer be changed during the current transaction"),
            ));
        }

        if hints.policy_hint.is_some() {
            Ok(hints.policy_hint)
        } else {
            Ok(fixed_policy)
        }
    }

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
    integrals: BTreeMap<NodeId, (NodeId, BalancerHints)>,
    joins: BTreeMap<NodeId, (NodeId, NodeId)>,
    clusters: Vec<Cluster>,
    stream_to_cluster: BTreeMap<NodeId, usize>,
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

    fn solve_cluster(
        &self,
        cluster_index: usize,
    ) -> Result<BTreeMap<NodeId, Policy>, BalancerError> {
        //let cluster = &mut self.clusters[cluster_index];

        let mut maxsat = MaxSat::new();
        let mut variable_indexes = BTreeMap::new();

        for stream in self.clusters[cluster_index].streams.clone().iter() {
            let (exchange_sender, hints) = self.integrals.get(&stream).unwrap();

            let exchange_sender_metadata: Vec<Option<RebalancingExchangeSenderExchangeMetadata>> = self
                .metadata_exchange
                .get_global_operator_metadata_typed::<RebalancingExchangeSenderExchangeMetadata>(
                    *exchange_sender,
                );

            let fixed_policy = RebalancingExchangeSenderExchangeMetadata::get_fixed_policy(
                &exchange_sender_metadata,
                hints,
            )?;

            let mut domain = self.compute_domain(&exchange_sender_metadata, hints);
            if let Some(fixed_policy) = &fixed_policy {
                domain.retain(|policy, _| *policy == *fixed_policy);
            }

            let variable_index = maxsat.add_variable(&stream.to_string(), domain);
            variable_indexes.insert(*stream, variable_index);
        }

        for (_join, (left, right)) in self.clusters[cluster_index].joins.iter() {
            maxsat.add_constraint(Box::new(JoinConstraint::new(
                variable_indexes[left],
                variable_indexes[right],
            )));
        }

        let solution = maxsat.solve().unwrap();
        let solution: BTreeMap<NodeId, Policy> = variable_indexes
            .iter()
            .map(|(stream, variable_index)| {
                (
                    *stream,
                    Policy::try_from(*solution.get(variable_index).unwrap()).unwrap(),
                )
            })
            .collect();

        Ok(solution)
    }

    fn solve_all_clusters(&mut self) -> Result<(), BalancerError> {
        let num_clusters = self.clusters.len();

        for i in 0..num_clusters {
            self.clusters[i].solution = Some(self.solve_cluster(i)?);
            // println!("cluster {i}; solution: {:?}", self.clusters[i].solution);
        }

        Ok(())
    }

    fn compute_domain(
        &self,
        metadata: &[Option<RebalancingExchangeSenderExchangeMetadata>],
        hints: &BalancerHints,
    ) -> BTreeMap<Policy, Cost> {
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
            (Policy::Shard, Cost(max_size)),
            (Policy::Broadcast, Cost(total_size)),
            (
                Policy::Balance,
                Cost((total_size / Runtime::num_workers() as u64) * 2),
            ),
        ])
    }

    fn get_policy(&self, node_id: NodeId) -> Option<Policy> {
        //println!("stream_to_cluster({node_id}): {:?}", self.stream_to_cluster);
        let cluster_index = *self.stream_to_cluster.get(&node_id).unwrap();
        self.clusters[cluster_index]
            .solution
            .as_ref()?
            .get(&node_id)
            .cloned()
    }

    fn set_hint(&mut self, node_id: NodeId, hint: BalancerHint) -> Result<(), BalancerError> {
        let (_exchange_sender, hints) = self
            .integrals
            .get_mut(&node_id)
            .ok_or_else(|| BalancerError::NotRegisteredWithBalancer(node_id))?;
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
}

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

    pub fn register_integral(&self, stream: NodeId, exchange_sender: NodeId) {
        self.inner
            .borrow_mut()
            .integrals
            .insert(stream, (exchange_sender, BalancerHints::default()));
    }

    pub fn register_join(&self, join: NodeId, left: NodeId, right: NodeId) {
        self.inner.borrow_mut().joins.insert(join, (left, right));
    }

    // Invoked before the circuit starts running.
    pub fn prepare(&self) {
        self.compute_clusters();
    }

    // Invoked at the start of each transaction. Resets variable domains.
    pub fn start_transaction(&self) {}

    // Invoked at the start of each step. Updates costs and solves the optimization problem.
    pub fn start_step(&self) {
        // This shouldn't fail since all hints are validated when the were installed.
        self.inner.borrow_mut().solve_all_clusters().unwrap();
    }

    /// Optimal policy computed at the start of the step.
    pub fn get_optimal_policy(&self, node_id: NodeId) -> Option<Policy> {
        self.inner.borrow().get_policy(node_id)
    }

    fn compute_clusters(&self) {
        let mut inner = self.inner.borrow_mut();
        let mut node_to_index = BTreeMap::new();

        let mut join_graph = UnGraph::<NodeId, NodeId>::new_undirected();

        for node_id in inner.integrals.keys() {
            let node_index = join_graph.add_node(*node_id);
            node_to_index.insert(*node_id, node_index);
        }

        for (join, (left, right)) in inner.joins.iter() {
            let left_index = *node_to_index.get(left).unwrap();
            let right_index = *node_to_index.get(right).unwrap();
            join_graph.add_edge(left_index, right_index, *join);
        }

        let clusters = components(&join_graph);

        let clusters: Vec<Cluster> = clusters
            .into_iter()
            .map(|cluster| Cluster {
                streams: cluster
                    .0
                    .into_iter()
                    .map(|node| *join_graph.node_weight(node).unwrap())
                    .collect(),
                joins: cluster
                    .1
                    .into_iter()
                    .map(|edge| {
                        let join_id = *join_graph.edge_weight(edge).unwrap();
                        let (left, right) = inner.joins.get(&join_id).unwrap();
                        (join_id, (*left, *right))
                    })
                    .collect(),
                solution: None,
            })
            .collect();

        for (index, cluster) in clusters.iter().enumerate() {
            for stream in cluster.streams.iter() {
                inner.stream_to_cluster.insert(*stream, index);
            }
        }

        // println!("balancer clusters: {:?}", clusters);

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
}
