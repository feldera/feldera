//! MaxSat solver.
//!
//! This is a poor man's MaxSat solver that is used to compute "optimal" balancing policies
//! for adaptive joins. It solves a combination of hard and soft constraints over discrete variables.
//!
//! ## Variables, domains, and costs
//!
//! A variable has a finite domain of possible values, each with associated cost.
//! The cost of a complete variable assignment is the total cost of the selected values
//! for individual variables.
//!
//! ## Hard constraints
//!
//! A hard constraint restricts possible assignments to a group of variables.
//! All hard constraints must be satisfied by any valid solution.
//! A hard constraint has a propagator algorithm attached to it, which prunes the domains
//! of the variables when some of the other variable domains change.
//!
//! ## Solutions
//!
//! A solution is a variable assignment that satisfies all hard constraints.
//!
//! This solver is an offense to the maxsat community. It is not at all suitable for solving non-trivial
//! maxsat instances. It doesn't guarantee optimality.
//! It is basically a convenient abstraction for a simple greedy algorithm that can be replaced
//! with a real solver if needed.
//!
//! Does it make sense to replace it with a real solver? We will see. It should be doable if we don't
//! mind some new dependencies, plus the overhead of solving (very simple) SAT instances every few steps
//! of the circuit.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::ops::{Add, AddAssign};
use std::rc::Rc;

use dyn_clone::DynClone;

use crate::operator::dynamic::balance::BalancerError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct VariableIndex(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConstraintIndex(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Cost(pub u64);

impl Add<Cost> for Cost {
    type Output = Cost;
    fn add(self, other: Cost) -> Cost {
        Cost(self.0 + other.0)
    }
}

impl AddAssign<Cost> for Cost {
    fn add_assign(&mut self, other: Cost) {
        self.0 += other.0;
    }
}

/// Variable value type.
///
/// We currently only have variables with small domains, so u8 is more than sufficient.
pub type VariableValue = u8;

/// A stack frame of the backtracking search.
#[derive(Debug, Clone)]
struct BacktrackFrame {
    /// The decision that initiated this branch of the backtracking search: remove the
    /// specified value from the domain of a variable.
    ///
    /// When backtracking this decision, the value will be inserted back and all other
    /// values are removed from the domain (i.e., if removing the value makes the problem
    /// unsatisfiable, then this value is the solution for this variable).
    decision: (VariableIndex, VariableValue, Cost),

    /// Additional variable values pruned by running propagators after the decision.
    removed_assignments: Vec<(VariableIndex, VariableValue, Cost)>,
}

impl BacktrackFrame {
    fn new(variable: VariableIndex, value: VariableValue, cost: Cost) -> Self {
        Self {
            decision: (variable, value, cost),
            removed_assignments: Vec::new(),
        }
    }
}

/// A MaxSat problem instance.
#[derive(Debug, Clone)]
pub struct MaxSat {
    constraints: Vec<Rc<dyn HardConstraint>>,
    variables: Vec<Variable>,
    backtrack_stack: Vec<BacktrackFrame>,
}

pub trait HardConstraint: Debug + DynClone {
    /// Apply constraint's propagator to prune variable domains.
    ///
    /// Any variables affected by the propagator must be added to the `affected_variables` set.
    fn propagate(&self, maxsat: &mut MaxSat, affected_variables: &mut BTreeSet<VariableIndex>);

    /// Variables in the domain of this constraint.
    fn variables(&self) -> Vec<VariableIndex>;
}

dyn_clone::clone_trait_object!(HardConstraint);

/// A MaxSat variable.
#[derive(Debug, Clone)]
pub struct Variable {
    /// Variable name for debugging purposes.
    pub name: String,

    /// Variable domain: value -> cost.
    pub domain: BTreeMap<VariableValue, Cost>,

    /// Constraints that this variable is part of.
    ///
    /// Whenever the domain of this variable changes, the propagator algorithms for these constraints
    /// need to be reevaluated.
    pub constraints: Vec<ConstraintIndex>,
}

impl Default for MaxSat {
    fn default() -> Self {
        Self::new()
    }
}

impl MaxSat {
    pub fn new() -> Self {
        Self {
            constraints: Vec::new(),
            variables: Vec::new(),
            backtrack_stack: Vec::new(),
        }
    }

    pub fn variables(&self) -> &[Variable] {
        &self.variables
    }

    /// Validate that a solution satisfies all hard constraints.
    ///
    /// A solution is valid if:
    /// 1. All variables have exactly one value assigned
    /// 2. All hard constraints are satisfied by the solution
    ///
    /// This is done by creating a temporary MaxSat instance with domains restricted
    /// to the solution values, then running all propagators. If any domain becomes
    /// empty, the solution violates at least one constraint.
    pub fn validate_solution(
        &self,
        solution: &BTreeMap<VariableIndex, VariableValue>,
    ) -> Result<(), BalancerError> {
        // Check that all variables have exactly one value assigned
        if solution.len() != self.variables.len() {
            return Err(BalancerError::NoSolution);
        }

        for (index, variable) in self.variables.iter().enumerate() {
            let var_index = VariableIndex(index);
            if let Some(&value) = solution.get(&var_index) {
                // Check that the assigned value is in the variable's domain
                if !variable.domain.contains_key(&value) {
                    return Err(BalancerError::NoSolution);
                }
            } else {
                return Err(BalancerError::NoSolution);
            }
        }

        // Create a temporary MaxSat copy with domains restricted to solution values,
        // then run propagators to check if any constraint is violated
        let mut temp_maxsat = self.clone();
        temp_maxsat.backtrack_stack.clear();

        // Restrict each variable's domain to only the solution value
        for (var_index, &solution_value) in solution.iter() {
            let variable = &mut temp_maxsat.variables[var_index.0];
            let cost = variable
                .domain
                .get(&solution_value)
                .copied()
                .ok_or(BalancerError::NoSolution)?;

            // Remove all values except the solution value
            variable.domain.clear();
            variable.domain.insert(solution_value, cost);
        }

        // Run all propagators to check if any constraint is violated
        let mut affected_variables = BTreeSet::new();
        for i in 0..temp_maxsat.variables.len() {
            affected_variables.insert(VariableIndex(i));
        }

        // Run propagators until fixpoint
        while !affected_variables.is_empty() {
            let mut new_affected_variables = BTreeSet::new();
            let mut constraints_to_reevaluate = BTreeSet::new();

            for variable in affected_variables.iter() {
                for constraint in temp_maxsat.variables[variable.0].constraints.iter() {
                    constraints_to_reevaluate.insert(*constraint);
                }
            }

            for constraint_index in constraints_to_reevaluate.iter() {
                let constraint = temp_maxsat.constraints[constraint_index.0].clone();
                constraint.propagate(&mut temp_maxsat, &mut new_affected_variables);
            }

            affected_variables = new_affected_variables;
        }

        // Check if any variable domain became empty (constraint violation)
        if temp_maxsat
            .variables
            .iter()
            .any(|variable| variable.domain.is_empty())
        {
            return Err(BalancerError::NoSolution);
        }

        Ok(())
    }

    /// Add a new variable to the maxsat instance.
    pub fn add_variable<T>(&mut self, name: &str, domain: &BTreeMap<T, Cost>) -> VariableIndex
    where
        u8: From<T>,
        T: TryFrom<u8, Error = ()> + Clone,
    {
        let index = self.variables.len();

        self.variables.push(Variable {
            name: name.to_string(),
            domain: domain
                .iter()
                .map(|(value, cost)| (VariableValue::from(value.clone()), *cost))
                .collect(),
            constraints: Vec::new(),
        });

        VariableIndex(index)
    }

    /// Add a new hard constraint to the maxsat instance.
    ///
    /// All variable that the constraint refers to must have been added to the instance first.
    pub fn add_constraint<C: HardConstraint + 'static>(&mut self, constraint: C) {
        let index = ConstraintIndex(self.constraints.len());

        for variable in constraint.variables() {
            assert!(variable.0 < self.variables.len());
            self.variables[variable.0].constraints.push(index);
        }

        self.constraints.push(Rc::new(constraint));
    }

    /// Find the most expensive variable value that can be pruned.
    ///
    /// Picks the most expensive variable value across all variable domains
    /// that can be pruned (i.e., it's not the only remaining value in the domain).
    ///
    /// Called after domain propagators cannot prune any further.
    fn highest_cost_variable_assignment(&self) -> Option<(VariableIndex, VariableValue, Cost)> {
        let mut highest_cost_variable: Option<(VariableIndex, Cost, VariableValue)> = None;

        for (index, variable) in self.variables.iter().enumerate() {
            if variable.domain.len() <= 1 {
                continue;
            }
            for (value, cost) in variable.domain.iter() {
                if highest_cost_variable.is_none() || *cost > highest_cost_variable.unwrap().1 {
                    highest_cost_variable = Some((VariableIndex(index), *cost, *value));
                }
            }
        }

        let (index, cost, value) = highest_cost_variable?;

        //self.variables[index.0].domain.remove(&value);
        Some((index, value, cost))
    }

    /// Record the decision to prune `value` from the domain of `variable`.
    ///
    /// Pushes new backtracking stack frame.
    fn decision(&mut self, variable: VariableIndex, value: VariableValue, cost: Cost) {
        // println!(
        //     "decision: variable {:?}, value {:?}, cost {:?}, domains: {:?}",
        //     variable, value, cost, self.variables
        // );
        self.variables[variable.0].domain.remove(&value).unwrap();

        self.backtrack_stack
            .push(BacktrackFrame::new(variable, value, cost));
        // println!("  STACK DEPTH [{:?}]", self.backtrack_stack.len());
    }

    /// Remove a variable assignment and record it in the current stack frame.
    ///
    /// Returns true if the variable assignment was removed, false otherwise.
    pub fn remove_variable_assignment(
        &mut self,
        variable: VariableIndex,
        value: VariableValue,
    ) -> bool {
        if let Some(cost) = self.variables[variable.0].domain.remove(&value) {
            if let Some(frame) = self.backtrack_stack.last_mut() {
                frame.removed_assignments.push((variable, value, cost))
            }
            true
        } else {
            false
        }
    }

    /// Revert all variable domain changes performed at the current level of the backtracking search
    /// after hitting a conflict.
    fn backtrack(
        &mut self,
        affected_variables: &mut BTreeSet<VariableIndex>,
    ) -> Result<(), BalancerError> {
        // println!("backtrack STACK DEPTH [{:?}]", self.backtrack_stack.len());
        // Nowhere to backtrack. The instant is undecidable.
        if self.backtrack_stack.is_empty() {
            return Err(BalancerError::NoSolution);
        }

        let BacktrackFrame {
            decision: (decision_variable, decision_value, decision_cost),
            removed_assignments,
        } = self.backtrack_stack.pop().unwrap();

        // println!(
        //     "backtrack [{:?}]\n  decision_variable: {:?}\n  decision_value: {:?}\n  decision_cost: {:?}\n  removed_assignments: {:?}",
        //     self.variables, decision_variable, decision_value, decision_cost, removed_assignments
        // );

        // Undo all propagations.
        for (variable, value, cost) in removed_assignments.iter() {
            self.variables[variable.0].domain.insert(*value, *cost);
        }

        // Revert the decision.
        let domain = self.variables[decision_variable.0].domain.clone();
        for (val, _cost) in domain.into_iter() {
            self.remove_variable_assignment(decision_variable, val);
        }
        self.variables[decision_variable.0]
            .domain
            .insert(decision_value, decision_cost);

        affected_variables.insert(decision_variable);
        // println!(
        //     "after backtrack [{:?}]\n   frame: {:?}",
        //     self.variables,
        //     self.backtrack_stack.last()
        // );

        Ok(())
    }

    /// Solve the MaxSat problem.
    pub fn solve(&mut self) -> Result<BTreeMap<VariableIndex, VariableValue>, BalancerError> {
        // println!("---------START-----------");
        assert!(
            self.variables
                .iter()
                .all(|variable| !variable.domain.is_empty())
        );

        // Initialize affected_variables to all variables before the first iteration.
        let mut affected_variables = BTreeSet::new();

        for i in 0..self.variables.len() {
            affected_variables.insert(VariableIndex(i));
        }

        loop {
            // Run all propagators.
            while !affected_variables.is_empty() {
                let mut new_affected_variables = BTreeSet::new();

                // Build a set of constraints to reevaluate based on the affected_variables.
                let mut constraints_to_reevaluate = BTreeSet::new();
                for variable in affected_variables.iter() {
                    for constraint in self.variables[variable.0].constraints.iter() {
                        constraints_to_reevaluate.insert(*constraint);
                    }
                }

                // println!("constraints_to_reevaluate: {:?}", constraints_to_reevaluate);

                // Reevaluate the constraints.
                for constraint_index in constraints_to_reevaluate.iter() {
                    let constraint = self.constraints[constraint_index.0].clone();
                    // println!("reevaluating constraint {:?}", constraint_index);
                    constraint.propagate(self, &mut new_affected_variables);
                    // println!("new_affected_variables: {:?}", new_affected_variables);
                }
                affected_variables = new_affected_variables;
            }

            assert!(affected_variables.is_empty());

            // If at least one variable has an empty domain at this point, pop the stack.
            // (or fail if the stack is empty).
            if self
                .variables
                .iter()
                .any(|variable| variable.domain.is_empty())
            {
                self.backtrack(&mut affected_variables)?;
                continue;
            }

            // Prune the highest-cost variable value.
            let Some((variable, value, cost)) = self.highest_cost_variable_assignment() else {
                // Every variable has a unique assignment. We have a solution.
                return Ok(self
                    .variables
                    .iter()
                    .enumerate()
                    .map(|(index, variable)| {
                        (
                            VariableIndex(index),
                            *variable.domain.iter().next().unwrap().0,
                        )
                    })
                    .collect());
            };

            // Record the decision on the stack.
            self.decision(variable, value, cost);

            affected_variables.insert(variable);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::operator::dynamic::balance::{
        JoinConstraint, MaxSat, PartitioningPolicy, maxsat::Cost,
    };

    #[cfg(test)]
    mod proptests {
        use super::*;
        use proptest::prelude::*;

        /// Strategy to generate a random partitioning policy domain with costs.
        /// Always includes all three policies (Shard, Broadcast, Balance) with random costs.
        fn partitioning_policy_domain() -> impl Strategy<Value = BTreeMap<PartitioningPolicy, Cost>>
        {
            (
                (1u64..1_000_000u64).prop_map(Cost), // Shard cost
                (1u64..1_000_000u64).prop_map(Cost), // Broadcast cost
                (1u64..1_000_000u64).prop_map(Cost), // Balance cost
            )
                .prop_map(|(shard_cost, broadcast_cost, balance_cost)| {
                    BTreeMap::from([
                        (PartitioningPolicy::Shard, shard_cost),
                        (PartitioningPolicy::Broadcast, broadcast_cost),
                        (PartitioningPolicy::Balance, balance_cost),
                    ])
                })
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(1000))]

            #[test]
            fn proptest_maxsat(
                variable_domains in prop::collection::vec(partitioning_policy_domain(), 2..=10),
                constraint_pairs in prop::collection::vec(
                    (0usize..10, 0usize..10, prop::bool::ANY),
                    1..=20
                )
            ) {
                // Ensure we have at least 2 variables
                let num_vars = variable_domains.len();
                if num_vars < 2 {
                    return Ok(());
                }

                let mut maxsat = MaxSat::new();
                let mut variable_indices = Vec::new();

                // Add variables
                for (i, domain) in variable_domains.iter().enumerate() {
                    let var_index = maxsat.add_variable(&format!("v{}", i), domain);
                    variable_indices.push(var_index);
                }

                // Add constraints (only valid pairs that reference existing variables)
                for (v1_idx, v2_idx, is_left_join) in constraint_pairs.iter() {
                    if *v1_idx < variable_indices.len()
                        && *v2_idx < variable_indices.len()
                        && *v1_idx != *v2_idx
                    {
                        JoinConstraint::create_join_constraint(
                            &mut maxsat,
                            variable_indices[*v1_idx],
                            variable_indices[*v2_idx],
                            *is_left_join,
                        );
                    }
                }

                // Try to solve
                match maxsat.solve() {
                    Ok(solution) => {
                        // If we got a solution, it must pass validation
                        prop_assert!(
                            maxsat.validate_solution(&solution).is_ok(),
                            "Solution {:?} failed validation for maxsat with {} variables and constraints",
                            solution,
                            maxsat.constraints.len()
                        );
                    }
                    Err(_) => {
                        // These instances always have a solution
                        panic!("solving failed");
                    }
                }
            }
        }
    }

    #[test]
    fn test_maxsat_join1() {
        let mut maxsat = MaxSat::new();
        // Collection 1: non-skewed
        let v1 = maxsat.add_variable(
            "left-non-skewed",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(0)),
                (PartitioningPolicy::Broadcast, Cost(1000)),
                (PartitioningPolicy::Balance, Cost(100)),
            ]),
        );

        // Collection 2: non-skewed
        let v2 = maxsat.add_variable(
            "right-non-skewed",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(0)),
                (PartitioningPolicy::Broadcast, Cost(1000)),
                (PartitioningPolicy::Balance, Cost(100)),
            ]),
        );

        // join(v1, v2)
        JoinConstraint::create_join_constraint(&mut maxsat, v1, v2, false);

        // Expected result: both collections are sharded
        let solution = maxsat.solve().unwrap();
        assert_eq!(
            solution,
            BTreeMap::from([
                (v1, PartitioningPolicy::Shard.into()),
                (v2, PartitioningPolicy::Shard.into())
            ])
        );
    }

    #[test]
    fn test_maxsat_join2() {
        let mut maxsat = MaxSat::new();
        // Collection 1: large, skewed
        let v1 = maxsat.add_variable(
            "left-large-skewed",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(1_000_000)),
                (PartitioningPolicy::Broadcast, Cost(1_000_000)),
                (PartitioningPolicy::Balance, Cost(100_000)),
            ]),
        );

        // Collection 2: small, skewed
        let v2 = maxsat.add_variable(
            "right-small-skewed",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(1_000)),
                (PartitioningPolicy::Broadcast, Cost(20_000)),
                (PartitioningPolicy::Balance, Cost(100)),
            ]),
        );

        // join(v1, v2)
        JoinConstraint::create_join_constraint(&mut maxsat, v1, v2, false);

        // Expected result: collection 1 is balanced, collection 2 is broadcast
        let solution = maxsat.solve().unwrap();
        assert_eq!(
            solution,
            BTreeMap::from([
                (v1, PartitioningPolicy::Balance.into()),
                (v2, PartitioningPolicy::Broadcast.into())
            ])
        );
    }

    #[test]
    fn test_maxsat_join3() {
        let mut maxsat = MaxSat::new();
        // Collection 1: large, skewed
        let v1 = maxsat.add_variable(
            "left1-large-skewed",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(1_000_000)),
                (PartitioningPolicy::Broadcast, Cost(1_000_000)),
                (PartitioningPolicy::Balance, Cost(100_000)),
            ]),
        );

        // Collection 2: large, skewed
        let v2 = maxsat.add_variable(
            "left2-large-skewed",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(1_000_000)),
                (PartitioningPolicy::Broadcast, Cost(1_000_000)),
                (PartitioningPolicy::Balance, Cost(100_000)),
            ]),
        );

        // Collection 3: small, skewed
        let v3 = maxsat.add_variable(
            "right-small-skewed",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(1_000)),
                (PartitioningPolicy::Broadcast, Cost(20_000)),
                (PartitioningPolicy::Balance, Cost(100)),
            ]),
        );

        // join(v1, v3)
        JoinConstraint::create_join_constraint(&mut maxsat, v1, v3, false);

        // join(v2, v3)
        JoinConstraint::create_join_constraint(&mut maxsat, v2, v3, false);

        // Expected result: collections 1 and 2 are balanced, collection 3 is broadcast
        let solution = maxsat.solve().unwrap();
        assert_eq!(
            solution,
            BTreeMap::from([
                (v1, PartitioningPolicy::Balance.into()),
                (v2, PartitioningPolicy::Balance.into()),
                (v3, PartitioningPolicy::Broadcast.into())
            ])
        );
    }

    #[test]
    fn test_maxsat_join4() {
        let mut maxsat = MaxSat::new();
        // Collection 1: large, skewed
        let v1 = maxsat.add_variable(
            "left1-large-skewed",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(1_000_000)),
                (PartitioningPolicy::Broadcast, Cost(1_000_000)),
                (PartitioningPolicy::Balance, Cost(100_000)),
            ]),
        );

        // Collection 2: small, not skewed
        let v2 = maxsat.add_variable(
            "left2-large-skewed",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(10_000)),
                (PartitioningPolicy::Broadcast, Cost(100_000)),
                (PartitioningPolicy::Balance, Cost(20_000)),
            ]),
        );

        // Collection 3: small, skewed
        let v3 = maxsat.add_variable(
            "right-small-skewed",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(1_000)),
                (PartitioningPolicy::Broadcast, Cost(20_000)),
                (PartitioningPolicy::Balance, Cost(100)),
            ]),
        );

        // join(v1, v3)
        JoinConstraint::create_join_constraint(&mut maxsat, v1, v3, false);

        // join(v2, v3)
        JoinConstraint::create_join_constraint(&mut maxsat, v2, v3, false);

        // Expected result: collections 1 and 2 are balanced, collection 3 is broadcast
        let solution = maxsat.solve().unwrap();
        assert_eq!(
            solution,
            BTreeMap::from([
                (v1, PartitioningPolicy::Balance.into()),
                (v2, PartitioningPolicy::Shard.into()),
                (v3, PartitioningPolicy::Broadcast.into())
            ])
        );
    }

    /// Test backtracking.
    #[test]
    fn test_maxsat_join5() {
        let mut maxsat = MaxSat::new();
        // Collection 1: large, skewed
        let v1 = maxsat.add_variable(
            "v1",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(1_000_000)),
                (PartitioningPolicy::Broadcast, Cost(1_000)),
                (PartitioningPolicy::Balance, Cost(1)),
            ]),
        );

        // Collection 2: small, not skewed
        let v2 = maxsat.add_variable(
            "v2",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(1_000_000)),
                (PartitioningPolicy::Broadcast, Cost(1_000)),
                (PartitioningPolicy::Balance, Cost(1)),
            ]),
        );

        // Collection 3: small, skewed
        let v3 = maxsat.add_variable(
            "v3",
            &BTreeMap::from([
                (PartitioningPolicy::Shard, Cost(1_000_000)),
                (PartitioningPolicy::Broadcast, Cost(1_000)),
                (PartitioningPolicy::Balance, Cost(1)),
            ]),
        );

        // join(v1, v2)
        JoinConstraint::create_join_constraint(&mut maxsat, v1, v2, false);

        // join(v1, v3)
        JoinConstraint::create_join_constraint(&mut maxsat, v1, v3, false);

        // join(v2, v3)
        JoinConstraint::create_join_constraint(&mut maxsat, v2, v3, false);

        // Expected result: collections 1 and 2 are balanced, collection 3 is broadcast
        let solution = maxsat.solve().unwrap();
        assert_eq!(
            solution,
            BTreeMap::from([
                (v1, PartitioningPolicy::Broadcast.into()),
                (v2, PartitioningPolicy::Shard.into()),
                (v3, PartitioningPolicy::Shard.into())
            ])
        );
    }
}
