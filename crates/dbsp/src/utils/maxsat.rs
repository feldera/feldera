use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::ops::{Add, AddAssign};

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

pub type VariableValue = u8;

#[derive(Debug)]
pub struct MaxSat {
    constraints: Vec<Box<dyn HardConstraint>>,
    variables: Vec<Variable>,
}

pub trait HardConstraint: Debug {
    fn propagate(
        &self,
        all_variables: &mut [Variable],
        affected_variables: &mut BTreeSet<VariableIndex>,
    );

    fn variables(&self) -> Vec<VariableIndex>;
}

#[derive(Debug)]
pub struct Variable {
    pub name: String,
    pub domain: BTreeMap<VariableValue, Cost>,
    pub constraints: Vec<ConstraintIndex>,
}

impl MaxSat {
    pub fn new() -> Self {
        Self {
            constraints: Vec::new(),
            variables: Vec::new(),
        }
    }

    pub fn validate_solution(
        &self,
        solution: &BTreeMap<VariableIndex, VariableValue>,
    ) -> Option<Cost> {
        let mut cost = Cost(0);

        for (variable_index, variable_value) in solution.iter() {
            let variable = &self.variables[variable_index.0];
            if !variable.domain.contains_key(variable_value) {
                return None;
            }
            cost += *variable.domain.get(variable_value).unwrap();
        }
        Some(cost)
    }

    pub fn add_variable<T>(&mut self, name: &str, domain: BTreeMap<T, Cost>) -> VariableIndex
    where
        u8: From<T>,
        T: TryFrom<u8, Error = ()>,
    {
        let index = self.variables.len();

        self.variables.push(Variable {
            name: name.to_string(),
            domain: domain
                .into_iter()
                .map(|(value, cost)| (VariableValue::from(value), cost))
                .collect(),
            constraints: Vec::new(),
        });

        VariableIndex(index)
    }

    pub fn add_constraint(&mut self, constraint: Box<dyn HardConstraint>) {
        let index = ConstraintIndex(self.constraints.len());

        for variable in constraint.variables() {
            assert!(variable.0 < self.variables.len());
            self.variables[variable.0].constraints.push(index);
        }

        self.constraints.push(constraint);
    }

    pub fn resolve_highest_cost_variable(&mut self) -> Option<VariableIndex> {
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

        let Some((index, _cost, value)) = highest_cost_variable else {
            return None;
        };

        self.variables[index.0].domain.remove(&value);
        Some(index)
    }

    pub fn solve(&mut self) -> Option<BTreeMap<VariableIndex, VariableValue>> {
        assert!(self
            .variables
            .iter()
            .all(|variable| !variable.domain.is_empty()));

        // Initialize affected_variables to all variables before the first iteration.
        let mut affected_variables = BTreeSet::new();

        for i in 0..self.variables.len() {
            affected_variables.insert(VariableIndex(i));
        }

        loop {
            while !affected_variables.is_empty() {
                let mut new_affected_variables = BTreeSet::new();

                // Build a set of constraints to reevaluate based on the affected_variables.
                let mut constraints_to_reevaluate = BTreeSet::new();
                for variable in affected_variables.iter() {
                    for constraint in self.variables[variable.0].constraints.iter() {
                        constraints_to_reevaluate.insert(*constraint);
                    }
                }

                // Reevaluate the constraints.
                for constraint_index in constraints_to_reevaluate.iter() {
                    self.constraints[constraint_index.0]
                        .propagate(&mut self.variables, &mut new_affected_variables)
                }
                affected_variables = new_affected_variables;
            }

            assert!(affected_variables.is_empty());
            assert!(self
                .variables
                .iter()
                .all(|variable| !variable.domain.is_empty()));

            let Some(variable) = self.resolve_highest_cost_variable() else {
                return Some(
                    self.variables
                        .iter()
                        .enumerate()
                        .map(|(index, variable)| {
                            (
                                VariableIndex(index),
                                *variable.domain.iter().next().unwrap().0,
                            )
                        })
                        .collect(),
                );
            };

            affected_variables.insert(variable);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::dynamic::balancer::{JoinConstraint, Policy};

    use super::*;

    #[test]
    fn test_maxsat_join1() {
        let mut maxsat = MaxSat::new();
        // Collection 1: non-skewed
        let v1 = maxsat.add_variable(
            "left-non-skewed",
            BTreeMap::from([
                (Policy::Shard, Cost(0)),
                (Policy::Broadcast, Cost(1000)),
                (Policy::Balance, Cost(100)),
            ]),
        );

        // Collection 2: non-skewed
        let v2 = maxsat.add_variable(
            "right-non-skewed",
            BTreeMap::from([
                (Policy::Shard, Cost(0)),
                (Policy::Broadcast, Cost(1000)),
                (Policy::Balance, Cost(100)),
            ]),
        );

        // join(v1, v2)
        JoinConstraint::create_join_constraint(&mut maxsat, v1, v2);

        // Expected result: both collections are sharded
        let solution = maxsat.solve();
        assert_eq!(
            solution,
            Some(BTreeMap::from([
                (v1, Policy::Shard.into()),
                (v2, Policy::Shard.into())
            ]))
        );
    }

    #[test]
    fn test_maxsat_join2() {
        let mut maxsat = MaxSat::new();
        // Collection 1: large, skewed
        let v1 = maxsat.add_variable(
            "left-large-skewed",
            BTreeMap::from([
                (Policy::Shard, Cost(1_000_000)),
                (Policy::Broadcast, Cost(1_000_000)),
                (Policy::Balance, Cost(100_000)),
            ]),
        );

        // Collection 2: small, skewed
        let v2 = maxsat.add_variable(
            "right-small-skewed",
            BTreeMap::from([
                (Policy::Shard, Cost(1_000)),
                (Policy::Broadcast, Cost(20_000)),
                (Policy::Balance, Cost(100)),
            ]),
        );

        // join(v1, v2)
        JoinConstraint::create_join_constraint(&mut maxsat, v1, v2);

        // Expected result: collection 1 is balanced, collection 2 is broadcast
        let solution = maxsat.solve();
        assert_eq!(
            solution,
            Some(BTreeMap::from([
                (v1, Policy::Balance.into()),
                (v2, Policy::Broadcast.into())
            ]))
        );
    }

    #[test]
    fn test_maxsat_join3() {
        let mut maxsat = MaxSat::new();
        // Collection 1: large, skewed
        let v1 = maxsat.add_variable(
            "left1-large-skewed",
            BTreeMap::from([
                (Policy::Shard, Cost(1_000_000)),
                (Policy::Broadcast, Cost(1_000_000)),
                (Policy::Balance, Cost(100_000)),
            ]),
        );

        // Collection 2: large, skewed
        let v2 = maxsat.add_variable(
            "left2-large-skewed",
            BTreeMap::from([
                (Policy::Shard, Cost(1_000_000)),
                (Policy::Broadcast, Cost(1_000_000)),
                (Policy::Balance, Cost(100_000)),
            ]),
        );

        // Collection 3: small, skewed
        let v3 = maxsat.add_variable(
            "right-small-skewed",
            BTreeMap::from([
                (Policy::Shard, Cost(1_000)),
                (Policy::Broadcast, Cost(20_000)),
                (Policy::Balance, Cost(100)),
            ]),
        );

        // join(v1, v3)
        JoinConstraint::create_join_constraint(&mut maxsat, v1, v3);

        // join(v2, v3)
        JoinConstraint::create_join_constraint(&mut maxsat, v2, v3);

        // Expected result: collections 1 and 2 are balanced, collection 3 is broadcast
        let solution = maxsat.solve();
        assert_eq!(
            solution,
            Some(BTreeMap::from([
                (v1, Policy::Balance.into()),
                (v2, Policy::Balance.into()),
                (v3, Policy::Broadcast.into())
            ]))
        );
    }

    #[test]
    fn test_maxsat_join4() {
        let mut maxsat = MaxSat::new();
        // Collection 1: large, skewed
        let v1 = maxsat.add_variable(
            "left1-large-skewed",
            BTreeMap::from([
                (Policy::Shard, Cost(1_000_000)),
                (Policy::Broadcast, Cost(1_000_000)),
                (Policy::Balance, Cost(100_000)),
            ]),
        );

        // Collection 2: small, not skewed
        let v2 = maxsat.add_variable(
            "left2-large-skewed",
            BTreeMap::from([
                (Policy::Shard, Cost(10_000)),
                (Policy::Broadcast, Cost(100_000)),
                (Policy::Balance, Cost(20_000)),
            ]),
        );

        // Collection 3: small, skewed
        let v3 = maxsat.add_variable(
            "right-small-skewed",
            BTreeMap::from([
                (Policy::Shard, Cost(1_000)),
                (Policy::Broadcast, Cost(20_000)),
                (Policy::Balance, Cost(100)),
            ]),
        );

        // join(v1, v3)
        JoinConstraint::create_join_constraint(&mut maxsat, v1, v3);

        // join(v2, v3)
        JoinConstraint::create_join_constraint(&mut maxsat, v2, v3);

        // Expected result: collections 1 and 2 are balanced, collection 3 is broadcast
        let solution = maxsat.solve();
        assert_eq!(
            solution,
            Some(BTreeMap::from([
                (v1, Policy::Balance.into()),
                (v2, Policy::Shard.into()),
                (v3, Policy::Broadcast.into())
            ]))
        );
    }
}
