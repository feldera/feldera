//! Evaluation of expression trees that read each other's results.
//!
//! Each tree node is a leaf with a value, which may be NULL; an `add` or
//! `mul` node over two child nodes of the same tree; or a reference to the
//! total of another tree, which is the value of that tree's root, with NULL
//! read as 0.  Two mutually recursive relations hold every node's value and
//! every tree's total.  In the rules below, `t` is a tree and `n` a node of it:
//!
//! * `leaves(t, n, v)`: `n` is a leaf with value `v`.
//! * `inner(t, n, is_add, left, right)`: `n` adds the values of nodes `left`
//!   and `right` if `is_add`, and multiplies them otherwise.
//! * `refs(t, n, target)`: `n` reads the total of tree `target`.
//! * `roots(t, n)`: `n` is the root of `t`.
//!
//! ```text
//! values(t, n, v) :- leaves(t, n, v).
//! values(t, n, apply(is_add, l, r)) :-
//!     inner(t, n, is_add, left, right),
//!     values(t, left, l),
//!     values(t, right, r).
//! values(t, n, total) :- refs(t, n, target), totals(target, total).
//! totals(t, coalesce(v, 0)) :- roots(t, root), values(t, root, v).
//! ```
//!
//! `apply` adds or multiplies, wrapping on overflow, and is NULL if either
//! operand is.  An inner node's value joins the value of its left child with
//! the value of its right child, so both inputs of that join depend on the
//! recursion.
//!
//! For example, tree 1 computes `2 * 3`, and tree 2 computes `ref(1) + 1`,
//! which adds 1 to the total of tree 1:
//!
//! ```text
//! leaves  (1, 0, 2)            tree 1, node 0 = 2
//!         (1, 1, 3)            tree 1, node 1 = 3
//!         (2, 1, 1)            tree 2, node 1 = 1
//! inner   (1, 2, false, 0, 1)  tree 1, node 2 = node 0 * node 1
//!         (2, 2, true, 0, 1)   tree 2, node 2 = node 0 + node 1
//! refs    (2, 0, 1)            tree 2, node 0 = the total of tree 1
//! roots   (1, 2)               tree 1's root is node 2
//!         (2, 2)               tree 2's root is node 2
//! ```
//!
//! A value or a total is derived one iteration after the rows it reads:
//!
//! ```text
//! iteration  derives
//! 0          values (1, 0, 2), (1, 1, 3), (2, 1, 1)  the leaves
//! 1          value (1, 2, 6)                         2 * 3
//! 2          total (1, 6)                            tree 1's root
//! 3          value (2, 0, 6)                         the reference
//! 4          value (2, 2, 7)                         6 + 1
//! 5          total (2, 7)                            tree 2's root
//! ```
//!
//! So each reference to another tree adds two iterations: one for that
//! tree's total, and one for the value that reads it.  Had leaf `(1, 0)` been
//! NULL, node `(1, 2)` would be NULL too, tree 1's total 0, and tree 2's
//! total 1.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use super::harness::{
    Program, Transaction, ZSet, any_config, check, configs, fixpoint, map_steps, read_zset,
    set_after, set_zset, workloads,
};
use crate::{
    OutputHandle, RootCircuit, Stream, ZSetHandle, ZWeight,
    typed_batch::{OrdZSet, SpineSnapshot},
    utils::{Tup2, Tup3, Tup5, test::CIRCUIT_CASES},
};

/// A leaf's value: `(tree, node, value)`.
type Leaf = Tup3<u64, u64, Option<i64>>;

/// An inner node: `(tree, node, is_add, left, right)`, which adds its
/// children's values if `is_add` is set, and multiplies them otherwise.
type Inner = Tup5<u64, u64, bool, u64, u64>;

/// A reference: `(tree, node, target)`, whose value is the total of tree
/// `target`.
type Ref = Tup3<u64, u64, u64>;

/// A tree's root: `(tree, node)`.
type Root = Tup2<u64, u64>;

/// A node's value: `(tree, node, value)`.
type Value = Tup3<u64, u64, Option<i64>>;

/// A tree's total: `(tree, total)`.
type Total = Tup2<u64, i64>;

/// Computes an inner node's value from its children's values.
///
/// # Arguments
///
/// * `is_add` - whether the node adds its children's values, rather than
///   multiplying them.
/// * `left` - the left child's value.
/// * `right` - the right child's value.
///
/// # Returns
///
/// The node's value, NULL if either child's value is.
fn apply(is_add: bool, left: Option<i64>, right: Option<i64>) -> Option<i64> {
    let (left, right) = (left?, right?);
    Some(if is_add {
        left.wrapping_add(right)
    } else {
        left.wrapping_mul(right)
    })
}

/// Changes to the four inputs in one step.
#[derive(Clone, Debug, Default)]
struct TreeInput {
    leaves: Vec<(Leaf, ZWeight)>,
    inner: Vec<(Inner, ZWeight)>,
    refs: Vec<(Ref, ZWeight)>,
    roots: Vec<(Root, ZWeight)>,
}

/// How [`TreeEvaluation`] computes the values of inner nodes.
#[derive(Clone, Copy, Debug)]
enum Plan {
    /// Join the left child's value with the node's definition, then with the
    /// right child's value.
    ByChild,

    /// Pair every two values of a tree, then join the pairs with the
    /// definitions of the nodes over them.  The SQL compiler may plan a join
    /// of a node's definition with its children's values this way.
    ByTree,
}

/// Evaluation of expression trees.
#[derive(Clone)]
struct TreeEvaluation(Plan);

impl Program for TreeEvaluation {
    type Input = TreeInput;
    type Handles = (
        ZSetHandle<Leaf>,
        ZSetHandle<Inner>,
        ZSetHandle<Ref>,
        ZSetHandle<Root>,
        OutputHandle<SpineSnapshot<OrdZSet<Value>>>,
        OutputHandle<SpineSnapshot<OrdZSet<Total>>>,
    );
    /// Every node's value and every tree's total.
    type Output = (ZSet<Value>, ZSet<Total>);

    fn build(&self, circuit: &mut RootCircuit) -> Self::Handles {
        let (leaves, leaves_handle) = circuit.add_input_zset::<Leaf>();
        let (inner, inner_handle) = circuit.add_input_zset::<Inner>();
        let (refs, refs_handle) = circuit.add_input_zset::<Ref>();
        let (roots, roots_handle) = circuit.add_input_zset::<Root>();
        let (values, totals) = circuit
            .recursive(
                |child, (values, totals): (Stream<_, OrdZSet<Value>>, Stream<_, OrdZSet<Total>>)| {
                    let values_by_node =
                        values.map_index(|Tup3(tree, node, value)| (Tup2(*tree, *node), *value));
                    let computed = match self.0 {
                        Plan::ByChild => {
                            let inner_by_left = inner.delta0(child).map_index(
                                |Tup5(tree, node, is_add, left, right)| {
                                    (Tup2(*tree, *left), Tup3(*node, *is_add, *right))
                                },
                            );
                            // Each inner node with its left child's value,
                            // keyed by its right child.
                            let with_left = values_by_node.join_index(
                                &inner_by_left,
                                |Tup2(tree, _left), left, Tup3(node, is_add, right)| {
                                    Some((Tup2(*tree, *right), Tup3(*node, *is_add, *left)))
                                },
                            );
                            // Both inputs of this join depend on the recursion.
                            with_left.join(
                                &values_by_node,
                                |Tup2(tree, _right), Tup3(node, is_add, left), right| {
                                    Tup3(*tree, *node, apply(*is_add, *left, *right))
                                },
                            )
                        }
                        Plan::ByTree => {
                            let values_by_tree = values
                                .map_index(|Tup3(tree, node, value)| (*tree, Tup2(*node, *value)));
                            // Every two values of a tree: a join of a
                            // recursive stream with itself.
                            let pairs = values_by_tree.join_index(
                                &values_by_tree,
                                |tree, Tup2(left, left_value), Tup2(right, right_value)| {
                                    Some((
                                        Tup3(*tree, *left, *right),
                                        Tup2(*left_value, *right_value),
                                    ))
                                },
                            );
                            pairs.join(
                                &inner.delta0(child).map_index(
                                    |Tup5(tree, node, is_add, left, right)| {
                                        (Tup3(*tree, *left, *right), Tup2(*node, *is_add))
                                    },
                                ),
                                |Tup3(tree, _, _), Tup2(left, right), Tup2(node, is_add)| {
                                    Tup3(*tree, *node, apply(*is_add, *left, *right))
                                },
                            )
                        }
                    };
                    let referenced = refs
                        .delta0(child)
                        .map_index(|Tup3(tree, node, target)| (*target, Tup2(*tree, *node)))
                        .join(
                            &totals.map_index(|Tup2(tree, total)| (*tree, *total)),
                            |_target, Tup2(tree, node), total| Tup3(*tree, *node, Some(*total)),
                        );
                    let values_next = leaves.delta0(child).plus(&computed).plus(&referenced);
                    let totals_next = values_by_node.join(
                        &roots
                            .delta0(child)
                            .map_index(|Tup2(tree, root)| (Tup2(*tree, *root), ())),
                        |Tup2(tree, _root), value, _| Tup2(*tree, value.unwrap_or(0)),
                    );
                    Ok((values_next, totals_next))
                },
            )
            .unwrap();
        (
            leaves_handle,
            inner_handle,
            refs_handle,
            roots_handle,
            values.accumulate_integrate().accumulate_output(),
            totals.accumulate_integrate().accumulate_output(),
        )
    }

    fn push(&self, (leaves, inner, refs, roots, _, _): &Self::Handles, input: &TreeInput) {
        for (row, weight) in &input.leaves {
            leaves.push(*row, *weight);
        }
        for (row, weight) in &input.inner {
            inner.push(*row, *weight);
        }
        for (row, weight) in &input.refs {
            refs.push(*row, *weight);
        }
        for (row, weight) in &input.roots {
            roots.push(*row, *weight);
        }
    }

    fn read(&self, (_, _, _, _, values, totals): &Self::Handles) -> Self::Output {
        (read_zset(values), read_zset(totals))
    }

    fn model(&self, inputs: &[TreeInput]) -> Self::Output {
        let leaves = set_after(inputs.iter().map(|input| input.leaves.as_slice()));
        let inner = set_after(inputs.iter().map(|input| input.inner.as_slice()));
        let refs = set_after(inputs.iter().map(|input| input.refs.as_slice()));
        let roots = set_after(inputs.iter().map(|input| input.roots.as_slice()));
        let value_of = |values: &BTreeSet<Value>, tree: u64, node: u64| {
            values
                .range(Tup3(tree, node, None)..=Tup3(tree, node, Some(i64::MAX)))
                .map(|Tup3(_, _, value)| *value)
                .collect::<Vec<_>>()
        };
        let (values, totals) = fixpoint(
            (BTreeSet::new(), BTreeSet::new()),
            |(values, totals): &(BTreeSet<Value>, BTreeSet<Total>)| {
                let mut values_next = leaves.clone();
                for Tup5(tree, node, is_add, left, right) in &inner {
                    for left in value_of(values, *tree, *left) {
                        for right in value_of(values, *tree, *right) {
                            values_next.insert(Tup3(*tree, *node, apply(*is_add, left, right)));
                        }
                    }
                }
                for Tup3(tree, node, target) in &refs {
                    for Tup2(_, total) in
                        totals.range(Tup2(*target, i64::MIN)..=Tup2(*target, i64::MAX))
                    {
                        values_next.insert(Tup3(*tree, *node, Some(*total)));
                    }
                }
                let mut totals_next = BTreeSet::new();
                for Tup2(tree, root) in &roots {
                    for value in value_of(values, *tree, *root) {
                        totals_next.insert(Tup2(*tree, value.unwrap_or(0)));
                    }
                }
                (values_next, totals_next)
            },
        );
        (set_zset(&values), set_zset(&totals))
    }
}

/// Changes that insert (weight 1) or delete (weight -1) rows.
///
/// # Arguments
///
/// * `rows` - the rows.
/// * `weight` - the weight of every row.
///
/// # Returns
///
/// The changes.
fn changes<T: Clone>(rows: &[T], weight: ZWeight) -> Vec<(T, ZWeight)> {
    rows.iter().map(|row| (row.clone(), weight)).collect()
}

/// Tree 3, `x * 5 + 0 * 1 * 2`, whose left subtree is one operator deep and
/// whose right subtree is two, so that its values are derived in iterations 0
/// to 3.
///
/// # Arguments
///
/// * `x` - the value of leaf `x`, node 0.
///
/// # Returns
///
/// The tree's leaves, inner nodes, and root.
fn uneven_tree(x: Option<i64>) -> TreeInput {
    TreeInput {
        leaves: changes(
            &[
                Tup3(3, 0, x),
                Tup3(3, 1, Some(5)),
                Tup3(3, 10, Some(0)),
                Tup3(3, 11, Some(1)),
                Tup3(3, 13, Some(2)),
            ],
            1,
        ),
        inner: changes(
            &[
                Tup5(3, 2, false, 0, 1),
                Tup5(3, 12, false, 10, 11),
                Tup5(3, 14, false, 12, 13),
                Tup5(3, 99, true, 2, 14),
            ],
            1,
        ),
        refs: vec![],
        roots: changes(&[Tup2(3, 99)], 1),
    }
}

/// Changes that replace the value of a leaf.
///
/// # Arguments
///
/// * `tree` - the leaf's tree.
/// * `node` - the leaf.
/// * `old` - the leaf's current value.
/// * `new` - the leaf's new value.
///
/// # Returns
///
/// The changes.
fn set_leaf(tree: u64, node: u64, old: Option<i64>, new: Option<i64>) -> TreeInput {
    TreeInput {
        leaves: vec![(Tup3(tree, node, old), -1), (Tup3(tree, node, new), 1)],
        ..TreeInput::default()
    }
}

/// A tree whose inner nodes `x = a + d` and `y = c + d` share a deep right
/// child `d`, as tree 0.
///
/// `d` is a chain of five multiplications by 1, so an earlier transaction
/// derives it in iteration 5, while `a` and `c`, one and two multiplications
/// deep, derive in iterations 1 and 2.  When the leaves under `a` and `c`
/// change, `a` and `c` change in consecutive iterations, and each meets the
/// value of `d` from iteration 5: the join computes output for the same later
/// iteration in two iterations of one run.
///
/// # Returns
///
/// The tree, with NULL leaves under `a` and `c`.
fn shared_deep_child() -> TreeInput {
    // Nodes: 0 = leaf a0, 1 = leaf c0, 2 = leaf d0, 3 = leaf 1, 4 = a,
    // 5-6 = c, 7-11 = d, 12 = x, 13 = y, 14 = root.
    let mut inner = vec![
        Tup5(0, 4, false, 0, 3),
        Tup5(0, 5, false, 1, 3),
        Tup5(0, 6, false, 5, 3),
    ];
    let mut below = 2;
    for node in 7..=11 {
        inner.push(Tup5(0, node, false, below, 3));
        below = node;
    }
    inner.extend([
        Tup5(0, 12, true, 4, 11),
        Tup5(0, 13, true, 6, 11),
        Tup5(0, 14, true, 12, 13),
    ]);
    TreeInput {
        leaves: changes(
            &[
                Tup3(0, 0, None),
                Tup3(0, 1, None),
                Tup3(0, 2, Some(7)),
                Tup3(0, 3, Some(1)),
            ],
            1,
        ),
        inner: changes(&inner, 1),
        refs: vec![],
        roots: changes(&[Tup2(0, 14)], 1),
    }
}

/// Three trees: tree 1, `x * 1`; tree 2, `ref(1) + 0`, which reads the total
/// of tree 1; and tree 3, [`uneven_tree`].
///
/// # Returns
///
/// The trees, one per element, with every `x` NULL.
fn three_trees() -> Vec<TreeInput> {
    let tree_1 = TreeInput {
        leaves: changes(&[Tup3(1, 0, None), Tup3(1, 1, Some(1))], 1),
        inner: changes(&[Tup5(1, 2, false, 0, 1)], 1),
        refs: vec![],
        roots: changes(&[Tup2(1, 2)], 1),
    };
    let tree_2 = TreeInput {
        leaves: changes(&[Tup3(2, 1, Some(0))], 1),
        inner: changes(&[Tup5(2, 2, true, 0, 1)], 1),
        refs: changes(&[Tup3(2, 0, 1)], 1),
        roots: changes(&[Tup2(2, 2)], 1),
    };
    vec![tree_1, tree_2, uneven_tree(None)]
}

/// Scripted workloads, whose later transactions set leaves of trees whose
/// values an earlier transaction derived over several iterations, so that the
/// inner join computes output for the same later iteration twice.
///
/// # Returns
///
/// The workloads, each a list of transactions.
fn tree_triggers() -> Vec<Vec<Transaction<TreeInput>>> {
    // Sets leaf `x` of a tree from NULL to 10.
    let set_x = |tree| set_leaf(tree, 0, None, Some(10));
    vec![
        // `x` of the uneven tree arrives in a later transaction.
        vec![vec![uneven_tree(None)], vec![set_x(3)]],
        // The same in one transaction: a control, since a recursion's first
        // run computes no output ahead of time.
        vec![vec![uneven_tree(Some(10))]],
        // Three trees, loaded one tree per step, then `x` of trees 1 and 3
        // set in two steps.
        vec![three_trees(), vec![set_x(1), set_x(3)]],
        // Shared deep child: both leaves change in one step, then in two
        // steps of one transaction, then back to NULL.
        vec![
            vec![shared_deep_child()],
            vec![TreeInput {
                leaves: [
                    set_leaf(0, 0, None, Some(2)).leaves,
                    set_leaf(0, 1, None, Some(3)).leaves,
                ]
                .concat(),
                ..TreeInput::default()
            }],
            vec![
                set_leaf(0, 0, Some(2), Some(4)),
                set_leaf(0, 1, Some(3), Some(5)),
            ],
            vec![set_leaf(0, 0, Some(4), None), set_leaf(0, 1, Some(5), None)],
        ],
    ]
}

/// A node's definition in a well-formed tree.
#[derive(Clone, Debug)]
enum Definition {
    Leaf(Option<i64>),
    /// `(is_add, left, right)`
    Inner(bool, u64, u64),
    /// The tree whose total the node reads.
    Ref(u64),
}

impl Definition {
    /// Adds the changes that insert or delete this definition of a node.
    ///
    /// # Arguments
    ///
    /// * `input` - the changes to add to.
    /// * `tree` - the node's tree.
    /// * `node` - the node.
    /// * `weight` - 1 to insert the definition, -1 to delete it.
    fn push(&self, input: &mut TreeInput, tree: u64, node: u64, weight: ZWeight) {
        match *self {
            Definition::Leaf(value) => input.leaves.push((Tup3(tree, node, value), weight)),
            Definition::Inner(is_add, left, right) => input
                .inner
                .push((Tup5(tree, node, is_add, left, right), weight)),
            Definition::Ref(target) => input.refs.push((Tup3(tree, node, target), weight)),
        }
    }
}

/// Generates a definition, or its absence, for a node of a well-formed tree.
///
/// # Arguments
///
/// * `tree` - the node's tree; references read trees with smaller ids.
/// * `node` - the node; inner nodes read nodes with smaller ids.
///
/// # Returns
///
/// A strategy for definitions.
fn definition(tree: u64, node: u64) -> BoxedStrategy<Option<Definition>> {
    let mut options = vec![
        Just(None).boxed(),
        prop::option::of(0..3i64)
            .prop_map(|value| Some(Definition::Leaf(value)))
            .boxed(),
    ];
    if node > 0 {
        options.push(
            (any::<bool>(), 0..node, 0..node)
                .prop_map(|(is_add, left, right)| Some(Definition::Inner(is_add, left, right)))
                .boxed(),
        );
    }
    if tree > 0 {
        options.push(
            (0..tree)
                .prop_map(|target| Some(Definition::Ref(target)))
                .boxed(),
        );
    }
    prop::strategy::Union::new(options).boxed()
}

/// Generates workloads over up to 3 well-formed trees of up to 6 nodes: every
/// node has at most one definition, and every tree at most one root.  A step
/// replaces the definitions of some nodes and the roots of some trees.
///
/// # Returns
///
/// A strategy for workloads: up to five transactions of up to three steps.
fn well_formed_workloads() -> impl Strategy<Value = Vec<Transaction<TreeInput>>> {
    let node_change = (0..3u64, 0..6u64)
        .prop_flat_map(|(tree, node)| (Just(tree), Just(node), definition(tree, node)));
    let root_change = (0..3u64, prop::option::of(0..6u64));
    let step = (
        prop::collection::vec(node_change, 0..4),
        prop::collection::vec(root_change, 0..2),
    );
    workloads(step, 5, 3).prop_map(|raw| {
        let mut definitions: BTreeMap<(u64, u64), Definition> = BTreeMap::new();
        let mut roots = BTreeMap::new();
        map_steps(raw, |(node_changes, root_changes)| {
            let mut input = TreeInput::default();
            for (tree, node, definition) in node_changes {
                if let Some(old) = definitions.remove(&(tree, node)) {
                    old.push(&mut input, tree, node, -1);
                }
                if let Some(new) = definition {
                    new.push(&mut input, tree, node, 1);
                    definitions.insert((tree, node), new);
                }
            }
            for (tree, root) in root_changes {
                if let Some(old) = roots.remove(&tree) {
                    input.roots.push((Tup2(tree, old), -1));
                }
                if let Some(new) = root {
                    input.roots.push((Tup2(tree, new), 1));
                    roots.insert(tree, new);
                }
            }
            input
        })
    })
}

/// Tree evaluation survives the scripted workloads under both plans and
/// every configuration.
#[test]
fn tree_evaluation_triggers() {
    for plan in [Plan::ByChild, Plan::ByTree] {
        for workload in tree_triggers() {
            for config in configs() {
                check(&TreeEvaluation(plan), &workload, config);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    /// Tree evaluation over random well-formed forests, joining through
    /// definitions.
    #[test]
    fn tree_evaluation_by_child_random(
        workload in well_formed_workloads(),
        config in any_config()
    ) {
        check(&TreeEvaluation(Plan::ByChild), &workload, config);
    }

    /// Tree evaluation over random well-formed forests, pairing the values of
    /// a tree.
    #[test]
    fn tree_evaluation_by_tree_random(workload in well_formed_workloads(), config in any_config()) {
        check(&TreeEvaluation(Plan::ByTree), &workload, config);
    }
}
