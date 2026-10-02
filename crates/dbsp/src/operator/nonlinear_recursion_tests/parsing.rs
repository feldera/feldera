//! Parsing with a context-free grammar in Chomsky normal form (CYK).
//!
//! A nonterminal derives a token by a unary rule `A -> t`, and a span of
//! tokens by a binary rule `A -> B C` when `B` derives the span's start and
//! `C` the rest.  In the rules below:
//!
//! * `tokens(s, i, t)`: sentence `s` has token `t` at position `i`.
//! * `unary(a, t)`: the grammar has the rule `a -> t`.
//! * `binary(a, b, c)`: the grammar has the rule `a -> b c`.
//! * `parses(s, a, i, j)`: nonterminal `a` derives tokens `i` to `j - 1` of
//!   sentence `s`.
//!
//! ```text
//! parses(s, a, i, i + 1) :- tokens(s, i, t), unary(a, t).
//! parses(s, a, i, k) :-
//!     parses(s, b, i, j),
//!     parses(s, c, j, k),
//!     binary(a, b, c).
//! ```
//!
//! The join that finds adjacent spans pairs two spans the recursion derived,
//! so both of its inputs depend on the recursion.  With the grammar
//! `S -> S S | a`, the recursion derives spans of 2, 3-4, and 5-8 tokens in
//! iterations 1, 2, and 3.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use super::harness::{
    Program, Transaction, ZSet, any_config, apply_proposals, check, configs, fixpoint, map_steps,
    proposals, read_zset, set_after, set_zset, workloads,
};
use crate::{
    OutputHandle, RootCircuit, Stream, ZSetHandle, ZWeight,
    typed_batch::{OrdZSet, SpineSnapshot},
    utils::{Tup2, Tup3, Tup4, test::CIRCUIT_CASES},
};

/// A token of a sentence: `(sentence, position, token)`.
type Token = Tup3<u64, u64, u64>;

/// A unary rule `nonterminal -> token`: `(nonterminal, token)`.
type Unary = Tup2<u64, u64>;

/// A binary rule `a -> b c`: `(a, b, c)`.
type Binary = Tup3<u64, u64, u64>;

/// A span that a nonterminal derives: `(sentence, nonterminal, start,
/// end)`, with `end` exclusive.
type Parse = Tup4<u64, u64, u64, u64>;

/// Changes to the three inputs in one step.
#[derive(Clone, Debug, Default)]
struct ParseInput {
    tokens: Vec<(Token, ZWeight)>,
    unary: Vec<(Unary, ZWeight)>,
    binary: Vec<(Binary, ZWeight)>,
}

/// CYK parsing of sentences.
#[derive(Clone)]
struct Cyk;

impl Program for Cyk {
    type Input = ParseInput;
    type Handles = (
        ZSetHandle<Token>,
        ZSetHandle<Unary>,
        ZSetHandle<Binary>,
        OutputHandle<SpineSnapshot<OrdZSet<Parse>>>,
    );
    type Output = ZSet<Parse>;

    fn build(&self, circuit: &mut RootCircuit) -> Self::Handles {
        let (tokens, tokens_handle) = circuit.add_input_zset::<Token>();
        let (unary, unary_handle) = circuit.add_input_zset::<Unary>();
        let (binary, binary_handle) = circuit.add_input_zset::<Binary>();
        let parses = circuit
            .recursive(|child, parses: Stream<_, OrdZSet<Parse>>| {
                let leaves = tokens
                    .delta0(child)
                    .map_index(|Tup3(sentence, position, token)| {
                        (*token, Tup2(*sentence, *position))
                    })
                    .join(
                        &unary
                            .delta0(child)
                            .map_index(|Tup2(nonterminal, token)| (*token, *nonterminal)),
                        |_token, Tup2(sentence, position), nonterminal| {
                            Tup4(*sentence, *nonterminal, *position, position + 1)
                        },
                    );
                let ending = parses.map_index(|Tup4(sentence, b, start, split)| {
                    (Tup2(*sentence, *split), Tup2(*b, *start))
                });
                let starting = parses.map_index(|Tup4(sentence, c, split, end)| {
                    (Tup2(*sentence, *split), Tup2(*c, *end))
                });
                // Adjacent spans: both inputs depend on the recursion.
                let adjacent = ending.join_index(
                    &starting,
                    |Tup2(sentence, _split), Tup2(b, start), Tup2(c, end)| {
                        Some((Tup2(*b, *c), Tup3(*sentence, *start, *end)))
                    },
                );
                let combined = adjacent.join(
                    &binary
                        .delta0(child)
                        .map_index(|Tup3(a, b, c)| (Tup2(*b, *c), *a)),
                    |_bc, Tup3(sentence, start, end), a| Tup4(*sentence, *a, *start, *end),
                );
                Ok(leaves.plus(&combined))
            })
            .unwrap();
        (
            tokens_handle,
            unary_handle,
            binary_handle,
            parses.accumulate_integrate().accumulate_output(),
        )
    }

    fn push(&self, (tokens, unary, binary, _): &Self::Handles, input: &ParseInput) {
        for (row, weight) in &input.tokens {
            tokens.push(*row, *weight);
        }
        for (row, weight) in &input.unary {
            unary.push(*row, *weight);
        }
        for (row, weight) in &input.binary {
            binary.push(*row, *weight);
        }
    }

    fn read(&self, (_, _, _, parses): &Self::Handles) -> ZSet<Parse> {
        read_zset(parses)
    }

    fn model(&self, inputs: &[ParseInput]) -> ZSet<Parse> {
        let tokens = set_after(inputs.iter().map(|input| input.tokens.as_slice()));
        let unary = set_after(inputs.iter().map(|input| input.unary.as_slice()));
        let binary = set_after(inputs.iter().map(|input| input.binary.as_slice()));
        let mut leaves = BTreeSet::new();
        for Tup3(sentence, position, token) in &tokens {
            for Tup2(nonterminal, rule_token) in &unary {
                if rule_token == token {
                    leaves.insert(Tup4(*sentence, *nonterminal, *position, position + 1));
                }
            }
        }
        let parses = fixpoint(BTreeSet::new(), |parses: &BTreeSet<Parse>| {
            let mut starting: BTreeMap<(u64, u64), Vec<(u64, u64)>> = BTreeMap::new();
            for Tup4(sentence, c, split, end) in parses {
                starting
                    .entry((*sentence, *split))
                    .or_default()
                    .push((*c, *end));
            }
            let mut next = leaves.clone();
            for Tup4(sentence, b, start, split) in parses {
                for (c, end) in starting.get(&(*sentence, *split)).into_iter().flatten() {
                    for Tup3(a, rule_b, rule_c) in &binary {
                        if rule_b == b && rule_c == c {
                            next.insert(Tup4(*sentence, *a, *start, *end));
                        }
                    }
                }
            }
            next
        });
        set_zset(&parses)
    }
}

/// Changes that insert or delete tokens of sentence 0.
///
/// # Arguments
///
/// * `positions` - the positions of the tokens, all of which are token 0.
/// * `weight` - 1 to insert, -1 to delete.
///
/// # Returns
///
/// The changes.
fn tokens(positions: impl IntoIterator<Item = u64>, weight: ZWeight) -> ParseInput {
    ParseInput {
        tokens: positions
            .into_iter()
            .map(|position| (Tup3(0, position, 0), weight))
            .collect(),
        ..ParseInput::default()
    }
}

/// Workloads whose later transactions make the adjacent-span join compute
/// output for the same later iteration in two iterations of one run.
///
/// The grammar is `S -> S S | a`, and the first transaction parses a sentence
/// of eight `a`s, deriving spans in iterations 0-3.  A token appended at
/// position 8 derives spans ending there in consecutive iterations, and each
/// of them meets spans of the old sentence derived in later iterations.  The
/// grammar is ambiguous, so every span has many derivations, and losing some
/// of them loses no span; deleting a token must retract every derivation of
/// the spans over it, which exposes any that were lost.
///
/// # Returns
///
/// The workloads, each a list of transactions.
fn cyk_triggers() -> Vec<Vec<Transaction<ParseInput>>> {
    let grammar = ParseInput {
        unary: vec![(Tup2(0, 0), 1)],
        binary: vec![(Tup3(0, 0, 0), 1)],
        ..ParseInput::default()
    };
    let setup = vec![grammar, tokens(0..8, 1)];
    vec![
        // Append a token, then delete the first one.
        vec![setup.clone(), vec![tokens([8], 1)], vec![tokens([0], -1)]],
        // Append two tokens, in two steps of one transaction, then delete one
        // in the middle.
        vec![
            setup.clone(),
            vec![tokens([8], 1), tokens([9], 1)],
            vec![tokens([4], -1)],
        ],
        // Append a token, then remove and restore one in the middle.
        vec![
            setup.clone(),
            vec![tokens([8], 1)],
            vec![tokens([4], -1)],
            vec![tokens([4], 1)],
        ],
        // A second nonterminal that the old sentence can derive too, then
        // a token deleted in the middle.
        vec![
            setup,
            vec![ParseInput {
                unary: vec![(Tup2(1, 0), 1)],
                binary: vec![(Tup3(1, 1, 0), 1)],
                ..ParseInput::default()
            }],
            vec![tokens([4], -1)],
        ],
    ]
}

/// Generates workloads over 2 sentences of up to 6 tokens from 2 tokens, and
/// grammars over 3 nonterminals.  A position may hold several tokens, which
/// parses a lattice of sentences.
///
/// # Returns
///
/// A strategy for workloads: up to five transactions of up to three steps.
fn cyk_workloads() -> impl Strategy<Value = Vec<Transaction<ParseInput>>> {
    let token = (0..2u64, 0..6u64, 0..2u64)
        .prop_map(|(sentence, position, token)| Tup3(sentence, position, token));
    let unary = (0..3u64, 0..2u64).prop_map(|(nonterminal, token)| Tup2(nonterminal, token));
    let binary = (0..3u64, 0..3u64, 0..3u64).prop_map(|(a, b, c)| Tup3(a, b, c));
    let step = (
        proposals(token, 4),
        proposals(unary, 2),
        proposals(binary, 2),
    );
    workloads(step, 5, 3).prop_map(|raw| {
        let mut live = (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
        map_steps(raw, |(tokens, unary, binary)| ParseInput {
            tokens: apply_proposals(&mut live.0, &tokens),
            unary: apply_proposals(&mut live.1, &unary),
            binary: apply_proposals(&mut live.2, &binary),
        })
    })
}

/// CYK parsing survives its scripted triggers under every configuration.
#[test]
fn cyk_triggers_hold() {
    for workload in cyk_triggers() {
        for config in configs() {
            check(&Cyk, &workload, config);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    /// CYK parsing of random sentences with random grammars.
    #[test]
    fn cyk_random(workload in cyk_workloads(), config in any_config()) {
        check(&Cyk, &workload, config);
    }
}
