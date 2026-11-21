//! Carry the hashes of recursive components and retention policies into the
//! next Merkle round. Global and local hashes occupy separate annotation slots.

use crate::stream_rewrites::body_members;
use crate::trace_retention::retention_slot;
use feldera_sql_ir::{BodyOrder, Circuit, Error, NodeId, OperatorKind as Op};
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;

type Hashes = BTreeMap<u64, String>;

fn number(id: NodeId) -> u64 {
    let NodeId::Outer(id) = id else {
        unreachable!("operator has an outer identity")
    };
    id
}

fn get(hashes: &Hashes, id: NodeId) -> Result<&str, Error> {
    hashes
        .get(&number(id))
        .map(String::as_str)
        .ok_or_else(|| Error::new(format!("missing Merkle hash for {id:?}")))
}

#[derive(Deserialize)]
pub struct PrefixInputs {
    hashes: Hashes,
    prefixes: Hashes,
}

impl PrefixInputs {
    /// Recursive imports take the first-round hash of their enclosing component.
    pub fn delta_prefixes(mut self, circuit: &Circuit) -> Result<Hashes, Error> {
        for (body, id) in body_members(circuit)? {
            if circuit.node(id).and_then(|op| op.operator_kind()) == Some(Op::Delta) {
                if circuit.node(body).and_then(|op| op.operator_kind()) != Some(Op::Nested) {
                    return Err(Error::new(
                        "Delta input must belong to a recursive component",
                    ));
                }
                let prefix = get(&self.hashes, body)?.to_owned();
                if self.prefixes.insert(number(id), prefix).is_some() {
                    return Err(Error::new("Delta input already has a hash prefix"));
                }
            }
        }
        Ok(self.prefixes)
    }

    /// The order of the policies in the circuit cannot change the persistent ID
    /// of their trace. A recursive trace keeps its component prefix first.
    pub fn gc_prefixes(mut self, circuit: &Circuit) -> Result<Hashes, Error> {
        let mut by_trace = BTreeMap::<u64, Vec<&str>>::new();
        for id in circuit.operator_order(BodyOrder::Postorder)? {
            let node = circuit.node(id).expect("body member");
            if retention_slot(node.operator_kind()).is_some() {
                let data = node
                    .inputs()
                    .next()
                    .ok_or_else(|| Error::new("missing retention data"))?;
                by_trace
                    .entry(number(data.operator))
                    .or_default()
                    .push(get(&self.hashes, id)?);
            }
        }
        for (trace, mut hashes) in by_trace {
            hashes.sort_unstable();
            if let Some(component) = self.prefixes.get(&trace) {
                hashes.insert(0, component);
            }
            self.prefixes.insert(trace, hashes.join(";"));
        }
        Ok(self.prefixes)
    }
}

#[derive(Deserialize)]
pub struct ComputedHashes {
    global: bool,
    hashes: Hashes,
}

impl ComputedHashes {
    /// Replace stale IDs at their original positions when rehashing a checkpoint.
    pub fn annotate(self, mut circuit: Circuit) -> Result<Circuit, Error> {
        for id in circuit.operator_order(BodyOrder::Postorder)? {
            let hash = get(&self.hashes, id)?;
            let replacement = json!({"class":"OperatorHash", "hash":hash, "global":self.global});
            let annotations = circuit.annotations_mut(id)?;
            if let Some(existing) = annotations.iter_mut().find(|a| {
                a["class"] == "OperatorHash" && a["global"].as_bool() == Some(self.global)
            }) {
                *existing = replacement;
            } else {
                annotations.push(replacement);
            }
        }
        Ok(circuit)
    }
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PersistentIdFinding {
    Missing { operator: u64 },
    Empty { operator: u64 },
    Duplicate { operator: u64, previous: u64 },
    IdenticalViews { operator: u64, previous: u64 },
}

/// Return warnings in traversal order through the first invalid ID. Two views
/// have identical contents only when they read the same numbered output.
pub fn check_persistent_ids(circuit: &Circuit) -> Result<Vec<PersistentIdFinding>, Error> {
    use std::collections::{hash_map::Entry, HashMap};
    use PersistentIdFinding::*;
    let mut producers = HashMap::new();
    let mut sinks = HashMap::new();
    let mut findings = Vec::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("body member");
        let operator = number(id);
        let Some(hash) = node.operator_hash(true)? else {
            findings.push(Missing { operator });
            break;
        };
        if hash.is_empty() {
            findings.push(Empty { operator });
            break;
        }
        if let Some(previous) = producers.insert(hash, operator) {
            findings.push(Duplicate { operator, previous });
            break;
        }
        if node.operator_kind() == Some(Op::Sink)
            && !crate::recursive_views::metadata_field(node, "system")?
                .as_bool()
                .ok_or_else(|| Error::new("sink requires a system flag"))?
        {
            let port = node
                .inputs()
                .next()
                .ok_or_else(|| Error::new("missing view input"))?;
            let Some(input_hash) = circuit
                .node(port.operator)
                .expect("validated producer")
                .operator_hash(true)?
            else {
                findings.push(Missing {
                    operator: number(port.operator),
                });
                break;
            };
            match sinks.entry((input_hash, port.number)) {
                Entry::Vacant(first) => {
                    first.insert(operator);
                }
                Entry::Occupied(first) => findings.push(IdenticalViews {
                    operator,
                    previous: *first.get(),
                }),
            }
        }
    }
    Ok(findings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn global(hash: &str) -> Value {
        json!({"class":"OperatorHash","global":true,"hash":hash})
    }

    fn view(id: u64, source: usize, system: bool) -> Value {
        json!({"class":"DBSPSinkOperator","id":id,"annotations":[global(&id.to_string())],
            "metadata":{"system":system}, "inputs":[{"operator":{"node":1},"outputNumber":source}]})
    }

    #[test]
    fn identical_views_use_the_first_user_view_on_each_numbered_port() {
        let circuit = Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[],
            "allOperators":[{"class":"DBSPInputMapWithWaterlineOperator","id":1,"inputs":[],"annotations":[global("table")]},
            view(2,0,true),view(3,0,false),view(4,1,false),view(5,0,false),view(6,1,false)]})).unwrap();
        assert_eq!(
            check_persistent_ids(&circuit).unwrap(),
            vec![
                PersistentIdFinding::IdenticalViews {
                    operator: 5,
                    previous: 3
                },
                PersistentIdFinding::IdenticalViews {
                    operator: 6,
                    previous: 4
                }
            ]
        );
    }

    #[test]
    fn local_hashes_do_not_satisfy_global_identity_and_rehash_preserves_order() {
        let local = json!({"class":"OperatorHash","global":false,"hash":"local"});
        let a = json!({"class":"CompactName","name":"s1"});
        let mut circuit = Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[],
            "allOperators":[{"class":"DBSPConstantOperator","id":1,"inputs":[],"annotations":[local,a]},
            {"class":"DBSPConstantOperator","id":2,"inputs":[],"annotations":[global("old"),a]}]})).unwrap();
        assert_eq!(
            check_persistent_ids(&circuit).unwrap(),
            vec![PersistentIdFinding::Missing { operator: 1 }]
        );
        let hashes = ComputedHashes {
            global: true,
            hashes: [(1, String::from("same")), (2, String::from("same"))].into(),
        };
        circuit = hashes.annotate(circuit).unwrap();
        assert_eq!(
            check_persistent_ids(&circuit).unwrap(),
            vec![PersistentIdFinding::Duplicate {
                operator: 2,
                previous: 1
            }]
        );
        assert_eq!(
            circuit.to_json()["allOperators"][1]["annotations"],
            json!([global("same"), a])
        );
        circuit.annotations_mut(NodeId::Outer(1)).unwrap()[2] = global("");
        assert_eq!(
            check_persistent_ids(&circuit).unwrap(),
            vec![PersistentIdFinding::Empty { operator: 1 }]
        );
    }
}
