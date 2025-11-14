//! Library to parse and compare Feldera SQL dataflow graphs.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

mod hir;
mod lir;
mod mir;

pub use hir::{CalciteId, CalcitePlan, Condition, Op, Operand, Rel};
pub use lir::{LirCircuit, LirEdge, LirNode, LirNodeId, LirStreamId};
pub use mir::{MirInput, MirNode, MirNodeId};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema, Debug, Eq, PartialEq, Clone, Copy)]
#[cfg_attr(feature = "testing", derive(proptest_derive::Arbitrary))]
pub struct SourcePosition {
    pub start_line_number: usize,
    pub start_column: usize,
    pub end_line_number: usize,
    pub end_column: usize,
}

/// Indicates what relations (views and tables) of a dataflow graph
/// are different when compared with another dataflow graph.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Hash)]
pub enum Changes {
    Added(String),
    Removed(String),
    Modified(String),
}

/// The JSON representation of a dataflow graph.
#[derive(Debug, Deserialize, Serialize, ToSchema, PartialEq, Eq, Clone)]
pub struct Dataflow {
    pub calcite_plan: HashMap<String, CalcitePlan>,
    pub mir: HashMap<MirNodeId, MirNode>,
}

impl Dataflow {
    pub fn new(
        calcite_plan: HashMap<String, CalcitePlan>,
        mir: HashMap<MirNodeId, MirNode>,
    ) -> Self {
        Self { calcite_plan, mir }
    }

    /// Reports the changes to relations (views and tables) of a dataflow graph when
    /// compared with another (newer/updated) dataflow graph.
    ///
    /// If the returned set is empty, the relations are unchanged and the dataflows
    /// are (should be) identical.
    pub fn diff(&self, other: &Dataflow) -> HashSet<Changes> {
        let mut changes = HashSet::new();
        let relation_hashes = self.ids_to_hashes(&self.relation_with_ids());
        let other_relation_hashes = other.ids_to_hashes(&other.relation_with_ids());

        for (relation, hashes) in relation_hashes.iter() {
            if let Some(other_hashes) = other_relation_hashes.get(relation) {
                if hashes != other_hashes {
                    changes.insert(Changes::Modified(relation.clone()));
                }
            } else {
                changes.insert(Changes::Removed(relation.clone()));
            }
        }

        for (relation, _hashes) in other_relation_hashes {
            if !relation_hashes.contains_key(&relation) {
                changes.insert(Changes::Added(relation));
            }
        }

        changes
    }

    /// Walks the calcite plan and returns a set of relations with their calcite node IDs
    /// they're dependent on.
    fn relation_with_ids(&self) -> HashMap<String, HashSet<usize>> {
        let mut relations_and_ids: HashMap<String, HashSet<usize>> = HashMap::new();
        for (key, cp) in self.calcite_plan.iter() {
            for rel in &cp.rels {
                relations_and_ids
                    .entry(key.clone())
                    .or_default()
                    .insert(rel.id);
            }
        }

        relations_and_ids
    }

    /// Given a set of relations and their associated IDs, returns a map of
    /// relation names to their corresponding persistent hashes.
    fn ids_to_hashes(
        &self,
        relations_and_ids: &HashMap<String, HashSet<usize>>,
    ) -> HashMap<String, HashSet<String>> {
        let mut hashes = HashMap::new();
        for node in self.mir.values() {
            let all_calcite_ids: Vec<usize> = node
                .calcite
                .as_ref()
                .map(|cid| cid.clone().into())
                .unwrap_or_default();
            let mut all_calcite_ids_of_node = HashSet::with_capacity(all_calcite_ids.len());
            for id in all_calcite_ids {
                all_calcite_ids_of_node.insert(id);
            }

            // Tables (unused ones) don't show up in calcite plan so we find them in the MIR
            // first
            if let Some(table) = node.table.as_ref() {
                if let Some(persistent_id) = &node.persistent_id {
                    hashes
                        .entry(table.clone())
                        .or_insert_with(HashSet::new)
                        .insert(persistent_id.clone());
                }
            }

            for (key, ids) in relations_and_ids {
                let node_is_dependency_for_relation = !ids.is_disjoint(&all_calcite_ids_of_node);
                if node_is_dependency_for_relation {
                    if let Some(persistent_id) = &node.persistent_id {
                        hashes
                            .entry(key.clone())
                            .or_insert_with(HashSet::new)
                            .insert(persistent_id.clone());
                    }
                }
            }
        }
        hashes
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use std::collections::HashSet;

    const SAMPLE_A: (&str, &str) = ("sample_a", include_str!("../test/sample_a.json"));
    const SAMPLE_B: (&str, &str) = ("sample_b", include_str!("../test/sample_b.json"));
    const SAMPLE_B_MOD1: (&str, &str) =
        ("sample_b_mod1", include_str!("../test/sample_b_mod1.json"));
    const SAMPLE_B_MOD2: (&str, &str) =
        ("sample_b_mod2", include_str!("../test/sample_b_mod2.json"));
    const SAMPLE_B_MOD3: (&str, &str) =
        ("sample_b_mod3", include_str!("../test/sample_b_mod3.json"));
    const SAMPLE_C: (&str, &str) = ("sample_c", include_str!("../test/sample_c.json"));
    const SAMPLES: &[(&str, &str)] = &[
        SAMPLE_A,
        SAMPLE_B,
        SAMPLE_B_MOD1,
        SAMPLE_B_MOD2,
        SAMPLE_B_MOD3,
        SAMPLE_C,
    ];

    #[test]
    fn can_parse_ir() {
        for (_name, json) in SAMPLES.iter() {
            //eprintln!("Parsing: {}", _name);
            let _plan: Dataflow = serde_json::from_str(json).unwrap();
            //eprintln!("{:#?}", _plan);
        }
    }

    #[test]
    fn can_get_relations_and_ids() {
        let plan: Dataflow = serde_json::from_str(SAMPLE_B.1).unwrap();
        let relations = plan.relation_with_ids();
        assert_eq!(relations["error_view"], HashSet::from([0]));

        let plan: Dataflow = serde_json::from_str(SAMPLE_A.1).unwrap();
        let relations = plan.relation_with_ids();
        assert_eq!(relations["error_view"], HashSet::from([0]));
        assert_eq!(
            relations["group_can_read"],
            HashSet::from([1, 2, 3, 4, 5, 6, 7])
        );
        assert_eq!(
            relations["group_can_write"],
            HashSet::from([8, 9, 10, 11, 12, 13])
        );
        assert_eq!(
            relations["user_can_read"],
            HashSet::from([14, 15, 16, 17, 18, 19, 20, 21])
        );
        assert_eq!(
            relations["user_can_write"],
            HashSet::from([22, 23, 24, 25, 26, 27, 28, 29])
        );
    }

    #[test]
    fn can_find_hashes_in_a() {
        let plan: Dataflow = serde_json::from_str(SAMPLE_A.1).unwrap();
        let relations = plan.relation_with_ids();
        let hashes = plan.ids_to_hashes(&relations);

        assert_eq!(
            hashes["error_view"],
            HashSet::from([
                "8b384059bdb44ad811ab341cc5e2a59697f39aac7b463cab027b185db8105e73".to_string(),
                "933ebf782e1fe804fe85c4d0f3688bdb5234b386c2834892776e692acd9781d9".to_string()
            ])
        );

        assert_eq!(
            hashes["group_file_viewer"],
            HashSet::from([
                "44b862944cb9ff1772f75112d6b74d87bcbbe770502fe47f91b07c0bb3987bb3".to_string()
            ])
        );

        assert_eq!(
            hashes["user_can_write"],
            HashSet::from([
                "2f90ee4cdb4895d44ac7efb7104402dcf39a5fcfbe90492cc95311d4c70f623e".to_string(),
                "db1532ae31ea981721261c4a3892a6f373f98ecce41c59b2b8a5f5186c3c7d69".to_string(),
                "53944e28b6a21187dccb34ee9859bddbb4266157b21541feb2e4166a4034e907".to_string(),
                "61a52a49c5285c66a9656f211205002d48bd282b9f7f48be666f7fe7c208a338".to_string(),
                "739c3d0dafe5c2f650824df3529602ddae8acfa8789b35d80ea3eb7c3b156796".to_string(),
                "e989408d6aaecac2943caac41fdab83aca539622526b4289303c6a4de6eb658f".to_string(),
                "71d57c70dd7a5e3ae6da1adc565f9c119f93d4dcd50c61573a117d5d9aac3389".to_string(),
                "a8918a1fd4c90f6091dade7d6a44d46bb72809da98262781240ca1be5d738271".to_string(),
                "94b255b29c463d2918fe4a8c23cc75e943cb10e486d21942c8cb8c124c31eb7f".to_string(),
            ])
        );
    }

    #[test]
    fn unchanged_diff_is_empty() {
        for (_name, json) in SAMPLES.iter() {
            let plan: Dataflow = serde_json::from_str(json).unwrap();
            assert!(plan.diff(&plan).is_empty());
        }
    }

    fn diff(json1: &str, json2: &str) -> HashSet<Changes> {
        let plan1: Dataflow = serde_json::from_str(json1).unwrap();
        let plan2: Dataflow = serde_json::from_str(json2).unwrap();
        plan1.diff(&plan2)
    }

    #[test]
    fn change_only_view() {
        let diff = diff(SAMPLE_B.1, SAMPLE_B_MOD1.1);
        assert_eq!(
            diff,
            HashSet::from([Changes::Modified("example_count".to_string())])
        );
    }

    #[test]
    fn change_table() {
        let diff = diff(SAMPLE_B.1, SAMPLE_B_MOD2.1);
        assert_eq!(
            diff,
            HashSet::from([
                Changes::Modified("example".to_string()),
                Changes::Modified("example_count".to_string())
            ])
        );
    }

    #[test]
    fn add_table() {
        let diff = diff(SAMPLE_B.1, SAMPLE_B_MOD3.1);
        assert_eq!(
            diff,
            HashSet::from([
                // tables that aren't used by any views currently don't appear in the graph
                Changes::Added("example_new".to_string()),
                Changes::Added("example_view_count".to_string())
            ])
        );
    }

    #[test]
    fn remove_table() {
        let diff = diff(SAMPLE_B_MOD3.1, SAMPLE_B.1);
        assert_eq!(
            diff,
            HashSet::from([
                Changes::Removed("example_new".to_string()),
                Changes::Removed("example_view_count".to_string())
            ])
        );
    }
}
