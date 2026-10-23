//! Convert `BootstrapInfo` and `ConcurrentRestoreOutcome` structs returned by the DBSP
//! engine to the engine-independent representation declared in `feldera_types`.

use std::collections::BTreeSet;

use dbsp::DBSPHandle;
use dbsp::circuit::bootstrap::{
    BootstrapInfo as DbspBootstrapInfo, ConcurrentRestoreOutcome as DbspConcurrentRestoreOutcome,
};
use feldera_adapterlib::catalog::CircuitCatalog;
use feldera_types::circuit::{BootstrapInfo, ConcurrentRestoreOutcome};
use feldera_types::program_schema::SqlIdentifier;

/// The input tables of a circuit, each with the persistent id of its input operator.
#[derive(Clone, Debug, Default)]
pub(crate) struct InputOperators(Vec<(SqlIdentifier, String)>);

impl InputOperators {
    /// The input tables of `catalog` whose input operators have persistent ids.
    pub(crate) fn new(catalog: &dyn CircuitCatalog) -> Self {
        Self(
            catalog
                .input_iter()
                .filter_map(|(name, input)| {
                    input
                        .persistent_id
                        .as_ref()
                        .map(|id| (name.clone(), id.clone()))
                })
                .collect(),
        )
    }
}

/// Read `circuit`'s bootstrap info and convert it to the engine-independent `BootstrapInfo` format.
///
/// # Arguments
///
/// * `circuit` - DBSP circuit.
/// * `inputs` - input table to persistent ID map used for the conversion.
pub(crate) fn bootstrap_info(
    circuit: &DBSPHandle,
    inputs: &InputOperators,
) -> Option<BootstrapInfo> {
    circuit
        .bootstrap_info()
        .as_ref()
        .map(|info| convert_bootstrap_info(info, inputs))
}

/// Convert the `outcome` of a concurrent bootstrap to the engine-independent format.
///
/// # Arguments
///
/// * `outcome` - outcome returned by the DBSP engine.
/// * `inputs` - input table to persistent ID map used for the conversion.
pub(crate) fn concurrent_restore_outcome(
    outcome: DbspConcurrentRestoreOutcome,
    inputs: &InputOperators,
) -> ConcurrentRestoreOutcome {
    match outcome {
        DbspConcurrentRestoreOutcome::UpToDate => ConcurrentRestoreOutcome::UpToDate,
        DbspConcurrentRestoreOutcome::Concurrent(info) => {
            ConcurrentRestoreOutcome::Concurrent(convert_bootstrap_info(&info, inputs))
        }
        DbspConcurrentRestoreOutcome::FellBack { reason, info } => {
            ConcurrentRestoreOutcome::FellBack {
                reason,
                info: info.map(|info| convert_bootstrap_info(&info, inputs)),
            }
        }
    }
}

/// Convert bootstrap info in the DBSP format to the engine-independent `BootstrapInfo` format.
///
/// # Arguments
///
/// * `inputs` - input table to persistent ID map used for the conversion.
fn convert_bootstrap_info(info: &DbspBootstrapInfo, inputs: &InputOperators) -> BootstrapInfo {
    let backfill_persistent_ids: BTreeSet<String> =
        info.need_backfill.values().flatten().cloned().collect();
    let backfill_tables = inputs
        .0
        .iter()
        .filter(|(_, id)| backfill_persistent_ids.contains(id))
        .map(|(name, _)| name.clone())
        .collect();
    BootstrapInfo {
        backfill_tables,
        backfill_persistent_ids,
    }
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, BTreeSet};

    use dbsp::circuit::bootstrap::{
        BootstrapInfo as DbspBootstrapInfo,
        ConcurrentRestoreOutcome as DbspConcurrentRestoreOutcome,
    };
    use dbsp::circuit::{CircuitConfig, NodeId};
    use feldera_types::circuit::{BootstrapInfo, ConcurrentRestoreOutcome};
    use feldera_types::program_schema::SqlIdentifier;

    use super::{InputOperators, concurrent_restore_outcome, convert_bootstrap_info};
    use crate::test::pipeline::TestCircuit;
    use crate::test::{StaticTestEngine, TestStruct};

    /// dbsp's info for a bootstrap that rebuilds the operators with `persistent_ids`, and one
    /// operator without a persistent id.
    fn dbsp_info(persistent_ids: &[&str]) -> DbspBootstrapInfo {
        let mut need_backfill: BTreeMap<NodeId, Option<String>> = persistent_ids
            .iter()
            .enumerate()
            .map(|(node, id)| (NodeId::new(node), Some(id.to_string())))
            .collect();
        need_backfill.insert(NodeId::new(persistent_ids.len()), None);
        DbspBootstrapInfo {
            replay_sources: BTreeMap::new(),
            need_backfill,
        }
    }

    /// A table is rebuilt exactly when the bootstrap rebuilds its input operator, and a
    /// concurrent outcome converts its info the same way.
    #[test]
    fn a_table_is_rebuilt_when_its_input_operator_is() {
        let (circuit, catalog) = StaticTestEngine::new()
            .circuit(
                &TestCircuit::Identity {
                    relation: TestStruct::relation_schema(),
                    persistent_output_ids: vec![Some("output".to_string())],
                    input_properties: Vec::new(),
                    materialized: true,
                    persistent_ids: true,
                    table_version: 0,
                    attribute_defaults: Vec::new(),
                },
                CircuitConfig::with_workers(1),
            )
            .unwrap();
        let table = SqlIdentifier::from("test_input1");
        let input_id = catalog
            .input_collection_handle(&table)
            .unwrap()
            .persistent_id
            .clone()
            .expect("the test circuit's input operator has a persistent id");

        let inputs = InputOperators::new(catalog.as_ref());
        let info = convert_bootstrap_info(&dbsp_info(&[&input_id, "view"]), &inputs);
        assert_eq!(info.backfill_tables, BTreeSet::from([table.clone()]));
        assert_eq!(
            info.backfill_persistent_ids,
            BTreeSet::from([input_id.clone(), "view".to_string()])
        );

        let info = convert_bootstrap_info(&dbsp_info(&["view"]), &inputs);
        assert!(info.backfill_tables.is_empty());

        let outcome = concurrent_restore_outcome(
            DbspConcurrentRestoreOutcome::FellBack {
                reason: "refused".to_string(),
                info: Some(dbsp_info(&[&input_id])),
            },
            &inputs,
        );
        assert_eq!(
            outcome,
            ConcurrentRestoreOutcome::FellBack {
                reason: "refused".to_string(),
                info: Some(BootstrapInfo {
                    backfill_tables: BTreeSet::from([table]),
                    backfill_persistent_ids: BTreeSet::from([input_id]),
                }),
            }
        );
        assert_eq!(
            concurrent_restore_outcome(DbspConcurrentRestoreOutcome::UpToDate, &inputs),
            ConcurrentRestoreOutcome::UpToDate
        );

        circuit.kill().unwrap();
    }
}
