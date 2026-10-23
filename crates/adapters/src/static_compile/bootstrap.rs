//! dbsp's bootstrap info in the form that the controller reads.
//!
//! dbsp names the operators that a bootstrap rebuilds by node, each with its persistent id.
//! The controller wants the persistent ids, and the input tables among the rebuilt operators,
//! which the catalog finds by the persistent ids of their input operators.

use std::collections::BTreeSet;

use dbsp::DBSPHandle;
use dbsp::circuit::bootstrap::{
    BootstrapInfo as DbspBootstrapInfo, ConcurrentRestoreOutcome as DbspConcurrentRestoreOutcome,
};
use feldera_adapterlib::catalog::CircuitCatalog;
use feldera_types::circuit::{BootstrapInfo, ConcurrentRestoreOutcome};

/// What `circuit` rebuilds, if it is bootstrapping, with the tables named as in `catalog`.
pub(crate) fn bootstrap_info(
    circuit: &DBSPHandle,
    catalog: &dyn CircuitCatalog,
) -> Option<BootstrapInfo> {
    circuit
        .bootstrap_info()
        .as_ref()
        .map(|info| convert_bootstrap_info(info, catalog))
}

/// The `outcome` of a concurrent bootstrap, with the tables named as in `catalog`.
pub(crate) fn concurrent_restore_outcome(
    outcome: DbspConcurrentRestoreOutcome,
    catalog: &dyn CircuitCatalog,
) -> ConcurrentRestoreOutcome {
    match outcome {
        DbspConcurrentRestoreOutcome::UpToDate => ConcurrentRestoreOutcome::UpToDate,
        DbspConcurrentRestoreOutcome::Concurrent(info) => {
            ConcurrentRestoreOutcome::Concurrent(convert_bootstrap_info(&info, catalog))
        }
        DbspConcurrentRestoreOutcome::FellBack { reason, info } => {
            ConcurrentRestoreOutcome::FellBack {
                reason,
                info: info.map(|info| convert_bootstrap_info(&info, catalog)),
            }
        }
    }
}

/// The persistent ids of the operators that `info` rebuilds, and the input tables of `catalog`
/// whose input operators are among them.
fn convert_bootstrap_info(info: &DbspBootstrapInfo, catalog: &dyn CircuitCatalog) -> BootstrapInfo {
    let backfill_persistent_ids: BTreeSet<String> =
        info.need_backfill.values().flatten().cloned().collect();
    let backfill_tables = catalog
        .input_iter()
        .filter(|(_, input)| {
            input
                .persistent_id
                .as_ref()
                .is_some_and(|id| backfill_persistent_ids.contains(id))
        })
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

    use super::{concurrent_restore_outcome, convert_bootstrap_info};
    use crate::test::{TestStruct, test_circuit};

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
        let (circuit, catalog) = test_circuit::<TestStruct>(
            CircuitConfig::with_workers(1),
            &TestStruct::schema(),
            &[Some("output")],
        );
        let table = SqlIdentifier::from("test_input1");
        let input_id = catalog
            .input_collection_handle(&table)
            .unwrap()
            .persistent_id
            .clone()
            .expect("the test circuit's input operator has a persistent id");

        let info = convert_bootstrap_info(&dbsp_info(&[&input_id, "view"]), catalog.as_ref());
        assert_eq!(info.backfill_tables, BTreeSet::from([table.clone()]));
        assert_eq!(
            info.backfill_persistent_ids,
            BTreeSet::from([input_id.clone(), "view".to_string()])
        );

        let info = convert_bootstrap_info(&dbsp_info(&["view"]), catalog.as_ref());
        assert!(info.backfill_tables.is_empty());

        let outcome = concurrent_restore_outcome(
            DbspConcurrentRestoreOutcome::FellBack {
                reason: "refused".to_string(),
                info: Some(dbsp_info(&[&input_id])),
            },
            catalog.as_ref(),
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
            concurrent_restore_outcome(DbspConcurrentRestoreOutcome::UpToDate, catalog.as_ref()),
            ConcurrentRestoreOutcome::UpToDate
        );

        circuit.kill().unwrap();
    }
}
