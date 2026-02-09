#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use crate::{
        Runtime,
        algebra::{
            DEFAULT_BRANCHING_FACTOR, F64,
            OrderStatisticsZSet,
        },
        indexed_zset,
        node_storage::NodeStorageConfig,
        operator::dynamic::percentile_op::{PercentileOperator, PercentileResult},
        trace::{BatchReader},
        utils::{Tup0, Tup2},
    };
    use feldera_storage::{FileCommitter, StoragePath};

    #[test]
    fn test_percentile_cont_basic() {
        // PERCENTILE_CONT requires floating-point types (implements Interpolate)
        let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
            let output = input.percentile_cont_stateful(None, &[0.5], true, |r| r[0].clone());
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Step 1: Insert values for key 1
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(10.0), 1)),
            Tup2(1, Tup2(F64::new(20.0), 1)),
            Tup2(1, Tup2(F64::new(30.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Median of [10.0, 20.0, 30.0] is 20.0
        assert_eq!(result, indexed_zset! { 1 => { Some(F64::new(20.0)) => 1 } });

        // Step 2: Add more values
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(40.0), 1)),
            Tup2(1, Tup2(F64::new(50.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Median of [10.0, 20.0, 30.0, 40.0, 50.0] is 30.0
        // Delta should be: remove old (20.0), add new (30.0)
        assert_eq!(
            result,
            indexed_zset! { 1 => { Some(F64::new(20.0)) => -1, Some(F64::new(30.0)) => 1 } }
        );

        // Step 3: Delete a value
        input.append(&mut vec![Tup2(1, Tup2(F64::new(30.0), -1))]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Median of [10.0, 20.0, 40.0, 50.0] with interpolation:
        // pos = 0.5 * (4-1) = 1.5, lower_idx = 1, upper_idx = 2
        // values are 20.0 and 40.0, fraction = 0.5
        // interpolated = 20.0 + 0.5 * (40.0 - 20.0) = 30.0
        assert_eq!(
            result,
            indexed_zset! { 1 => { Some(F64::new(30.0)) => -1, Some(F64::new(30.0)) => 1 } }
        );
    }

    #[test]
    fn test_percentile_disc_basic() {
        // PERCENTILE_DISC works with any ordered type (no Interpolate required)
        let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, i32>();
            let output = input.percentile_disc_stateful(None, &[0.5], true, |r| r[0].clone());
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Step 1: Insert values for key 1
        input.append(&mut vec![
            Tup2(1, Tup2(10, 1)),
            Tup2(1, Tup2(20, 1)),
            Tup2(1, Tup2(30, 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Median of [10, 20, 30] = 20 (discrete, no interpolation)
        assert_eq!(result, indexed_zset! { 1 => { Some(20) => 1 } });
    }

    #[test]
    fn test_percentile_operator_multiple_keys() {
        let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, i32>();
            let output = input.percentile_disc_stateful(None, &[0.5], true, |r| r[0].clone());
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Insert values for multiple keys
        input.append(&mut vec![
            Tup2(1, Tup2(10, 1)),
            Tup2(1, Tup2(20, 1)),
            Tup2(1, Tup2(30, 1)),
            Tup2(2, Tup2(100, 1)),
            Tup2(2, Tup2(200, 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Key 1: Median of [10, 20, 30] = 20
        // Key 2: Median of [100, 200] = 100 or 200
        assert!(result.key_count() == 2);
    }

    #[test]
    fn test_percentile_operator_empty_group() {
        // Use F64 for PERCENTILE_CONT test
        let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
            let output = input.percentile_cont_stateful(None, &[0.5], true, |r| r[0].clone());
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Insert then delete all values
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(10.0), 1)),
            Tup2(1, Tup2(F64::new(20.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        assert_eq!(result.key_count(), 1);

        // Delete all values
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(10.0), -1)),
            Tup2(1, Tup2(F64::new(20.0), -1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Should emit deletion of the previous value and None for empty group
        assert!(result.key_count() > 0);
    }

    #[test]
    fn test_percentile_cont_interpolation() {
        // Test that PERCENTILE_CONT correctly interpolates between values
        let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
            let output = input.percentile_cont_stateful(None, &[0.5], true, |r| r[0].clone());
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // With 2 values, median should interpolate: (1.0 + 2.0) / 2 = 1.5
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(1.0), 1)),
            Tup2(1, Tup2(F64::new(2.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // For 2 elements at percentile 0.5: pos = 0.5 * 1 = 0.5
        // lower_idx = 0, upper_idx = 1, fraction = 0.5
        // interpolated = 1.0 + 0.5 * (2.0 - 1.0) = 1.5
        assert_eq!(result, indexed_zset! { 1 => { Some(F64::new(1.5)) => 1 } });
    }

    #[test]
    fn test_percentile_operator_checkpoint_restore() {
        use crate::circuit::operator_traits::Operator;
        use crate::circuit::{GlobalNodeId, NodeId};
        use crate::storage::backend::posixio_impl::PosixBackend;
        use tempfile::TempDir;

        // Create a temp directory for checkpoint files
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let checkpoint_dir = temp_dir.path();

        let backend: Arc<dyn crate::storage::backend::StorageBackend> =
            Arc::new(PosixBackend::new(
                checkpoint_dir.to_path_buf(),
                feldera_types::config::StorageCacheConfig::default(),
                &feldera_types::config::FileBackendConfig::default(),
            ));

        let storage_config = NodeStorageConfig {
            enable_spill: true,
            spill_threshold_bytes: 0,
            target_segment_size: 64 * 1024 * 1024,
            spill_directory: None,
            segment_path_prefix: String::new(),
            storage_backend: Some(backend),
            buffer_cache: None,
        };

        let global_id = GlobalNodeId::child(&GlobalNodeId::root(), NodeId::new(42));

        // Use the unified API with all-CONT
        let build_output = |r: &[PercentileResult<F64>]| -> Option<F64> {
            match &r[0] {
                PercentileResult::Cont(Some((lower, upper, fraction))) => {
                    let l = lower.into_inner();
                    let u = upper.into_inner();
                    Some(F64::new(l + fraction * (u - l)))
                }
                PercentileResult::Cont(None) => None,
                PercentileResult::Disc(_) => unreachable!(),
            }
        };

        let mut op1 = PercentileOperator::<i32, F64, Option<F64>, _>::new(
            vec![0.5], vec![true], true, build_output.clone(),
        );
        op1.storage_config = storage_config.clone();
        op1.global_id = global_id.clone();

        // Insert some data into the trees, each with unique segment_path_prefix
        let mut config1 = storage_config.clone();
        config1.segment_path_prefix = "t0_".to_string();
        let mut tree1 = OrderStatisticsZSet::with_config(DEFAULT_BRANCHING_FACTOR, config1);
        tree1.insert(F64::new(10.0), 1);
        tree1.insert(F64::new(20.0), 1);
        tree1.insert(F64::new(30.0), 1);
        op1.trees.insert(1, tree1);
        op1.tree_ids.insert(1, 0);

        let mut config2 = storage_config.clone();
        config2.segment_path_prefix = "t1_".to_string();
        let mut tree2 = OrderStatisticsZSet::with_config(DEFAULT_BRANCHING_FACTOR, config2);
        tree2.insert(F64::new(100.0), 1);
        tree2.insert(F64::new(200.0), 1);
        op1.trees.insert(2, tree2);
        op1.tree_ids.insert(2, 1);

        op1.next_tree_id = 2;

        // Set some prev_output
        op1.prev_output.insert(1, Some(F64::new(20.0)));
        op1.prev_output.insert(2, Some(F64::new(150.0)));

        // Verify the trees are set up correctly
        assert_eq!(op1.trees.len(), 2);
        assert_eq!(op1.trees.get(&1).unwrap().num_keys(), 3);
        assert_eq!(op1.trees.get(&2).unwrap().num_keys(), 2);

        // Checkpoint using the Operator trait method
        let base = StoragePath::default();
        let persistent_id = Some("test_op");
        let mut files: Vec<Arc<dyn FileCommitter>> = Vec::new();
        op1.checkpoint(&base, persistent_id, &mut files)
            .expect("checkpoint should work");

        // Create a new operator with different initial values (to verify restore overwrites)
        let mut op2 = PercentileOperator::<i32, F64, Option<F64>, _>::new(
            vec![0.0], vec![true], false, build_output,
        );
        op2.storage_config = storage_config.clone();
        op2.global_id = global_id.clone();

        // Restore using the Operator trait method
        op2.restore(&base, persistent_id)
            .expect("restore should work");

        // Verify the restored operator matches the original
        assert_eq!(op2.trees.len(), op1.trees.len());
        assert_eq!(op2.trees.get(&1).unwrap().num_keys(), 3);
        assert_eq!(op2.trees.get(&2).unwrap().num_keys(), 2);

        // verify next_tree_id is restored
        assert_eq!(op2.next_tree_id, 2, "next_tree_id should be restored");

        // Verify tree_ids are restored
        assert_eq!(op2.tree_ids.len(), 2);
        assert_eq!(op2.tree_ids.get(&1), Some(&0));
        assert_eq!(op2.tree_ids.get(&2), Some(&1));

        // Reload evicted leaves before iterating (after restore, leaves are on disk)
        for tree in op2.trees.values_mut() {
            tree.reload_evicted_leaves()
                .expect("Reloading evicted leaves should work");
        }

        // Verify tree contents
        let tree1_entries: Vec<_> = op2.trees.get(&1).unwrap().iter().collect();
        assert_eq!(tree1_entries.len(), 3);
        assert_eq!(*tree1_entries[0].0, F64::new(10.0));
        assert_eq!(*tree1_entries[1].0, F64::new(20.0));
        assert_eq!(*tree1_entries[2].0, F64::new(30.0));

        let tree2_entries: Vec<_> = op2.trees.get(&2).unwrap().iter().collect();
        assert_eq!(tree2_entries.len(), 2);
        assert_eq!(*tree2_entries[0].0, F64::new(100.0));
        assert_eq!(*tree2_entries[1].0, F64::new(200.0));

        // Verify prev_output
        assert_eq!(op2.prev_output.len(), 2);
        assert_eq!(op2.prev_output.get(&1), Some(&Some(F64::new(20.0))));
        assert_eq!(op2.prev_output.get(&2), Some(&Some(F64::new(150.0))));

        // Verify config
        assert_eq!(op2.percentiles, vec![0.5]);
        assert!(op2.ascending);
    }

    #[test]
    fn test_clock_end_triggers_flush() {
        use crate::circuit::operator_traits::Operator;
        use crate::circuit::{GlobalNodeId, NodeId};
        use crate::storage::backend::memory_impl::MemoryBackend;

        let backend: Arc<dyn crate::storage::backend::StorageBackend> =
            Arc::new(MemoryBackend::new());

        // Create operator with a very low spill threshold to force flushing
        let storage_config = NodeStorageConfig {
            enable_spill: true,
            spill_threshold_bytes: 0, // Force flush on any data
            target_segment_size: 64 * 1024 * 1024,
            spill_directory: None,
            segment_path_prefix: String::new(),
            storage_backend: Some(backend),
            buffer_cache: None,
        };

        let global_id = GlobalNodeId::child(&GlobalNodeId::root(), NodeId::new(99));

        let mut op = PercentileOperator::<i32, F64, Option<F64>, _>::new(
            vec![0.5], vec![true], true,
            |r: &[PercentileResult<F64>]| -> Option<F64> {
                match &r[0] {
                    PercentileResult::Cont(Some((lower, upper, fraction))) => {
                        let l = lower.into_inner();
                        let u = upper.into_inner();
                        Some(F64::new(l + fraction * (u - l)))
                    }
                    PercentileResult::Cont(None) => None,
                    PercentileResult::Disc(_) => unreachable!(),
                }
            },
        );
        op.storage_config = storage_config.clone();
        op.global_id = global_id.clone();

        // Insert data into trees directly
        let mut config1 = storage_config.clone();
        config1.segment_path_prefix = "t0_".to_string();
        let mut tree1 = OrderStatisticsZSet::with_config(DEFAULT_BRANCHING_FACTOR, config1);
        for i in 0..200 {
            tree1.insert(F64::new(i as f64), 1);
        }
        op.trees.insert(1, tree1);
        op.tree_ids.insert(1, 0);
        op.next_tree_id = 1;

        // Verify no evicted leaves before clock_end
        let evicted_before: usize = op.trees.values().map(|t| t.evicted_leaf_count()).sum();
        assert_eq!(evicted_before, 0);

        // Call clock_end - should trigger flush and eviction
        op.clock_end(0);

        // After clock_end, some leaves should be evicted
        let evicted_after: usize = op.trees.values().map(|t| t.evicted_leaf_count()).sum();
        assert!(evicted_after > 0, "clock_end should trigger eviction");

        // Verify tree still works correctly after eviction
        let tree = op.trees.get_mut(&1).unwrap();
        assert_eq!(tree.select_kth(0, true), Some(&F64::new(0.0)));
        assert_eq!(tree.select_kth(199, true), Some(&F64::new(199.0)));
    }

    /// Circuit-level checkpoint/restore test for PERCENTILE_CONT.
    #[test]
    fn test_percentile_cont_circuit_checkpoint_restore() {
        use crate::circuit::{CircuitConfig, CircuitStorageConfig};
        use anyhow::Result as AnyResult;
        use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
        use tempfile::tempdir;

        type CR = (
            crate::IndexedZSetHandle<i32, F64>,
            crate::OutputHandle<crate::OrdIndexedZSet<i32, Option<F64>>>,
        );

        fn constructor(circuit: &mut crate::RootCircuit) -> AnyResult<CR> {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
            let output = input.percentile_cont_stateful(Some("pct_cont"), &[0.5], true, |r| r[0].clone());
            Ok((input_handle, output.output()))
        }

        // Test data: 5 steps of input for multiple keys
        let steps: Vec<Vec<Tup2<i32, Tup2<F64, i64>>>> = vec![
            // Step 1: key=1 gets [10,20,30], key=2 gets [100,200]
            vec![
                Tup2(1, Tup2(F64::new(10.0), 1)),
                Tup2(1, Tup2(F64::new(20.0), 1)),
                Tup2(1, Tup2(F64::new(30.0), 1)),
                Tup2(2, Tup2(F64::new(100.0), 1)),
                Tup2(2, Tup2(F64::new(200.0), 1)),
            ],
            // Step 2: key=1 gets +40,+50; key=3 gets [5]
            vec![
                Tup2(1, Tup2(F64::new(40.0), 1)),
                Tup2(1, Tup2(F64::new(50.0), 1)),
                Tup2(3, Tup2(F64::new(5.0), 1)),
            ],
            // -- checkpoint after step 2 --
            // Step 3: key=1 delete 30; key=2 gets +300
            vec![
                Tup2(1, Tup2(F64::new(30.0), -1)),
                Tup2(2, Tup2(F64::new(300.0), 1)),
            ],
            // Step 4: key=3 gets +15; key=4 gets [42] (new group after restore)
            vec![
                Tup2(3, Tup2(F64::new(15.0), 1)),
                Tup2(4, Tup2(F64::new(42.0), 1)),
            ],
            // Step 5: key=2 emptied (delete all)
            vec![
                Tup2(2, Tup2(F64::new(100.0), -1)),
                Tup2(2, Tup2(F64::new(200.0), -1)),
                Tup2(2, Tup2(F64::new(300.0), -1)),
            ],
        ];

        // Reference run: all 5 steps without interruption
        let reference_outputs = {
            let (mut circuit, (input, output)) = Runtime::init_circuit(1, constructor).unwrap();
            let mut outputs = Vec::new();
            for step_data in &steps {
                input.append(&mut step_data.clone());
                circuit.transaction().unwrap();
                outputs.push(output.consolidate());
            }
            circuit.kill().unwrap();
            outputs
        };

        // Checkpoint run: steps 1-2, checkpoint, kill, restore, steps 3-5
        let temp = tempdir().expect("Can't create temp dir");
        let storage = CircuitStorageConfig::for_config(
            StorageConfig {
                path: temp.path().to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions::default(),
        )
        .unwrap();
        let mut cconf = CircuitConfig::from(1).with_storage(storage);

        let (mut circuit, (input, output)) =
            Runtime::init_circuit(cconf.clone(), constructor).unwrap();

        let mut checkpoint_outputs = Vec::new();

        // Steps 1-2
        for step_data in &steps[0..2] {
            input.append(&mut step_data.clone());
            circuit.transaction().unwrap();
            checkpoint_outputs.push(output.consolidate());
        }

        // Checkpoint and kill
        let cpm = circuit
            .checkpoint()
            .run()
            .expect("checkpoint should succeed");
        circuit.kill().unwrap();

        // Restore from checkpoint
        cconf.storage.as_mut().unwrap().init_checkpoint = Some(cpm.uuid);
        let (mut circuit, (input, output)) =
            Runtime::init_circuit(cconf.clone(), constructor).unwrap();

        // Steps 3-5
        for step_data in &steps[2..5] {
            input.append(&mut step_data.clone());
            circuit.transaction().unwrap();
            checkpoint_outputs.push(output.consolidate());
        }
        circuit.kill().unwrap();

        // Verify outputs match at every step
        for (i, (ref_out, chk_out)) in reference_outputs
            .iter()
            .zip(checkpoint_outputs.iter())
            .enumerate()
        {
            assert_eq!(
                ref_out, chk_out,
                "Output mismatch at step {} (0-indexed)",
                i
            );
        }
    }

    /// Circuit-level checkpoint/restore test for PERCENTILE_DISC.
    #[test]
    fn test_percentile_disc_circuit_checkpoint_restore() {
        use crate::circuit::{CircuitConfig, CircuitStorageConfig};
        use anyhow::Result as AnyResult;
        use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
        use tempfile::tempdir;

        type CR = (
            crate::IndexedZSetHandle<i32, i32>,
            crate::OutputHandle<crate::OrdIndexedZSet<i32, Option<i32>>>,
        );

        fn constructor(circuit: &mut crate::RootCircuit) -> AnyResult<CR> {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, i32>();
            let output = input.percentile_disc_stateful(Some("pct_disc"), &[0.5], true, |r| r[0].clone());
            Ok((input_handle, output.output()))
        }

        // Test data: 5 steps of input for multiple keys
        let steps: Vec<Vec<Tup2<i32, Tup2<i32, i64>>>> = vec![
            // Step 1: key=1 gets [10,20,30], key=2 gets [100,200]
            vec![
                Tup2(1, Tup2(10, 1)),
                Tup2(1, Tup2(20, 1)),
                Tup2(1, Tup2(30, 1)),
                Tup2(2, Tup2(100, 1)),
                Tup2(2, Tup2(200, 1)),
            ],
            // Step 2: key=1 gets +40,+50; key=3 gets [5]
            vec![
                Tup2(1, Tup2(40, 1)),
                Tup2(1, Tup2(50, 1)),
                Tup2(3, Tup2(5, 1)),
            ],
            // -- checkpoint after step 2 --
            // Step 3: key=1 delete 30; key=2 gets +300
            vec![Tup2(1, Tup2(30, -1)), Tup2(2, Tup2(300, 1))],
            // Step 4: key=3 gets +15; key=4 gets [42] (new group after restore)
            vec![Tup2(3, Tup2(15, 1)), Tup2(4, Tup2(42, 1))],
            // Step 5: key=2 emptied (delete all)
            vec![
                Tup2(2, Tup2(100, -1)),
                Tup2(2, Tup2(200, -1)),
                Tup2(2, Tup2(300, -1)),
            ],
        ];

        // Reference run: all 5 steps without interruption
        let reference_outputs = {
            let (mut circuit, (input, output)) = Runtime::init_circuit(1, constructor).unwrap();
            let mut outputs = Vec::new();
            for step_data in &steps {
                input.append(&mut step_data.clone());
                circuit.transaction().unwrap();
                outputs.push(output.consolidate());
            }
            circuit.kill().unwrap();
            outputs
        };

        // Checkpoint run: steps 1-2, checkpoint, kill, restore, steps 3-5
        let temp = tempdir().expect("Can't create temp dir");
        let storage = CircuitStorageConfig::for_config(
            StorageConfig {
                path: temp.path().to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions::default(),
        )
        .unwrap();
        let mut cconf = CircuitConfig::from(1).with_storage(storage);

        let (mut circuit, (input, output)) =
            Runtime::init_circuit(cconf.clone(), constructor).unwrap();

        let mut checkpoint_outputs = Vec::new();

        // Steps 1-2
        for step_data in &steps[0..2] {
            input.append(&mut step_data.clone());
            circuit.transaction().unwrap();
            checkpoint_outputs.push(output.consolidate());
        }

        // Checkpoint and kill
        let cpm = circuit
            .checkpoint()
            .run()
            .expect("checkpoint should succeed");
        circuit.kill().unwrap();

        // Restore from checkpoint
        cconf.storage.as_mut().unwrap().init_checkpoint = Some(cpm.uuid);
        let (mut circuit, (input, output)) =
            Runtime::init_circuit(cconf.clone(), constructor).unwrap();

        // Steps 3-5
        for step_data in &steps[2..5] {
            input.append(&mut step_data.clone());
            circuit.transaction().unwrap();
            checkpoint_outputs.push(output.consolidate());
        }
        circuit.kill().unwrap();

        // Verify outputs match at every step
        for (i, (ref_out, chk_out)) in reference_outputs
            .iter()
            .zip(checkpoint_outputs.iter())
            .enumerate()
        {
            assert_eq!(
                ref_out, chk_out,
                "Output mismatch at step {} (0-indexed)",
                i
            );
        }
    }

    /// Test multi-percentile: compute p25, p50, p75 from a single operator.
    #[test]
    fn test_multi_percentile_cont() {
        let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
            let output = input.percentile_cont_stateful(
                None, &[0.25, 0.5, 0.75], true,
                |r| Tup2(Tup2(r[0].clone(), r[1].clone()), r[2].clone()),
            );
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // [0, 10, 20, 30, 40] -> p25=10, p50=20, p75=30
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(0.0), 1)),
            Tup2(1, Tup2(F64::new(10.0), 1)),
            Tup2(1, Tup2(F64::new(20.0), 1)),
            Tup2(1, Tup2(F64::new(30.0), 1)),
            Tup2(1, Tup2(F64::new(40.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        let expected_val = Tup2(
            Tup2(Some(F64::new(10.0)), Some(F64::new(20.0))),
            Some(F64::new(30.0)),
        );
        assert_eq!(
            result,
            indexed_zset! { 1 => { expected_val => 1 } }
        );
    }

    /// Test mixed CONT + DISC from a single operator using the unified API.
    #[test]
    fn test_mixed_cont_disc_single_operator() {
        let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
            // p50 CONT, p50 DISC from the same tree
            let output = input.percentile_stateful(
                None,
                &[0.5, 0.5],
                &[true, false],  // first is CONT, second is DISC
                true,
                |results: &[PercentileResult<F64>]| {
                    let cont_val: Option<F64> = results[0].cont_interpolate(|v| v.into_inner());
                    let disc_val: Option<F64> = results[1].clone().disc_value();
                    Tup2(cont_val, disc_val)
                },
            );
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // [10, 20, 30, 40] -> CONT p50 = 25.0, DISC p50 = 20.0
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(10.0), 1)),
            Tup2(1, Tup2(F64::new(20.0), 1)),
            Tup2(1, Tup2(F64::new(30.0), 1)),
            Tup2(1, Tup2(F64::new(40.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        let expected_val = Tup2(Some(F64::new(25.0)), Some(F64::new(20.0)));
        assert_eq!(
            result,
            indexed_zset! { 1 => { expected_val => 1 } }
        );

        // Step 2: add a value, verify delta
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(50.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // [10, 20, 30, 40, 50] -> CONT p50 = 30.0, DISC p50 = 30
        let old_val = Tup2(Some(F64::new(25.0)), Some(F64::new(20.0)));
        let new_val = Tup2(Some(F64::new(30.0)), Some(F64::new(30.0)));
        assert_eq!(
            result,
            indexed_zset! { 1 => { old_val => -1, new_val => 1 } }
        );
    }

    /// Test percentile operator with 2 workers to exercise parallel routing paths.
    ///
    /// With 2 workers, `ParallelRouting::new()` returns `Some(...)`, and the
    /// operator uses `process_delta_with_partition` + exchange-based parallel routing
    /// when a single key has >= 256 entries. We test both:
    /// 1. Small batches (below threshold, sequential path)
    /// 2. Large batch (above threshold, parallel routing path)
    #[test]
    fn test_percentile_cont_multi_worker() {
        let (mut circuit, (input, output)) = Runtime::init_circuit(2, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
            let output = input.percentile_cont_stateful(None, &[0.5], true, |r| r[0].clone());
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Step 1: Small batch (below parallel threshold, sequential path)
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(10.0), 1)),
            Tup2(1, Tup2(F64::new(20.0), 1)),
            Tup2(1, Tup2(F64::new(30.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Median of [10, 20, 30] = 20.0
        assert_eq!(
            result,
            indexed_zset! { 1 => { Some(F64::new(20.0)) => 1 } }
        );

        // Step 2: Large batch (above parallel routing threshold of 256)
        // Insert 300 values for a single key to trigger parallel routing
        let mut batch: Vec<Tup2<i32, Tup2<F64, i64>>> = (0..300)
            .map(|i| Tup2(1, Tup2(F64::new(100.0 + i as f64), 1)))
            .collect();
        input.append(&mut batch);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Now tree has [10, 20, 30, 100..399] = 303 values
        // Median = value at position 151 (0-indexed) = 151st value
        // Sorted: 10, 20, 30, 100, 101, ..., 399
        // Position 151 (0-based) = 148.0 (10, 20, 30 are first 3, then 100+145=248? Let's compute)
        // With 303 elements, p50 continuous: target_rank = 0.5 * (303-1) = 151.0
        // Element at rank 151 (0-indexed): [10, 20, 30, 100, 101, ..., 399]
        // Rank 0=10, 1=20, 2=30, 3=100, 4=101, ... rank k+3 = 100+(k) for k>=0
        // Rank 151: 151-3 = 148, so value = 100+148 = 248
        // Previous output was 20.0, so delta is -20.0, +248.0
        assert_eq!(
            result,
            indexed_zset! { 1 => { Some(F64::new(20.0)) => -1, Some(F64::new(248.0)) => 1 } }
        );

        // Step 3: Delete some values - verify incremental update still works
        let mut deletes: Vec<Tup2<i32, Tup2<F64, i64>>> = (0..100)
            .map(|i| Tup2(1, Tup2(F64::new(100.0 + i as f64), -1)))
            .collect();
        input.append(&mut deletes);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Tree now has [10, 20, 30, 200..399] = 203 values
        // target_rank = 0.5 * (203-1) = 101.0
        // Rank 0=10, 1=20, 2=30, 3=200, 4=201, ... rank k+3 = 200+k for k>=0
        // Rank 101: 101-3=98, value = 200+98 = 298
        assert_eq!(
            result,
            indexed_zset! { 1 => { Some(F64::new(248.0)) => -1, Some(F64::new(298.0)) => 1 } }
        );
    }

    /// Test percentile DISC with 2 workers.
    #[test]
    fn test_percentile_disc_multi_worker() {
        let (mut circuit, (input, output)) = Runtime::init_circuit(2, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
            let output = input.percentile_disc_stateful(None, &[0.5], true, |r| r[0].clone());
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Insert a large batch for a single key
        let mut batch: Vec<Tup2<i32, Tup2<F64, i64>>> = (0..300)
            .map(|i| Tup2(1, Tup2(F64::new(i as f64), 1)))
            .collect();
        input.append(&mut batch);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // DISC p50 of [0..299] with 300 elements
        // target_rank = ceil(0.5 * 300) - 1 = 150 - 1 = 149
        assert_eq!(
            result,
            indexed_zset! { 1 => { Some(F64::new(149.0)) => 1 } }
        );
    }

    /// Test PERCENTILE_CONT with Tup0 key (no GROUP BY) and 2 workers.
    ///
    /// This exercises the unsharded single-key path where:
    /// - Shard is skipped (input stays round-robin distributed)
    /// - Worker 0 owns the tree; all workers participate in Exchange barriers
    /// - Each worker routes its own local entries via the shared tree view
    #[test]
    fn test_percentile_cont_tup0_multi_worker() {
        let (mut circuit, (input, output)) = Runtime::init_circuit(2, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<Tup0, F64>();
            let output = input.percentile_cont_stateful(None, &[0.5], true, |r| r[0].clone());
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Step 1: Small batch
        input.append(&mut vec![
            Tup2(Tup0(), Tup2(F64::new(10.0), 1)),
            Tup2(Tup0(), Tup2(F64::new(20.0), 1)),
            Tup2(Tup0(), Tup2(F64::new(30.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        assert_eq!(
            result,
            indexed_zset! { Tup0() => { Some(F64::new(20.0)) => 1 } }
        );

        // Step 2: Add more values
        input.append(&mut vec![
            Tup2(Tup0(), Tup2(F64::new(40.0), 1)),
            Tup2(Tup0(), Tup2(F64::new(50.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        assert_eq!(
            result,
            indexed_zset! { Tup0() => { Some(F64::new(20.0)) => -1, Some(F64::new(30.0)) => 1 } }
        );

        // Step 3: Delete a value
        input.append(&mut vec![Tup2(Tup0(), Tup2(F64::new(30.0), -1))]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // [10, 20, 40, 50]: p50 continuous, pos = 0.5*(4-1) = 1.5
        // lower=20, upper=40, fraction=0.5 -> 30.0
        assert_eq!(
            result,
            indexed_zset! { Tup0() => { Some(F64::new(30.0)) => -1, Some(F64::new(30.0)) => 1 } }
        );
    }

    /// Test PERCENTILE_CONT with Tup0 key and 2 workers with a large batch.
    ///
    /// With enough entries, the tree should have internal nodes and the
    /// parallel routing distributes leaf routing across workers.
    #[test]
    fn test_percentile_cont_tup0_large_batch() {
        let (mut circuit, (input, output)) = Runtime::init_circuit(2, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<Tup0, F64>();
            let output = input.percentile_cont_stateful(None, &[0.5], true, |r| r[0].clone());
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Insert 1000 entries
        let mut batch: Vec<Tup2<Tup0, Tup2<F64, i64>>> = (0..1000)
            .map(|i| Tup2(Tup0(), Tup2(F64::new(i as f64), 1)))
            .collect();
        input.append(&mut batch);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Median of [0..999]: target_rank = 0.5 * 999 = 499.5
        // lower=499, upper=500, fraction=0.5 -> 499.5
        assert_eq!(
            result,
            indexed_zset! { Tup0() => { Some(F64::new(499.5)) => 1 } }
        );

        // Step 2: Delete first 500, add 500 more
        let mut deletes: Vec<Tup2<Tup0, Tup2<F64, i64>>> = (0..500)
            .map(|i| Tup2(Tup0(), Tup2(F64::new(i as f64), -1)))
            .collect();
        let mut inserts: Vec<Tup2<Tup0, Tup2<F64, i64>>> = (1000..1500)
            .map(|i| Tup2(Tup0(), Tup2(F64::new(i as f64), 1)))
            .collect();
        deletes.append(&mut inserts);
        input.append(&mut deletes);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Now [500..1499], 1000 values, target_rank = 0.5 * 999 = 499.5
        // Rank 499 = 999, Rank 500 = 1000, fraction=0.5 -> 999.5
        assert_eq!(
            result,
            indexed_zset! { Tup0() => { Some(F64::new(499.5)) => -1, Some(F64::new(999.5)) => 1 } }
        );
    }

    /// Verify that 1-worker and 2-worker produce identical results for Tup0 key.
    #[test]
    fn test_percentile_cont_tup0_1_vs_2_workers() {
        let steps: Vec<Vec<Tup2<Tup0, Tup2<F64, i64>>>> = vec![
            // Step 1: insert 100 values
            (0..100).map(|i| Tup2(Tup0(), Tup2(F64::new(i as f64), 1))).collect(),
            // Step 2: delete 20, add 50
            {
                let mut v: Vec<_> = (0..20).map(|i| Tup2(Tup0(), Tup2(F64::new(i as f64), -1))).collect();
                v.extend((100..150).map(|i| Tup2(Tup0(), Tup2(F64::new(i as f64), 1))));
                v
            },
            // Step 3: delete 30 more
            (20..50).map(|i| Tup2(Tup0(), Tup2(F64::new(i as f64), -1))).collect(),
            // Step 4: insert with weights > 1
            vec![Tup2(Tup0(), Tup2(F64::new(200.0), 3))],
        ];

        // Run with 1 worker
        let outputs_1 = {
            let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
                let (input, input_handle) = circuit.add_input_indexed_zset::<Tup0, F64>();
                let output = input.percentile_cont_stateful(None, &[0.5], true, |r| r[0].clone());
                Ok((input_handle, output.output()))
            }).unwrap();
            let mut outputs = Vec::new();
            for step_data in &steps {
                input.append(&mut step_data.clone());
                circuit.transaction().unwrap();
                outputs.push(output.consolidate());
            }
            circuit.kill().unwrap();
            outputs
        };

        // Run with 2 workers
        let outputs_2 = {
            let (mut circuit, (input, output)) = Runtime::init_circuit(2, |circuit| {
                let (input, input_handle) = circuit.add_input_indexed_zset::<Tup0, F64>();
                let output = input.percentile_cont_stateful(None, &[0.5], true, |r| r[0].clone());
                Ok((input_handle, output.output()))
            }).unwrap();
            let mut outputs = Vec::new();
            for step_data in &steps {
                input.append(&mut step_data.clone());
                circuit.transaction().unwrap();
                outputs.push(output.consolidate());
            }
            circuit.kill().unwrap();
            outputs
        };

        // Verify identical results
        for (i, (out_1, out_2)) in outputs_1.iter().zip(outputs_2.iter()).enumerate() {
            assert_eq!(
                out_1, out_2,
                "1-worker vs 2-worker output mismatch at step {} (0-indexed)",
                i
            );
        }
    }

    /// Test multi-key scenario with parallel routing (2 workers).
    /// Only one key at a time gets parallel routing; others are sequential.
    #[test]
    fn test_percentile_multi_key_multi_worker() {
        let (mut circuit, (input, output)) = Runtime::init_circuit(2, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
            let output = input.percentile_cont_stateful(None, &[0.5], true, |r| r[0].clone());
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Insert large batch for key 1 and small batch for key 2
        let mut batch: Vec<Tup2<i32, Tup2<F64, i64>>> = Vec::new();
        // Key 1: 300 values (triggers parallel routing)
        for i in 0..300 {
            batch.push(Tup2(1, Tup2(F64::new(i as f64), 1)));
        }
        // Key 2: 5 values (sequential)
        for i in 0..5 {
            batch.push(Tup2(2, Tup2(F64::new(i as f64 * 10.0), 1)));
        }
        input.append(&mut batch);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Key 1: 300 values [0..299], median = 0.5*(300-1)=149.5
        // Key 2: [0, 10, 20, 30, 40], median = 20.0
        assert_eq!(
            result,
            indexed_zset! {
                1 => { Some(F64::new(149.5)) => 1 },
                2 => { Some(F64::new(20.0)) => 1 }
            }
        );
    }
}
