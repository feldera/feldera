//! The connector test suites, by name: each lives next to the code that it tests, and
//! [`connector_suites`](crate::connector_suites) defines a `#[test]` for each of its cases.

pub use crate::controller::ft_tests as controller_ft;
pub use crate::controller::output_tests as controller_output;
pub use crate::controller::processors_tests as controller_processors;
pub use crate::controller::suspend_tests as controller_suspend;
pub use crate::controller::tests as controller;
#[cfg(feature = "with-avro")]
pub use crate::format::avro::encoder_tests as avro_encoder;
#[cfg(feature = "with-avro")]
pub use crate::format::avro::parser_tests as avro_parser;
pub use crate::format::csv::tests as csv_parser;
pub use crate::format::json::encoder_tests as json_encoder;
pub use crate::format::json::parser_tests as json_parser;
pub use crate::format::parquet::tests as parquet;
pub use crate::format::raw::tests as raw_parser;
pub use crate::format::soft_delete_tests as soft_delete;
#[cfg(feature = "with-deltalake")]
pub use crate::integrated::delta_table::input::tests as delta_input;
#[cfg(feature = "with-deltalake")]
pub use crate::integrated::delta_table::merge::tests as delta_merge;
#[cfg(feature = "with-deltalake")]
pub use crate::integrated::delta_table::output::tests as delta_output;
#[cfg(all(feature = "with-deltalake", feature = "delta-s3-test"))]
pub use crate::integrated::delta_table::s3_tests as delta_s3;
#[cfg(feature = "with-deltalake")]
pub use crate::integrated::delta_table::tests as delta_table;
#[cfg(all(feature = "with-deltalake", feature = "delta-unity-test"))]
pub use crate::integrated::delta_table::unity_tests as delta_unity;
#[cfg(feature = "with-dynamodb")]
pub use crate::integrated::dynamodb::tests as dynamodb;
#[cfg(feature = "with-iceberg")]
pub use crate::integrated::iceberg::follow_tests as iceberg_follow;
#[cfg(feature = "with-iceberg")]
pub use crate::integrated::iceberg::glue_tests as iceberg_glue;
#[cfg(feature = "with-iceberg")]
pub use crate::integrated::iceberg::rest_tests as iceberg_rest;
#[cfg(feature = "with-iceberg")]
pub use crate::integrated::iceberg::s3tables_tests as iceberg_s3tables;
#[cfg(feature = "with-iceberg")]
pub use crate::integrated::iceberg::tests as iceberg;
#[cfg(feature = "with-postgres-cdc")]
pub use crate::integrated::postgres::cdc_input::destination_tests as postgres_cdc_destination;
#[cfg(feature = "with-postgres-cdc")]
pub use crate::integrated::postgres::cdc_privilege_tests as postgres_cdc_privileges;
#[cfg(feature = "with-postgres-cdc")]
pub use crate::integrated::postgres::cdc_scenario_tests as postgres_cdc_scenarios;
#[cfg(feature = "with-postgres-cdc")]
pub use crate::integrated::postgres::cdc_tests as postgres_cdc;
pub use crate::integrated::postgres::output::endpoint_tests as postgres_output;
pub use crate::integrated::postgres::tests as postgres;
pub use crate::server::tests as server;
pub use crate::transport::clock::tests as clock;
pub use crate::transport::datagen_tests as datagen;
pub use crate::transport::file::tests as file_input;
#[cfg(feature = "with-kafka")]
pub use crate::transport::kafka::tests as kafka;
#[cfg(feature = "with-nats")]
pub use crate::transport::nats::input_tests as nats_input;
#[cfg(feature = "with-nats")]
pub use crate::transport::nats::pipeline_tests as nats_pipeline;
#[cfg(feature = "with-pubsub")]
pub use crate::transport::pubsub::gcp_tests as pubsub_gcp;
#[cfg(feature = "with-pubsub")]
pub use crate::transport::pubsub::tests as pubsub;
#[cfg(feature = "with-redis")]
pub use crate::transport::redis::tests as redis;
pub use crate::transport::s3::tests as s3_input;
pub use crate::transport::url::tests as url_input;

/// Defines a `#[test]` for each connector suite, run against the engine that `$engine`
/// builds.
///
/// The tests live in nested modules, so `$engine` must name its items by absolute paths. A
/// case written `case if with_x` exists only when this crate has the feature that the macro
/// `with_x!` checks.
#[macro_export]
macro_rules! connector_suites {
    ($engine:expr) => {
        $crate::with_avro! {
            $crate::connector_suites!(@module $engine, avro_encoder: [
                variant_output,
                random_raw_output,
                random_raw_output_indexed,
                random_raw_output_indexed_with_key,
                random_confluent_output,
                random_confluent_output_indexed,
                non_unique_keys,
                parallel_indexed_output,
                cdc_field_output,
            ]);
            $crate::connector_suites!(@module $engine, avro_parser: [
                raw_records,
                debezium_records,
                soft_delete_debezium,
                debezium_ignored_coerced_field,
                cdc_field,
                cdc_field_invalid_op,
                cdc_field_config_errors,
                extra_columns,
                non_null_to_nullable,
                nullable_to_non_nullable,
                binary,
                uuid,
                variant,
                enums,
                metadata,
                timestamp_millis,
                debezium_temporal_types,
                debezium_timestamp_column_targets,
                debezium_variable_scale_decimal,
                int_conversions,
                random_raw_records,
                random_debezium_records,
            ]);
        }
        $crate::connector_suites!(@module $engine, clock: [
            clock,
            timezone_offset_http_driven,
            timezone_offset_pinned_across_restart,
            advance_http_driven,
            advance_rejected_when_wall_driven,
            advance_survives_checkpoint,
        ]);
        $crate::connector_suites!(@module $engine, controller: [
            proptest_csv_file,
            start_after_cyclic,
            start_after,
            circuit_thread_panic_is_reported,
            concurrent_init,
            connector_init_error,
            fault_tolerance_mismatch_unregisters_the_endpoint,
            fault_tolerance_mismatch_does_not_open_the_endpoint,
            step_does_not_wait_for_endpoint_without_reader,
            committed_connector_input_does_not_spin_the_circuit,
            removed_input_endpoint_input_is_still_counted,
            stop_cancels_the_shutdown_signal,
            output_endpoint_records_transaction_after_enable,
            view_and_aliased_index_connectors_receive_the_same_output,
            closing_a_view_connector_does_not_starve_its_aliased_index,
            aliased_index_connector_receives_the_snapshot,
            connector_attached_between_transactions_receives_no_earlier_output,
            input_batch_wakes_the_circuit_thread,
            api_status_reports_disk_usage_from_storage_backend,
            a_reader_is_told_the_step_being_fed_not_the_one_the_outputs_finished,
            stopped_connector_ends_its_transaction,
            stopped_connector_finishes_all_hosts_transaction,
            paused_connector_holds_transaction,
            transaction_leaves_views_on_the_same_input_prefix,
            transaction_committing_on_a_short_tail_leaves_views_on_the_same_input_prefix,
            transaction_with_a_backlog_leaves_views_on_the_same_input_prefix,
            many_transactions_leave_views_on_the_same_input_prefix,
            many_transactions_with_a_high_cardinality_aggregate,
            many_transactions_with_large_records,
        ]);
        $crate::connector_suites!(@module $engine, controller_ft: [
            ft_with_checkpoints,
            ft_immediate_checkpoints,
            ft_without_checkpoints,
            ft_with_checkpoints_with_pauses,
            ft_without_checkpoints_with_pauses,
            ft_alternating,
            ft_initially_zero_without_checkpoint,
            ft_initially_zero_with_checkpoint,
            ft_modified_connectors,
            ft_send_snapshot_delivered_once,
            ft_send_snapshot_resent_on_flag_flip,
            ft_send_snapshot_delta_delivered_while_paused if with_deltalake,
            checkpoint_publishes_only_after_output_completes,
            checkpoint_state_file_names_its_own_circuit,
            restart_resumes_from_newest_published_checkpoint,
            ft_restart_resumes_from_newest_published_checkpoint,
            checkpoint_activity_state_transitions,
            checkpoint_activity_async_checkpoint,
            permanent_checkpoint_errors_without_storage,
        ]);
        $crate::connector_suites!(@module $engine, controller_output: [
            output_connector_pause_drops_output,
            output_connector_paused_on_startup,
            output_connector_pause_flushes_the_output_buffer,
            output_connector_pause_lifts_backpressure,
            output_connector_paused_defers_the_initial_snapshot,
            output_connector_pause_replays_without_the_endpoint,
            output_connector_pause_survives_restart,
        ]);
        $crate::connector_suites!(@module $engine, controller_processors: [
            preprocessor,
            decryption_preprocessor,
            base64_csv_preprocessor,
            base64_sponge_csv_preprocessor,
            postprocessor,
            encryption_postprocessor,
            base64_postprocessor,
            postprocessor_error_reported,
            postprocessor_on_delta_output_fails,
        ]);
        $crate::connector_suites!(@module $engine, controller_suspend: [
            suspend,
            suspend_empty,
            #[cfg_attr(target_arch = "aarch64", ignore)]
            suspend_barrier,
            suspend_barrier2,
            suspend_barrier3,
            suspend_barrier4,
            suspend_barrier5,
            bootstrap,
        ]);
        $crate::connector_suites!(@module $engine, csv_parser: [
            quote,
            escape,
            double_quote,
            double_quote_disabled,
            trim_none,
            trim_fields,
            quoting_disabled,
            comment,
            flexible,
            inflexible,
            headers,
            random_rows,
        ]);
        $crate::connector_suites!(@module $engine, datagen: [
            transaction,
            extend_and_queue_at_end_of_input,
            limit_increment,
            scaled_range_increment,
            scaled_range_increment_reals,
            decimal_increment,
            decimal_uniform,
            decimal_zipf,
            array_increment,
            uniform_range,
            values,
            invalid_integer_values,
            invalid_string_values,
            invalid_timestamp_values,
            invalid_time_values,
            invalid_date_values,
            null,
            null_percentage,
            string_generators,
            byte_array_with_values,
            byte_array_with_increment,
            byte_array_with_value,
            time_types,
            time_types_with_integer_range,
            uniform_dates_times_timestamps,
            time_types_with_string_range,
            case_sensitivity,
            case_insensitivity,
            #[ignore = "a benchmark"]
            throughput,
        ]);
        $crate::connector_suites!(@module $engine, postgres: [
            #[serial_test::serial]
            on_conflict_do_update,
            #[serial_test::serial]
            on_conflict_do_nothing,
            #[serial_test::serial]
            insert0,
            #[serial_test::serial]
            insert0_cdc,
            #[serial_test::serial]
            insert_omitted_default_column,
            #[serial_test::serial]
            error_message_is_bounded,
            #[serial_test::serial]
            transmitted_records_excludes_rolled_back_rows,
            #[serial_test::serial]
            transmitted_records_excludes_rows_lost_to_a_later_failure,
            #[serial_test::serial]
            reconnect_does_not_lose_the_batch,
            #[serial_test::serial]
            reconnect_does_not_lose_the_batch_multi_thread,
            #[serial_test::serial]
            reconnect_survives_repeated_drops,
            #[serial_test::serial]
            reconnect_waits_out_a_refusing_database,
            #[serial_test::serial]
            stops_while_the_database_is_unreachable,
            #[serial_test::serial]
            transmitted_records_counts_committed_rows,
            #[serial_test::serial]
            insert,
            #[serial_test::serial]
            upsert0,
            #[serial_test::serial]
            upsert0_cdc,
            #[serial_test::serial]
            upsert,
            #[serial_test::serial]
            delete,
            #[serial_test::serial]
            delete_cdc,
            #[serial_test::serial]
            simple,
            #[serial_test::serial]
            simple_tls,
            #[serial_test::serial]
            input_tls,
            #[serial_test::serial]
            input_query_error,
            #[serial_test::serial]
            non_unique_keys_skipped,
        ]);
        $crate::with_postgres_cdc! {
            $crate::connector_suites!(@module $engine, postgres_cdc: [
                #[serial_test::serial]
                #[ignore]
                basic_insert,
                #[serial_test::serial]
                #[ignore]
                pause_unpause,
                #[serial_test::serial]
                #[ignore]
                basic_insert_tls,
                #[serial_test::serial]
                #[ignore]
                all_data_types,
                #[serial_test::serial]
                #[ignore]
                update_delete,
                #[serial_test::serial]
                #[ignore]
                compatible_schema_changes,
                #[serial_test::serial]
                source_table_not_in_publication_fails_at_start,
                #[serial_test::serial]
                #[ignore]
                drop_primary_key_column_rejected,
                #[serial_test::serial]
                stop_with_buffered_rows_releases_slots_promptly,
                #[serial_test::serial]
                restart_resumes_from_slot,
                #[serial_test::serial]
                ft_mode_holds_slot,
            ]);
            $crate::connector_suites!(@module $engine, postgres_cdc_destination: [
                a_queued_snapshot_buffer_holds_the_terminal_barrier_until_the_reader_flushes_it,
                a_streamed_write_is_answered_only_when_its_last_buffer_reaches_the_circuit,
                a_write_that_arrives_while_the_handler_runs_is_not_answered_by_it,
                a_write_that_starts_once_the_connector_stops_is_turned_away_without_error,
                a_queued_snapshot_buffer_stays_charged_to_the_copy_barrier_until_flushed,
            ]);
            $crate::connector_suites!(@module $engine, postgres_cdc_privileges: [
                #[serial_test::serial]
                snapshot_stream_restart_and_ddl,
                #[serial_test::serial]
                switch_credentials_resumes,
                #[serial_test::serial]
                source_migration_switch,
                #[serial_test::serial]
                missing_setup_errors,
                #[serial_test::serial]
                event_trigger_required,
                #[serial_test::serial]
                unbootstrapped_database,
                #[serial_test::serial]
                documented_recipe,
            ]);
            $crate::connector_suites!(@module $engine, postgres_cdc_scenarios: [
                #[serial_test::serial]
                snapshot_replayed_when_stopped_before_checkpoint,
                #[serial_test::serial]
                checkpoint_mid_snapshot_waits_for_the_whole_copy,
                #[serial_test::serial]
                checkpoint_mid_snapshot_waits_for_the_whole_copy_multiworker,
                #[serial_test::serial]
                suspend_mid_copy_is_refused,
                #[serial_test::serial]
                #[ignore]
                streamed_rows_after_checkpoint_are_redelivered,
                #[serial_test::serial]
                #[ignore]
                streamed_rows_after_checkpoint_are_redelivered_multiworker,
                #[serial_test::serial]
                #[ignore]
                restart_after_stop_with_inflight_acks,
                #[serial_test::serial]
                #[ignore]
                etl_state_lives_in_source_database,
                #[serial_test::serial]
                #[ignore]
                update_delete_after_checkpoint_are_redelivered,
                #[serial_test::serial]
                #[ignore]
                update_delete_after_checkpoint_are_redelivered_multiworker,
                #[serial_test::serial]
                #[ignore]
                all_types_replayed_after_restart,
                #[serial_test::serial]
                #[ignore]
                replication_slots_survive_restart,
                #[serial_test::serial]
                #[ignore]
                other_tables_in_publication_are_filtered,
                #[serial_test::serial]
                #[ignore]
                transactions_across_restart,
                #[serial_test::serial]
                a_checkpoint_inside_one_write_does_not_acknowledge_it,
            ]);
        }
        $crate::connector_suites!(@module $engine, postgres_output: [
            #[serial_test::serial]
            insert_single_thread,
            #[serial_test::serial]
            insert_multi_thread,
            #[serial_test::serial]
            extra_columns_test,
            #[serial_test::serial]
            upsert_single_thread,
            #[serial_test::serial]
            upsert_multi_thread,
            #[serial_test::serial]
            delete_single_thread,
            #[serial_test::serial]
            delete_multi_thread,
            #[serial_test::serial]
            empty_batch_single_thread,
            #[serial_test::serial]
            empty_batch_multi_thread,
            #[serial_test::serial]
            single_record_multi_thread,
            #[serial_test::serial]
            multiple_batches_single_thread,
            #[serial_test::serial]
            multiple_batches_multi_thread,
            #[serial_test::serial]
            progress_counter_single_thread,
            #[serial_test::serial]
            progress_counter_multi_thread,
            #[serial_test::serial]
            progress_counter_empty_batch,
            #[serial_test::serial]
            progress_counter_advances_mid_batch_single_thread,
            #[serial_test::serial]
            progress_counter_multiple_batches,
            #[serial_test::serial]
            progress_counter_advances_mid_batch_multi_thread,
        ]);
        $crate::with_deltalake! {
            $crate::connector_suites!(@module $engine, delta_table: [
                output_non_unique_keys_skipped_test,
                cdc_file_test,
                cdc_soft_delete_test,
                cdc_skip_unused_columns_test,
                cdc_skip_unused_columns_catchup_test,
                cdc_skip_unused_columns_catchup_suspend_test,
                cdc_file_catchup_test,
                cdc_file_catchup_suspend_test,
                cdc_file_indexed_test,
                #[cfg_attr(target_arch = "aarch64", ignore = "flaky on aarch64")]
                cdc_file_suspend_test,
                cdc_rewrite_test,
                cdc_multi_key_order_by_test,
                cdc_invalid_delete_filter_test,
                except_all_cancels_a_rewrite_across_all_types,
                snapshot_and_follow_file_test,
                transactional_snapshot_and_follow_file_test,
                transactional_always_snapshot_and_follow_file_test,
                catchup_batches_multiple_versions_test,
                #[cfg(unix)]
                catchup_pause_during_target_listing_test,
                always_one_transaction_per_version_test,
                catchup_end_version_caps_window_test,
                catchup_rapid_versions_test,
                #[cfg(unix)]
                catchup_transient_read_failure_test,
                #[cfg(unix)]
                catchup_max_retries_fatal_test,
                follow_file_catchup_test,
                snapshot_and_follow_catchup_snapshot_transaction_test,
                snapshot_mode_snapshot_transaction_test,
                snapshot_and_follow_file_catchup_test,
                follow_file_catchup_end_version_test,
                follow_file_catchup_suspend_test,
                snapshot_and_follow_file_catchup_suspend_test,
                snapshot_and_follow_file_catchup_end_version_test,
                follow_file_test,
                follow_file_test_suspend,
                follow_file_test_end_version,
                snapshot_and_follow_file_test_suspend,
                transactional_snapshot_and_follow_file_test_suspend,
                transactional_always_snapshot_and_follow_file_test_suspend,
                snapshot_and_follow_file_test_suspend_end_version,
                completed_frontier_null_version_after_resume,
                ordered_snapshot_resume_test,
                ordered_snapshot_resume_legacy_checkpoint_test,
                snapshot_datetime_test,
                file_output_proptest,
                file_output_proptest_index_alias,
                follow_partition_column_test,
                follow_partition_column_types_test,
                follow_partition_uppercase_columns_test,
                follow_partition_unquoted_sql_columns_test,
                snapshot_uppercase_timestamp_column_test,
                cdc_partition_column_test,
                change_feed_keyed_update_test,
                change_feed_filter_undeclared_column_test,
                change_feed_filter_struct_field_test,
                change_feed_filter_partition_column_test,
                change_feed_merge_test,
                change_feed_off_reads_file_actions_test,
                change_feed_auto_without_feed_test,
                change_feed_catchup_test,
                change_feed_always_transaction_test,
                change_feed_matches_file_actions_test,
                change_feed_reads_change_data_test,
                change_feed_partition_column_test,
                change_feed_filter_test,
                change_feed_require_not_enabled_test,
                change_feed_rejected_outside_follow_modes_test,
                change_feed_accepted_in_follow_modes_test,
                change_feed_suspend_test,
                change_feed_skip_unused_columns_test,
                follow_filter_undeclared_column_test,
                follow_filter_undeclared_struct_field_test,
                cdc_filter_undeclared_column_test,
                cdc_filter_undeclared_struct_field_test,
                follow_invalid_filter_test,
                foreign_variant_test,
                output_variant_encoding_test,
                follow_uniform_field_id_test,
                cdc_uniform_field_id_test,
                follow_id_mapped_physical_names_test,
                follow_id_mapped_change_data_test,
                follow_id_mapped_change_data_logical_names_test,
                follow_id_mapped_nulls_test,
                follow_id_mapped_filter_test,
                follow_id_mapped_all_types_test,
                follow_id_mapped_partitioned_test,
                cdc_id_mapped_all_types_test,
                cdc_id_mapped_deletion_vector_test,
                snapshot_id_mapped_reordered_nested_fields_test,
                snapshot_out_of_range_date_stats_test,
                snapshot_unlisted_reader_feature_test,
                variant_shredding_is_refused_test,
                variant_shredding_turned_on_while_following_test,
                snapshot_and_follow_id_mapped_test,
            ]);
            $crate::with_delta_s3! {
                $crate::connector_suites!(@module $engine, delta_s3: [
                    #[serial_test::parallel(delta_s3)]
                    cdc_s3_test_suspend,
                    #[serial_test::parallel(delta_s3)]
                    follow_s3_test,
                    #[serial_test::parallel(delta_s3)]
                    snapshot_and_follow_s3_test,
                    #[serial_test::parallel(delta_s3)]
                    snapshot_and_follow_s3_test_suspend,
                    #[serial_test::parallel(delta_s3)]
                    s3_output_proptest,
                    #[serial_test::parallel(delta_s3)]
                    s3_people_2m,
                ]);
            }
            $crate::with_delta_unity! {
                $crate::connector_suites!(@module $engine, delta_unity: [
                    #[serial_test::serial(delta_s3)]
                    unity_change_feed,
                    #[serial_test::serial(delta_s3)]
                    unity_people_2m,
                ]);
            }
            $crate::connector_suites!(@module $engine, delta_input: [
                distributed_snapshot_error_keeps_transaction_open,
            ]);
            $crate::connector_suites!(@module $engine, delta_merge: [
                the_table_tracks_the_view_across_random_sequences,
                replaying_a_batch_converges_across_a_restart,
                retrying_a_landed_batch_converges_in_the_owned_regime,
                a_key_set_past_the_budget_is_looked_up_in_several_passes,
                auto_compaction_keeps_a_partitioned_table_equal_to_the_view,
                an_update_across_partitions_supersedes_the_old_row,
                a_flush_times_every_phase_it_runs,
                a_parallel_flush_leaves_the_table_where_a_sequential_one_does,
                a_small_batch_is_not_split_across_ranges,
                tombstones_from_every_range_survive_the_join,
                compaction_and_reclaim_preserve_a_table_written_in_parallel,
                maintenance_interleaved_with_flushes_preserves_the_view,
                a_parallel_retry_converges_like_a_sequential_one,
                a_parallel_flush_moves_rows_between_partitions,
                violations_from_every_range_are_reported,
                a_panicked_range_is_reported_as_deterministic,
                a_parallel_lookup_past_the_budget_runs_several_passes,
                a_batch_worth_too_few_files_is_left_whole_once_the_rate_is_known,
                a_failing_range_fails_the_flush_without_committing,
            ]);
            $crate::connector_suites!(@module $engine, delta_output: [
                merge_batch_larger_than_target_file_size_rolls_over,
                merge_tracks_the_view,
                merge_supersedes_a_row_left_by_an_earlier_run,
                merge_is_unaffected_by_chunking,
                merge_skips_the_lookup_for_inserts_into_an_empty_table,
                merge_prunes_files_outside_the_changed_key_range,
                merge_converges_after_a_concurrent_compaction,
                auto_compaction_never_races_a_flush,
                the_next_flush_waits_for_the_compaction_step,
                a_uniqueness_violation_is_reported_once_across_retries,
                a_failed_commit_keeps_its_deletion_vector_file,
                a_retry_after_a_commit_that_landed_does_not_duplicate,
                a_batch_after_a_failed_batch_whose_commit_landed_does_not_duplicate,
                the_insert_shortcut_does_not_survive_a_dropped_batch,
                the_insert_shortcut_does_not_survive_a_skipped_key,
                compaction_drops_the_rows_a_vector_marks_deleted,
                reclaim_rewrites_every_column_type_unchanged,
                merge_never_rewrites_a_data_file,
                merge_treats_signed_zeroes_as_one_key,
                merge_supersedes_a_row_whose_key_is_null,
                insert_single_thread,
                shutdown_aborts_the_write,
                merge_stops_when_the_table_no_longer_fits,
                merge_survives_an_unrelated_table_change,
                shutdown_aborts_the_merge_flush,
                a_shutdown_ends_an_unbounded_retry_loop,
                a_shutdown_ends_an_unbounded_commit_loop,
                insert_multi_thread,
                batch_larger_than_target_file_size_rolls_over,
                a_stalled_object_store_stops_the_encoder,
                upsert_single_thread,
                upsert_multi_thread,
                delete_single_thread,
                delete_multi_thread,
                non_indexed_insert_single_thread,
                empty_batch_single_thread,
                empty_batch_multi_thread,
                multiple_batches_single_thread,
                multiple_batches_multi_thread,
                write_failure_readonly_dir,
                retry_exhaustion,
                merge_reports_the_rows_it_writes,
                progress_counter_single_thread,
                progress_counter_multi_thread,
                progress_counter_empty_batch,
                progress_counter_multiple_batches,
                progress_counter_resets_on_batch_start,
                progress_counter_resets_on_write_failure,
                progress_counter_resets_after_retries,
                progress_large_batch_increments,
                truncate_preserves_data_across_restart,
                truncate_clears_data_on_first_start,
                error_if_exists_succeeds_on_restart,
                merge_keyed_on_int,
                merge_keyed_on_string,
                merge_keyed_on_binary,
                merge_keyed_on_timestamp,
                merge_keyed_on_decimal,
                merge_keyed_on_uuid,
                merge_keyed_on_date,
                merge_keyed_on_row,
                merge_keyed_on_double,
                merge_keyed_on_float,
                merge_keyed_on_tinyint,
                merge_keyed_on_smallint,
            ]);
        }
        $crate::with_dynamodb! {
            $crate::connector_suites!(@module $engine, dynamodb: [
                encoder_insert_as_put_item,
                encoder_delete_as_delete_item_key_only,
                encoder_upsert_as_put_item_with_new_value,
                encoder_stages_until_batch_end,
                encoder_flushes_when_buffer_limit_is_reached,
                encoder_flushes_when_record_threshold_is_reached,
                encoder_pending_never_buffers_whole_batch,
                encoder_empty_batch_single_thread,
                encoder_empty_batch_multi_thread,
                encoder_multiple_batches_insert_upsert_delete,
                encoder_non_unique_key_is_skipped_without_failing_batch,
                encoder_non_unique_key_updates_connector_metric,
                encoder_drops_oversized_item_keeping_the_rest,
                encoder_oversized_item_updates_connector_metric,
                worker_surfaces_write_failure_at_batch_end,
                encoder_all_supported_sql_types,
                encoder_variant_decimal_serializes_as_number,
                encoder_variant_map_preserves_private_number_key,
                encoder_variant_list_recursively_serializes_numbers,
                encoder_split_cursor_partitions_encode_disjoint_key_ranges,
                encoder_composite_key_delete_excludes_non_key_fields,
                dynamodb_insert,
                dynamodb_upsert,
                dynamodb_delete,
                dynamodb_conditional_put_skips_existing_row,
                dynamodb_conditional_put_writes_all_new_rows,
                dynamodb_conditional_delete_skips_absent_row,
                dynamodb_all_supported_sql_types_round_trip,
                dynamodb_output_buffer_flushes_by_size,
                dynamodb_output_buffer_flushes_by_time,
                dynamodb_oversized_item_dropped_batch_mode,
                dynamodb_oversized_item_dropped_transactional_mode,
                dynamodb_empty_batch,
                dynamodb_multiple_batches,
                dynamodb_backpressure_single_thread,
                dynamodb_backpressure_multi_thread,
                dynamodb_progress_counter_single_thread,
                dynamodb_progress_counter_multi_thread,
                dynamodb_progress_counter_empty_batch,
                dynamodb_progress_counter_advances_mid_batch_single_thread,
                dynamodb_progress_counter_advances_mid_batch_multi_thread,
                dynamodb_composite_key_same_hash_different_sort,
                dynamodb_progress_counter_multiple_batches,
            ]);
        }
        $crate::connector_suites!(@module $engine, file_input: [
            csv_file_nofollow,
            csv_file_follow,
        ]);
        $crate::with_iceberg_fs! {
            $crate::connector_suites!(@module $engine, iceberg: [
                localfs_input_test_unordered,
                localfs_input_test_ordered,
                localfs_input_test_ordered_with_filter,
                localfs_input_test_ordered_with_row_filter,
                localfs_input_test_single_parser,
                localfs_input_test_transactional,
                localfs_input_test_ordered_transactional,
                localfs_input_test_datetime,
                localfs_input_test_subset_schema,
                localfs_input_test_skip_unused_columns,
                localfs_input_test_resume_completed_snapshot,
            ]);
        }
        $crate::with_iceberg_follow! {
            $crate::connector_suites!(@module $engine, iceberg_follow: [
                rest_follow_snapshot_and_follow,
                rest_follow_resume_does_not_reread_snapshot,
                rest_follow_empty_table,
                rest_follow_live_append,
                rest_follow_start_from_snapshot_id,
                rest_follow_end_snapshot_id,
                rest_follow_transaction_always,
                rest_follow_transaction_catchup,
                rest_follow_transaction_catchup_end_snapshot_id,
                rest_follow_copy_on_write_delete,
            ]);
        }
        $crate::with_iceberg_glue! {
            $crate::connector_suites!(@module $engine, iceberg_glue: [
                glue_s3_input_test,
            ]);
        }
        $crate::with_iceberg_rest! {
            $crate::connector_suites!(@module $engine, iceberg_rest: [
                rest_s3_input_test,
            ]);
        }
        $crate::with_iceberg_s3tables! {
            $crate::connector_suites!(@module $engine, iceberg_s3tables: [
                s3tables_input_test,
            ]);
        }
        $crate::connector_suites!(@module $engine, json_encoder: [
            insert_delete_lines,
            insert_delete_arrays,
            snowflake_lines,
            snowflake_arrays,
            debezium_lines,
            record_longer_than_buffer,
            debezium_key_fields,
            redis,
            random_insert_delete_lines,
            random_insert_delete_arrays,
            random_snowflake_lines,
            random_snowflake_arrays,
            random_debezium_lines,
        ]);
        $crate::connector_suites!(@module $engine, json_parser: [
            raw_records,
            raw_errors,
            insert_delete_records,
            insert_delete_errors,
            debezium_records,
            debezium_errors,
            metadata,
            random_rows,
        ]);
        $crate::with_kafka! {
            $crate::connector_suites!(@module $engine, kafka: [
                #[ignore]
                output_errors_fault_tolerant,
                output_errors,
                ft_with_checkpoints,
                ft_with_topic_expiration,
                ft_with_topic_expiration_and_resume_earliest,
                ft_with_tolerance_topic_expiration,
                ft_without_checkpoints,
                ft_alternating,
                ft_initially_zero_without_checkpoint,
                ft_initially_zero_with_checkpoint,
                ft_with_adding_partitions,
                input_offset,
                input_timestamp,
                #[should_panic(expected = "provided offset '3' not currently in partition '0'")]
                input_offset_doesnt_exist,
                #[should_panic(expected = "provided offset '3' not currently in partition '0'")]
                input_offset_doesnt_exist_2,
                input_offset_earliest,
                input_offset_earliest_2,
                input_offset_latest,
                input_offset_latest_2,
                input_partitions_latest,
                input_partitions_earliest,
                input_partitions_offsets,
                input_trivial,
                input_trivial_threaded,
                random_input,
                random_input_threaded,
                buffer,
                random_end_to_end_csv_large,
                random_end_to_end_csv_small,
                random_end_to_end_json_small,
                random_end_to_end_json_array_small,
                metadata_json,
                metadata_raw,
            ]);
        }
        $crate::with_nats! {
            $crate::connector_suites!(@module $engine, nats_input: [
                basic_input_consumption,
                retry_loop_when_server_unavailable,
                resume_after_server_becomes_available,
                pause_during_retry_loop,
                no_stale_error_after_pause_extend,
                quiet_but_healthy_no_false_alarm,
                mid_run_server_restart_recovers_no_fatal,
                inactivity_timeout_config_is_honored,
                connection_refused_enters_retry_loop,
                startup_connection_refused_retries_repeatedly,
                stream_not_found_enters_retry_loop,
                connection_timeout,
                pause_interrupts_inflight_retry_attempt,
                retry_interval_config_is_honored,
                retry_interval_zero_rejected,
                replay_basic,
                replay_partial_range,
                replay_then_extend,
                replay_empty_range,
                replay_multiple_ranges,
                replay_stream_deleted_retries_and_recovers,
                replay_server_killed_retries_non_fatal,
                replay_after_purge_errors,
                replay_end_after_last_sequence_fails_fast,
                replay_start_before_first_sequence_fails_fast,
                replay_empty_stream_fails_fast,
                disconnect_stops_delivery,
            ]);
            $crate::connector_suites!(@module $engine, nats_pipeline: [
                ft_simple,
                ft_with_checkpoints,
                ft_without_checkpoints,
                ft_alternating,
                ft_initially_zero_without_checkpoint,
                ft_initially_zero_with_checkpoint,
                ft_empty_step_checkpoint,
                ft_replay_after_stream_purge,
                ft_with_named_consumer,
                ft_stream_deletion_and_recreation,
                ft_stream_deletion_after_full_checkpoint,
                ft_startup_retries_when_stream_missing,
                output_pipeline_end_to_end,
            ]);
        }
        $crate::connector_suites!(@module $engine, parquet: [
            input_uncompressed,
            input_snappy,
            output,
            output_in_buffers,
            input_variant,
        ]);
        $crate::with_pubsub_emulator! {
            $crate::connector_suites!(@module $engine, pubsub: [
                input_errors,
                random_input_with_emulator,
            ]);
        }
        $crate::with_pubsub_gcp! {
            $crate::connector_suites!(@module $engine, pubsub_gcp: [
                random_input_with_auth,
                random_input_with_credentials,
                random_multiple_subscribers,
            ]);
        }
        $crate::connector_suites!(@module $engine, raw_parser: [
            varchar_lines,
            varchar_lines_with_metadata,
            nullable_varchar_blob,
            varbinary_lines,
            nullable_varbinary_blob,
            nullable_varbinary_blob_with_metadata,
            nullable_varbinary_blob_with_nulls,
        ]);
        $crate::with_redis! {
            $crate::connector_suites!(@module $engine, redis: [
                redis_output,
                redis_output_fail,
            ]);
        }
        $crate::connector_suites!(@module $engine, s3_input: [
            single_key_read,
            preprocessed_read_split_across_batches,
            single_object_with_prefix_read,
            multi_object_read_with_continuation,
            multi_object_read,
            list_object_read_error,
            get_object_read_error,
        ]);
        $crate::connector_suites!(@module $engine, server: [
            completed_suspend_is_reported_before_the_phase_is_recorded,
            fatal_error_during_bootstrap_tail_is_reported,
            a_refused_bootstrap_result_stops_the_circuit,
            profiles_stream_from_running_pipeline,
            ingress_connector_metadata,
            ingress_rejects_unsupported_update_formats,
            ingress_aborted_upload_is_not_fatal,
            ft_replay_connector_metadata,
            http_ingress_avro if with_avro,
            http_egress_send_snapshot,
            http_egress_send_snapshot_while_paused,
            egress_step_chunk_without_output,
            egress_step_chunks,
            silent_bootstrap,
            concurrent_bootstrap,
            concurrent_bootstrap_output_progress_waits_for_cutover,
            concurrent_bootstrap_snapshot_progress_waits_for_cutover,
            concurrent_bootstrap_adhoc_snapshot,
            ft_replay_adhoc_snapshot,
            checkpoint_status_detects_restart,
            #[cfg(unix)]
            checkpoint_before_exit,
            checkpoint_before_exit_without_ft,
            checkpoint_before_exit_without_storage,
            #[cfg(unix)]
            sigterm_process_without_ft,
            transactions,
            transaction_completion_without_output_connectors,
            kafka_server if with_kafka,
        ]);
        $crate::connector_suites!(@module $engine, soft_delete: [
            json_insert_delete,
            json_debezium,
            json_raw,
            csv,
            raw,
            parquet,
            arrow_delete,
            arrow_polarities,
            arrow_polarities_length_mismatch,
            staged_buffers,
            fork_preserves_soft_delete,
            controller_ingests_history,
            controller_rejects_primary_key,
            controller_rejects_output_connector,
        ]);
        $crate::connector_suites!(@module $engine, url_input: [
            success,
            failure,
            #[ignore = "depends on timing"]
            pause_with_reconnect,
            #[ignore = "depends on timing"]
            pause_without_reconnect,
        ]);
    };
    (@module $engine:expr, $module:ident: [$($(#[$attr:meta])* $suite:ident $(if $gate:ident)?),* $(,)?]) => {
        mod $module {
            $(
                $crate::connector_suites!(@case $engine, $module, [$(#[$attr])*], $suite $(, $gate)?);
            )*
        }
    };
    (@case $engine:expr, $module:ident, [$(#[$attr:meta])*], $suite:ident) => {
        #[test]
        $(#[$attr])*
        fn $suite() {
            $crate::test::suites::$module::$suite(&$engine)
        }
    };
    (@case $engine:expr, $module:ident, [$(#[$attr:meta])*], $suite:ident, $gate:ident) => {
        $crate::$gate! {
            $crate::connector_suites!(@case $engine, $module, [$(#[$attr])*], $suite);
        }
    };
}

/// Expands its input when the crate has the `with-avro` feature, and to nothing otherwise, so
/// that `connector_suites!` checks the feature of this crate, not of the crate that calls it.
#[cfg(feature = "with-avro")]
#[doc(hidden)]
#[macro_export]
macro_rules! with_avro {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-avro` feature, and to nothing otherwise, so
/// that `connector_suites!` checks the feature of this crate, not of the crate that calls it.
#[cfg(not(feature = "with-avro"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_avro {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-kafka` feature, and to nothing otherwise.
#[cfg(feature = "with-kafka")]
#[doc(hidden)]
#[macro_export]
macro_rules! with_kafka {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-kafka` feature, and to nothing otherwise.
#[cfg(not(feature = "with-kafka"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_kafka {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `delta-s3-test` feature, and to nothing otherwise.
#[cfg(feature = "delta-s3-test")]
#[doc(hidden)]
#[macro_export]
macro_rules! with_delta_s3 {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `delta-s3-test` feature, and to nothing otherwise.
#[cfg(not(feature = "delta-s3-test"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_delta_s3 {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `delta-unity-test` feature, and to nothing otherwise.
#[cfg(feature = "delta-unity-test")]
#[doc(hidden)]
#[macro_export]
macro_rules! with_delta_unity {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `delta-unity-test` feature, and to nothing otherwise.
#[cfg(not(feature = "delta-unity-test"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_delta_unity {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-postgres-cdc` feature, and to nothing
/// otherwise.
#[cfg(feature = "with-postgres-cdc")]
#[doc(hidden)]
#[macro_export]
macro_rules! with_postgres_cdc {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-postgres-cdc` feature, and to nothing
/// otherwise.
#[cfg(not(feature = "with-postgres-cdc"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_postgres_cdc {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-deltalake` feature, and to nothing otherwise.
#[cfg(feature = "with-deltalake")]
#[doc(hidden)]
#[macro_export]
macro_rules! with_deltalake {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-deltalake` feature, and to nothing otherwise.
#[cfg(not(feature = "with-deltalake"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_deltalake {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-dynamodb` feature, and to nothing otherwise.
#[cfg(feature = "with-dynamodb")]
#[doc(hidden)]
#[macro_export]
macro_rules! with_dynamodb {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-dynamodb` feature, and to nothing otherwise.
#[cfg(not(feature = "with-dynamodb"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_dynamodb {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-nats` feature, and to nothing otherwise.
#[cfg(feature = "with-nats")]
#[doc(hidden)]
#[macro_export]
macro_rules! with_nats {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-nats` feature, and to nothing otherwise.
#[cfg(not(feature = "with-nats"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_nats {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-redis` feature, and to nothing otherwise.
#[cfg(feature = "with-redis")]
#[doc(hidden)]
#[macro_export]
macro_rules! with_redis {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-redis` feature, and to nothing otherwise.
#[cfg(not(feature = "with-redis"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_redis {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-pubsub` and `pubsub-emulator-test` features,
/// and to nothing otherwise.
#[cfg(all(feature = "with-pubsub", feature = "pubsub-emulator-test"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_pubsub_emulator {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-pubsub` and `pubsub-emulator-test` features,
/// and to nothing otherwise.
#[cfg(not(all(feature = "with-pubsub", feature = "pubsub-emulator-test")))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_pubsub_emulator {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-pubsub` and `pubsub-gcp-test` features,
/// and to nothing otherwise.
#[cfg(all(feature = "with-pubsub", feature = "pubsub-gcp-test"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_pubsub_gcp {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-pubsub` and `pubsub-gcp-test` features,
/// and to nothing otherwise.
#[cfg(not(all(feature = "with-pubsub", feature = "pubsub-gcp-test")))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_pubsub_gcp {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-iceberg` and `iceberg-tests-fs` features,
/// and to nothing otherwise.
#[cfg(all(feature = "with-iceberg", feature = "iceberg-tests-fs"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_iceberg_fs {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-iceberg` and `iceberg-tests-fs` features,
/// and to nothing otherwise.
#[cfg(not(all(feature = "with-iceberg", feature = "iceberg-tests-fs")))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_iceberg_fs {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-iceberg` and `iceberg-tests-follow` features,
/// and to nothing otherwise.
#[cfg(all(feature = "with-iceberg", feature = "iceberg-tests-follow"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_iceberg_follow {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-iceberg` and `iceberg-tests-follow` features,
/// and to nothing otherwise.
#[cfg(not(all(feature = "with-iceberg", feature = "iceberg-tests-follow")))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_iceberg_follow {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-iceberg` and `iceberg-tests-glue` features,
/// and to nothing otherwise.
#[cfg(all(feature = "with-iceberg", feature = "iceberg-tests-glue"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_iceberg_glue {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-iceberg` and `iceberg-tests-glue` features,
/// and to nothing otherwise.
#[cfg(not(all(feature = "with-iceberg", feature = "iceberg-tests-glue")))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_iceberg_glue {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-iceberg` and `iceberg-tests-rest` features,
/// and to nothing otherwise.
#[cfg(all(feature = "with-iceberg", feature = "iceberg-tests-rest"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_iceberg_rest {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-iceberg` and `iceberg-tests-rest` features,
/// and to nothing otherwise.
#[cfg(not(all(feature = "with-iceberg", feature = "iceberg-tests-rest")))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_iceberg_rest {
    ($($item:tt)*) => {};
}

/// Expands its input when the crate has the `with-iceberg` and `iceberg-tests-s3tables` features,
/// and to nothing otherwise.
#[cfg(all(feature = "with-iceberg", feature = "iceberg-tests-s3tables"))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_iceberg_s3tables {
    ($($item:tt)*) => { $($item)* };
}

/// Expands its input when the crate has the `with-iceberg` and `iceberg-tests-s3tables` features,
/// and to nothing otherwise.
#[cfg(not(all(feature = "with-iceberg", feature = "iceberg-tests-s3tables")))]
#[doc(hidden)]
#[macro_export]
macro_rules! with_iceberg_s3tables {
    ($($item:tt)*) => {};
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::{Path, PathBuf};

    /// Returns the names of the suite functions in `source`: the public functions whose first
    /// argument is an engine.
    fn suite_functions(source: &str) -> BTreeSet<String> {
        source
            .split("\npub fn ")
            .skip(1)
            .filter(|function| {
                let signature = &function[..function.find('{').unwrap_or(function.len())];
                signature.contains("engine: &dyn")
            })
            .map(|function| function[..function.find('(').unwrap()].to_string())
            .collect()
    }

    /// Returns the names that `connector_suites!` lists for `module`.
    fn listed_suites(macro_source: &str, module: &str) -> BTreeSet<String> {
        let start = format!(" {module}: [");
        let Some(position) = macro_source.find(&start) else {
            return BTreeSet::new();
        };
        macro_source[position + start.len()..]
            .lines()
            .map(str::trim)
            .take_while(|line| !line.starts_with(']'))
            .filter(|line| !line.is_empty() && !line.starts_with("#["))
            .filter_map(|line| line.trim_end_matches(',').split_whitespace().next())
            .map(str::to_string)
            .collect()
    }

    /// Returns the suites that this module re-exports: each name, and the file that holds it.
    fn named_suites(source: &str, root: &Path) -> Vec<(String, PathBuf)> {
        source
            .lines()
            .filter_map(|line| {
                let (path, name) = line
                    .strip_prefix("pub use crate::")?
                    .strip_suffix(';')?
                    .split_once(" as ")?;
                Some((name.to_string(), root.join(path.replace("::", "/") + ".rs")))
            })
            .collect()
    }

    /// Returns the files under `dir`, named `tests.rs` or `*_tests.rs`, that hold suite
    /// functions.
    fn suite_files(dir: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files.extend(suite_files(&path));
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "tests.rs" || name.ends_with("_tests.rs"))
                && !suite_functions(&fs::read_to_string(&path).unwrap()).is_empty()
            {
                files.push(path);
            }
        }
        files
    }

    /// Every suite function has a test in `connector_suites!`, and every file of suite
    /// functions is named here, so that no suite is left out of the engines' test runs.
    #[test]
    fn every_suite_function_is_listed() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let source = fs::read_to_string(root.join("test/suites.rs")).unwrap();
        let suites = named_suites(&source, &root);
        let mut checked = 0;
        for (module, path) in &suites {
            let functions = suite_functions(&fs::read_to_string(path).unwrap());
            let listed = listed_suites(&source, module);
            let missing: Vec<&String> = functions.difference(&listed).collect();
            assert!(
                missing.is_empty(),
                "connector_suites! does not list {module}::{missing:?}"
            );
            let unknown: Vec<&String> = listed.difference(&functions).collect();
            assert!(
                unknown.is_empty(),
                "connector_suites! lists unknown {module}::{unknown:?}"
            );
            checked += functions.len();
        }
        assert!(checked > 0);

        let named: BTreeSet<&PathBuf> = suites.iter().map(|(_, path)| path).collect();
        for path in suite_files(&root) {
            assert!(
                named.contains(&path),
                "test::suites does not name {}",
                path.display()
            );
        }
    }
}
