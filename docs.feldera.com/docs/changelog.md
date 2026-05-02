---
pagination_next: null
pagination_prev: null
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Changelog

<Tabs>
    <TabItem className="changelogItem" value="enterprise"
        label="Enterprise">

        ## Unreleased

        New API `/egress` that allows more control over output endpoint configuration compared to
        the existing `/egress/{table_name}` endpoint.  Please refer to the API documentation for
        details.

        ## v0.289.0

        API changes:
        - (New) Details about the storage status is a new pipeline field: `storage_status_details`.
          It does not get get cleared when the pipeline stops, only when the storage is cleared.
        - (Fix) Dedicated error `BootstrapPolicyImmutableUnlessStopped` for repeated `/start` of a
          pipeline but with a different bootstrap policy.
        - (New) Recent per-endpoint connector error messages are now persisted in the pipeline
          checkpoint and restored on resume, so debugging information survives a restart. The
          `/stats` endpoint gains an opt-in `?include_connector_errors=true` selector that inlines
          these messages alongside the counters; the default response is unchanged so hot pollers
          stay lightweight. The support bundle collector uses the selector automatically.
        - (Checkpoint format) Backwards-compatible extension: `CheckpointInputEndpointMetrics` and
          `CheckpointOutputEndpointMetrics` gain optional `parse_errors` / `transport_errors` /
          `encode_errors` fields. Old checkpoints load as empty lists; checkpoints with no errors
          still serialize without the new keys, so unaffected files stay byte-identical.

        `CAST(variant AS VARCHAR)` will return a meaningful value for all
        scalar variant values, and not just for `VARIANT` objects with a
        string value.  In the past this cast used to return `NULL`.

        `(CAST(string AS VARIANT) AS type)` will now behave like
        `CAST(string AS type)`.  Previously the result was `NULL`.

        Conversion of short intervals including seconds to strings will
        now include the fractional seconds as well.  Previously the
        fractional seconds were ignored.

        ## v0.288.0

        Delta Lake input connector error handling behavior change:

        In the past if the connector wasn't able to read a table version, it
        signaled an error and moved to the next version. This could cause data loss.
        With this change the connector will either retry forever or fail and stop
        producing input after exhausting retry attempts.

        The second behavioral change is that the connector can now produce
        duplicate inputs even without a pipeline restart as the connector retries
        processing delta log entries.

        Functions `RLIKE` and `REPLACE_REGEXP` will crash for invalid
        regular expressions.  Previously they treated such as expressions
        as expressions which never match.  The new behavior more closely
        aligns with other databases.

        ## v0.281.0

        ### New dbt adapter for Feldera (`dbt-feldera`)

        A new [dbt](https://www.getdbt.com/) adapter that lets you build streaming data
        pipelines using standard dbt workflows. Install from PyPI:

        ```bash
        pip install dbt-feldera
        ```

        Feldera's DBSP engine automatically incrementalizes every query, so `incremental` models
        get true IVM without watermarks or manual merge logic.

        Supported materializations: `table`, `view`, `incremental`, `seed`, and
        `streaming_pipeline`.

        See the [README](https://github.com/feldera/feldera/tree/main/python/dbt-feldera) for
        configuration and usage details.

        Starting a pipeline while storage is still clearing (`storage_status=Clearing`) now returns
        `CannotStartWhileClearingStorage` instead of succeeding. Clearing storage while a start
        is in progress but hasn't yet transitioned to `Provisioning` now returns
        `StorageStatusImmutableUnlessStopped` instead of succeeding.

        Backward-incompatible Delta Lake output connector change. The new `max_retries` setting configures
        the number of times the connector retries failed Delta Lake operations like writing Parquet files
        and committing transactions. The setting is unset by default, causing the connector to retry
        indefinitely.  This behavior prevents data loss due to transient or permanent write errors.

        ### Checkpoint sync: `read_bucket` and checkpoint loading priority

        The checkpoint sync configuration now supports a `read_bucket` field — a read-only
        fallback bucket used to seed a pipeline's initial state. The pipeline **never writes**
        to `read_bucket`.

        Checkpoint sources are now resolved in priority order:
        1. **Local checkpoint** — if the pipeline already has a local checkpoint, it resumes
           from that without contacting any remote bucket.
        2. **`bucket`** — the pipeline's own S3-compatible bucket. If a checkpoint is found
           here, it is used and `read_bucket` is ignored.
        3. **`read_bucket`** — consulted only when both local storage and `bucket` are empty.
           This allows a new pipeline to seed from another pipeline's checkpoint, avoiding
           a full backfill. `read_bucket` must point to a different location than `bucket`.

        Refer to the [checkpoint sync documentation](/pipelines/checkpoint-sync#checkpoint-resolution-priority) for details.

        ### NATS input connector retry and health check support

        The NATS input connector now supports automatic reconnection with
        configurable retry behavior. Two new configuration fields have been added:
        - `inactivity_timeout_secs`: Maximum time in seconds to wait for the
          next message before running a stream/server health check.
        - `retry_interval_secs`: Delay between automatic reconnect attempts
          while in retry mode.

        The connector now supports pause and resume (start) lifecycle
        operations, validates replay and resume sequence bounds, and
        provides improved error messages during retries and health checks.

        ### NATS input connector timeout and probe updates

        - The default value for `inactivity_timeout_secs` has been increased
          from `10` to `60` seconds.
        - Health probes now avoid duplicate JetStream stream-info requests,
          reducing API pressure during retry and recovery loops.

        NATS retry classification during resume and replay validation has also been refined:
        transient failures while fetching JetStream stream metadata are now treated as retryable,
        while logical sequence-range validation failures remain fatal.

        ## v0.278.0

        ### Checkpoint sync: remote checkpoints older than v0.225.0 are no longer supported

        Checkpoints pushed to object storage by a Feldera pipeline older than v0.225.0 can no
        longer be used with Feldera v0.278.0 or later. Remote checkpoints must be stored as zip
        archives; the legacy unzipped format is no longer accepted.

        ## v0.263.0

        Added connector error list to input/output connector stats.
        [Input](https://docs.feldera.com/api/get-input-status) and
        [output](https://docs.feldera.com/api/get-output-status)
        status endpoints now list up to 100 most recent transport, parser, and
        encoder errors of each type.

        In addition, the openapi spec for both endpoints now specifies strongly typed return values
        of type `InputConnectorStatus` and `OutputConnectorStatus` respectively.

        ## v0.252.0

        ### Python API removed `ignore_deployment_error`

        The `ignore_deployment_error` parameter has been removed from the Python
        `pipeline.start()` method. Instead, make use of the newly added `dismiss_error` parameter.
        If you do not want the pipeline to start if there is a pre-existing deployment error,
        you should call `pipeline.start(dismiss_error=False)`. Otherwise call `pipeline.start()`
        which is by default equivalent to `pipeline.start(dismiss_error=True)` (preserving
        existing behavior). If the start results in an error occurring, the method will still
        throw an error as before. A pipeline deployment error can now also be separately dismissed
        using a dedicated endpoint and the corresponding client functions (e.g.,
        `pipeline.dismiss_error()` for the Python client).

        ### Kafka input connector `synchronize_partitions` option

        The Kafka input connector has a new setting `synchronize_partitions`.  When it is
        set to `true`, the connector will read messages in order of their Kafka timestamps
        across partitions.  Refer to the documentation for more information.

        ## 0.227.0

        Loading data from checkpoints made in earlier versions of feldera (0.226.0 and below)
        are not compatible with versions 0.227.0 and above.
        When upgrading to a version >=0.227, existing pipelines should be backfilled
        rather than starting from a previous checkpoint.

        ## 0.226.0

        The Delta Lake connector's `skip_unused_columns` property has been deprecated. Use
        table-level [`skip_unused_colums`](https://docs.feldera.com/sql/grammar#ignoring-unused-columns)
        instead.

        ## 0.201.0

        Cluster monitoring: Feldera now monitors the control plane components (api-server,
        kubernetes-runner and compiler-server) health and stores these as events in the
        database. They are exposed via `/v0/cluster/events` and further details of a specific
        event can be retrieved via `/v0/cluster/events/[<id>|latest]`.
        The `/v0/cluster_healthz` endpoint now returns the latest recorded event.
        All API clients support these endpoints. The Web Console will soon expose these
        events via a panel too.

        It monitors both what the services report themselves, as well as the status of the
        resources backing them. The resources monitoring feature is not yet stabilized,
        but can already be activated by adding `cluster_monitor_resources` to the
        Helm chart `unstableFeatures` array value. The kubernetes-runner, being responsible
        for the monitoring, is configured with an additional RBAC permission needed for this
        feature (see `kubernetes-runner-rbac.yaml` for changes).

        ## 0.188.0

        Prometheus metrics output now also contains pipeline names with a
        "pipeline_name" label, in addition to the exist "pipeline" label,
        which still contains the pipeline UUID.

        ## 0.186.0

        The Kafka input connector will now start reading partitions added
        to a topic upon resuming from a checkpoint.  Previously, the
        pipeline would not start in this case.  Please refer to the Kafka
        input connector documentation for details.

        ## 0.156.0

        BACKWARD-INCOMPATIBLE PYTHON SDK CHANGES

        - The `Pipeline.listen` method can now only be called when the pipeline is running or paused. Previously
          it was possible to call `Pipeline.listen` before starting the pipeline in order to guarantee that all
          outputs produced by the pipeline are captured by the listener. With the new API, you can achieve the
          same by starting the pipeline in a paused state using `Pipeline.start_paused` and calling `Pipeline.listen`
          before unpausing the pipeline using `Pipeline.resume`.

        ## 0.148.0

        API CHANGES: BACKWARD INCOMPATIBLE

        **API pipeline endpoints**
        - `/v0/pipelines/<name>/start`: no longer resumes a pipeline (instead use `/resume`)
        - `/v0/pipelines/<name>/start`: new parameter `?initial=running/paused/standby` (default: `running`)
        - `/v0/pipelines/<name>/pause`: no longer starts a pipeline as paused (instead use `/start?initial=paused`)
        - `/v0/pipelines/<name>/resume`: newly added

        **API pipeline field changes:**
        - `deployment_status`:
          - 1 removed variant: `Suspending`
          - 4 new variants: `Suspended`, `Replaying`, `Standby`, `Bootstrapping`
        - `deployment_desired_status`:
          - 2 new variants: `Standby`, `Unavailable`

        **API pipeline runtime configuration:**
        - Deprecated: `storage.backend.config.sync.standby`.
          It no longer has an effect, and is replaced by starting with `initial`.

        **Python**
        - Important: update your Python clients to the latest version, prior versions will not work properly
          (in particular, pipelines won't start because it used `/pause` to start them)
        - `Pipeline.start()`: no longer resumes a pipeline (instead use `Pipeline.resume()`)
        - `Pipeline.pause()`: no longer starts a pipeline as paused (instead use `Pipeline.start_paused()`)
        - `Pipeline.resume()`: no longer starts a pipeline as running (instead use `Pipeline.start()`)
        - `Pipeline.start_paused()`: newly added
        - `Pipeline.start_standby()`: newly added

        **fda**
        - `fda start`: no longer resumes a pipeline (instead use `fda resume`)
        - `fda pause`: no longer starts a pipeline as paused (instead use `fda start -i paused`)
        - `fda resume`: newly added

        API CHANGES: BACKWARD COMPATIBLE

        **API pipeline field additions:**
        - `deployment_id`
        - `deployment_initial`
        - `deployment_desired_status_since`
        - `deployment_resources_status`
        - `deployment_resources_status_since`
        - `deployment_resources_desired_status`
        - `deployment_resources_desired_status_since`
        - `deployment_runtime_status`
        - `deployment_runtime_status_since`
        - `deployment_runtime_desired_status`
        - `deployment_runtime_desired_status_since`

        Simplified the way user-defined aggregates are defined -- the
        compiler now automates the handling of NULL values.

        The Bloom filter implementation in Feldera storage has been replaced
        with a faster version that is incompatible with the previous version.
        This means that a checkpoint written by an older version may not
        perform as well when resumed with this or a later version, and
        checkpoints made with this or a later version cannot be resumed with
        earlier versions.

        ## 0.138.0

        [Transaction (also known as huge-step) support](/pipelines/transactions).

        TIMESTAMP is now the same as TIMESTAMP(3); TIME is now the same as
        TIME(9) (the default precision has been changed from 0 to 3; the
        documentation always claimed that the precision is 3).  Precisions
        that differ from the default ones are ignored (and the compiler
        gives a warning).

        ## 0.136.0

        In the Feldera Python SDK, `Pipeline.sync_checkpoint` will now raise a
        runtime error if `wait` is set to `True` and pushing this checkpoint
        fails.

        ## 0.135.0

        In the pipeline API available from a sidecar container only (not the
        external Feldera API), the `/status` endpoint no longer returns HTTP
        status 503 (SERVICE_UNAVAILABLE) while the pipeline is initializing.
        Instead, it returns status OK with message body containing the
        "Initializing" string.

        ## 0.129.0

        Values that are late in the NOW stream are no longer logged to the
        error stream.

        ## 0.126.0

        Until now, when fault tolerance was not enabled, resuming from a
        checkpoint would delete the checkpoint, so that it could only be resumed
        once.  This was intended to avoid the surprise of resuming from a very
        old checkpoint.  However, some users expect to be able to resume from a
        given checkpoint more than once.  This release changes the semantics, so
        that resume does not delete the checkpoint, and thus now it may be
        resumed more than once.  (This does not change behavior when fault
        tolerance is enabled, because multiple resumes from a given checkpoint
        were always allowed in that case.)

        ## 0.125.0

        Changed the default character set from ISO-8859-1 to UTF-8.
        Removed from the documentation the ability to specify a different
        character set for strings.  Removed mentions of trailing space
        trimming from strings.

        ## 0.124.0

        We have changed the documentation for the SUBSTR and SUBSTRING
        function to specify correctly their behaviors when arguments are
        negative.  Their behavior has not changed, but the documentation
        was incorrect.

        ## 0.105.0

        Changed the semantics of functions `ARRAY_CONTAINS`,
        `ARRAY_REMOVE`, `ARRAY_POSITION` so that the right argument being
        `NULL` does not cause the result being `NULL`.

        ## 0.105.0

        We switched the implementation of DECIMAL numbers to a new DECIMAL
        library that we have developed in house.  The library uses 3 times
        less space and is up to 100 times faster than our prior
        implementation.  This is a breaking change for user-defined
        functions.  The class exposed for DECIMALS has the same name as
        the previous implementation (`SqlDecimal`), but its API is
        completely different.

        ## 0.103.0

        This version changes the default values of various worker threads in our HTTP and IO runtime
        to be equal to the `worker` field in the runtime config.
        This is a change from the previous default where it was configured to use the number of
        CPU cores available on the node that a pod is running on.

        This change was made to ensure that the number of threads is sized more appropriately
        for the resources available to the pod. It also adds two new fields to the runtime config,
        `http_workers` and `io_workers` which can be used to set the number of threads for both
        runtimes explicitly.

        We also changed the amount of HTTP worker threads for control plane services (kubernetes-runner,
        api-server, pipeline-manager) to be equal to the number of cores
        allocated for them.

        ## 0.97.0

        This release modifies the state machine of a pipeline. The biggest user-facing change is that stopping a pipeline
        now acts similar to `Suspend` where a checkpoint is taken before stopping the pipeline. With this change, the
        `Suspend` state is redundant and removed from APIs and SDKs.

        Stopping a pipeline now takes a checkpoint before shutting down. Alternatively, "Force Stop" stops
        a pipeline without taking one, which means any progress since the last checkpoint was taken is lost.

        Pipeline state now persists between the runs; clearing it requires an explicit action.

        ### Changes to Web Console

        - Pipeline actions `Suspend` and `Shutdown` are now replaced with `Stop` and `Force Stop` respectively.
        - The new storage indicator shows whether storage is `In Use` (and allows to clear the storage) or `Cleared`.
        - Pipeline code and some configuration options cannot be edited while a pipeline's storage is in use.
        - The reason for the latest pipeline crash is now displayed as a banner above the code editor.

        ### Changes to REST API

        - Pipeline statuses `SuspendingCompute`, `Suspended`, `Failed`, `ShuttingDown`,
          `Shutdown` are removed and replaced with two new ones: `Stopping` and `Stopped`
        - Renamed pipeline status `SuspendingCircuit` to `Suspending`
        - New: storage status, which is either `Cleared`, `InUse` or `Clearing`
        - New: `/stop?force=false/true`, which deprovisions the compute resources of a
          pipeline. If `force=false` (default), a checkpoint is attempted before the
          deprovisioning.
        - New: `/clear`, which clears the storage of a pipeline
        - Removed: `/shutdown`, it should be replaced with `/stop?force=true` followed by
         `/clear` once stopped
        - Removed: `/suspend`, instead use `/stop?force=false`
        - `/logs` is now always available and does not get cleared when a pipeline is stopped
        - Changed: `/delete` now requires the storage to be cleared (`/clear`) to succeed.
        - Deprecated: `runtime_config.checkpoint_during_suspend`, instead call
          `/stop?force=false` if want to have a checkpoint taken before
          the deprovisioning, (`/stop?force=true` if not).

        ### Changes to Python SDK `feldera`:
        - Pipeline `shutdown` method replaced with new `stop`
        - Pipeline `suspend` method removed, use the `force = False` argument in `stop`
        - Added `clear_storage` argument to `delete`.

        ### Changes to CLI `fda`:
        - Added a `--force` option to `fda delete` to clear the storage of a pipeline.
        - Removed the `fda suspend` command, use `fda stop` instead (which can be set to take a checkpoint using `--checkpoint`).

        ### Changes to Rust SDK `feldera-rest-api`:

        - `PipelineStatus::Shutdown`, `PipelineStatus::Suspend`, `PipelineStatus::Stopped` all map to `PipelineStatus::Stopped` now
        - API calls to start/pause pipeline functions are replaced with individual functions, e.g.,

          ```rust
          let response = client
              .post_pipeline_action()
              .pipeline_name("my-pipeline")
              .action("start")
              .send()
              .await?;
          ```

          becomes

          ```rust
          let response = client
              .post_pipeline_start()
              .pipeline_name("my-pipeline")
              .send()
              .await?;
          ```

        ### Pipeline Manager

        - Changed the pipeline manager CLI argument `sql-compiler-home` to `sql-compiler-path`: Now a path to the sql-to-dbsp JAR file has to be provided rather than a path to the sql-to-dbsp directory.
              If the provided docker images are used (and the entrypoint is not modified), no change/migration is necessary.

        ## 0.90.0 (2025-06-20)
            - **Aligned Open Source and Enterprise version:** The enterprise edition of Feldera is now aligned with the Open Source edition. Versions will share the same codebase for a given release but the enterprise edition will include additional features and support.
    </TabItem>

    <TabItem className="changelogItem" value="oss" label="Open Source">
     [Release notes](https://github.com/feldera/feldera/releases/) for the Open Source edition can be found on github.
    </TabItem>

</Tabs>
