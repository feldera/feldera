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

        TIMESTAMP is now the same as TIMESTAMP(3); TIME is now the same as
        TIME(9) (the default precision has been changed from 0 to 3; the
        documentation always claimed that the precision is 3).  Precisions
        that differ from the default ones are ignored (and the compiler
        gives a warning).

        The following change doesn't affect the external Feldera API, only the
        pipeline's API available from a sidecare container. The `/status`
        endpoint no longer returns HTTP status 503 (SERVICE_UNAVAILABLE) while
        the pipeline is initializing. Instead it returns status OK with message
        body containing the "Initializing" string.

        ### Changes to Python SDK `feldera`:
        - `Pipeline.sync_checkpoint` will now raise a runtime error if `wait`
          is set to `True` and pushing this checkpoint fails.

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
