---
pagination_next: null
pagination_prev: null
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Changelog


<Tabs>
    <TabItem className="changelogItem" value="enterprise" label="Enterprise">

        ## Unreleased

        This release modifies the state-machine of a pipeline. The biggest user-facing change is that stopping a pipeline
        will now act like suspend (e.g., take a checkpoint before stopping). Hence the suspend state is redundant now and was removed
        from APIs and SDKs.

        TODO: Add more details.

        ### Changes to Web Console
        - TODO

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
        - `/logs` is now always available and does not get cleared upon stop
        - Deprecated: `runtime_config.checkpoint_during_suspend`, instead call
          `/stop?force=false` if you do wish to have a checkpoint attempted before
          the deprovisioning, and `/stop?force=true` if not.

        ### Changes to Python SDK `feldera`:
        - TODO

        ### Changes to CLI `fda`:
        - Removed the `fda suspend` command, use `fda stop` instead (which will suspend by default as long as the `--no-checkpoint` flag is not set).


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

        ## 0.90.0 (2025-06-20)
            - **Aligned Open Source and Enterprise version:** The enterprise edition of Feldera is now aligned with the Open Source edition. Versions will share the same codebase for a given release but the enterprise edition will include additional features and support.
    </TabItem>

    <TabItem className="changelogItem" value="oss" label="Open Source">
     [Release notes](https://github.com/feldera/feldera/releases/) for the Open Source edition can be found on github.
    </TabItem>
</Tabs>
