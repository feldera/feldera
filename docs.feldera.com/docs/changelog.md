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

        - The `bloom_false_positive_rate` storage setting now applies when a
          Bloom filter is read as well as when it is written.  Lowering it and
          restarting a pipeline reduces Bloom filter memory without rewriting
          any batches, and raising it again restores the accuracy the batches
          were written with.  See [Memory management](/operations/memory).

          Batches written at a rate finer than 0.1 use a new filter format, so
          a checkpoint written by this or a later version cannot be resumed by
          an earlier version.  Checkpoints written by earlier versions continue
          to be read.

        - Output connectors can be paused, like input connectors: a paused
          output connector discards the output of its view instead of writing it
          to its sink, which lets a pipeline run on while a sink is unavailable.
          Set `paused` in the connector configuration to create it paused, and
          use `POST /v0/pipelines/{pipeline}/views/{view}/connectors/{connector}/{start,pause}`
          or `fda connector <pipeline> <view> <connector> start|pause` at
          runtime. The connector status reports a `paused` field, the web console
          shows it, and the paused state survives a restart. Output produced
          while a connector is paused is not replayed when it is started. See
          [connector orchestration](/connectors/orchestration#output-connectors).

        - Python: `Pipeline.pause_input_connector` and
          `Pipeline.start_input_connector` (and their `FelderaClient`
          counterparts) name the kind of connector they act on, matching the new
          `pause_output_connector` and `start_output_connector`.
          `pause_connector` and `resume_connector` still work but are
          deprecated and now raise a `DeprecationWarning`.

        - Breaking change (fda): `--auth-token-command` and
          `FELDERA_AUTH_TOKEN_COMMAND` are removed. `--oidc-token-file` and
          `FELDERA_OIDC_TOKEN_FILE` take the path of a file holding a bearer
          token, which `fda` reads once per invocation; `feldera/oidc-auth-action`
          v3.0.1 and later export that variable. For a command that prints a
          token, pass its output as `--auth "$(...)"`.

        - The Delta Lake and Iceberg input connectors read a `VARIANT` column
          stored in the Parquet variant binary encoding, which is how Delta
          Lake's `variant` type stores one. Values keep the types the writer
          encoded, so a date inside a `VARIANT` arrives as a date rather than
          as a string. A `VARIANT` column stored as JSON text still reads as
          before, and the two can sit side by side in one table.

        - Breaking change (Delta Lake output connector): a `VARIANT` column is
          now written as the Delta `variant` type rather than as JSON text in a
          `string` column, so values keep the types they have in Feldera. Set
          `variant_encoding` to `json_string` for the previous encoding, which
          is also what appending to a table whose `VARIANT` column is already a
          `string` requires. See
          [VARIANT](/connectors/sinks/delta#variant).

        - The Kafka connector's `sasl.mechanism = OAUTHBEARER` authentication can now
          target GCP Managed Service for Apache Kafka, in addition to AWS MSK. Set the
          new `oauth_provider` field to `gcp` to mint tokens from Google Application
          Default Credentials, including the GKE metadata server under Workload
          Identity. See
          [Kafka input connector](/connectors/sources/kafka#how-to-write-connector-config) (#6886).

        - The NATS input connector implements the authentication methods its
          schema already declared: a bare `nkey` seed (Ed25519
          challenge-response), `jwt` with the seed that signs the connection
          nonce (decentralized/operator-mode auth with the JWT and seed as two
          secrets rather than one `.creds` file), `token`, and
          `user_and_password`. Previously only `credentials` took effect and
          the rest were silently ignored; configuring more than one method is
          now rejected. A new `tls` section in `connection_config` sets
          `require_tls` and `root_certificates_file` for servers whose
          certificates a private CA signs. See
          [NATS input connector](/connectors/sources/nats).

        ## v0.337.0

        - Breaking change (SQL): comparing a `UUID` with a character or binary value
          now converts that value to a `UUID`, the same direction as comparing a
          string with a number.  Previously the `UUID` was converted to the other
          operand's type, so `u <> ''` was always `FALSE`.  A value that does not
          denote a `UUID` is now a runtime error rather than a comparison that
          silently fails (#6883).

        - Breaking change (SQL): converting a string to a `UUID` now follows
          PostgreSQL: 32 hexadecimal digits of either case, optionally enclosed in
          braces, optionally separated by a hyphen after any complete group of four
          digits.  Forms such as `123e4567e89b12d3a456426655440000` and
          `{123e4567-e89b-12d3-a456-426655440000}` are now accepted, while the URN
          form `urn:uuid:123e4567-e89b-12d3-a456-426655440000` is rejected, as are
          strings with leading or trailing blanks.  Malformed strings are rejected
          instead of denoting a different `UUID`: the literal `UUID '1-2-3-4-5'`
          used to mean `00000001-0002-0003-0004-000000000005`.  `UUID` literals
          follow the same rules as the cast.  Converting a binary value to a `UUID`
          now requires exactly 16 bytes; a longer value used to be truncated.  See
          [UUID operations](https://docs.feldera.com/sql/uuid).

        - `/checkpoint` and `/checkpoint/sync` now return an `incarnation_uuid`
          that can be used with their corresponding status calls to detect if
          the pipeline restarts.  If it does, the operation itself must be restarted
          (the most common use of the Python API does this automatically).

        ## v0.333.0

        - Breaking change (SQL): `=`, `<>` and `!=` are no longer allowed between
          `ROW` values, so some programs that used to compile are now rejected.
          The previous implementation of these operations did not follow the
          standard SQL semantics, which requires that `ROW(NULL) = ROW(NULL)` evaluates
          to `NULL` (in our implementation it would evaluate to `TRUE`).

          Instead of these operators, use `IS NOT DISTINCT FROM` (or its
          shorthand `<=>`) and `IS DISTINCT FROM` instead.  A user-defined type declared with
          `CREATE TYPE ... AS (...)` is a `ROW` type, so the restriction covers its
          values too.

          The restriction also covers the forms that ask for `ROW` equality without
          writing `=`: a join condition, `NATURAL JOIN`, `USING`, `IN`,
          `CASE value WHEN`, `NULLIF`, and a comparison between two row constructors
          such as `(a, b) = (c, d)`.  See
          [comparing `ROW` values](https://docs.feldera.com/sql/comparisons#comparing-row-values)
          for the accepted rewrite of each form.

        - Joining on `ROW` values is now supported, using
          `ON left.r IS NOT DISTINCT FROM right.r` (#3398).

        ## v0.330.0

        - Feldera's membership table now authorizes every login: a user acts
          in the tenants they hold a membership in, whether or not the token's
          `tenants` claim names them. The claim, the issuer tenant, and the
          per-`sub` personal tenant become provisioning strategies that create
          memberships at login, gated by the new
          `authorization.provisionOnLogin` (default `true`). Set it to `false`
          so that access comes only from memberships granted through
          `POST /v0/tenant/users` and the web console, with no claim mapping
          maintained at the identity provider. See
          [Tenant Assignment Strategies](/get-started/enterprise/authentication#tenant-assignment-strategies)
          and the
          [migration guide](/get-started/enterprise/authentication#migrating-to-feldera-managed-memberships).

        - Breaking change (revocation): narrowing a token's `tenants` claim no
          longer revokes access, because membership rows from past logins stay
          live. While `provisionOnLogin` is `true`, removing a member does not
          keep them out either: the claim re-enrolls them on their next login,
          so full revocation takes both levers. Audit memberships per tenant
          with `GET /v0/tenant/users` before or right after upgrading, and see
          [Revoking access](/get-started/enterprise/authentication/roles#revoking-access).
          Managed tenancy (the `tenants` claim) is deprecated in favor of
          Feldera-managed memberships. Two smaller visible changes: some
          login-path refusals answer `403` or `400` where they answered `401`,
          and a claim entry a multi-tenant user does not select no longer
          creates a missing tenant at login (create tenants explicitly with
          `POST /v0/tenants`).

        - Member lists now carry the name and email the identity provider
          holds for each member, and whether that provider vouches for the
          email, so an administrator recognizes a member without decoding an
          OIDC `sub`. `GET /v0/tenant/users` gains `display_name` and
          `email_verified`; the web console's member list and `fda member list`
          show both.

        - Renaming a tenant no longer requires updating identity provider
          claim mappings once `provisionOnLogin` is `false`: memberships
          reference the tenant by id, not by name.

        - Owners can retrieve a single tenant by name or identifier through
          `GET /v0/tenants/{tenant_id}` or `fda tenant get`, so provisioning
          automation such as an operator reconcile loop checks for a tenant
          with one request instead of filtering `GET /v0/tenants`. See
          [Changing your authentication setup](/get-started/enterprise/authentication#changing-your-authentication-setup).

        - `POST /v0/tenants` is now idempotent: creating a name that already
          exists returns the existing tenant with `200 OK` instead of failing
          with `409 Conflict`, and a fresh name still returns `201 Created`.
          Both responses carry the tenant's `id`, `name`, and
          `initial_provider`. `fda tenant create` is the CLI counterpart.

        ## v0.329.0

        - Backward-incompatible change in the `dbsp` crate Rust API: the
          `add_input_set` operator was removed, together with the `SetHandle`
          type and the dynamic variants `dyn_add_input_set` and
          `dyn_add_input_set_mono`. To keep set semantics (duplicate inserts
          and deletes of absent elements are no-ops), use `add_input_map` with
          the element as both key and value; its upsert semantics subsumes set
          semantics. If the input contains no duplicates, `add_input_zset` is
          a cheaper alternative, but the caller must guarantee that weights
          stay 0 or 1. SQL pipelines are not affected: the SQL compiler never
          generated input sets.

        ## v0.328.0

        - Input connectors support the `soft_delete` property, which ingests
          deletions as insertions and reports the original polarity of each
          record in the `is_delete` metadata attribute, so that a table
          represents the stream of updates it receives rather than the current
          contents of that stream. See
          [Soft deletes](/connectors#soft-deletes).

        - Connectors that ingest columnar data, e.g., Delta Lake, Iceberg, and
          the Parquet format, now populate columns that default to
          `CONNECTOR_METADATA()`. Previously such a column was always `NULL`
          for these connectors.

        ## v0.327.0

        - Role-based access control (RBAC). Access is now governed by per-user,
          per-tenant roles (`read` < `write` < `admin` < `owner`) rather than every
          authenticated user having full access to their tenant. See
          [Roles](/get-started/enterprise/authentication/roles) for the model.

          Upgrading an existing authenticated installation: before RBAC every
          authenticated user had read and write access; after the upgrade a returning
          user starts with no membership and is admitted at the configured default role.
          Set `authorization.defaultRole: write` (the binary default is `read`; the Helm
          chart sets it to `write` in `values.yaml`) so returning users keep their read
          and write access, and set
          `authorization.owners` to bootstrap a platform owner, who can then grant
          `admin` to whoever manages users and OIDC trust. The default role is applied
          and recorded on a user's first login after the upgrade, so set it before the
          first post-upgrade logins; changing it later does not re-grade users who have
          already logged in (an owner or admin adjusts those individually). Tighten
          `defaultRole` back to `read` once explicit roles are provisioned.

        - A tenant is now identified by its name alone. Before, a tenant was keyed by
          `(name, OIDC issuer)`, so changing `auth.issuer` created a second, empty
          tenant of the same name and stranded the pipelines on the first one, which
          no login could reach. Logins now resolve the existing tenant across an
          issuer change. A deployment whose issuer already changed holds two tenants
          of the same name: the upgrade keeps the name on the one its users reach
          today and appends the id to the other's name, merging and deleting nothing.
          `GET /v0/tenants` lists both.

        - Owners can rename a tenant, through `PATCH /v0/tenants/{tenant_id}` or the
          web console's admin page. A rename changes only the name, which is what a
          login resolves, so it is how an owner reunites users with a tenant they can
          no longer reach: the `default` tenant after authentication is switched on,
          or a tenant left behind by an identity-provider change. Pass
          `displace_existing` to take a name the first login already created a tenant
          under; that tenant becomes `<name> (<id>)` and keeps everything it had. See
          [Changing your authentication setup](/get-started/enterprise/authentication#changing-your-authentication-setup).

        - Breaking change (API keys): the `scopes` array on API-key responses
          (`GET /v0/api_keys`, `GET /v0/api_keys/{api_key_name}`) is replaced by a single
          `role` string, one of `read` or `write` (lower-case). Clients that read the
          `scopes` field must read `role` instead. Existing keys are migrated to `write`,
          so their access is unchanged.

        - Breaking change (API keys): a new key now defaults to `read` instead of
          carrying read and write access. `POST /v0/api_keys` without a `role` field
          creates a `read`-only key; pass `{"role": "write"}` to keep the previous
          behavior. `fda apikey create` defaults to `--role read` for the same reason;
          pass `--role write` where a key needs to make changes.

        ## v0.325.0

        - The SQL compiler was incorrectly garbage-collecting input
          tables with a primary key and a column with LATENESS (#6690).  Such
          tables can only be GC-ed if the column with LATENESS is part of
          the primary key.  As a result some programs that used to run
          with finite state will now have unbounded state.

        ## v0.322.0

        - Pipeline API field `deployment_runtime_status_details` is now strongly typed,
          whereas before it was just a generic JSON value type. While `AwaitingApproval`,
          the diff is now located at `deployment_runtime_status_details.approval_diff`
          instead of being the whole details itself.

        - Pipelines from v0.322.0 onward will have their GET selector
          `status_with_connectors` connector stats cached, which are now updated along
          with the runtime status details within roughly 1-15s.

        ## v0.319.0

        - Cluster monitor events with information on the backing (Kubernetes) resources is
          no longer gated behind unstable feature `cluster_monitor_resources` (deprecated).
          It is now enabled by default. This change adds RBAC permissions to get the
          deployments of the API server and the runner. The status of the backing Kubernetes
          resources is shown in the Feldera Health page to every (authenticated) user.
          The cluster monitoring of resources can still be disabled by setting in the Helm
          chart `disableClusterMonitorResources` to `true`.

        ## v0.316.0

        - A bug fix introduced a backward incompatible change to the replay journal format.
          This only affects pipelines configured with exactly-once fault tolerance. Such
          pipelines should not be upgraded to the new Feldera runtime if they are in a failed
          state with non-empty replay journal.  Upgrade is possible once the pipeline has been
          cleanly stopped without a failure.

        ## v0.313.0

        - New DynamoDB output connector. It writes a SQL view to an Amazon
          DynamoDB table, mapping inserts and updates to upserts and deletes to
          deletes keyed by the table's primary key. See the
          [connector documentation](/connectors/sinks/dynamodb) for configuration
          details.

        - Pipelines now have a `tags` field: free-form labels for organizing and
          filtering pipelines, exposed across the API, `fda` CLI, web console, and
          Python SDK (`PipelineBuilder(tags=...)`, `pipeline.modify(tags=...)`,
          `pipeline.tags()`).

        - Editing only a pipeline's `description` or `tags` no longer bumps its
          `version` or `refresh_version`, nor triggers recompilation. These fields can
          therefore be edited at any state of a pipeline (e.g. while running).

        ## v0.311.0

        - The default value of `max_output_buffer_size_records` is now 10,000,000
          instead of unbounded.

        ## v0.309.0

        - Rust compiler will clean up the `target` directory automatically
          when its usage reaches the disk limit. This is currently behind
          the unstable feature flag `rust_compiler_full_cleanup`.

        - Pipeline name is now limited to 63 characters and must follow the Kubernetes
          label pattern (and be non-empty and contain no dots as before). The check is only enforced
          when the pipeline is being newly created, its `name` field is being PATCHed
          or it is getting fully updated via PUT even if the name does not change.
          Otherwise, existing pipelines with a now invalid name will continue to function.
          This change is not backward compatible for scripts that create pipelines with names that are
          no longer valid, in which case they will now receive an error instead of succeeding.
          However, especially in the Kubernetes runner these pipelines would already not work.

        ## v0.307.0

        - Casts of strings to Boolean and floating point values will
        produce runtime errors instead of legal values for illegal string
        values.  The set of strings that can be legally converted to
        Booleans has been changed.

        ## v0.306.0

        - No longer allowed to edit `runtime_config.resources.storage_class` if the pipeline storage is not cleared.

        - Calling `/start` on a pipeline that already failed to compile will directly return an error instead of
          the runner later on setting the `deployment_error` during its check whether to proceed to provisioning.

        - New `max_queued_bytes` setting for input connectors.  The default is 1,000 times
          `max_queued_records`, whether that is explicitly set or the default of 1,000,000.
          This is a change in behavior, since previously there was no byte limit.  We
          believe that the new behavior is generally an improvement that will prevent using
          excessive memory or even running out of memory but, to restore the previous
          behavior, specify a large number for `max_queued_bytes`.

        - Delta Lake output connector:

        `log_retention_duration` and
        `enable_expired_log_cleanup` config options to control transaction-log retention on newly created
        tables.

        Both are only applied at table creation ( i.e. new table or truncate mode ); against an existing table they
        are ignored. Defaults are unchanged (Delta Lake's own: 30 days, cleanup enabled).

        The connector now logs a warning at startup when `checkpoint_interval`,
        `log_retention_duration`, or `enable_expired_log_cleanup` in the connector config
        differs from the existing table's metadata.

        - Large Delta Lake, Iceberg scans (e.g. Delta CDC `ORDER BY`) and ad-hoc queries now share a bounded memory pool
        and spill to disk under `<storage>/datafusion-tmp/`.

        A new `runtime_config.datafusion_memory_mb` setting controls the pool size
        (defaults to 5% of the pipeline's memory budget, capped at 2 GB).

        ## v0.294.0

        The HTTP egress API endpoint now accepts a connector configuration as the JSON body.
        This allows more control over connector configuration.  For example:

        ```
        curl -s -N -X POST 'http://127.0.0.1:8080/v0/pipelines/PIPELINE_NAME/egress/VIEW_NAME' --json '{"format": {"name": "json", "config": {"array": true}}}'
        ```

        ## v0.292.0

        Pipeline monitoring: Feldera now monitors and persists a pipeline's health.
        Events are queryable via `/v0/pipelines/[pipeline]/events?selector=[all|status]`
        and a specific event via `/v0/pipelines/[pipeline]/events/[<event-id>|latest]?selector=[all|status]`.
        All API clients support these endpoints. The Web Console will soon expose these
        events via a tab too.

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
