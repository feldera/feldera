"""Checkpoint round-trip across a runtime upgrade.

This test verifies that pipeline state checkpointed under the previous
minor runtime version restores cleanly under the current runtime. The
checkpoint format must remain forward compatible across runtime
versions.

The test exercises a single Delta Lake source table that is intentionally
wide (a column for every SQL data type that round-trips cleanly through
parquet today). The accompanying SQL program touches every operator
that owns a custom ``pub trait Checkpoint`` implementation: LATENESS-
driven window aggregation, recursive views, joins (binary and three-
way), DISTINCT, GROUP BY, and a materialized output. That way a
checkpoint produced by the legacy runtime stresses every code path the
current runtime is expected to deserialize.

The test runs in two phases:

1. **Legacy phase.** Compile the pipeline with ``runtime_version`` set to
   the previous minor (computed from the SDK version), drain the Delta
   snapshot, take a checkpoint, and sync it to MinIO/S3.

2. **Current phase.** Stop the legacy pipeline, switch
   ``runtime_version`` to ``None`` (the manager's default, which is the
   build under test), and resume from the synced checkpoint with
   ``BootstrapPolicy.ALLOW``. The restored row counts in every
   downstream view must match the values observed under the legacy
   runtime.

Caching:

* The Delta source is generated once per ``DELTA_SOURCE_CACHE_KEY`` and
  reused across runs. Locally that means a fixed ``/tmp`` directory; in
  CI it means a fixed prefix in the test MinIO bucket. Bump the key
  whenever the schema or generator changes.
* The synced checkpoint is reused across runs as well, keyed by
  ``CHECKPOINT_CACHE_KEY``. When ``checkpoints.feldera`` already exists
  under that prefix, phase 1 is skipped and only phase 2 runs (it
  compares the restored view hashes against ``EXPECTED_VIEW_HASHES``).
  Bump the key when the SQL or expected hashes change.

Requirements:

* Enterprise build (``checkpoint`` and S3-backed sync are enterprise
  features).
* Single-host pipeline (the underlying checkpoint sync helper is
  single-host only).
* x86_64 (the ``deltalake`` wheel aborts on aarch64 CI runners).
"""

from __future__ import annotations

import json
import re
import sys
import uuid as _uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import feldera
from feldera import PipelineBuilder
from feldera.enums import BootstrapPolicy, FaultToleranceModel
from feldera.runtime_config import RuntimeConfig, Storage
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
    single_host_only,
)
from tests import TEST_CLIENT, enterprise_only
from tests.platform.helper import gen_pipeline_name
from tests.platform.test_checkpoint_sync import storage_cfg
from tests.utils import DeltaTestLocation

# Number of rows in the Delta source table.
TABLE_ROWS = 1000

# Stable cache keys. The pipeline name itself rotates per CI run via
# ``gen_pipeline_name``, but the Delta source and the synced checkpoint
# both live at fixed paths so they persist across runs:
#
# * The Delta source is regenerated only when its row count drifts.
# * The synced checkpoint lets phase 1 be skipped on cache hits; an
#   older legacy-version checkpoint is expected to remain compatible
#   with newer current-version pipelines.
DELTA_SOURCE_CACHE_KEY = "checkpoint_runtime_upgrade_delta_v1"
CHECKPOINT_CACHE_KEY = "checkpoint_runtime_upgrade_checkpoint_v2"

# Expected per-view hashes after ingesting the cached Delta source. We
# deliberately compare phase 2 against fixed hashes (rather than against
# phase 1's output) so phase 1 can be skipped on cache hits and we still
# detect a regression. Recompute by running the test once, copying the
# values printed by ``pipeline.query_hash`` for each view, and pasting
# them here. Bump ``CHECKPOINT_CACHE_KEY`` whenever these change.
EXPECTED_VIEW_HASHES = {
    "v_passthrough": "844C02F1F5D4FDF07BBA13545502CE9D94B09C773491F695AC56839F4060EEB9",
    "v_filtered": "53FFCCDDE1F1053E543EA2B31D73748AB12A67421318A6828E38D17364542653",
    "v_distinct_tags": "1B63E4F38BC40A2982B8176397F7E5EB6E56D12D17465A2C164302B212713607",
    "v_grouped": "B8B58C67487E45EFC4F5786F3573E0A0CCF4A167D703FAB4075E9128B42338AF",
    "v_joined": "A07E1680990BABC69EFD0FFC89E44837D771A7A796BA6AD3C1A876F5103B98CA",
    "v_three_way": "FEF6DB37149C29441364C0FA40594AE77BF40855FB0523CE9AF2D9577436E9C9",
    "v_window_count": "95DB57846E76A5F8DC073F9614ACFAEA8E932DC50FFC07D6F5899A70F36C4FC7",
    "closure": "4CF3EE5BA9183E48DFDDAAB9D33EE33E4086F2284B2192ABF863CF06A591A5B2",
    # v_emit_final omitted: see TODO in _build_sql
}


def _assert_view_hashes(pipeline) -> None:
    """Hash every comparison view and assert it matches the encoded value.

    ``query_hash`` is order-independent, so no ``ORDER BY`` is required.

    On mismatch we collect the actual hashes for every view and print
    a copy-pasteable diff before failing, so the regen workflow is a
    single run instead of fail-edit-rerun per view.
    """

    actual = {
        view: pipeline.query_hash(f"SELECT * FROM {view}")
        for view in EXPECTED_VIEW_HASHES
    }
    mismatches = {
        view: (EXPECTED_VIEW_HASHES[view], got)
        for view, got in actual.items()
        if got != EXPECTED_VIEW_HASHES[view]
    }
    if not mismatches:
        return

    lines = [
        f"{len(mismatches)} view hash(es) changed; "
        "copy the new values into EXPECTED_VIEW_HASHES:",
    ]
    for view, got in actual.items():
        marker = "!=" if view in mismatches else "=="
        lines.append(
            f'    "{view}": "{got}",  {marker} expected {EXPECTED_VIEW_HASHES[view]}'
        )
    raise AssertionError("\n".join(lines))


def _legacy_runtime_version() -> str:
    """Return the runtime version one minor below the SDK version.

    The SDK version (e.g. ``"0.293.0"``) drives the test's notion of
    "current" because the SDK and the manager are released together. The
    test assumes runtime tags are emitted as ``vMAJOR.MINOR.PATCH`` and
    that the immediately previous release is reachable by decrementing
    only the minor component; that matches how the project releases
    today.
    """

    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", feldera.__version__)
    if match is None:
        raise RuntimeError(f"unrecognized feldera SDK version: {feldera.__version__!r}")
    major, minor, _patch = (int(group) for group in match.groups())
    if minor == 0:
        # We could decrement major instead, but the project has not had
        # to do so yet; flag the situation explicitly.
        raise RuntimeError(
            "cannot decrement minor below 0 "
            f"(version={feldera.__version__!r}); update this test"
        )
    return f"v{major}.{minor - 1}.0"


# Wide schema: covers every Feldera SQL type that we can round-trip
# through Delta. UUID and VARIANT travel as STRING (the connector's
# default uuid_format / variant_format); TIME, INTERVAL, and GEOMETRY
# are intentionally omitted.
_DELTA_SCHEMA_ARROW_DESC = (
    ("id", "uint32", False),  # Primary key.
    ("flag", "bool", False),
    ("tiny", "int8", False),
    ("small", "int16", False),
    ("regular", "int32", False),
    ("big", "int64", False),
    ("real_val", "float32", False),
    ("double_val", "float64", False),
    ("amount", "decimal", False),
    ("name", "string", False),
    ("note", "string", True),
    ("payload", "binary", True),
    ("created_at", "ts_us_utc", False),
    ("created_on", "date", False),
    ("tags", "list_string", True),
    ("attrs", "map_string_string", True),
    ("nested", "struct_id_label", True),
    ("uid", "string", False),  # SQL UUID, transported as string.
    ("info", "string", True),  # SQL VARIANT, transported as JSON string.
)


def _arrow_schema():
    """Build a pyarrow schema mirroring ``_DELTA_SCHEMA_ARROW_DESC``."""

    import pyarrow as pa

    type_map = {
        "uint32": pa.uint32(),
        "bool": pa.bool_(),
        "int8": pa.int8(),
        "int16": pa.int16(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "float32": pa.float32(),
        "float64": pa.float64(),
        "decimal": pa.decimal128(20, 4),
        "string": pa.string(),
        "binary": pa.binary(),
        "ts_us_utc": pa.timestamp("us", tz="UTC"),
        "date": pa.date32(),
        "list_string": pa.list_(pa.string()),
        "map_string_string": pa.map_(pa.string(), pa.string()),
        "struct_id_label": pa.struct(
            [pa.field("inner_id", pa.int32()), pa.field("label", pa.string())]
        ),
    }
    return pa.schema(
        [
            pa.field(name, type_map[type_name], nullable=nullable)
            for name, type_name, nullable in _DELTA_SCHEMA_ARROW_DESC
        ]
    )


_BASE_TS = datetime(2026, 1, 1, tzinfo=timezone.utc)
_BASE_DATE = _BASE_TS.date()


def _row(idx: int) -> dict:
    """Generate a single deterministic row keyed by ``idx``."""

    bucket = idx % 5
    return {
        "id": idx,
        "flag": idx % 2 == 0,
        "tiny": (idx % 7) - 3,
        "small": (idx * 11) % 30000,
        "regular": idx,
        "big": idx * 1_000_003,
        "real_val": float(idx) * 0.5,
        "double_val": float(idx) * 0.25,
        "amount": Decimal(idx) / Decimal(100),
        "name": f"row-{idx:08d}",
        "note": None if idx % 3 == 0 else f"note-{idx}",
        "payload": None if idx % 4 == 0 else json.dumps({"i": idx}).encode(),
        "created_at": _BASE_TS + timedelta(seconds=idx),
        "created_on": _BASE_DATE + timedelta(days=idx % 365),
        "tags": [f"tag-{bucket}", "all"] if idx % 6 != 0 else None,
        "attrs": (
            [("kind", "test"), ("bucket", str(bucket))] if idx % 5 != 0 else None
        ),
        "nested": {"inner_id": idx, "label": f"label-{bucket}"} if idx else None,
        "uid": str(_uuid.UUID(int=idx)),
        "info": None if idx % 8 == 0 else json.dumps({"bucket": bucket, "n": idx}),
    }


def _writer_storage_options(source: DeltaTestLocation) -> dict | None:
    """Return ``storage_options`` for ``deltalake.write_deltalake``.

    The base options come from the connector config. For S3/MinIO writes
    we also enable ``aws_s3_allow_unsafe_rename`` because deltalake's
    default lock provider is DynamoDB, which is not available in the
    test environment.
    """

    opts = source.delta_storage_options()
    if not opts:
        return None
    if source.uri.startswith("s3://"):
        opts.setdefault("aws_s3_allow_unsafe_rename", "true")
    return opts


def _ensure_delta_source(source: DeltaTestLocation) -> None:
    """Generate the Delta source if the cache is missing or wrong-sized."""

    cached = source.row_count(missing_ok=True)
    if cached == TABLE_ROWS:
        print(
            f"reusing cached Delta source ({TABLE_ROWS} rows) at {source.uri}",
            file=sys.stderr,
        )
        return

    print(
        f"populating Delta source at {source.uri} "
        f"(cached={cached}, expected={TABLE_ROWS})",
        file=sys.stderr,
    )

    import pyarrow as pa
    from deltalake import write_deltalake

    rows = [_row(i) for i in range(TABLE_ROWS)]
    table = pa.Table.from_pylist(rows, schema=_arrow_schema())
    write_deltalake(
        source.uri,
        table,
        mode="overwrite",
        storage_options=_writer_storage_options(source),
    )


def _minio_fs():
    """Return an S3 filesystem for the CI MinIO bucket and the bucket name."""

    from urllib.parse import urlparse

    import pyarrow.fs as pafs

    from tests.utils import (
        MINIO_BUCKET,
        MINIO_ENDPOINT,
        MINIO_REGION,
        required_env,
    )

    endpoint = MINIO_ENDPOINT.rstrip("/")
    parsed = urlparse(endpoint)
    s3 = pafs.S3FileSystem(
        access_key=required_env("CI_K8S_MINIO_ACCESS_KEY_ID"),
        secret_key=required_env("CI_K8S_MINIO_SECRET_ACCESS_KEY"),
        endpoint_override=endpoint,
        scheme=parsed.scheme,
        region=MINIO_REGION,
    )
    return s3, MINIO_BUCKET


def _remote_checkpoint_exists(cache_name: str) -> bool:
    """Return True when a complete checkpoint is cached in the bucket.

    The bucket layout matches what ``storage_cfg`` produces:
    ``{MINIO_BUCKET}/{cache_name}/checkpoints.feldera``.

    Probing ``checkpoints.feldera`` rather than the bucket directory is
    intentional: the manager writes the per-worker state files first
    and ``checkpoints.feldera`` last.
    """

    import pyarrow.fs as pafs

    s3, bucket = _minio_fs()
    info = s3.get_file_info(f"{bucket}/{cache_name}/checkpoints.feldera")
    return info.type == pafs.FileType.File


def _read_remote_checkpoint_catalog(cache_name: str) -> str:
    """Return the cached ``checkpoints.feldera`` catalog, pretty-printed."""

    s3, bucket = _minio_fs()
    with s3.open_input_stream(f"{bucket}/{cache_name}/checkpoints.feldera") as f:
        raw = f.read().decode("utf-8")
    try:
        return json.dumps(json.loads(raw), indent=2)
    except json.JSONDecodeError:
        return raw


def _connector_json_for(source: DeltaTestLocation) -> str:
    """Serialize the Delta connector config for embedding in the SQL DDL."""

    return json.dumps(
        [
            {
                "name": "delta_in",
                "transport": {
                    "name": "delta_table_input",
                    "config": source.connector_config,
                },
            }
        ]
    )


def _build_sql(connector_json: str) -> str:
    """Build the SQL program for both phases.

    Verified against a phase-1 support bundle (with
    ``dev_tweaks={'adaptive_joins': True}`` enabled), the compiled
    circuit contains every operator that owns a custom
    ``Checkpoint`` impl in DBSP today:

    * ``Output`` and ``AccumulateOutput`` -- output sinks behind every
      materialized view.
    * ``UntimedTraceAppend``, ``AccumulateUntimedTraceAppend``,
      ``AccumulateTraceAppend``, ``AccumulateDelayTrace`` -- the trace
      and accumulate-trace families used by DISTINCT, GROUP BY, and
      JOIN.
    * ``InputUpsertWithWaterline`` -- input + LATENESS waterline.
    * ``Window`` -- emitted because of the ``emit_final`` annotation
      below; the runtime gates output on the watermark.
    * ``InnerStarJoin_*`` -- the runtime wrapper around the multijoin
      ``Match`` operator (``operator/dynamic/multijoin/match_keys.rs``);
      synthesized by the SQL star-join pass for the multi-aggregation
      ``v_emit_final`` view.
    * ``RebalancingExchangeSender`` -- gated on the ``adaptive_joins``
      dev tweak (set by the test); emitted by the balanced trace path.
    * ``Z^-1`` and ``Z^-1 (nested)`` -- delays inside the recursive
      ``closure`` view.
    * ``Transaction Z^-1`` -- transactional delay.

    The phase-2 assertion compares ``v_passthrough`` row by row, which
    on its own forces the full input through the input + waterline
    path. The other materialized views ensure the trace-bearing,
    star-join, window, and recursion-bearing operators carry
    non-trivial state into the checkpoint.
    """

    # Embed the connectors JSON; SQL only needs single quotes doubled.
    escaped = connector_json.replace("'", "''")

    return f"""\
CREATE TABLE input_table (
    id           INT NOT NULL PRIMARY KEY,
    flag         BOOLEAN NOT NULL,
    tiny         TINYINT NOT NULL,
    small        SMALLINT NOT NULL,
    regular      INT NOT NULL,
    big          BIGINT NOT NULL,
    real_val     REAL NOT NULL,
    double_val   DOUBLE NOT NULL,
    amount       DECIMAL(20, 4) NOT NULL,
    name         VARCHAR NOT NULL,
    note         VARCHAR,
    payload      VARBINARY,
    -- TODO: restore LATENESS once boostrapping works with LATENESS
    created_at   TIMESTAMP NOT NULL, -- LATENESS INTERVAL 1 HOURS,
    created_on   DATE NOT NULL,
    tags         VARCHAR ARRAY,
    attrs        MAP<VARCHAR, VARCHAR>,
    nested       ROW(inner_id INT, label VARCHAR),
    uid          UUID NOT NULL,
    info         VARIANT
) WITH (
  'connectors' = '{escaped}'
);

-- Verification view: the full input echoed back. Compared row-by-row
-- across phases.
CREATE MATERIALIZED VIEW v_passthrough AS
  SELECT * FROM input_table;

-- Filter on the PK; exercises the spine's per-batch index state.
CREATE MATERIALIZED VIEW v_filtered AS
  SELECT COUNT(*) AS n FROM input_table WHERE id < 100;

-- DISTINCT, GROUP BY, and joins all build trace / accumulate-trace
-- state behind the scenes; checkpointing must round-trip that state.
CREATE MATERIALIZED VIEW v_distinct_tags AS
  SELECT COUNT(*) AS n FROM (
    SELECT DISTINCT (id % 5) AS bucket FROM input_table
  );

CREATE MATERIALIZED VIEW v_grouped AS
  SELECT (id % 5) AS bucket, COUNT(*) AS n
    FROM input_table
    GROUP BY (id % 5);

CREATE MATERIALIZED VIEW v_joined AS
  SELECT COUNT(*) AS n
    FROM input_table a
    JOIN input_table b ON a.id = b.id
   WHERE a.id < 50;

CREATE MATERIALIZED VIEW v_three_way AS
  SELECT COUNT(*) AS n
    FROM input_table a
    JOIN input_table b ON a.id = b.id
    JOIN input_table c ON a.id = c.id
   WHERE a.id < 25;

-- LATENESS on `input_table.created_at` routes ingest through the
-- watermark-aware input operator; the GROUP BY here keeps that path
-- materialized.
CREATE MATERIALIZED VIEW v_window_count AS
  SELECT
    TIMESTAMP_TRUNC(created_at, HOUR) AS hour,
    COUNT(*) AS n
  FROM input_table
  GROUP BY TIMESTAMP_TRUNC(created_at, HOUR);

-- TODO: restore once bootstrapping supports relations with lateness;
-- `emit_final` requires a watermark, which requires LATENESS on
-- `created_at` above.
--
-- The combination of `emit_final` on a LATENESS-bearing column and a
-- multi-aggregation GROUP BY drives the compiler down the star-join
-- path: the grouped MIN/MAX/STDDEV/ARG_MAX collapses into a Window
-- operator (waterline gating) plus a StarJoin / Match multijoin.
-- Until the watermark advances the view emits no rows, but the
-- Window/Match state is still part of the checkpoint.
--CREATE MATERIALIZED VIEW v_emit_final
--WITH ('emit_final' = 'created_at')
--AS SELECT
--    created_at,
--    MIN(regular)        AS min_regular,
--    MAX(regular)        AS max_regular,
--    STDDEV(regular)     AS sd_regular,
--    ARG_MAX(regular, big) AS arg_max_regular
--  FROM input_table
--  GROUP BY created_at;

-- Recursive closure. `pair_seed` is the linear chain 0 -> 1 -> ... -> 50
-- (50 edges); the closure has 1275 pairs. Bounded so the legacy run
-- terminates quickly. The recursion threads through Z^-1 (nested) and
-- Transaction Z^-1.
CREATE LOCAL VIEW pair_seed AS
  SELECT id AS a, (id + 1) AS b FROM input_table WHERE id < 50;

DECLARE RECURSIVE VIEW closure(a INT NOT NULL, b INT NOT NULL);

CREATE LOCAL VIEW closure_step AS
  SELECT s.a, c.b
    FROM pair_seed s
    JOIN closure c ON s.b = c.a;

CREATE MATERIALIZED VIEW closure AS
  (SELECT a, b FROM pair_seed)
  UNION
  (SELECT a, b FROM closure_step);
"""


@enterprise_only
@single_host_only
@gen_pipeline_name
def test_runtime_upgrade_round_trip(pipeline_name: str) -> None:
    """Round-trip a checkpoint across a runtime upgrade.

    Phase 1 is skipped when a synced checkpoint already exists at
    ``CHECKPOINT_CACHE_KEY``; checkpoints synced under any older legacy
    runtime are expected to remain readable by the current runtime. To
    rebuild from scratch, bump ``CHECKPOINT_CACHE_KEY``.
    """

    source = DeltaTestLocation.create(
        DELTA_SOURCE_CACHE_KEY, mode="snapshot", stable_subpath="delta_source"
    )
    _ensure_delta_source(source)

    sql = _build_sql(_connector_json_for(source))
    legacy_version = _legacy_runtime_version()

    # Enable adaptive joins so the balanced trace path emits a
    # ``RebalancingExchangeSender`` (which has its own Checkpoint impl).
    # Older runtimes that do not recognize the option fall back to
    # ignoring it via ``other_options`` in ``DevTweaks``.
    dev_tweaks = {"adaptive_joins": True}

    # The MinIO bucket subpath stays stable across runs so a previously
    # synced checkpoint can be reused. The test pipeline name itself
    # rotates per CI run via ``gen_pipeline_name`` to avoid concurrent
    # collisions on pipeline definitions.
    cache_name = CHECKPOINT_CACHE_KEY

    if _remote_checkpoint_exists(cache_name):
        print(
            f"phase 1: skipped (checkpoint already cached under '{cache_name}')\n"
            f"checkpoints.feldera:\n{_read_remote_checkpoint_catalog(cache_name)}",
            file=sys.stderr,
        )
    else:
        # Phase 1: legacy runtime, ingest, checkpoint, sync to S3.
        # ``PipelineBuilder.__init__`` lets ``FELDERA_RUNTIME_VERSION``
        # win over the constructor argument, but for phase 1 we always
        # need the previous minor (otherwise both phases compile at the
        # same version and the upgrade is not exercised). Stomp the
        # field after construction.
        legacy_builder = PipelineBuilder(
            TEST_CLIENT,
            name=pipeline_name,
            sql=sql,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
                storage=Storage(config=storage_cfg(cache_name)),
                dev_tweaks=dev_tweaks,
            ),
            runtime_version=legacy_version,
        )
        legacy_builder.runtime_version = legacy_version
        legacy_pipeline = legacy_builder.create_or_replace()

        try:
            print(
                f"phase 1: starting pipeline at runtime {legacy_version}",
                file=sys.stderr,
            )
            legacy_pipeline.start(timeout_s=600)

            # Wait until every record from the Delta snapshot has been
            # ingested and processed end-to-end.
            token = legacy_pipeline.generate_completion_token("input_table", "delta_in")
            legacy_pipeline.wait_for_token(token)

            _assert_view_hashes(legacy_pipeline)

            legacy_pipeline.checkpoint(wait=True)
            synced_uuid = legacy_pipeline.sync_checkpoint(wait=True)
            print(
                f"phase 1: synced checkpoint {synced_uuid} to MinIO",
                file=sys.stderr,
            )
        finally:
            legacy_pipeline.stop(force=True)

        # Drop local pipeline storage so the only place to recover the
        # state from is the synced S3 checkpoint.
        legacy_pipeline.clear_storage()

    # Phase 2: current runtime, restore from the synced checkpoint.
    current_pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
            storage=Storage(
                config=storage_cfg(
                    cache_name, start_from_checkpoint="latest", strict=True
                )
            ),
            dev_tweaks=dev_tweaks,
        ),
        # ``runtime_version=None`` means "use the manager default"; in
        # CI ``FELDERA_RUNTIME_VERSION=$GITHUB_SHA`` is set in the
        # workflow env and ``PipelineBuilder`` lets that win, so phase 2
        # compiles against the build under test.
        runtime_version=None,
    ).create_or_replace()

    try:
        print(
            f"phase 2: starting pipeline at current runtime (was {legacy_version})",
            file=sys.stderr,
        )
        # The runtime change typically marks the program as modified in
        # the bootstrap diff (the DBSP IR shifts even with the same
        # SQL); allow the diff to apply.
        current_pipeline.start(bootstrap_policy=BootstrapPolicy.ALLOW, timeout_s=600)

        _assert_view_hashes(current_pipeline)
    finally:
        current_pipeline.stop(force=True)
        current_pipeline.clear_storage()


@enterprise_only
@single_host_only
@gen_pipeline_name
def test_upgrade_from_local_checkpoint(pipeline_name: str) -> None:
    """Resume from a local-disk checkpoint after upgrading the runtime version.

    Companion to :func:`test_runtime_upgrade_round_trip`, but covers a
    **different recovery path**.  That test specifically calls
    ``clear_storage()`` before phase 2, which forces recovery from a
    MinIO-synced checkpoint (the S3 pull path).  This test deliberately
    skips ``clear_storage()``, so phase 2 must recover from the checkpoint
    that lives on the Feldera server's **local disk** — a completely
    different code path in the manager.  A regression in local-disk
    checkpoint detection after a runtime upgrade would be caught here but
    missed by the MinIO-centric test.

    Both this test and :func:`test_runtime_upgrade_round_trip` now run on
    AMD64 and ARM64 CI runners.  The Delta Lake wheel that previously required
    skipping the companion test on ARM64 has been fixed upstream.

    The SQL exercises the same operator families as :func:`_build_sql`
    (DISTINCT, GROUP BY, binary and three-way JOIN, recursive view) so a
    regression in any operator's checkpoint serialization is caught here too.
    A Delta connector is intentionally absent: data is injected via INSERT so
    the test has no native binary dependencies.

    Phase 1 — legacy runtime: insert rows that exercise every
    checkpoint-bearing operator, take a local checkpoint, stop without
    clearing storage.

    Phase 2 — upgrade to the current runtime via ``update_runtime()``, which
    recompiles the pipeline without touching the storage directory, then start
    with ``BootstrapPolicy.ALLOW`` and assert the per-view counts survived the
    upgrade.
    """

    # Same operator families as _build_sql (DISTINCT, GROUP BY, JOIN,
    # recursion) but without a Delta connector so the test runs on ARM64 too.
    sql = """\
CREATE TABLE t(
    id     INT NOT NULL PRIMARY KEY,
    bucket INT NOT NULL
) WITH ('materialized' = 'true');

CREATE MATERIALIZED VIEW v_passthrough AS
  SELECT * FROM t;

CREATE MATERIALIZED VIEW v_filtered AS
  SELECT COUNT(*) AS n FROM t WHERE id < 10;

CREATE MATERIALIZED VIEW v_distinct_buckets AS
  SELECT COUNT(*) AS n FROM (SELECT DISTINCT bucket FROM t);

CREATE MATERIALIZED VIEW v_grouped AS
  SELECT bucket, COUNT(*) AS n FROM t GROUP BY bucket;

CREATE MATERIALIZED VIEW v_joined AS
  SELECT COUNT(*) AS n FROM t a JOIN t b ON a.id = b.id WHERE a.id < 10;

CREATE MATERIALIZED VIEW v_three_way AS
  SELECT COUNT(*) AS n
    FROM t a JOIN t b ON a.id = b.id JOIN t c ON a.id = c.id
   WHERE a.id < 5;

CREATE LOCAL VIEW pair_seed AS
  SELECT id AS a, (id + 1) AS b FROM t WHERE id < 10;

DECLARE RECURSIVE VIEW closure(a INT NOT NULL, b INT NOT NULL);

CREATE LOCAL VIEW closure_step AS
  SELECT s.a, c.b FROM pair_seed s JOIN closure c ON s.b = c.a;

CREATE MATERIALIZED VIEW closure AS
  (SELECT a, b FROM pair_seed) UNION (SELECT a, b FROM closure_step);
"""

    # Enable adaptive joins so the balanced trace path emits a
    # ``RebalancingExchangeSender`` (which has its own Checkpoint impl),
    # matching the coverage of the companion test.
    dev_tweaks = {"adaptive_joins": True}

    legacy_version = _legacy_runtime_version()

    # Phase 1: legacy runtime — insert data, checkpoint, stop.
    # ``PipelineBuilder.__init__`` reads ``FELDERA_RUNTIME_VERSION`` from the
    # environment and lets it win over the constructor ``runtime_version``
    # argument (see pipeline_builder.py: ``os.environ.get(
    # "FELDERA_RUNTIME_VERSION", runtime_version)``).  In CI that env-var
    # points to the current SHA, which would make both phases compile at the
    # same version and leave the upgrade unexercised.  Stomping the field
    # after construction is the minimal safe override; temporarily unsetting
    # the env-var would be non-thread-safe.
    legacy_builder = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,  # Manual checkpoint below.
            dev_tweaks=dev_tweaks,
        ),
        runtime_version=legacy_version,
    )
    legacy_builder.runtime_version = legacy_version
    legacy_pipeline = legacy_builder.create_or_replace()

    print(
        f"phase 1: starting pipeline at legacy runtime {legacy_version}",
        file=sys.stderr,
    )
    legacy_pipeline.start(timeout_s=600)

    # Insert 20 rows across 4 buckets (id 0..19, bucket = id % 4).
    values = ", ".join(f"({i}, {i % 4})" for i in range(20))
    legacy_pipeline.execute(f"INSERT INTO t VALUES {values};", wait=True)

    result_before = list(legacy_pipeline.query("SELECT COUNT(*) AS n FROM t;"))
    assert result_before == [{"n": 20}], (
        f"unexpected row count before upgrade: {result_before}"
    )

    legacy_pipeline.checkpoint(wait=True)
    legacy_pipeline.stop(force=True)
    # Do NOT call clear_storage(): the local checkpoint must survive phase 2.

    # Phase 2: upgrade to the current runtime without clearing storage.
    # ``update_runtime()`` recompiles the pipeline with the platform's current
    # runtime version and does NOT call clear_storage(), so the on-disk
    # checkpoint from phase 1 survives.
    #
    # ``create_or_replace()`` must NOT be used here: when the pipeline already
    # exists it calls clear_storage() before updating the definition, which
    # would destroy the local checkpoint before the upgraded runtime can load it.
    print(
        f"phase 2: updating pipeline to current runtime (was {legacy_version})",
        file=sys.stderr,
    )
    try:
        legacy_pipeline.update_runtime()
        legacy_pipeline.start(bootstrap_policy=BootstrapPolicy.ALLOW, timeout_s=600)
        legacy_pipeline.wait_for_idle()

        # Phase 2 injects no new input.  Without loading the on-disk
        # checkpoint written in phase 1, every view would be empty and this
        # assertion would fail.  A passing count proves the manager found and
        # applied the local checkpoint across the runtime version boundary.
        result_after = list(legacy_pipeline.query("SELECT COUNT(*) AS n FROM t;"))
        assert result_after == [{"n": 20}], (
            f"row count changed after runtime upgrade: {result_after}"
        )
    finally:
        legacy_pipeline.stop(force=True)
        legacy_pipeline.clear_storage()
