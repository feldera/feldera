"""
End-to-end tests for concurrent bootstrapping.

Concurrent bootstrapping keeps the pre-existing views live and serving while
new and modified views backfill in a background copy of the circuit; once the
backfill catches up, the pipeline atomically cuts over to the new circuit.

Timing insensitivity
--------------------
A concurrent bootstrap passes through several transient runtime statuses on its
way to ``RUNNING`` (``PROVISIONING``, ``BOOTSTRAPPING``,
``CONCURRENTBOOTSTRAPPING``, ``SYNCHRONIZING``, ...). Any of these can be held
for a long time or skipped entirely, depending on data size and machine speed.
These tests therefore never *wait for* a transient status: they only ever wait
for a terminal status (``RUNNING`` or ``STOPPED``) and tolerate skipping every
state in between. ``Pipeline.start(wait=True)`` and ``Pipeline.wait_for_status``
already poll this way -- they advance on the target status and fail fast only on
a ``STOPPED``-with-error -- so correctness never depends on observing a
particular phase. ("Old views stay live during the backfill" is verified
deterministically by the dbsp-level ``run_concurrent_bootstrap_test`` harness;
it is inherently timing-dependent to observe end-to-end and is not asserted
here.)

Crash simulation
----------------
A real crash (SIGKILL, pod eviction, OOM) cannot be injected portably -- SIGKILL
is unavailable under Kubernetes, hence in CI. We approximate a crash with
``stop(force=True)``, which scales the pipeline's compute to zero *without*
taking a checkpoint: all volatile state (both circuit copies, backfill progress,
the output cache, the mailboxes) is discarded and no new checkpoint is written.
Recovery is an explicit ``start`` that restores the last durable checkpoint.
Because no checkpoint is taken during the bootstrap window, that is always the
pre-bootstrap checkpoint, so the restarted pipeline re-detects the schema change
and re-drives the bootstrap. The oracle is convergence: regardless of when (or
how many times) we crash, the final view contents must equal those of an
uninterrupted bootstrap.
"""

import time

from feldera.enums import BootstrapPolicy, PipelineStatus
from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from tests import TEST_CLIENT, enterprise_only
from .helper import gen_pipeline_name
from feldera.testutils import (
    FELDERA_TEST_NUM_WORKERS,
    FELDERA_TEST_NUM_HOSTS,
)

# Rows present before the bootstrap and added afterwards.
PRELOAD_ROWS = 1000
POSTLOAD_ROWS = 100


def _sum_to(n: int) -> int:
    """SUM(x) for x in 1..n."""
    return n * (n + 1) // 2


def _runtime_config() -> RuntimeConfig:
    return RuntimeConfig(
        workers=FELDERA_TEST_NUM_WORKERS,
        hosts=FELDERA_TEST_NUM_HOSTS,
        # Concurrent bootstrap restores from the checkpoint we take manually;
        # we don't want the periodic checkpointer interfering.
        fault_tolerance_model=None,
    )


def _create(pipeline_name: str, sql: str):
    return PipelineBuilder(
        TEST_CLIENT, pipeline_name, sql=sql, runtime_config=_runtime_config()
    ).create_or_replace()


def _insert_range(pipeline, table: str, lo: int, hi: int) -> None:
    """Insert x = lo, lo+1, ..., hi-1 into `table` and wait for it to land."""
    values = ",".join(f"({x})" for x in range(lo, hi))
    pipeline.execute(f"INSERT INTO {table} VALUES {values};")
    pipeline.wait_for_idle()


def _scalar(pipeline, sql: str, column: str):
    """Run an ad-hoc query expected to return a single row; return one column."""
    rows = list(pipeline.query(sql))
    assert len(rows) == 1, f"expected one row from `{sql}`, got {rows}"
    return rows[0][column]


def _seed_and_checkpoint(pipeline_name: str, base_sql: str):
    """
    Create the baseline pipeline, load `PRELOAD_ROWS` rows into `t1`, take a
    durable checkpoint, and stop (without a new checkpoint). Returns the pipeline
    in the STOPPED state with a pre-bootstrap checkpoint; the modified program is
    not applied yet.
    """
    pipeline = _create(pipeline_name, base_sql)
    pipeline.start()
    _insert_range(pipeline, "t1", 1, PRELOAD_ROWS + 1)
    assert _scalar(pipeline, "SELECT c FROM v1;", "c") == PRELOAD_ROWS
    pipeline.checkpoint(wait=True)
    pipeline.stop(force=True)
    pipeline.wait_for_status(PipelineStatus.STOPPED, timeout=120)
    return pipeline


# Baseline: a materialized table and one materialized view over it.
BASE_SQL = """CREATE TABLE t1(x int) WITH ('materialized' = 'true');
CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) AS c FROM t1;
"""

# Adds a brand-new view that must backfill the pre-existing rows.
ADD_VIEW_SQL = BASE_SQL + "CREATE MATERIALIZED VIEW v2 AS SELECT SUM(x) AS s FROM t1;\n"


@enterprise_only
@gen_pipeline_name
def test_concurrent_bootstrap_new_view(pipeline_name):
    """
    Add a new view and bootstrap it concurrently while the existing view stays
    live. The new view must reflect the rows that existed before the bootstrap,
    and both views must track subsequent updates.
    """
    pipeline = _seed_and_checkpoint(pipeline_name, BASE_SQL)

    pipeline.modify(sql=ADD_VIEW_SQL)
    pipeline.start(
        bootstrap_policy=BootstrapPolicy.ALLOW,
        concurrent_bootstrap=True,
        timeout_s=600,
    )
    pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=600)

    # New view backfilled over the pre-existing rows; old view intact.
    assert _scalar(pipeline, "SELECT s FROM v2;", "s") == _sum_to(PRELOAD_ROWS)
    assert _scalar(pipeline, "SELECT c FROM v1;", "c") == PRELOAD_ROWS

    # Both views track post-bootstrap updates.
    _insert_range(pipeline, "t1", PRELOAD_ROWS + 1, PRELOAD_ROWS + POSTLOAD_ROWS + 1)
    total = PRELOAD_ROWS + POSTLOAD_ROWS
    assert _scalar(pipeline, "SELECT c FROM v1;", "c") == total
    assert _scalar(pipeline, "SELECT s FROM v2;", "s") == _sum_to(total)

    pipeline.stop(force=True)


# Modifies the existing view's definition (and schema), so it must be recomputed
# over the pre-existing rows during the bootstrap.
MODIFY_VIEW_SQL = """CREATE TABLE t1(x int) WITH ('materialized' = 'true');
CREATE MATERIALIZED VIEW v1 AS SELECT SUM(x) AS s FROM t1;
"""


@enterprise_only
@gen_pipeline_name
def test_concurrent_bootstrap_modified_view(pipeline_name):
    """
    Change an existing view's definition and bootstrap concurrently. The modified
    view must be recomputed over the pre-existing rows.
    """
    pipeline = _seed_and_checkpoint(pipeline_name, BASE_SQL)

    pipeline.modify(sql=MODIFY_VIEW_SQL)
    pipeline.start(
        bootstrap_policy=BootstrapPolicy.ALLOW,
        concurrent_bootstrap=True,
        timeout_s=600,
    )
    pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=600)

    # `v1` now has its new definition, recomputed over the pre-existing rows.
    assert _scalar(pipeline, "SELECT s FROM v1;", "s") == _sum_to(PRELOAD_ROWS)

    _insert_range(pipeline, "t1", PRELOAD_ROWS + 1, PRELOAD_ROWS + POSTLOAD_ROWS + 1)
    assert _scalar(pipeline, "SELECT s FROM v1;", "s") == _sum_to(
        PRELOAD_ROWS + POSTLOAD_ROWS
    )

    pipeline.stop(force=True)


@enterprise_only
@gen_pipeline_name
def test_concurrent_bootstrap_crash_recovery(pipeline_name):
    """
    Interrupt the concurrent bootstrap repeatedly and verify it converges.

    Each iteration kicks off the bootstrap and force-stops the pipeline at an
    arbitrary, unobserved point (see the module docstring: this models a crash).
    The crash may land during compilation, provisioning, backfill,
    synchronization, after cutover, or after completion -- the test asserts only
    that, after a final clean recovery, the views hold the correct values.
    """
    pipeline = _seed_and_checkpoint(pipeline_name, BASE_SQL)
    pipeline.modify(sql=ADD_VIEW_SQL)

    # Crash at a spread of unobserved points. The exact delays do not affect
    # correctness -- they only vary where the (unobserved) crash lands -- so the
    # test is insensitive to timing.
    for delay in (1.0, 3.0, 6.0):
        pipeline.start(
            bootstrap_policy=BootstrapPolicy.ALLOW,
            concurrent_bootstrap=True,
            wait=False,
        )
        time.sleep(delay)
        # "Crash": deprovision without a checkpoint, from whatever state we are
        # in. `stop(force=True)` waits for the pipeline to reach STOPPED.
        pipeline.stop(force=True, timeout_s=120)

    # Recover for real and let the bootstrap finish this time.
    pipeline.start(
        bootstrap_policy=BootstrapPolicy.ALLOW,
        concurrent_bootstrap=True,
        timeout_s=600,
    )
    pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=600)

    # Convergence: the bootstrap that finally completed produced the same result
    # an uninterrupted bootstrap would have, despite the repeated crashes.
    assert _scalar(pipeline, "SELECT s FROM v2;", "s") == _sum_to(PRELOAD_ROWS)
    assert _scalar(pipeline, "SELECT c FROM v1;", "c") == PRELOAD_ROWS

    # The recovered pipeline keeps processing input correctly.
    _insert_range(pipeline, "t1", PRELOAD_ROWS + 1, PRELOAD_ROWS + POSTLOAD_ROWS + 1)
    total = PRELOAD_ROWS + POSTLOAD_ROWS
    assert _scalar(pipeline, "SELECT c FROM v1;", "c") == total
    assert _scalar(pipeline, "SELECT s FROM v2;", "s") == _sum_to(total)

    pipeline.stop(force=True)


# An unsupported diff: the new view reads an UNMATERIALIZED table that no
# existing view consumes, so its stream has no checkpointed integral to serve as
# a replay source -- it cannot be bootstrapped concurrently. (A materialized
# table, or an unmaterialized table already consumed by an existing aggregate or
# join, would have a replay source and bootstrap fine; this case deliberately
# has neither.) `keep` is an unrelated materialized table so the base program is
# a normal, runnable pipeline.
UNSUPPORTED_BASE_SQL = """CREATE TABLE u(x int);
CREATE TABLE keep(y int) WITH ('materialized' = 'true');
CREATE MATERIALIZED VIEW keepv AS SELECT COUNT(*) AS c FROM keep;
"""
UNSUPPORTED_MODIFIED_SQL = (
    UNSUPPORTED_BASE_SQL
    + "CREATE MATERIALIZED VIEW uv AS SELECT COUNT(*) AS c FROM u;\n"
)


@enterprise_only
@gen_pipeline_name
def test_concurrent_bootstrap_unsupported_diff(pipeline_name):
    """
    A diff that cannot be bootstrapped concurrently fails fatally, with no
    silent fallback to a stop-the-world bootstrap.

    The new view reads an unmaterialized, previously-unconsumed table, so its
    input stream has no replay source. Concurrent bootstrap refuses it; the
    refusal is surfaced as a start failure (the pipeline is left STOPPED with
    the deployment error), rather than the pipeline quietly degrading to a
    stop-the-world bootstrap.

    (Note: a diff that concurrent bootstrap refuses but a stop-the-world
    bootstrap accepts requires a connector-backed table -- stop-the-world
    re-ingests from the connector, which a concurrent bootstrap cannot replay.
    Such a table is out of scope here; this test covers the fatal-refusal half.
    The supported cases -- materialized tables and tables consumed by an
    existing aggregate -- are exercised by the other tests in this module.)
    """
    pipeline = _create(pipeline_name, UNSUPPORTED_BASE_SQL)
    pipeline.start()
    _insert_range(pipeline, "keep", 1, 4)
    assert _scalar(pipeline, "SELECT c FROM keepv;", "c") == 3
    pipeline.checkpoint(wait=True)
    pipeline.stop(force=True)
    pipeline.wait_for_status(PipelineStatus.STOPPED, timeout=120)

    pipeline.modify(sql=UNSUPPORTED_MODIFIED_SQL)

    # Concurrent bootstrap is refused (fatal): the new view's input stream has
    # no replay source.
    try:
        pipeline.start(
            bootstrap_policy=BootstrapPolicy.ALLOW,
            concurrent_bootstrap=True,
            timeout_s=600,
        )
        raise AssertionError(
            "expected concurrent bootstrap to be refused for a stream with no "
            "replay source, but the pipeline started"
        )
    except RuntimeError as e:
        assert "bootstrapped concurrently" in str(e) or "replay source" in str(e), e

    pipeline.wait_for_status(PipelineStatus.STOPPED, timeout=120)
