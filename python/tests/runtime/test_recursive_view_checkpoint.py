"""Restart a pipeline with a recursive view from its own checkpoint.

A recursive view maintains its relation inside a nested circuit, and that state
used to be missing from checkpoints: the restart reported neither an error nor a
bootstrap, and the view then produced wrong results
(https://github.com/feldera/feldera/issues/6765).

Two properties of this test are what make it catch that:

* **The program does not change across the restart.**  When it changes, the
  bootstrap replay rebuilds the recursive view from replayed input and hides the
  loss.  That is why the runtime-upgrade tests never caught this: they restart
  into a program the bootstrap diff marks as modified, and so always bootstrap.
* **``edges`` is not materialized**, so there is no replay source for it.
  Nothing can reconstruct the relation from input history; the state either
  comes from the checkpoint or it is gone.

The decisive assertion is the one that feeds another edge after the restart.
The view's own contents live outside the recursive scope and come back either
way; deriving new paths through the older edges is what needs the state inside
the scope.  Verified by reverting the fix: the closure then grows by the new
edge alone, missing every path that runs through it.
"""

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT, enterprise_only
from tests.platform.helper import gen_pipeline_name

# Transitive closure of a directed graph: the smallest program that keeps state
# inside a recursive scope.
SQL = """
CREATE TABLE edges (a INT NOT NULL, b INT NOT NULL);

DECLARE RECURSIVE VIEW closure(a INT NOT NULL, b INT NOT NULL);

CREATE MATERIALIZED VIEW closure AS
  (SELECT a, b FROM edges)
  UNION
  (SELECT e.a, c.b FROM edges e JOIN closure c ON e.b = c.a);
"""


def transitive_closure(edges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Oracle: the transitive closure of `edges`, computed outside Feldera."""
    closure = set(edges)
    while True:
        grown = closure | {(a, d) for (a, b) in closure for (c, d) in closure if b == c}
        if grown == closure:
            return sorted(closure)
        closure = grown


def query_closure(pipeline) -> list[tuple[int, int]]:
    return sorted(
        (row["a"], row["b"]) for row in pipeline.query("SELECT a, b FROM closure;")
    )


def insert_edges(pipeline, edges: list[tuple[int, int]]) -> None:
    """Inserts `edges` and waits for the pipeline to finish processing them."""
    values = ", ".join(f"({a}, {b})" for a, b in edges)
    pipeline.execute(f"INSERT INTO edges VALUES {values};", wait=True)


@enterprise_only
@gen_pipeline_name
def test_recursive_view_survives_restart(pipeline_name: str) -> None:
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql=SQL,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            # Checkpoint explicitly below rather than on a timer.
            fault_tolerance_model=None,
        ),
    ).create_or_replace()

    try:
        pipeline.start()

        # A chain 0 -> 1 -> 2 -> 3.
        edges = [(0, 1), (1, 2), (2, 3)]
        insert_edges(pipeline, edges)
        assert query_closure(pipeline) == transitive_closure(edges)

        pipeline.checkpoint(wait=True)
        # Stop without clearing storage, so the checkpoint survives.
        pipeline.stop(force=True)

        # Restart the same program at the same runtime version.
        pipeline.start()

        # The view's contents are stored outside the recursive scope, so a
        # mismatch here means the checkpoint was not loaded at all.
        assert query_closure(pipeline) == transitive_closure(edges)

        # Extending the chain derives paths that run through the edges fed
        # before the restart, which only the recursive scope's restored state
        # supplies.
        edges.append((3, 4))
        insert_edges(pipeline, [(3, 4)])
        assert query_closure(pipeline) == transitive_closure(edges)

        # Closing a cycle exercises that state again, deriving paths in both
        # directions.
        edges.append((4, 0))
        insert_edges(pipeline, [(4, 0)])
        assert query_closure(pipeline) == transitive_closure(edges)
    finally:
        pipeline.stop(force=True)
        pipeline.clear_storage()
