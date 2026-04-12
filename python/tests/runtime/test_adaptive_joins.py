"""
Integration tests for adaptive joins: balancer policies in the circuit JSON profile.

Expects a running Feldera instance (see other tests under python/tests/runtime).
Adaptive join rebalancing is only compiled in when ``num_workers > 1``.
"""

from __future__ import annotations

import unittest

from feldera import PipelineBuilder
from feldera.enums import BootstrapPolicy
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS
from tests import TEST_CLIENT, unique_pipeline_name
from tests.platform.helper import (
    API_PREFIX,
    http_request,
    wait_for_condition,
    wait_for_program_success,
)


SQL_JOIN_AB = """
CREATE TABLE tab_a (
  k BIGINT NOT NULL,
  v BIGINT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE tab_b (
  k BIGINT NOT NULL,
  w BIGINT NOT NULL
) WITH ('materialized' = 'true');

CREATE MATERIALIZED VIEW join_ab AS
SELECT tab_a.k, tab_a.v, tab_b.w
FROM tab_a JOIN tab_b ON tab_a.k = tab_b.k;
"""

SQL_JOIN_ABC = """
CREATE TABLE tab_a (
  k BIGINT NOT NULL,
  v BIGINT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE tab_b (
  k BIGINT NOT NULL,
  w BIGINT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE tab_c (
  k BIGINT NOT NULL,
  u BIGINT NOT NULL
) WITH ('materialized' = 'true');

CREATE MATERIALIZED VIEW join_ab AS
SELECT tab_a.k, tab_a.v, tab_b.w
FROM tab_a JOIN tab_b ON tab_a.k = tab_b.k;

CREATE MATERIALIZED VIEW join_ac AS
SELECT tab_a.k, tab_a.v, tab_c.u
FROM tab_a JOIN tab_c ON tab_a.k = tab_c.k;
"""

# Lower balancer thresholds so modest skew triggers a policy change in CI.
_ADAPTIVE_DEV_TWEAKS = {
    "adaptive_joins": True,
}


def _fetch_circuit_json_profile(pipeline_name: str) -> dict:
    resp = http_request(
        "GET",
        f"{API_PREFIX}/pipelines/{pipeline_name}/circuit_json_profile",
        headers={"Accept-Encoding": "gzip"},
        timeout=120,
    )
    assert resp.status_code == 200, (resp.status_code, resp.text[:500])
    return resp.json()


def _balancer_policy_values(profile: dict) -> list[str]:
    """
    Collect ``balancer_policy`` metric values from worker 0.

    Values use the engine's ``Display`` spelling: ``shard``, ``broadcast``, ``balance``.
    """
    workers = profile.get("worker_profiles") or []
    assert workers, "circuit profile missing worker_profiles"
    wp0 = workers[0]
    meta = wp0.get("metadata") or {}
    out: list[str] = []
    for readings in meta.values():
        if not isinstance(readings, list):
            continue
        for r in readings:
            if r.get("metric_id") != "balancer_policy":
                continue
            val = r.get("value")
            if isinstance(val, dict) and val.get("type") == "string":
                out.append(str(val["value"]).lower())
    return out


def _push_chunked_json(
    pipeline, table: str, rows: list[dict], chunk_size: int = 4000
) -> None:
    for i in range(0, len(rows), chunk_size):
        pipeline.input_json(table, rows[i : i + chunk_size])


class TestAdaptiveJoins(unittest.TestCase):
    def test_adaptive_joins_balancer_policies(self):
        if FELDERA_TEST_NUM_WORKERS < 2:
            self.skipTest("adaptive join balancing requires at least two workers")

        name = unique_pipeline_name("test_adaptive_joins")
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            name=name,
            sql=SQL_JOIN_AB,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                dev_tweaks=_ADAPTIVE_DEV_TWEAKS,
            ),
        ).create_or_replace()

        try:
            pipeline.start()

            print(
                "Single-join pipeline started; populating tables with balanced inputs"
            )
            # Uniform keys so both sides are ~evenly sharded (avoid k=0 used later for skew).
            n_uniform = 4000
            uniform_a = [{"k": i, "v": i} for i in range(1, n_uniform + 1)]
            uniform_b = [{"k": i, "w": i} for i in range(1, n_uniform + 1)]
            _push_chunked_json(pipeline, "tab_a", uniform_a)
            _push_chunked_json(pipeline, "tab_b", uniform_b)
            pipeline.wait_for_idle()

            def all_sharded() -> bool:
                pvals = _balancer_policy_values(_fetch_circuit_json_profile(name))
                return len(pvals) > 0 and all(v == "shard" for v in pvals)

            wait_for_condition(
                "all balancer_policy values are shard after uniform load",
                all_sharded,
                timeout_s=120.0,
                poll_interval_s=0.5,
            )

            # Heavy skew on a single join key to trigger (balance, broadcast).
            print("Introducing heavy skew to trigger (balance, broadcast)")
            skew_n = 25_000
            skew_a = [{"k": 0, "v": i} for i in range(skew_n)]
            _push_chunked_json(pipeline, "tab_a", skew_a)
            pipeline.wait_for_idle()
            pipeline.rebalance()
            pipeline.wait_for_idle()

            def skew_policies() -> bool:
                pvals = _balancer_policy_values(_fetch_circuit_json_profile(name))
                return "balance" in pvals and "broadcast" in pvals

            wait_for_condition(
                "skewed join uses balance on one stream and broadcast on another",
                skew_policies,
                timeout_s=180.0,
                poll_interval_s=0.5,
            )

            print("Stopping pipeline to modify SQL")
            pipeline.stop(force=False)

            prev_ver = pipeline.program_version()
            pipeline.modify(sql=SQL_JOIN_ABC)
            wait_for_program_success(name, prev_ver + 1)
            pipeline.start(bootstrap_policy=BootstrapPolicy.ALLOW)

            print("Running pipeline with three-way join; populating the new table")

            # Same join key cluster: third input should participate with policies compatible
            # with the existing skewed cluster.
            uniform_c = [{"k": i, "u": i} for i in range(1, n_uniform + 1)]

            # skew_c = [{"k": 0, "u": i} for i in range(int(skew_n/2))]
            _push_chunked_json(pipeline, "tab_c", uniform_c)
            pipeline.rebalance()

            def cluster_policies() -> bool:
                pvals = _balancer_policy_values(_fetch_circuit_json_profile(name))
                print("cluster_policies: ", pvals)
                if not pvals:
                    return False
                if "shard" in pvals:
                    return False
                if "balance" not in pvals or "broadcast" not in pvals:
                    return False
                # Extra probe input (tab_c) should stay on the broadcast side like tab_b.
                return pvals.count("broadcast") == 2

            wait_for_condition(
                "three-way cluster: extra input stays broadcast with the other probe",
                cluster_policies,
                timeout_s=180.0,
                poll_interval_s=0.5,
            )

            pipeline.stop(force=False)
            pipeline.start()

            print("Diluting skew with large uniform key ranges so sharding wins again.")
            dilute_n = 80_000
            base = 1_000_000
            dilute_a = [{"k": base + i, "v": i} for i in range(dilute_n)]
            dilute_b = [{"k": base + i, "w": i} for i in range(dilute_n)]
            dilute_c = [{"k": base + i, "u": i} for i in range(dilute_n)]
            _push_chunked_json(pipeline, "tab_a", dilute_a)
            _push_chunked_json(pipeline, "tab_b", dilute_b)
            _push_chunked_json(pipeline, "tab_c", dilute_c)
            pipeline.rebalance()

            wait_for_condition(
                "all balancer_policy values return to shard after dilution",
                all_sharded,
                timeout_s=180.0,
                poll_interval_s=0.5,
            )

        finally:
            pipeline.stop(force=True)
            pipeline.clear_storage()


if __name__ == "__main__":
    unittest.main()
