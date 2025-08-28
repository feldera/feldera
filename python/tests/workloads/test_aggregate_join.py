import os
import unittest
from string import Template

from feldera.testutils import run_workload

tables = {
    "t1": Template("""
    create table t1(
        id bigint not null primary key,
        group_id bigint,
        s string
    ) with (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
        "name": "datagen",
        "config": {
            "plan": [{
                "limit": $limit,
                "fields": {
                    "group_id": { "range": [1, 10000] },
                    "s": { "strategy": "word" }
                }
            }]
        }
        }
    }]');
    """).substitute(limit=os.environ.get("ROW_LIMIT", "1000000")),
    "t2": Template("""
    create table t2(
        id bigint not null primary key,
        group_id bigint,
        s string
    ) with (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
        "name": "datagen",
        "config": {
            "plan": [{
                "limit": $limit,
                "fields": {
                    "group_id": { "range": [1, 10000] },
                    "s": { "strategy": "word" }
                }
            }]
        }
        }
    }]');
    """).substitute(limit=os.environ.get("ROW_LIMIT", "1000000")),
}

views = {
    "t1_aggregate": """
    select
        group_id,
        count(*) as cnt,
        SORT_ARRAY(array_agg(s)) as arr
    from t1
    group by group_id
    """,
    "t2_aggregate": """
    select
        group_id,
        count(*) as cnt,
        SORT_ARRAY(array_agg(s)) as arr
    from t2
    group by group_id
    """,
    "result": """
    select
        t1_aggregate.group_id,
        t1_aggregate.cnt as cnt1,
        t1_aggregate.arr as arr1,
        t2_aggregate.cnt as cnt2,
        t2_aggregate.arr as arr2
    from
        t1_aggregate join t2_aggregate
    on
        t1_aggregate.group_id = t2_aggregate.group_id
    """,
}


class TestPipelineBuilder(unittest.TestCase):
    def test_aggregate_joins(self):
        run_workload("aggregate-join-test", tables, views)


if __name__ == "__main__":
    unittest.main()
