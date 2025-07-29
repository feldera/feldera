from util import run_pipeline

tables = {
    "t1": """
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
                "limit": 10000000,
                "fields": {
                    "group_id": { "range": [1, 10000] },
                    "s": { "strategy": "word" }
                }
            }]
        }
        }
    }]');
    """,
    "t2": """
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
                "limit": 10000000,
                "fields": {
                    "group_id": { "range": [1, 10000] },
                    "s": { "strategy": "word" }
                }
            }]
        }
        }
    }]');
    """,
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

run_pipeline("aggregate-join-test", tables, views)
