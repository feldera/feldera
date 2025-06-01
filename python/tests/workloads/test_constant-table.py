import unittest
from feldera.testutils import ViewSpec, run_workload, unique_pipeline_name

# The compiler compiles `where t.company_id in <long list of constant values>` queries
# into a join with a constant table, created using a DBSP Generator operator to produce
# a stream of constant value followed by `differentiate` to convert it into a change
# stream.
#
# This test checks that the constant table is created correctly.

INPUT_RECORDS = 5000000

tables = {
    "t": f"""
    create table t(
        id bigint not null primary key,
        company_id bigint,
        name string
    ) with (
    'materialized' = 'true',
    'connectors' = '[{{
        "name": "datagen",
        "transport": {{
            "name": "datagen",
            "config": {{
                "plan": [{{
                    "limit": {INPUT_RECORDS},
                    "fields": {{
                        "name": {{ "strategy": "sentence" }}
                    }}
                }}]
            }}
        }}
    }}]');
    """
}

views = [
    ViewSpec(
        "v",
        """
    select
        t.*
    from t
    where t.company_id in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31)
    """,
    )
]


class TestConstantTable(unittest.TestCase):
    def test_constant_table(self):
        run_workload(
            unique_pipeline_name("constant-table"), tables, views, transaction=True
        )


if __name__ == "__main__":
    unittest.main()
