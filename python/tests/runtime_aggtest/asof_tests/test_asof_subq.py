from tests.runtime_aggtest.aggtst_base import TstView


class asof_test_select_subquery(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "filtered_intt": 10, "t2_intt": 5},
            {"id": 2, "filtered_intt": 15, "t2_intt": None},
            {"id": 3, "filtered_intt": 20, "t2_intt": None},
            {"id": 4, "filtered_intt": 25, "t2_intt": 12},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test_select_subquery AS SELECT
                        sub.id, sub.filtered_intt, t2.intt AS t2_intt
                        FROM (
                            SELECT id, intt AS filtered_intt
                            FROM asof_tbl1
                            WHERE intt IS NOT NULL AND id < 5
                        ) sub
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (sub.filtered_intt >= t2.intt)
                        ON sub.id = t2.id;"""


class asof_test_aggregate_subquery(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "avg_intt": 10, "t2_intt": 5},
            {"id": 2, "avg_intt": 15, "t2_intt": None},
            {"id": 3, "avg_intt": 20, "t2_intt": None},
            {"id": 4, "avg_intt": 25, "t2_intt": 12},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test_aggregate_subquery AS SELECT
                        sub.id, sub.avg_intt, t2.intt AS t2_intt
                        FROM (
                            SELECT id, AVG(intt) AS avg_intt
                            FROM asof_tbl1
                            WHERE intt IS NOT NULL
                            GROUP BY id
                        ) sub
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (sub.avg_intt >= t2.intt)
                        ON sub.id = t2.id;"""


class asof_test_with_cte(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "doubled": 20, "t2_intt": 5},
            {"id": 2, "doubled": 30, "t2_intt": 16},
            {"id": 3, "doubled": 40, "t2_intt": None},
            {"id": 4, "doubled": 50, "t2_intt": 12},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test_with_cte AS
                        WITH doubled_values AS (
                            SELECT id, intt * 2 AS doubled
                            FROM asof_tbl1
                            WHERE intt IS NOT NULL
                        )
                        SELECT
                            dv.id, dv.doubled, t2.intt AS t2_intt
                        FROM doubled_values dv
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (dv.doubled >= t2.intt)
                        ON dv.id = t2.id;"""


class asof_test_union_subquery(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "combined_intt": 10, "source": "tbl1", "t2_intt": 5},
            {"id": 1, "combined_intt": 5, "source": "tbl3", "t2_intt": 5},
            {"id": 2, "combined_intt": 15, "source": "tbl1", "t2_intt": None},
            {"id": 2, "combined_intt": 16, "source": "tbl3", "t2_intt": 16},
            {"id": 3, "combined_intt": 20, "source": "tbl1", "t2_intt": None},
            {"id": 3, "combined_intt": 70, "source": "tbl3", "t2_intt": 70},
            {"id": 4, "combined_intt": 12, "source": "tbl3", "t2_intt": 12},
            {"id": 4, "combined_intt": 25, "source": "tbl1", "t2_intt": 12},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test_union_subquery AS SELECT
                        sub.id, sub.combined_intt, sub.source, t2.intt AS t2_intt
                        FROM (
                            SELECT id, intt AS combined_intt, 'tbl1' AS source
                            FROM asof_tbl1
                            WHERE intt IS NOT NULL
                            UNION ALL
                            SELECT id, intt AS combined_intt, 'tbl3' AS source
                            FROM asof_tbl3
                            WHERE id < 5
                        ) sub
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (sub.combined_intt >= t2.intt)
                        ON sub.id = t2.id;"""


class asof_test_window_function_subquery(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "intt": 10, "avg_intt": 17, "t2_intt": 5},
            {"id": 2, "intt": 15, "avg_intt": 17, "t2_intt": None},
            {"id": 3, "intt": 20, "avg_intt": 17, "t2_intt": None},
            {"id": 4, "intt": 25, "avg_intt": 17, "t2_intt": 12},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test_window_function_subquery AS SELECT
                        sub.id, sub.intt, sub.avg_intt, t2.intt AS t2_intt
                        FROM (
                            SELECT id, intt, AVG(intt) OVER () AS avg_intt
                            FROM asof_tbl1
                            WHERE intt IS NOT NULL
                        ) sub
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (sub.intt >= t2.intt)
                        ON sub.id = t2.id;"""
