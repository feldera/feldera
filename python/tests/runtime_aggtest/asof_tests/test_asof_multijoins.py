from tests.runtime_aggtest.aggtst_base import TstView


class asof_lasof_int_chain(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 5},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": None},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": None},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 12},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW lasof_int_chain AS SELECT
                        t1.id,
                        t1.intt AS t1_int,
                        t2.intt AS t2_int,
                        t3.intt AS t3_int
                    FROM asof_tbl1 t1
                    LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt >= t2.intt)
                        ON t1.id = t2.id
                    LEFT ASOF JOIN asof_tbl3 t3
                        MATCH_CONDITION (t2.intt >= t3.intt)
                        ON t2.id = t3.id;"""


class asof_lasof_map_chain(TstView):
    def __init__(self):
        # Validated on DuckDB
        # We differ with DuckDB on NULL handling in map comparisons; id = 5
        # ({'id': 5, 't1_mapp': None, 't2_mapp': None, 't3_mapp': {'a': 1000, 'b': 2000}})
        self.data = [
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": None,
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 1, "b": 9},
            },
            {"id": 3, "t1_mapp": {"a": 11, "b": 22}, "t2_mapp": None, "t3_mapp": None},
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 100, "b": 200},
            },
            {"id": 5, "t1_mapp": None, "t2_mapp": None, "t3_mapp": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW lasof_map_chain AS SELECT
                        t1.id,
                        t1.mapp AS t1_mapp,
                        t2.mapp AS t2_mapp,
                        t3.mapp AS t3_mapp
                    FROM asof_tbl1 t1
                    LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.mapp >= t2.mapp)
                        ON t1.id = t2.id
                    LEFT ASOF JOIN asof_tbl3 t3
                        MATCH_CONDITION (t2.mapp >= t3.mapp)
                        ON t2.id = t3.id;"""


class asof_lasof_cross_int_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 112},
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 12},
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 16},
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 5},
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 70},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": 112},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": 12},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": 16},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": 5},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": 70},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": 112},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": 12},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": 16},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": 5},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": 70},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 112},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 12},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 16},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 5},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 70},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": 112},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": 12},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": 16},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": 5},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": 70},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_combined_cross_int_join AS SELECT
                        t1.id, t1.intt AS t1_int, t2.intt AS t2_int, t3.intt AS t3_int
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt >= t2.intt)
                        ON t1.id = t2.id
                        CROSS JOIN asof_tbl3 t3;"""


class asof_lasof_cross_map_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": {"a": 1, "b": 9},
            },
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": {"a": 1000, "b": 2000},
            },
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": {"a": 21, "b": 22},
            },
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": {"a": 25, "b": None},
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 1, "b": 9},
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 1000, "b": 2000},
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 21, "b": 22},
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 25, "b": None},
            },
            {
                "id": 3,
                "t1_mapp": {"a": 11, "b": 22},
                "t2_mapp": None,
                "t3_mapp": {"a": 1, "b": 9},
            },
            {
                "id": 3,
                "t1_mapp": {"a": 11, "b": 22},
                "t2_mapp": None,
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": 3,
                "t1_mapp": {"a": 11, "b": 22},
                "t2_mapp": None,
                "t3_mapp": {"a": 1000, "b": 2000},
            },
            {
                "id": 3,
                "t1_mapp": {"a": 11, "b": 22},
                "t2_mapp": None,
                "t3_mapp": {"a": 21, "b": 22},
            },
            {
                "id": 3,
                "t1_mapp": {"a": 11, "b": 22},
                "t2_mapp": None,
                "t3_mapp": {"a": 25, "b": None},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 1, "b": 9},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 1000, "b": 2000},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 21, "b": 22},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 25, "b": None},
            },
            {"id": 5, "t1_mapp": None, "t2_mapp": None, "t3_mapp": {"a": 1, "b": 9}},
            {
                "id": 5,
                "t1_mapp": None,
                "t2_mapp": None,
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": 5,
                "t1_mapp": None,
                "t2_mapp": None,
                "t3_mapp": {"a": 1000, "b": 2000},
            },
            {"id": 5, "t1_mapp": None, "t2_mapp": None, "t3_mapp": {"a": 21, "b": 22}},
            {
                "id": 5,
                "t1_mapp": None,
                "t2_mapp": None,
                "t3_mapp": {"a": 25, "b": None},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_lasof_cross_map_join AS SELECT
                        t1.id, t1.mapp AS t1_mapp, t2.mapp AS t2_mapp, t3.mapp AS t3_mapp
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.mapp >= t2.mapp)
                        ON t1.id = t2.id
                        CROSS JOIN asof_tbl3 t3;"""


class asof_cross_lasof_int_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 112},
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 12},
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 16},
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 5},
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 70},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": 112},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": 12},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": 16},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": 5},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": 70},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": 112},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": 12},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": 16},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": 5},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": 70},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 112},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 12},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 16},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 5},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 70},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": 112},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": 12},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": 16},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": 5},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": 70},
        ]
        self.sql = """CREATE MATERIALIZED VIEW cross_lasof_int_join AS SELECT
                        t1.id, t1.intt AS t1_int, t2.intt AS t2_int, t3.intt AS t3_int
                        FROM asof_tbl1 t1
                        CROSS JOIN asof_tbl3 t3
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt >= t2.intt)
                        ON t1.id = t2.id;"""


class asof_cross_lasof_map_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": {"a": 1, "b": 9},
            },
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": {"a": 1000, "b": 2000},
            },
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": {"a": 21, "b": 22},
            },
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": {"a": 25, "b": None},
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 1, "b": 9},
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 1000, "b": 2000},
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 21, "b": 22},
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 25, "b": None},
            },
            {
                "id": 3,
                "t1_mapp": {"a": 11, "b": 22},
                "t2_mapp": None,
                "t3_mapp": {"a": 1, "b": 9},
            },
            {
                "id": 3,
                "t1_mapp": {"a": 11, "b": 22},
                "t2_mapp": None,
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": 3,
                "t1_mapp": {"a": 11, "b": 22},
                "t2_mapp": None,
                "t3_mapp": {"a": 1000, "b": 2000},
            },
            {
                "id": 3,
                "t1_mapp": {"a": 11, "b": 22},
                "t2_mapp": None,
                "t3_mapp": {"a": 21, "b": 22},
            },
            {
                "id": 3,
                "t1_mapp": {"a": 11, "b": 22},
                "t2_mapp": None,
                "t3_mapp": {"a": 25, "b": None},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 1, "b": 9},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 1000, "b": 2000},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 21, "b": 22},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 25, "b": None},
            },
            {"id": 5, "t1_mapp": None, "t2_mapp": None, "t3_mapp": {"a": 1, "b": 9}},
            {
                "id": 5,
                "t1_mapp": None,
                "t2_mapp": None,
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": 5,
                "t1_mapp": None,
                "t2_mapp": None,
                "t3_mapp": {"a": 1000, "b": 2000},
            },
            {"id": 5, "t1_mapp": None, "t2_mapp": None, "t3_mapp": {"a": 21, "b": 22}},
            {
                "id": 5,
                "t1_mapp": None,
                "t2_mapp": None,
                "t3_mapp": {"a": 25, "b": None},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_cross_lasof_map_join AS SELECT
                        t1.id, t1.mapp AS t1_mapp, t2.mapp AS t2_mapp, t3.mapp AS t3_mapp
                        FROM asof_tbl1 t1
                        CROSS JOIN asof_tbl3 t3
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.mapp >= t2.mapp)
                        ON t1.id = t2.id;"""


class asof_lasof_inner_int_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 5},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": None},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": None},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 12},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW lasof_inner_int_join AS SELECT
                        t1.id,
                        t1.intt AS t1_int,
                        joined.t2_int,
                        joined.t3_int
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN (
                            SELECT
                                t2.id,
                                t2.intt AS t2_int,
                                t3.intt AS t3_int
                            FROM asof_tbl2 t2
                            INNER JOIN asof_tbl3 t3
                                ON t2.id = t3.id
                        ) AS joined
                        MATCH_CONDITION (t1.intt >= joined.t2_int)
                        ON t1.id = joined.id;"""


class asof_lasof_inner_mapp_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        # We differ with DuckDB on NULL handling in map comparisons; id = 5
        # {'id': 5, 't1_mapp': None, 't2_mapp': None, 't3_mapp': {'a': 1000, 'b': 2000}}
        self.data = [
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": None,
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 1, "b": 9},
            },
            {"id": 3, "t1_mapp": {"a": 11, "b": 22}, "t2_mapp": None, "t3_mapp": None},
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 100, "b": 200},
            },
            {"id": 5, "t1_mapp": None, "t2_mapp": None, "t3_mapp": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW lasof_inner_mapp_join AS SELECT
                        t1.id,
                        t1.mapp AS t1_mapp,
                        joined.t2_mapp,
                        joined.t3_mapp
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN (
                            SELECT
                                t2.id,
                                t2.mapp AS t2_mapp,
                                t3.mapp AS t3_mapp
                            FROM asof_tbl2 t2
                            INNER JOIN asof_tbl3 t3
                                ON t2.id = t3.id
                        ) AS joined
                        MATCH_CONDITION (t1.mapp >= joined.t2_mapp)
                        ON t1.id = joined.id;"""


class asof_lasof_outer_int_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": 5},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": None},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": None},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 12},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW lasof_outer_int_join AS SELECT
                        t1.id,
                        t1.intt AS t1_int,
                        joined.t2_int,
                        joined.t3_int
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN (
                            SELECT
                                t2.id,
                                t2.intt AS t2_int,
                                t3.intt AS t3_int
                            FROM asof_tbl2 t2
                            FULL OUTER JOIN asof_tbl3 t3
                                ON t2.id = t3.id
                        ) AS joined
                        MATCH_CONDITION (t1.intt >= joined.t2_int)
                        ON t1.id = joined.id;"""


class asof_lasof_outer_map_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        # We differ with DuckDB on NULL handling in map comparisons; id = 5
        # {'id': 5, 't1_mapp': None, 't2_mapp': None, 't3_mapp': {'a': 1000, 'b': 2000}}
        self.data = [
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": None,
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 1, "b": 9},
            },
            {"id": 3, "t1_mapp": {"a": 11, "b": 22}, "t2_mapp": None, "t3_mapp": None},
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 100, "b": 200},
            },
            {"id": 5, "t1_mapp": None, "t2_mapp": None, "t3_mapp": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW lasof_outer_map_join AS SELECT
                        t1.id,
                        t1.mapp AS t1_mapp,
                        joined.t2_mapp,
                        joined.t3_mapp
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN (
                            SELECT
                                t2.id,
                                t2.mapp AS t2_mapp,
                                t3.mapp AS t3_mapp
                            FROM asof_tbl2 t2
                            FULL OUTER JOIN asof_tbl3 t3
                                ON t2.id = t3.id
                        ) AS joined
                        MATCH_CONDITION (t1.mapp >= joined.t2_mapp)
                        ON t1.id = joined.id;"""


class asof_lasof_left_int_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": None},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": None},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": None},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 12},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW lasof_left_int_join AS SELECT
                        t1.id, t1.intt AS t1_int, t2.intt AS t2_int, t3.intt AS t3_int
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt >= t2.intt)
                        ON t1.id = t2.id
                        LEFT JOIN asof_tbl3 t3
                        ON t2.id = t3.id AND t3.intt > 10;"""


class asof_lasof_left_map_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        # We differ with DuckDB on NULL handling in map comparisons; id = 5
        # {'id': 5, 't1_mapp': None, 't2_mapp': None, 't3_mapp': {'a': 1000, 'b': 2000}}
        self.data = [
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": None,
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 1, "b": 9},
            },
            {"id": 3, "t1_mapp": {"a": 11, "b": 22}, "t2_mapp": None, "t3_mapp": None},
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 100, "b": 200},
            },
            {"id": 5, "t1_mapp": None, "t2_mapp": None, "t3_mapp": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW lasof_left_map_join AS SELECT
                        t1.id, t1.mapp AS t1_mapp, t2.mapp AS t2_mapp, t3.mapp AS t3_mapp
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.mapp >= t2.mapp)
                        ON t1.id = t2.id
                        LEFT JOIN asof_tbl3 t3
                        ON t2.id = t3.id AND t3.mapp > MAP['a', 0];"""


class asof_lasof_right_int_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 12},
            {"id": None, "t1_int": None, "t2_int": None, "t3_int": 112},
            {"id": None, "t1_int": None, "t2_int": None, "t3_int": 16},
            {"id": None, "t1_int": None, "t2_int": None, "t3_int": 5},
            {"id": None, "t1_int": None, "t2_int": None, "t3_int": 70},
        ]
        self.sql = """CREATE MATERIALIZED VIEW lasof_right_int_join AS SELECT
                        t1.id, t1.intt AS t1_int, t2.intt AS t2_int, t3.intt AS t3_int
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt >= t2.intt)
                        ON t1.id = t2.id
                        RIGHT JOIN asof_tbl3 t3
                        ON t2.id = t3.id AND t3.intt > 10;"""


class asof_lasof_right_map_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 1, "b": 9},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": None,
                "t1_mapp": None,
                "t2_mapp": None,
                "t3_mapp": {"a": 1000, "b": 2000},
            },
            {
                "id": None,
                "t1_mapp": None,
                "t2_mapp": None,
                "t3_mapp": {"a": 21, "b": 22},
            },
            {
                "id": None,
                "t1_mapp": None,
                "t2_mapp": None,
                "t3_mapp": {"a": 25, "b": None},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW lasof_right_map_join AS SELECT
                        t1.id, t1.mapp AS t1_mapp, t2.mapp AS t2_mapp, t3.mapp AS t3_mapp
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.mapp >= t2.mapp)
                        ON t1.id = t2.id
                        RIGHT JOIN asof_tbl3 t3
                        ON t2.id = t3.id AND t3.mapp > MAP['a', 0];"""


class asof_left_lasof_int_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_int": 10, "t2_int": 5, "t3_int": None},
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": 16},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": 70},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 12},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": 112},
        ]
        self.sql = """CREATE MATERIALIZED VIEW left_lasof_int_join AS SELECT
                        t1.id, t1.intt AS t1_int, t2.intt AS t2_int, t3.intt AS t3_int
                        FROM asof_tbl1 t1
                        LEFT JOIN asof_tbl3 t3
                        ON t1.id = t3.id AND t3.intt > 10
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt >= t2.intt)
                        ON t1.id = t2.id;"""


class asof_left_lasof_map_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": {"a": 25, "b": None},
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 1, "b": 9},
            },
            {
                "id": 3,
                "t1_mapp": {"a": 11, "b": 22},
                "t2_mapp": None,
                "t3_mapp": {"a": 21, "b": 22},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": 5,
                "t1_mapp": None,
                "t2_mapp": None,
                "t3_mapp": {"a": 1000, "b": 2000},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW left_lasof_map_join AS SELECT
                        t1.id, t1.mapp AS t1_mapp, t2.mapp AS t2_mapp, t3.mapp AS t3_mapp
                        FROM asof_tbl1 t1
                        LEFT JOIN asof_tbl3 t3
                        ON t1.id = t3.id AND t3.mapp > MAP['a', 0]
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.mapp >= t2.mapp)
                        ON t1.id = t2.id;"""


class asof_right_lasof_int_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 2, "t1_int": 15, "t2_int": None, "t3_int": 16},
            {"id": 3, "t1_int": 20, "t2_int": None, "t3_int": 70},
            {"id": 4, "t1_int": 25, "t2_int": 12, "t3_int": 12},
            {"id": 5, "t1_int": None, "t2_int": None, "t3_int": 112},
            {"id": None, "t1_int": None, "t2_int": None, "t3_int": 5},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_right_lasof_int_join AS SELECT
                        t1.id, t1.intt AS t1_int, t2.intt AS t2_int, t3.intt AS t3_int
                        FROM asof_tbl1 t1
                        RIGHT JOIN asof_tbl3 t3
                        ON t1.id = t3.id AND t3.intt > 10
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt >= t2.intt)
                        ON t1.id = t2.id;"""


class asof_right_lasof_map_join(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {
                "id": 1,
                "t1_mapp": {"a": 15, "b": None},
                "t2_mapp": None,
                "t3_mapp": {"a": 25, "b": None},
            },
            {
                "id": 2,
                "t1_mapp": {"a": 3, "b": 9},
                "t2_mapp": {"a": 1, "b": 9},
                "t3_mapp": {"a": 1, "b": 9},
            },
            {
                "id": 3,
                "t1_mapp": {"a": 11, "b": 22},
                "t2_mapp": None,
                "t3_mapp": {"a": 21, "b": 22},
            },
            {
                "id": 4,
                "t1_mapp": {"a": 200, "b": 200},
                "t2_mapp": {"a": 100, "b": 200},
                "t3_mapp": {"a": 100, "b": 200},
            },
            {
                "id": 5,
                "t1_mapp": None,
                "t2_mapp": None,
                "t3_mapp": {"a": 1000, "b": 2000},
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_right_lasof_map_join AS SELECT
                        t1.id, t1.mapp AS t1_mapp, t2.mapp AS t2_mapp, t3.mapp AS t3_mapp
                        FROM asof_tbl1 t1
                        RIGHT JOIN asof_tbl3 t3
                        ON t1.id = t3.id AND t3.mapp > MAP['a', 0]
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.mapp >= t2.mapp)
                        ON t1.id = t2.id;"""


class asof_left_right_lasof_join_chain(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_intt": 10, "t2_intt": 5, "t3_intt": 5, "t4_intt": None},
            {"id": 2, "t1_intt": 15, "t2_intt": 16, "t3_intt": 16, "t4_intt": None},
            {"id": 3, "t1_intt": 20, "t2_intt": 70, "t3_intt": 70, "t4_intt": 37},
            {"id": 4, "t1_intt": 25, "t2_intt": 12, "t3_intt": 12, "t4_intt": None},
            {
                "id": 5,
                "t1_intt": None,
                "t2_intt": None,
                "t3_intt": 112,
                "t4_intt": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_left_right_lasof_join AS SELECT
                        t1.id, t1.intt AS t1_intt, t2.intt AS t2_intt, t3.intt AS t3_intt, t4.intt AS t4_intt
                        FROM asof_tbl1 t1
                        LEFT JOIN asof_tbl2 t2
                            ON t1.id = t2.id
                        RIGHT JOIN asof_tbl3 t3
                            ON t2.id = t3.id
                        LEFT ASOF JOIN asof_tbl4 t4
                            MATCH_CONDITION (t3.intt >= t4.intt)
                            ON t3.id = t4.id;"""


class asof_left_lasof_right_join_chain(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_intt": 10, "t2_intt": 5, "t3_intt": 5, "t4_intt": 8},
            {"id": 2, "t1_intt": 15, "t2_intt": 16, "t3_intt": 16, "t4_intt": 22},
            {"id": 3, "t1_intt": 20, "t2_intt": 70, "t3_intt": 70, "t4_intt": 37},
            {"id": 4, "t1_intt": 25, "t2_intt": 12, "t3_intt": 12, "t4_intt": 50},
            {
                "id": None,
                "t1_intt": None,
                "t2_intt": None,
                "t3_intt": None,
                "t4_intt": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_left_lasof_right_join_chain AS SELECT
                        t1.id, t1.intt AS t1_intt, t2.intt AS t2_intt, t3.intt AS t3_intt, t4.intt AS t4_intt
                        FROM asof_tbl1 t1
                        LEFT JOIN asof_tbl2 t2
                            ON t1.id = t2.id
                        LEFT ASOF JOIN asof_tbl3 t3
                            MATCH_CONDITION (t2.intt >= t3.intt)
                            ON t2.id = t3.id
                        RIGHT JOIN asof_tbl4 t4
                            ON t3.id = t4.id;"""


class asof_lasof_left_right_join_chain(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_intt": 10, "t2_intt": 5, "t3_intt": 5, "t4_intt": 8},
            {"id": 4, "t1_intt": 25, "t2_intt": 12, "t3_intt": 12, "t4_intt": 50},
            {
                "id": None,
                "t1_intt": None,
                "t2_intt": None,
                "t3_intt": None,
                "t4_intt": 22,
            },
            {
                "id": None,
                "t1_intt": None,
                "t2_intt": None,
                "t3_intt": None,
                "t4_intt": 37,
            },
            {
                "id": None,
                "t1_intt": None,
                "t2_intt": None,
                "t3_intt": None,
                "t4_intt": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_lasof_left_right_join_chain AS SELECT
                        t1.id, t1.intt AS t1_intt, t2.intt AS t2_intt, t3.intt AS t3_intt, t4.intt AS t4_intt
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                            MATCH_CONDITION (t1.intt >= t2.intt)
                            ON t1.id = t2.id
                        LEFT JOIN asof_tbl3 t3
                            ON t2.id = t3.id
                        RIGHT JOIN asof_tbl4 t4
                            ON t3.id = t4.id;"""
