from tests.aggregate_tests.aggtst_base import TstView


class aggtst_array_arr_agg_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": [[12, 22], [23, 56, 16], [23, 56, 16], [49]],
                "c2": [None, [55, 66, None], [99], [32, 34, 22, 12]],
                "c3": [[{"a": 5, "b": 66}], [{"c": 2}], None, [{"x": 1}]],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_arr_agg AS SELECT
                      ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3
                      FROM array_tbl"""


class aggtst_array_arr_agg_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": [[12, 22], [23, 56, 16]],
                "c2": [None, [55, 66, None]],
                "c3": [[{"a": 5, "b": 66}], [{"c": 2}]],
            },
            {
                "id": 1,
                "c1": [[23, 56, 16], [49]],
                "c2": [[99], [32, 34, 22, 12]],
                "c3": [None, [{"x": 1}]],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_arr_agg_gby AS SELECT
                      id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3
                      FROM array_tbl
                      GROUP BY id"""


class aggtst_array_arr_agg_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": [[12, 22], [23, 56, 16], [49]],
                "c2": [None, [32, 34, 22, 12], [55, 66, None], [99]],
                "c3": [None, [{"a": 5, "b": 66}], [{"c": 2}], [{"x": 1}]],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_arr_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3
                      FROM array_tbl"""


class aggtst_array_arr_agg_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": [[12, 22], [23, 56, 16]],
                "c2": [None, [55, 66, None]],
                "c3": [[{"a": 5, "b": 66}], [{"c": 2}]],
            },
            {
                "id": 1,
                "c1": [[23, 56, 16], [49]],
                "c2": [[32, 34, 22, 12], [99]],
                "c3": [None, [{"x": 1}]],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_arr_agg_distinct_gby AS SELECT
                      id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3
                      FROM array_tbl
                      GROUP BY id"""


class aggtst_array_arr_agg_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "f_c1": [[23, 56, 16], [23, 56, 16]],
                "f_c2": [[55, 66, None], [99]],
                "f_c3": [[{"c": 2}], None],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_arr_agg_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2, ARRAY_AGG(c3) FILTER(WHERE c1 < c2) AS f_c3
                      FROM array_tbl"""


class aggtst_array_arr_agg_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "f_c1": [[23, 56, 16]],
                "f_c2": [[55, 66, None]],
                "f_c3": [[{"c": 2}]],
            },
            {"id": 1, "f_c1": [[23, 56, 16]], "f_c2": [[99]], "f_c3": [None]},
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_arr_agg_where_gby AS SELECT
                      id, ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2, ARRAY_AGG(c3) FILTER(WHERE c1 < c2) AS f_c3
                      FROM array_tbl
                      GROUP BY id"""
