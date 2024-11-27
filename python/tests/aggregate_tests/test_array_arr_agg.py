from .aggtst_base import TstView


class aggtst_array_arr_agg_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                'c1': [[12, 22], [23, 56, 16], [23, 56, 16], [49]],
                'c2': [None, [55, 66, 77], [99], [32, 34, 22, 12]]
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_arr_agg AS SELECT
                      ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM array_tbl"""


class aggtst_array_arr_agg_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'id': 0, 'c1': [[12, 22], [23, 56, 16]], 'c2': [None, [55, 66, 77]]},
            {'id': 1, 'c1': [[23, 56, 16], [49]], 'c2': [[99], [32, 34, 22, 12]]}
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_arr_agg_gby AS SELECT
                      id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM array_tbl
                      GROUP BY id"""


class aggtst_array_arr_agg_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                'c1': [[12, 22], [23, 56, 16], [49]],
                'c2': [None, [32, 34, 22, 12], [55, 66, 77], [99]]
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_arr_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM array_tbl"""


class aggtst_array_arr_agg_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'id': 0, 'c1': [[12, 22], [23, 56, 16]], 'c2': [None, [55, 66, 77]]},
            {'id': 1, 'c1': [[23, 56, 16], [49]], 'c2': [[32, 34, 22, 12], [99]]}
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_arr_agg_distinct_gby AS SELECT
                      id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM array_tbl
                      GROUP BY id"""


class aggtst_array_arr_agg_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'f_c1': [[23, 56, 16], [23, 56, 16]], 'f_c2': [[55, 66, 77], [99]]}
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_arr_agg_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2
                      FROM array_tbl"""


class aggtst_array_arr_agg_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {'id': 0, 'f_c1': [[23, 56, 16]], 'f_c2': [[55, 66, 77]]},
            {'id': 1, 'f_c1': [[23, 56, 16]], 'f_c2': [[99]]}
        ]
        self.sql = """CREATE MATERIALIZED VIEW array_arr_agg_where_gby AS SELECT
                      id, ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2
                      FROM array_tbl
                      GROUP BY id"""
