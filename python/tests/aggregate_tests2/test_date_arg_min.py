from tests.aggregate_tests.aggtst_base import TstView


class aggtst_date_arg_min_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "2024-12-05", "c2": "2015-09-07"}]
        self.sql = """CREATE MATERIALIZED VIEW date_arg_min AS SELECT
                      ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM date_tbl"""


class aggtst_date_arg_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "2014-11-05", "c2": "2024-12-05"},
            {"id": 1, "c1": "2024-12-05", "c2": "2015-09-07"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_arg_min_gby AS SELECT
                      id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_arg_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "2024-12-05", "c2": "2015-09-07"}]
        self.sql = """CREATE MATERIALIZED VIEW date_arg_min_distinct AS SELECT
                      ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM date_tbl"""


class aggtst_date_arg_min_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "2014-11-05", "c2": "2024-12-05"},
            {"id": 1, "c1": "2024-12-05", "c2": "2015-09-07"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_arg_min_distinct_gby AS SELECT
                      id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_arg_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "2024-12-05", "c2": "2023-02-26"}]
        self.sql = """CREATE MATERIALIZED VIEW date_arg_min_where AS SELECT
                      ARG_MIN(c1, c2) FILTER(WHERE c1 > '2014-11-05') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '2014-11-05') AS c2
                      FROM date_tbl"""


class aggtst_date_arg_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "2020-06-21", "c2": None},
            {"id": 1, "c1": "2024-12-05", "c2": "2023-02-26"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_arg_min_where_gby AS SELECT
                      id, ARG_MIN(c1, c2) FILTER(WHERE c1 > '2014-11-05') AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 > '2014-11-05') AS c2
                      FROM date_tbl
                      GROUP BY id"""
