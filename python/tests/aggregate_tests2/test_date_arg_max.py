from tests.aggregate_tests.aggtst_base import TstView


class aggtst_date_arg_max_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "2014-11-05", "c2": "2014-11-05"}]
        self.sql = """CREATE MATERIALIZED VIEW date_arg_max AS SELECT
                      ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
                      FROM date_tbl"""


class aggtst_date_arg_max_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "2014-11-05", "c2": None},
            {"id": 1, "c1": "2020-06-21", "c2": "2014-11-05"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_arg_max_gby AS SELECT
                      id, ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_arg_max_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "2014-11-05", "c2": "2014-11-05"}]
        self.sql = """CREATE MATERIALIZED VIEW date_arg_max_distinct AS SELECT
                      ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
                      FROM date_tbl"""


class aggtst_date_arg_max_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "2014-11-05", "c2": None},
            {"id": 1, "c1": "2020-06-21", "c2": "2014-11-05"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_arg_max_distinct_gby AS SELECT
                      id, ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_arg_max_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "2020-06-21", "c2": "2014-11-05"}]
        self.sql = """CREATE MATERIALIZED VIEW date_arg_max_where AS SELECT
                      ARG_MAX(c1, c2) FILTER(WHERE c1 > '2014-11-05') AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 > '2014-11-05') AS c2
                      FROM date_tbl"""


class aggtst_date_arg_max_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "2020-06-21", "c2": None},
            {"id": 1, "c1": "2020-06-21", "c2": "2014-11-05"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_arg_max_where_gby AS SELECT
                      id, ARG_MAX(c1, c2) FILTER(WHERE c1 > '2014-11-05') AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 > '2014-11-05') AS c2
                      FROM date_tbl
                      GROUP BY id"""
