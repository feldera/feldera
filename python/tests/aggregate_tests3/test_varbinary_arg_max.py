from tests.aggregate_tests.aggtst_base import TstView


class aggtst_varbinary_arg_max_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "17382115", "c2": "2022"}]
        self.sql = """CREATE MATERIALIZED VIEW varbinary_arg_max AS SELECT
                      ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
                      FROM varbinary_tbl"""


class aggtst_varbinary_arg_max_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "17382115", "c2": "37424d58"},
            {"id": 1, "c1": "17382115", "c2": "2022"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varbinary_arg_max_gby AS SELECT
                      id, ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
                      FROM varbinary_tbl
                      GROUP BY id"""


class aggtst_varbinary_arg_max_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "17382115", "c2": "2022"}]
        self.sql = """CREATE MATERIALIZED VIEW varbinary_arg_max_distinct AS SELECT
                      ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
                      FROM varbinary_tbl"""


class aggtst_varbinary_arg_max_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "17382115", "c2": "37424d58"},
            {"id": 1, "c1": "17382115", "c2": "2022"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varbinary_arg_max_distinct_gby AS SELECT
                      id, ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
                      FROM varbinary_tbl
                      GROUP BY id"""


class aggtst_varbinary_arg_max_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "17382115", "c2": "63141f4d"}]
        self.sql = """CREATE MATERIALIZED VIEW varbinary_arg_max_where AS SELECT
                      ARG_MAX(c1, c2) FILTER(WHERE c1 < c2) AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 < c2) AS c2
                      FROM varbinary_tbl"""


class aggtst_varbinary_arg_max_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "17382115", "c2": "37424d58"},
            {"id": 1, "c1": "17382115", "c2": "63141f4d"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varbinary_arg_max_where_gby AS SELECT
                      id, ARG_MAX(c1, c2) FILTER(WHERE c1 < c2) AS c1, ARG_MAX(c2, c1) FILTER(WHERE c1 < c2) AS c2
                      FROM varbinary_tbl
                      GROUP BY id"""
