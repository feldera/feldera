from tests.aggregate_tests.aggtst_base import TstView


class aggtst_varchar_arg_max_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "@abc", "c2": "fred"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_max AS SELECT
                      ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_arg_max_value_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "@abcvariable-length", "c2": "fredhello"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_max_diff AS SELECT
                      ARG_MAX(c1||c2, c2||c1) AS c1, ARG_MAX(c2||c1, c1||c2) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_arg_max_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hello", "c2": "fred"},
            {"id": 1, "c1": "@abc", "c2": "exampl e"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_max_gby AS SELECT
                      id, ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_arg_max_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "@abc", "c2": "fred"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_max_distinct AS SELECT
                      ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_arg_max_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hello", "c2": "fred"},
            {"id": 1, "c1": "@abc", "c2": "exampl e"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_max_distinct_gby AS SELECT
                      id, ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_arg_max_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "hello", "c2": "fred"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_max_where AS SELECT
                      ARG_MAX(c1, c2) FILTER(WHERE len(c1)>4) AS c1, ARG_MAX(c2, c1) FILTER(WHERE len(c1)>4) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_arg_max_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hello", "c2": "fred"},
            {"id": 1, "c1": "hello", "c2": "exampl e"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_max_where_gby AS SELECT
                      id, ARG_MAX(c1, c2) FILTER(WHERE len(c1)>4) AS c1, ARG_MAX(c2, c1) FILTER(WHERE len(c1)>4) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_arg_max_where_gby_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hellofred", "c2": "fredhello"},
            {"id": 1, "c1": "helloexampl e", "c2": "exampl ehello"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_max_where_gby_diff AS SELECT
                      id, ARG_MAX(c1||c2, c2||c1) FILTER(WHERE len(c1)>4) AS c1, ARG_MAX(c2||c1, c1||c2) FILTER(WHERE len(c1)>4) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""
