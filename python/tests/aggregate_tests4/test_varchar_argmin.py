from tests.aggregate_tests.aggtst_base import TstView


class aggtst_varchar_arg_min_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": None, "c2": "variable-length"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_min AS SELECT
                      ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_arg_min_value_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "helloexampl e", "c2": "variable-length@abc"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_min_diff AS SELECT
                      ARG_MIN(c1||c2, c2||c1) AS c1, ARG_MIN(c2||c1, c1||c2) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_arg_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None, "c2": "fred"},
            {"id": 1, "c1": "hello", "c2": "variable-length"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_min_gby AS SELECT
                      id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_arg_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": None, "c2": "variable-length"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_min_distinct AS SELECT
                      ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_arg_min_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None, "c2": "fred"},
            {"id": 1, "c1": "hello", "c2": "variable-length"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_min_distinct_gby AS SELECT
                      id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_arg_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "hello", "c2": "fred"}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_min_where AS SELECT
                      ARG_MIN(c1, c2) FILTER(WHERE len(c1)>4) AS c1, ARG_MIN(c2, c1) FILTER(WHERE len(c1)>4) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_arg_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hello", "c2": "fred"},
            {"id": 1, "c1": "hello", "c2": "exampl e"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_min_where_gby AS SELECT
                      id, ARG_MIN(c1, c2) FILTER(WHERE len(c1)>4) AS c1, ARG_MIN(c2, c1) FILTER(WHERE len(c1)>4) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_arg_min_where_gby_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hellofred", "c2": "fredhello"},
            {"id": 1, "c1": "helloexampl e", "c2": "exampl ehello"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varchar_arg_min_where_gby_diff AS SELECT
                      id, ARG_MIN(c1||c2, c2||c1) FILTER(WHERE len(c1)>4) AS c1, ARG_MIN(c2||c1, c1||c2) FILTER(WHERE len(c1)>4) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""
