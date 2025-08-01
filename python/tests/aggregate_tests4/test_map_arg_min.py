from tests.aggregate_tests.aggtst_base import TstView


class aggtst_map_arg_min_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": {"f": 45, "h": 66}, "c2": None}]
        self.sql = """CREATE MATERIALIZED VIEW map_arg_min AS SELECT
                      ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM map_tbl"""


class aggtst_map_arg_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": {"q": 11, "v": 66}, "c2": None},
            {"id": 1, "c1": {"f": 45, "h": 66}, "c2": {"f": 1}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_arg_min_gby AS SELECT
                      id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2
                      FROM map_tbl
                      GROUP BY id"""


class aggtst_map_arg_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": {"f": 45, "h": 66}, "c2": None}]
        self.sql = """CREATE MATERIALIZED VIEW map_arg_min_distinct AS SELECT
                      ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM map_tbl"""


class aggtst_map_arg_min_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": {"q": 11, "v": 66}, "c2": None},
            {"id": 1, "c1": {"f": 45, "h": 66}, "c2": {"f": 1}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_arg_min_distinct_gby AS SELECT
                      id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2
                      FROM map_tbl
                      GROUP BY id"""


class aggtst_map_arg_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": {"q": 11, "v": 66}, "c2": {"q": 11, "v": 66, "x": None}}]
        self.sql = """CREATE MATERIALIZED VIEW map_arg_min_where AS SELECT
                      ARG_MIN(c1, c2) FILTER(WHERE c1 < c2) AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 < c2) AS c2
                      FROM map_tbl"""


class aggtst_map_arg_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": {"q": 11, "v": 66}, "c2": {"q": 22}},
            {"id": 1, "c1": {"q": 11, "v": 66}, "c2": {"q": 11, "v": 66, "x": None}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_arg_min_where_gby AS SELECT
                      id, ARG_MIN(c1, c2) FILTER(WHERE c1 < c2) AS c1, ARG_MIN(c2, c1) FILTER(WHERE c1 < c2) AS c2
                      FROM map_tbl
                      GROUP BY id"""
