from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_map_min_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": {"a": 75, "b": 66}, "c2": {"f": 1}}]
        self.sql = """CREATE MATERIALIZED VIEW map_min AS SELECT
                      MIN(c1) AS c1, MIN(c2) AS c2
                      FROM map_tbl"""


class aggtst_map_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": {"a": 75, "b": 66}, "c2": {"q": 22}},
            {"id": 1, "c1": {"f": 45, "h": 66}, "c2": {"f": 1}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_min_gby AS SELECT
                      id, MIN(c1) AS c1, MIN(c2) AS c2
                      FROM map_tbl
                      GROUP BY id"""


class aggtst_map_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": {"a": 75, "b": 66}, "c2": {"f": 1}}]
        self.sql = """CREATE MATERIALIZED VIEW map_min_distinct AS SELECT
                      MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM map_tbl"""


class aggtst_map_min_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": {"a": 75, "b": 66}, "c2": {"q": 22}},
            {"id": 1, "c1": {"f": 45, "h": 66}, "c2": {"f": 1}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_min_distinct_gby AS SELECT
                      id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM map_tbl
                      GROUP BY id"""


class aggtst_map_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": {"q": 11, "v": 66}, "c2": {"q": 11, "v": 66, "x": None}}]
        self.sql = """CREATE MATERIALIZED VIEW map_min_where AS SELECT
                      MIN(c1) FILTER(WHERE c1 < c2) AS c1, MIN(c2) FILTER(WHERE c1 < c2) AS c2
                      FROM map_tbl"""


class aggtst_map_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": {"q": 11, "v": 66}, "c2": {"q": 22}},
            {"id": 1, "c1": {"q": 11, "v": 66}, "c2": {"q": 11, "v": 66, "x": None}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_min_where_gby AS SELECT
                      id, MIN(c1) FILTER(WHERE c1 < c2) AS c1, MIN(c2) FILTER(WHERE c1 < c2) AS c2
                      FROM map_tbl
                      GROUP BY id"""
