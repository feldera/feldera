from .aggtst_base import TstView


class aggtst_map_max_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": {"x": 8, "y": 6}, "c2": {"q": 22}}]
        self.sql = """CREATE MATERIALIZED VIEW map_max AS SELECT
                      MAX(c1) AS c1, MAX(c2) AS c2
                      FROM map_tbl"""


class aggtst_map_max_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": {"q": 11, "v": 66}, "c2": {"q": 22}},
            {"id": 1, "c1": {"x": 8, "y": 6}, "c2": {"q": 11, "v": 66, "x": None}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_max_gby AS SELECT
                      id, MAX(c1) AS c1, MAX(c2) AS c2
                      FROM map_tbl
                      GROUP BY id"""


class aggtst_map_max_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": {"x": 8, "y": 6}, "c2": {"q": 22}}]
        self.sql = """CREATE MATERIALIZED VIEW map_max_distinct AS SELECT
                      MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM map_tbl"""


class aggtst_map_max_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": {"q": 11, "v": 66}, "c2": {"q": 22}},
            {"id": 1, "c1": {"x": 8, "y": 6}, "c2": {"q": 11, "v": 66, "x": None}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_max_distinct_gby AS SELECT
                      id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM map_tbl
                      GROUP BY id"""


class aggtst_map_max_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": {"q": 11, "v": 66}, "c2": {"q": 22}}]
        self.sql = """CREATE MATERIALIZED VIEW map_max_where AS SELECT
                      MAX(c1) FILTER(WHERE c1 < c2) AS c1, MAX(c2) FILTER(WHERE c1 < c2) AS c2
                      FROM map_tbl"""


class aggtst_map_max_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": {"q": 11, "v": 66}, "c2": {"q": 22}},
            {"id": 1, "c1": {"q": 11, "v": 66}, "c2": {"q": 11, "v": 66, "x": None}},
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_max_where_gby AS SELECT
                      id, MAX(c1) FILTER(WHERE c1 < c2) AS c1, MAX(c2) FILTER(WHERE c1 < c2) AS c2
                      FROM map_tbl
                      GROUP BY id"""
