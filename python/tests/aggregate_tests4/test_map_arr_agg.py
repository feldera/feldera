from tests.aggregate_tests.aggtst_base import TstView


class aggtst_map_array_agg_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": [
                    {"a": 75, "b": 66},
                    {"f": 45, "h": 66},
                    {"q": 11, "v": 66},
                    {"q": 11, "v": 66},
                    {"x": 8, "y": 6},
                ],
                "c2": [
                    None,
                    {"f": 1},
                    {"q": 11, "v": 66, "x": None},
                    {"q": 22},
                    {"i": 5, "j": 66},
                ],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_array_agg AS SELECT
                      ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM map_tbl"""


class aggtst_map_array_agg_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": [{"a": 75, "b": 66}, {"q": 11, "v": 66}],
                "c2": [None, {"q": 22}],
            },
            {
                "id": 1,
                "c1": [{"f": 45, "h": 66}, {"q": 11, "v": 66}, {"x": 8, "y": 6}],
                "c2": [{"f": 1}, {"q": 11, "v": 66, "x": None}, {"i": 5, "j": 66}],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_array_agg_gby AS SELECT
                      id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2
                      FROM map_tbl
                      GROUP BY id"""


class aggtst_map_array_agg_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": [
                    {"a": 75, "b": 66},
                    {"f": 45, "h": 66},
                    {"q": 11, "v": 66},
                    {"x": 8, "y": 6},
                ],
                "c2": [
                    None,
                    {"f": 1},
                    {"i": 5, "j": 66},
                    {"q": 11, "v": 66, "x": None},
                    {"q": 22},
                ],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM map_tbl"""


class aggtst_map_array_agg_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": [{"a": 75, "b": 66}, {"q": 11, "v": 66}],
                "c2": [None, {"q": 22}],
            },
            {
                "id": 1,
                "c1": [{"f": 45, "h": 66}, {"q": 11, "v": 66}, {"x": 8, "y": 6}],
                "c2": [{"f": 1}, {"i": 5, "j": 66}, {"q": 11, "v": 66, "x": None}],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_array_agg_distinct_gby AS SELECT
                      id, ARRAY_AGG(DISTINCT c1) AS c1, ARRAY_AGG(DISTINCT c2) AS c2
                      FROM map_tbl
                      GROUP BY id"""


class aggtst_map_array_agg_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "f_c1": [{"q": 11, "v": 66}, {"q": 11, "v": 66}],
                "f_c2": [{"q": 11, "v": 66, "x": None}, {"q": 22}],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_array_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2
                      FROM map_tbl"""


class aggtst_map_array_agg_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "f_c1": [{"q": 11, "v": 66}], "f_c2": [{"q": 22}]},
            {
                "id": 1,
                "f_c1": [{"q": 11, "v": 66}],
                "f_c2": [{"q": 11, "v": 66, "x": None}],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW map_array_where_gby AS SELECT
                      id, ARRAY_AGG(c1) FILTER(WHERE c1 < c2) AS f_c1, ARRAY_AGG(c2) FILTER(WHERE c1 < c2) AS f_c2
                      FROM map_tbl
                      GROUP BY id"""
