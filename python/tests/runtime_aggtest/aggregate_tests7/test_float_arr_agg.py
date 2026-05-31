from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_float_array_agg_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": [None, -76543.2109375, -9.876543045043945, 3.141592502593994],
                "c2": [
                    12345.6787109375,
                    -1.2299999980314169e-05,
                    -8.765432357788086,
                    2.7182817459106445,
                ],
                "c3": [
                    1.618033988749894,
                    -123456789.98765431,
                    -7.654321098765432,
                    None,
                ],
                "c4": [
                    9876543210.123455,
                    -10000000000.0,
                    -6.543210987654321,
                    0.577215664901532,
                ],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW float_array_agg AS SELECT
                      ARRAY_AGG(c1) AS c1,
                      ARRAY_AGG(c2) AS c2,
                      ARRAY_AGG(c3) AS c3,
                      ARRAY_AGG(c4) AS c4
                      FROM float_tbl"""


class aggtst_float_array_agg_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": [None, -76543.2109375],
                "c2": [12345.6787109375, -1.2299999980314169e-05],
                "c3": [1.618033988749894, -123456789.98765431],
                "c4": [9876543210.123455, -10000000000.0],
            },
            {
                "id": 1,
                "c1": [-9.876543045043945, 3.141592502593994],
                "c2": [-8.765432357788086, 2.7182817459106445],
                "c3": [-7.654321098765432, None],
                "c4": [-6.543210987654321, 0.577215664901532],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW float_array_agg_gby AS SELECT
                      id,
                      ARRAY_AGG(c1) AS c1,
                      ARRAY_AGG(c2) AS c2,
                      ARRAY_AGG(c3) AS c3,
                      ARRAY_AGG(c4) AS c4
                      FROM float_tbl
                      GROUP BY id"""


class aggtst_float_distinct_array_agg_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": [None, -76543.2109375, -9.876543045043945, 3.141592502593994],
                "c2": [
                    -8.765432357788086,
                    -1.2299999980314169e-05,
                    2.7182817459106445,
                    12345.6787109375,
                ],
                "c3": [
                    None,
                    -123456789.98765431,
                    -7.654321098765432,
                    1.618033988749894,
                ],
                "c4": [
                    -10000000000.0,
                    -6.543210987654321,
                    0.577215664901532,
                    9876543210.123455,
                ],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW float_distinct_array_agg AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1,
                      ARRAY_AGG(DISTINCT c2) AS c2,
                      ARRAY_AGG(DISTINCT c3) AS c3,
                      ARRAY_AGG(DISTINCT c4) AS c4
                      FROM float_tbl"""


class aggtst_float_distinct_array_agg_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": [None, -76543.2109375],
                "c2": [-1.2299999980314169e-05, 12345.6787109375],
                "c3": [-123456789.98765431, 1.618033988749894],
                "c4": [-10000000000.0, 9876543210.123455],
            },
            {
                "id": 1,
                "c1": [-9.876543045043945, 3.141592502593994],
                "c2": [-8.765432357788086, 2.7182817459106445],
                "c3": [None, -7.654321098765432],
                "c4": [-6.543210987654321, 0.577215664901532],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW float_distinct_array_agg_gby AS SELECT
                      id,
                      ARRAY_AGG(DISTINCT c1) AS c1,
                      ARRAY_AGG(DISTINCT c2) AS c2,
                      ARRAY_AGG(DISTINCT c3) AS c3,
                      ARRAY_AGG(DISTINCT c4) AS c4
                      FROM float_tbl
                      GROUP BY id"""


class aggtst_float_array_agg_value_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": [None],
                "c2": [12345.6787109375],
                "c3": [1.618033988749894],
                "c4": [9876543210.123455],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW float_array_agg_value_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE c1 IS NULL) AS c1,
                      ARRAY_AGG(c2) FILTER(WHERE c1 IS NULL) AS c2,
                      ARRAY_AGG(c3) FILTER(WHERE c1 IS NULL) AS c3,
                      ARRAY_AGG(c4) FILTER(WHERE c1 IS NULL) AS c4
                      FROM float_tbl"""


class aggtst_float_array_agg_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": [None],
                "c2": [12345.6787109375],
                "c3": [1.618033988749894],
                "c4": [9876543210.123455],
            },
            {"id": 1, "c1": [], "c2": [], "c3": [], "c4": []},
        ]
        self.sql = """CREATE MATERIALIZED VIEW float_array_agg_where_gby AS SELECT
                      id,
                      ARRAY_AGG(c1) FILTER(WHERE c1 IS NULL) AS c1,
                      ARRAY_AGG(c2) FILTER(WHERE c1 IS NULL)AS c2,
                      ARRAY_AGG(c3) FILTER(WHERE c1 IS NULL)AS c3,
                      ARRAY_AGG(c4) FILTER(WHERE c1 IS NULL)AS c4
                      FROM float_tbl
                      GROUP BY id"""
