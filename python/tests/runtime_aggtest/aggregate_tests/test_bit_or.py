from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_bit_or(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 5, "c2": 7, "c3": 7, "c4": 6, "c5": 7, "c6": 7, "c7": 7, "c8": 15}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_or AS SELECT
                      BIT_OR(c1) AS c1, BIT_OR(c2) AS c2, BIT_OR(c3) AS c3, BIT_OR(c4) AS c4, BIT_OR(c5) AS c5, BIT_OR(c6) AS c6, BIT_OR(c7) AS c7, BIT_OR(c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_bit_or_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 6,
                "c5": 7,
                "c6": 6,
                "c7": 3,
                "c8": 11,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 7,
                "c3": 6,
                "c4": 6,
                "c5": 2,
                "c6": 3,
                "c7": 4,
                "c8": 7,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_or_gby AS
                      SELECT id, BIT_OR(c1) AS c1, BIT_OR(c2) AS c2, BIT_OR(c3) AS c3, BIT_OR(c4) AS c4, BIT_OR(c5) AS c5, BIT_OR(c6) AS c6, BIT_OR(c7) AS c7, BIT_OR(c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_bit_or_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 5, "c2": 7, "c3": 7, "c4": 6, "c5": 7, "c6": 7, "c7": 7, "c8": 15}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_or_distinct AS SELECT
                      BIT_OR(DISTINCT c1) AS c1, BIT_OR(DISTINCT c2) AS c2, BIT_OR(DISTINCT c3) AS c3, BIT_OR(DISTINCT c4) AS c4, BIT_OR(DISTINCT c5) AS c5, BIT_OR(DISTINCT c6) AS c6, BIT_OR(DISTINCT c7) AS c7, BIT_OR(DISTINCT c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_bit_or_distinct_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 6,
                "c5": 7,
                "c6": 6,
                "c7": 3,
                "c8": 11,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 7,
                "c3": 6,
                "c4": 6,
                "c5": 2,
                "c6": 3,
                "c7": 4,
                "c8": 7,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_or_distinct_gby AS SELECT
                      id, BIT_OR(DISTINCT c1) AS c1, BIT_OR(DISTINCT c2) AS c2, BIT_OR(DISTINCT c3) AS c3, BIT_OR(DISTINCT c4) AS c4, BIT_OR(DISTINCT c5) AS c5, BIT_OR(DISTINCT c6) AS c6, BIT_OR(DISTINCT c7) AS c7, BIT_OR(DISTINCT c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_bit_or_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": 5,
                "f_c2": 7,
                "f_c3": 7,
                "f_c4": 6,
                "f_c5": 7,
                "f_c6": 7,
                "f_c7": 3,
                "f_c8": 15,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_or_where AS SELECT
                      BIT_OR(c1) FILTER(WHERE c8>2) AS f_c1, BIT_OR(c2) FILTER(WHERE c8>2) AS f_c2, BIT_OR(c3) FILTER(WHERE c8>2) AS f_c3, BIT_OR(c4) FILTER(WHERE c8>2) AS f_c4, BIT_OR(c5) FILTER(WHERE c8>2) AS f_c5, BIT_OR(c6) FILTER(WHERE c8>2) AS f_c6, BIT_OR(c7) FILTER(WHERE c8>2) AS f_c7, BIT_OR(c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl"""


class aggtst_int_bit_or_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": 5,
                "f_c2": 2,
                "f_c3": 3,
                "f_c4": 6,
                "f_c5": 7,
                "f_c6": 6,
                "f_c7": 3,
                "f_c8": 11,
            },
            {
                "id": 1,
                "f_c1": None,
                "f_c2": 5,
                "f_c3": 6,
                "f_c4": 2,
                "f_c5": 2,
                "f_c6": 1,
                "f_c7": None,
                "f_c8": 5,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_or_where_gby AS SELECT
                       id, BIT_OR(c1) FILTER(WHERE c8>2) AS f_c1, BIT_OR(c2) FILTER(WHERE c8>2) AS f_c2, BIT_OR(c3) FILTER(WHERE c8>2) AS f_c3, BIT_OR(c4) FILTER(WHERE c8>2) AS f_c4, BIT_OR(c5) FILTER(WHERE c8>2) AS f_c5, BIT_OR(c6) FILTER(WHERE c8>2) AS f_c6, BIT_OR(c7) FILTER(WHERE c8>2) AS f_c7, BIT_OR(c8) FILTER(WHERE c8>2) AS f_c8
                       FROM int0_tbl
                       GROUP BY id"""
