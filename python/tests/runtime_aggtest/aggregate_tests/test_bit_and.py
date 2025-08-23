from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_bit_and(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 4, "c2": 0, "c3": 0, "c4": 0, "c5": 0, "c6": 0, "c7": 0, "c8": 0}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_and AS SELECT
                      BIT_AND(c1) AS c1, BIT_AND(c2) AS c2, BIT_AND(c3) AS c3, BIT_AND(c4) AS c4, BIT_AND(c5) AS c5, BIT_AND(c6) AS c6, BIT_AND(c7) AS c7, BIT_AND(c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_bit_and_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 0,
                "c5": 1,
                "c6": 4,
                "c7": 3,
                "c8": 0,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 1,
                "c3": 4,
                "c4": 2,
                "c5": 2,
                "c6": 1,
                "c7": 4,
                "c8": 0,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_and_groupby AS SELECT
                      id, BIT_AND(c1) AS c1, BIT_AND(c2) AS c2, BIT_AND(c3) AS c3, BIT_AND(c4) AS c4, BIT_AND(c5) AS c5, BIT_AND(c6) AS c6, BIT_AND(c7) AS c7, BIT_AND(c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_bit_and_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 4, "c2": 0, "c3": 0, "c4": 0, "c5": 0, "c6": 0, "c7": 0, "c8": 0}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_and_distinct AS SELECT
                      BIT_AND(DISTINCT c1) AS c1, BIT_AND(DISTINCT c2) AS c2, BIT_AND(DISTINCT c3) AS c3, BIT_AND(DISTINCT c4) AS c4, BIT_AND(DISTINCT c5) AS c5, BIT_AND(DISTINCT c6) AS c6, BIT_AND(DISTINCT c7) AS c7, BIT_AND(DISTINCT c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_bit_and_distinct_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 0,
                "c5": 1,
                "c6": 4,
                "c7": 3,
                "c8": 0,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 1,
                "c3": 4,
                "c4": 2,
                "c5": 2,
                "c6": 1,
                "c7": 4,
                "c8": 0,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_and_distinct_groupby AS SELECT
                      id, BIT_AND(DISTINCT c1) AS c1, BIT_AND(DISTINCT c2) AS c2, BIT_AND(DISTINCT c3) AS c3, BIT_AND(DISTINCT c4) AS c4, BIT_AND(DISTINCT c5) AS c5, BIT_AND(DISTINCT c6) AS c6, BIT_AND(DISTINCT c7) AS c7, BIT_AND(DISTINCT c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_bit_and_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": 5,
                "f_c2": 0,
                "f_c3": 2,
                "f_c4": 0,
                "f_c5": 0,
                "f_c6": 0,
                "f_c7": 3,
                "f_c8": 0,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_and_where AS SELECT
                      BIT_AND(c1) FILTER(WHERE c8>2) AS f_c1, BIT_AND(c2) FILTER(WHERE c8>2) AS f_c2, BIT_AND(c3) FILTER(WHERE c8>2) AS f_c3, BIT_AND(c4) FILTER(WHERE c8>2) AS f_c4, BIT_AND(c5) FILTER(WHERE c8>2) AS f_c5, BIT_AND(c6) FILTER(WHERE c8>2) AS f_c6, BIT_AND(c7) FILTER(WHERE c8>2) AS f_c7, BIT_AND(c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl"""


class aggtst_int_bit_and_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": 5,
                "f_c2": 2,
                "f_c3": 3,
                "f_c4": 0,
                "f_c5": 1,
                "f_c6": 4,
                "f_c7": 3,
                "f_c8": 0,
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
        self.sql = """CREATE MATERIALIZED VIEW int_bit_and_where_groupby AS SELECT
                      id, BIT_AND(c1) FILTER(WHERE c8>2) AS f_c1, BIT_AND(c2) FILTER(WHERE c8>2) AS f_c2, BIT_AND(c3) FILTER(WHERE c8>2) AS f_c3, BIT_AND(c4) FILTER(WHERE c8>2) AS f_c4, BIT_AND(c5) FILTER(WHERE c8>2) AS f_c5, BIT_AND(c6) FILTER(WHERE c8>2) AS f_c6, BIT_AND(c7) FILTER(WHERE c8>2) AS f_c7, BIT_AND(c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""
