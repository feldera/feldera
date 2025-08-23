from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_bit_xor(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 1, "c2": 6, "c3": 1, "c4": 2, "c5": 6, "c6": 0, "c7": 7, "c8": 12}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_xor_view AS SELECT
                      BIT_XOR(c1) AS c1, BIT_XOR(c2) AS c2, BIT_XOR(c3) AS c3, BIT_XOR(c4) AS c4, BIT_XOR(c5) AS c5, BIT_XOR(c6) AS c6, BIT_XOR(c7) AS c7, BIT_XOR(c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_bit_xor_Groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 0,
                "c3": 3,
                "c4": 6,
                "c5": 6,
                "c6": 2,
                "c7": 3,
                "c8": 11,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 6,
                "c3": 2,
                "c4": 4,
                "c5": 0,
                "c6": 2,
                "c7": 4,
                "c8": 7,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_xor_gby AS SELECT
                      id, BIT_XOR(c1) AS c1, BIT_XOR(c2) AS c2, BIT_XOR(c3) AS c3, BIT_XOR(c4) AS c4, BIT_XOR(c5) AS c5, BIT_XOR(c6) AS c6, BIT_XOR(c7) AS c7, BIT_XOR(c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_bit_xor_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"c1": 1, "c2": 4, "c3": 1, "c4": 0, "c5": 4, "c6": 0, "c7": 7, "c8": 12}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_xor_distinct AS SELECT
                      BIT_XOR(DISTINCT c1) AS c1, BIT_XOR(DISTINCT c2) AS c2, BIT_XOR(DISTINCT c3) AS c3, BIT_XOR(DISTINCT c4) AS c4, BIT_XOR(DISTINCT c5) AS c5, BIT_XOR(DISTINCT c6) AS c6, BIT_XOR(DISTINCT c7) AS c7, BIT_XOR(DISTINCT c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_bit_xor_distinct_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 6,
                "c5": 6,
                "c6": 2,
                "c7": 3,
                "c8": 11,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 6,
                "c3": 2,
                "c4": 4,
                "c5": 2,
                "c6": 2,
                "c7": 4,
                "c8": 7,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_xor_distinct_gby AS SELECT
                      id, BIT_XOR(DISTINCT c1) AS c1, BIT_XOR(DISTINCT c2) AS c2, BIT_XOR(DISTINCT c3) AS c3, BIT_XOR(DISTINCT c4) AS c4, BIT_XOR(DISTINCT c5) AS c5, BIT_XOR(DISTINCT c6) AS c6, BIT_XOR(DISTINCT c7) AS c7, BIT_XOR(DISTINCT c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_bit_xor_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "f_c1": 5,
                "f_c2": 5,
                "f_c3": 5,
                "f_c4": 4,
                "f_c5": 4,
                "f_c6": 3,
                "f_c7": 3,
                "f_c8": 14,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_bit_xor_where AS SELECT
                      BIT_XOR(c1) FILTER(WHERE c8>2) AS f_c1, BIT_XOR(c2) FILTER(WHERE c8>2) AS f_c2, BIT_XOR(c3) FILTER(WHERE c8>2) AS f_c3, BIT_XOR(c4) FILTER(WHERE c8>2) AS f_c4, BIT_XOR(c5) FILTER(WHERE c8>2) AS f_c5, BIT_XOR(c6) FILTER(WHERE c8>2) AS f_c6, BIT_XOR(c7) FILTER(WHERE c8>2) AS f_c7, BIT_XOR(c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl"""


class aggtst_int_bit_xor_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {
                "id": 0,
                "f_c1": 5,
                "f_c2": 0,
                "f_c3": 3,
                "f_c4": 6,
                "f_c5": 6,
                "f_c6": 2,
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
        self.sql = """CREATE MATERIALIZED VIEW int_bit_xor_where_gby AS SELECT
                      id, BIT_XOR(c1) FILTER(WHERE c8>2) AS f_c1, BIT_XOR(c2) FILTER(WHERE c8>2) AS f_c2, BIT_XOR(c3) FILTER(WHERE c8>2) AS f_c3, BIT_XOR(c4) FILTER(WHERE c8>2) AS f_c4, BIT_XOR(c5) FILTER(WHERE c8>2) AS f_c5, BIT_XOR(c6) FILTER(WHERE c8>2) AS f_c6, BIT_XOR(c7) FILTER(WHERE c8>2) AS f_c7, BIT_XOR(c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""
