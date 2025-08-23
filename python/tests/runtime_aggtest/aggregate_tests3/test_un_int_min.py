from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_un_int_min(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_min_value AS SELECT
                      MIN(c1) AS c1,
                      MIN(c2) AS c2,
                      MIN(c3) AS c3,
                      MIN(c4) AS c4,
                      MIN(c5) AS c5,
                      MIN(c6) AS c6,
                      MIN(c7) AS c7,
                      MIN(c8) AS c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "c1": 61,
                "c2": 45,
                "c3": 12876,
                "c4": 13532,
                "c5": 709123456,
                "c6": 651238977,
                "c7": 367192837461,
                "c8": 265928374652,
            }
        ]


class aggtst_un_int_min_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_min_gby AS SELECT
                      id,
                      MIN(c1) AS c1,
                      MIN(c2) AS c2,
                      MIN(c3) AS c3,
                      MIN(c4) AS c4,
                      MIN(c5) AS c5,
                      MIN(c6) AS c6,
                      MIN(c7) AS c7,
                      MIN(c8) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 72,
                "c2": 45,
                "c3": 13450,
                "c4": 14123,
                "c5": 781245123,
                "c6": 651238977,
                "c7": 419283746512,
                "c8": 283746512983,
            },
            {
                "id": 1,
                "c1": 61,
                "c2": 48,
                "c3": 12876,
                "c4": 13532,
                "c5": 709123456,
                "c6": 749321014,
                "c7": 367192837461,
                "c8": 265928374652,
            },
        ]


class aggtst_un_int_min_distinct(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_min_distinct AS SELECT
                      MIN(DISTINCT c1) AS c1,
                      MIN(DISTINCT c2) AS c2,
                      MIN(DISTINCT c3) AS c3,
                      MIN(DISTINCT c4) AS c4,
                      MIN(DISTINCT c5) AS c5,
                      MIN(DISTINCT c6) AS c6,
                      MIN(DISTINCT c7) AS c7,
                      MIN(DISTINCT c8) AS c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "c1": 61,
                "c2": 45,
                "c3": 12876,
                "c4": 13532,
                "c5": 709123456,
                "c6": 651238977,
                "c7": 367192837461,
                "c8": 265928374652,
            }
        ]


class aggtst_un_int_min_distinct_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_min_distinct_gby AS SELECT
                      id,
                      MIN(DISTINCT c1) AS c1,
                      MIN(DISTINCT c2) AS c2,
                      MIN(DISTINCT c3) AS c3,
                      MIN(DISTINCT c4) AS c4,
                      MIN(DISTINCT c5) AS c5,
                      MIN(DISTINCT c6) AS c6,
                      MIN(DISTINCT c7) AS c7,
                      MIN(DISTINCT c8) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 72,
                "c2": 45,
                "c3": 13450,
                "c4": 14123,
                "c5": 781245123,
                "c6": 651238977,
                "c7": 419283746512,
                "c8": 283746512983,
            },
            {
                "id": 1,
                "c1": 61,
                "c2": 48,
                "c3": 12876,
                "c4": 13532,
                "c5": 709123456,
                "c6": 749321014,
                "c7": 367192837461,
                "c8": 265928374652,
            },
        ]


class aggtst_un_int_min_where(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_min_where AS SELECT
                      MIN(c1) FILTER(WHERE c4>13450) AS f_c1,
                      MIN(c2) FILTER(WHERE c4>13450) AS f_c2,
                      MIN(c3) FILTER(WHERE c4>13450) AS f_c3,
                      MIN(c4) FILTER(WHERE c4>13450) AS f_c4,
                      MIN(c5) FILTER(WHERE c4>13450) AS f_c5,
                      MIN(c6) FILTER(WHERE c4>13450) AS f_c6,
                      MIN(c7) FILTER(WHERE c4>13450) AS f_c7,
                      MIN(c8) FILTER(WHERE c4>13450) AS f_c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "f_c1": 61,
                "f_c2": 45,
                "f_c3": 12876,
                "f_c4": 13532,
                "f_c5": 709123456,
                "f_c6": 651238977,
                "f_c7": 367192837461,
                "f_c8": 265928374652,
            }
        ]


class aggtst_un_int_min_where_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_min_where_gby AS SELECT
                      id,
                      MIN(c1) FILTER(WHERE c4>13450) AS f_c1,
                      MIN(c2) FILTER(WHERE c4>13450) AS f_c2,
                      MIN(c3) FILTER(WHERE c4>13450) AS f_c3,
                      MIN(c4) FILTER(WHERE c4>13450) AS f_c4,
                      MIN(c5) FILTER(WHERE c4>13450) AS f_c5,
                      MIN(c6) FILTER(WHERE c4>13450) AS f_c6,
                      MIN(c7) FILTER(WHERE c4>13450) AS f_c7,
                      MIN(c8) FILTER(WHERE c4>13450) AS f_c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "f_c1": 72,
                "f_c2": 45,
                "f_c3": 13450,
                "f_c4": 14123,
                "f_c5": 781245123,
                "f_c6": 651238977,
                "f_c7": 419283746512,
                "f_c8": 283746512983,
            },
            {
                "id": 1,
                "f_c1": 61,
                "f_c2": 48,
                "f_c3": 12876,
                "f_c4": 13532,
                "f_c5": 709123456,
                "f_c6": 749321014,
                "f_c7": 367192837461,
                "f_c8": 265928374652,
            },
        ]
