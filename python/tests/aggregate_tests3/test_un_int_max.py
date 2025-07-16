from tests.aggregate_tests.aggtst_base import TstView


class aggtst_un_int_max(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_max_value AS SELECT
                      MAX(c1) AS c1,
                      MAX(c2) AS c2,
                      MAX(c3) AS c3,
                      MAX(c4) AS c4,
                      MAX(c5) AS c5,
                      MAX(c6) AS c6,
                      MAX(c7) AS c7,
                      MAX(c8) AS c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "c1": 72,
                "c2": 64,
                "c3": 14257,
                "c4": 16002,
                "c5": 963218731,
                "c6": 786452310,
                "c7": 419283746512,
                "c8": 284792878783,
            }
        ]


class aggtst_un_int_max_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_max_gby AS SELECT
                      id,
                      MAX(c1) AS c1,
                      MAX(c2) AS c2,
                      MAX(c3) AS c3,
                      MAX(c4) AS c4,
                      MAX(c5) AS c5,
                      MAX(c6) AS c6,
                      MAX(c7) AS c7,
                      MAX(c8) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 72,
                "c2": 64,
                "c3": 13450,
                "c4": 16002,
                "c5": 812347981,
                "c6": 698123417,
                "c7": 419283746512,
                "c8": 284792878783,
            },
            {
                "id": 1,
                "c1": 61,
                "c2": 64,
                "c3": 14257,
                "c4": 15342,
                "c5": 963218731,
                "c6": 786452310,
                "c7": 367192837461,
                "c8": 274839201928,
            },
        ]


class aggtst_un_int_max_distinct(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_max_distinct AS SELECT
                      MAX(DISTINCT c1) AS c1,
                      MAX(DISTINCT c2) AS c2,
                      MAX(DISTINCT c3) AS c3,
                      MAX(DISTINCT c4) AS c4,
                      MAX(DISTINCT c5) AS c5,
                      MAX(DISTINCT c6) AS c6,
                      MAX(DISTINCT c7) AS c7,
                      MAX(DISTINCT c8) AS c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "c1": 72,
                "c2": 64,
                "c3": 14257,
                "c4": 16002,
                "c5": 963218731,
                "c6": 786452310,
                "c7": 419283746512,
                "c8": 284792878783,
            }
        ]


class aggtst_un_int_max_distinct_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_max_distinct_gby AS SELECT
                      id,
                      MAX(DISTINCT c1) AS c1,
                      MAX(DISTINCT c2) AS c2,
                      MAX(DISTINCT c3) AS c3,
                      MAX(DISTINCT c4) AS c4,
                      MAX(DISTINCT c5) AS c5,
                      MAX(DISTINCT c6) AS c6,
                      MAX(DISTINCT c7) AS c7,
                      MAX(DISTINCT c8) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 72,
                "c2": 64,
                "c3": 13450,
                "c4": 16002,
                "c5": 812347981,
                "c6": 698123417,
                "c7": 419283746512,
                "c8": 284792878783,
            },
            {
                "id": 1,
                "c1": 61,
                "c2": 64,
                "c3": 14257,
                "c4": 15342,
                "c5": 963218731,
                "c6": 786452310,
                "c7": 367192837461,
                "c8": 274839201928,
            },
        ]


class aggtst_un_int_max_where(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_max_where AS SELECT
                      MAX(c1) FILTER(WHERE c4>13450) AS f_c1,
                      MAX(c2) FILTER(WHERE c4>13450) AS f_c2,
                      MAX(c3) FILTER(WHERE c4>13450) AS f_c3,
                      MAX(c4) FILTER(WHERE c4>13450) AS f_c4,
                      MAX(c5) FILTER(WHERE c4>13450) AS f_c5,
                      MAX(c6) FILTER(WHERE c4>13450) AS f_c6,
                      MAX(c7) FILTER(WHERE c4>13450) AS f_c7,
                      MAX(c8) FILTER(WHERE c4>13450) AS f_c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "f_c1": 72,
                "f_c2": 64,
                "f_c3": 14257,
                "f_c4": 16002,
                "f_c5": 963218731,
                "f_c6": 786452310,
                "f_c7": 419283746512,
                "f_c8": 284792878783,
            }
        ]


class aggtst_un_int_max_where_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_max_where_gby AS SELECT
                      id,
                      MAX(c1) FILTER(WHERE c4>13450) AS f_c1,
                      MAX(c2) FILTER(WHERE c4>13450) AS f_c2,
                      MAX(c3) FILTER(WHERE c4>13450) AS f_c3,
                      MAX(c4) FILTER(WHERE c4>13450) AS f_c4,
                      MAX(c5) FILTER(WHERE c4>13450) AS f_c5,
                      MAX(c6) FILTER(WHERE c4>13450) AS f_c6,
                      MAX(c7) FILTER(WHERE c4>13450) AS f_c7,
                      MAX(c8) FILTER(WHERE c4>13450) AS f_c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "f_c1": 72,
                "f_c2": 64,
                "f_c3": 13450,
                "f_c4": 16002,
                "f_c5": 812347981,
                "f_c6": 698123417,
                "f_c7": 419283746512,
                "f_c8": 284792878783,
            },
            {
                "id": 1,
                "f_c1": 61,
                "f_c2": 64,
                "f_c3": 14257,
                "f_c4": 15342,
                "f_c5": 963218731,
                "f_c6": 786452310,
                "f_c7": 367192837461,
                "f_c8": 274839201928,
            },
        ]
