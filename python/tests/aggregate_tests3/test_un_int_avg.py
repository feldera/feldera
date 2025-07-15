from tests.aggregate_tests.aggtst_base import TstView


class aggtst_un_int_avg(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_avg_value AS SELECT
                      AVG(c1) AS c1,
                      AVG(c2) AS c2,
                      AVG(c3) AS c3,
                      AVG(c4) AS c4,
                      AVG(c5) AS c5,
                      AVG(c6) AS c6,
                      AVG(c7) AS c7,
                      AVG(c8) AS c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "c1": 66,
                "c2": 55,
                "c3": 13527,
                "c4": 14749,
                "c5": 816483822,
                "c6": 721283929,
                "c7": 393238291986,
                "c8": 277326742086,
            }
        ]


class aggtst_un_int_avg_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_avg_gby AS SELECT
                      id,
                      AVG(c1) AS c1,
                      AVG(c2) AS c2,
                      AVG(c3) AS c3,
                      AVG(c4) AS c4,
                      AVG(c5) AS c5,
                      AVG(c6) AS c6,
                      AVG(c7) AS c7,
                      AVG(c8) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 72,
                "c2": 54,
                "c3": 13450,
                "c4": 15062,
                "c5": 796796552,
                "c6": 674681197,
                "c7": 419283746512,
                "c8": 284269695883,
            },
            {
                "id": 1,
                "c1": 61,
                "c2": 56,
                "c3": 13566,
                "c4": 14437,
                "c5": 836171093,
                "c6": 767886662,
                "c7": 367192837461,
                "c8": 270383788290,
            },
        ]


class aggtst_un_int_avg_distinct(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_avg_distinct AS SELECT
                      AVG(DISTINCT c1) AS c1,
                      AVG(DISTINCT c2) AS c2,
                      AVG(DISTINCT c3) AS c3,
                      AVG(DISTINCT c4) AS c4,
                      AVG(DISTINCT c5) AS c5,
                      AVG(DISTINCT c6) AS c6,
                      AVG(DISTINCT c7) AS c7,
                      AVG(DISTINCT c8) AS c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "c1": 66,
                "c2": 52,
                "c3": 13527,
                "c4": 14749,
                "c5": 816483822,
                "c6": 721283929,
                "c7": 393238291986,
                "c8": 277326742086,
            }
        ]


class aggtst_un_int_avg_distinct_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_avg_distinct_gby AS SELECT
                      id,
                      AVG(DISTINCT c1) AS c1,
                      AVG(DISTINCT c2) AS c2,
                      AVG(DISTINCT c3) AS c3,
                      AVG(DISTINCT c4) AS c4,
                      AVG(DISTINCT c5) AS c5,
                      AVG(DISTINCT c6) AS c6,
                      AVG(DISTINCT c7) AS c7,
                      AVG(DISTINCT c8) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 72,
                "c2": 54,
                "c3": 13450,
                "c4": 15062,
                "c5": 796796552,
                "c6": 674681197,
                "c7": 419283746512,
                "c8": 284269695883,
            },
            {
                "id": 1,
                "c1": 61,
                "c2": 56,
                "c3": 13566,
                "c4": 14437,
                "c5": 836171093,
                "c6": 767886662,
                "c7": 367192837461,
                "c8": 270383788290,
            },
        ]


class aggtst_un_int_avg_where(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_avg_where AS SELECT
                      AVG(c1) FILTER(WHERE c4>13450) AS f_c1,
                      AVG(c2) FILTER(WHERE c4>13450) AS f_c2,
                      AVG(c3) FILTER(WHERE c4>13450) AS f_c3,
                      AVG(c4) FILTER(WHERE c4>13450) AS f_c4,
                      AVG(c5) FILTER(WHERE c4>13450) AS f_c5,
                      AVG(c6) FILTER(WHERE c4>13450) AS f_c6,
                      AVG(c7) FILTER(WHERE c4>13450) AS f_c7,
                      AVG(c8) FILTER(WHERE c4>13450) AS f_c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "f_c1": 66,
                "f_c2": 55,
                "f_c3": 13527,
                "f_c4": 14749,
                "f_c5": 816483822,
                "f_c6": 721283929,
                "f_c7": 393238291986,
                "f_c8": 277326742086,
            }
        ]


class aggtst_un_int_avg_where_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_avg_where_gby AS SELECT
                      id,
                      AVG(c1) FILTER(WHERE c4>13450) AS f_c1,
                      AVG(c2) FILTER(WHERE c4>13450) AS f_c2,
                      AVG(c3) FILTER(WHERE c4>13450) AS f_c3,
                      AVG(c4) FILTER(WHERE c4>13450) AS f_c4,
                      AVG(c5) FILTER(WHERE c4>13450) AS f_c5,
                      AVG(c6) FILTER(WHERE c4>13450) AS f_c6,
                      AVG(c7) FILTER(WHERE c4>13450) AS f_c7,
                      AVG(c8) FILTER(WHERE c4>13450) AS f_c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "f_c1": 72,
                "f_c2": 54,
                "f_c3": 13450,
                "f_c4": 15062,
                "f_c5": 796796552,
                "f_c6": 674681197,
                "f_c7": 419283746512,
                "f_c8": 284269695883,
            },
            {
                "id": 1,
                "f_c1": 61,
                "f_c2": 56,
                "f_c3": 13566,
                "f_c4": 14437,
                "f_c5": 836171093,
                "f_c6": 767886662,
                "f_c7": 367192837461,
                "f_c8": 270383788290,
            },
        ]
