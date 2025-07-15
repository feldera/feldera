from tests.aggregate_tests.aggtst_base import TstView


class aggtst_un_int_sum(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_sum_value AS SELECT
                      SUM(c1) AS c1,
                      SUM(c2) AS c2,
                      SUM(c3) AS c3,
                      SUM(c4) AS c4,
                      SUM(c5) AS c5,
                      SUM(c6) AS c6,
                      SUM(c7) AS c7,
                      SUM(c8) AS c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "c1": 133,
                "c2": 221,
                "c3": 40583,
                "c4": 58999,
                "c5": 3265935291,
                "c6": 2885135718,
                "c7": 786476583973,
                "c8": 1109306968346,
            }
        ]


class aggtst_un_int_sum_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_sum_gby AS SELECT
                      id,
                      SUM(c1) AS c1,
                      SUM(c2) AS c2,
                      SUM(c3) AS c3,
                      SUM(c4) AS c4,
                      SUM(c5) AS c5,
                      SUM(c6) AS c6,
                      SUM(c7) AS c7,
                      SUM(c8) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 72,
                "c2": 109,
                "c3": 13450,
                "c4": 30125,
                "c5": 1593593104,
                "c6": 1349362394,
                "c7": 419283746512,
                "c8": 568539391766,
            },
            {
                "id": 1,
                "c1": 61,
                "c2": 112,
                "c3": 27133,
                "c4": 28874,
                "c5": 1672342187,
                "c6": 1535773324,
                "c7": 367192837461,
                "c8": 540767576580,
            },
        ]


class aggtst_un_int_sum_distinct(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_sum_distinct AS SELECT
                      SUM(DISTINCT c1) AS c1,
                      SUM(DISTINCT c2) AS c2,
                      SUM(DISTINCT c3) AS c3,
                      SUM(DISTINCT c4) AS c4,
                      SUM(DISTINCT c5) AS c5,
                      SUM(DISTINCT c6) AS c6,
                      SUM(DISTINCT c7) AS c7,
                      SUM(DISTINCT c8) AS c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "c1": 133,
                "c2": 157,
                "c3": 40583,
                "c4": 58999,
                "c5": 3265935291,
                "c6": 2885135718,
                "c7": 786476583973,
                "c8": 1109306968346,
            }
        ]


class aggtst_un_int_sum_distinct_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_sum_distinct_gby AS SELECT
                      id,
                      SUM(DISTINCT c1) AS c1,
                      SUM(DISTINCT c2) AS c2,
                      SUM(DISTINCT c3) AS c3,
                      SUM(DISTINCT c4) AS c4,
                      SUM(DISTINCT c5) AS c5,
                      SUM(DISTINCT c6) AS c6,
                      SUM(DISTINCT c7) AS c7,
                      SUM(DISTINCT c8) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 72,
                "c2": 109,
                "c3": 13450,
                "c4": 30125,
                "c5": 1593593104,
                "c6": 1349362394,
                "c7": 419283746512,
                "c8": 568539391766,
            },
            {
                "id": 1,
                "c1": 61,
                "c2": 112,
                "c3": 27133,
                "c4": 28874,
                "c5": 1672342187,
                "c6": 1535773324,
                "c7": 367192837461,
                "c8": 540767576580,
            },
        ]


class aggtst_un_int_sum_where(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_sum_where AS SELECT
                      SUM(c1) FILTER(WHERE c4>13450) AS f_c1,
                      SUM(c2) FILTER(WHERE c4>13450) AS f_c2,
                      SUM(c3) FILTER(WHERE c4>13450) AS f_c3,
                      SUM(c4) FILTER(WHERE c4>13450) AS f_c4,
                      SUM(c5) FILTER(WHERE c4>13450) AS f_c5,
                      SUM(c6) FILTER(WHERE c4>13450) AS f_c6,
                      SUM(c7) FILTER(WHERE c4>13450) AS f_c7,
                      SUM(c8) FILTER(WHERE c4>13450) AS f_c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "f_c1": 133,
                "f_c2": 221,
                "f_c3": 40583,
                "f_c4": 58999,
                "f_c5": 3265935291,
                "f_c6": 2885135718,
                "f_c7": 786476583973,
                "f_c8": 1109306968346,
            }
        ]


class aggtst_un_int_sum_where_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_sum_where_gby AS SELECT
                      id,
                      SUM(c1) FILTER(WHERE c4>13450) AS f_c1,
                      SUM(c2) FILTER(WHERE c4>13450) AS f_c2,
                      SUM(c3) FILTER(WHERE c4>13450) AS f_c3,
                      SUM(c4) FILTER(WHERE c4>13450) AS f_c4,
                      SUM(c5) FILTER(WHERE c4>13450) AS f_c5,
                      SUM(c6) FILTER(WHERE c4>13450) AS f_c6,
                      SUM(c7) FILTER(WHERE c4>13450) AS f_c7,
                      SUM(c8) FILTER(WHERE c4>13450) AS f_c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "f_c1": 72,
                "f_c2": 109,
                "f_c3": 13450,
                "f_c4": 30125,
                "f_c5": 1593593104,
                "f_c6": 1349362394,
                "f_c7": 419283746512,
                "f_c8": 568539391766,
            },
            {
                "id": 1,
                "f_c1": 61,
                "f_c2": 112,
                "f_c3": 27133,
                "f_c4": 28874,
                "f_c5": 1672342187,
                "f_c6": 1535773324,
                "f_c7": 367192837461,
                "f_c8": 540767576580,
            },
        ]
