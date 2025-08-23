from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_un_int_arg_min(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_arg_min_value AS SELECT
                      ARG_MIN(c1, c2) AS c1,
                      ARG_MIN(c2, c1) AS c2,
                      ARG_MIN(c3, c4) AS c3,
                      ARG_MIN(c4, c3) AS c4,
                      ARG_MIN(c5, c6) AS c5,
                      ARG_MIN(c6, c5) AS c6,
                      ARG_MIN(c7, c8) AS c7,
                      ARG_MIN(c8, c7) AS c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "c1": None,
                "c2": 64,
                "c3": 12876,
                "c4": 13532,
                "c5": 781245123,
                "c6": 786452310,
                "c7": 367192837461,
                "c8": 265928374652,
            }
        ]


class aggtst_un_int_arg_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_arg_min_gby AS SELECT
                      id,
                      ARG_MIN(c1, c2) AS c1,
                      ARG_MIN(c2, c1) AS c2,
                      ARG_MIN(c3, c4) AS c3,
                      ARG_MIN(c4, c3) AS c4,
                      ARG_MIN(c5, c6) AS c5,
                      ARG_MIN(c6, c5) AS c6,
                      ARG_MIN(c7, c8) AS c7,
                      ARG_MIN(c8, c7) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": 64,
                "c3": 13450,
                "c4": 14123,
                "c5": 781245123,
                "c6": 651238977,
                "c7": 419283746512,
                "c8": 283746512983,
            },
            {
                "id": 1,
                "c1": None,
                "c2": 64,
                "c3": 12876,
                "c4": 13532,
                "c5": 963218731,
                "c6": 786452310,
                "c7": 367192837461,
                "c8": 265928374652,
            },
        ]


class aggtst_un_int_arg_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_arg_min_distinct AS SELECT
                      ARG_MIN(DISTINCT c1, c2) AS c1,
                      ARG_MIN(DISTINCT c2, c1) AS c2,
                      ARG_MIN(DISTINCT c3, c4) AS c3,
                      ARG_MIN(DISTINCT c4, c3) AS c4,
                      ARG_MIN(DISTINCT c5, c6) AS c5,
                      ARG_MIN(DISTINCT c6, c5) AS c6,
                      ARG_MIN(DISTINCT c7, c8) AS c7,
                      ARG_MIN(DISTINCT c8, c7) AS c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "c1": None,
                "c2": 64,
                "c3": 12876,
                "c4": 13532,
                "c5": 781245123,
                "c6": 786452310,
                "c7": 367192837461,
                "c8": 265928374652,
            }
        ]


class aggtst_un_int_arg_min_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_arg_min_distinct_gby AS SELECT
                      id,
                      ARG_MIN(DISTINCT c1, c2) AS c1,
                      ARG_MIN(DISTINCT c2, c1) AS c2,
                      ARG_MIN(DISTINCT c3, c4) AS c3,
                      ARG_MIN(DISTINCT c4, c3) AS c4,
                      ARG_MIN(DISTINCT c5, c6) AS c5,
                      ARG_MIN(DISTINCT c6, c5) AS c6,
                      ARG_MIN(DISTINCT c7, c8) AS c7,
                      ARG_MIN(DISTINCT c8, c7) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": 64,
                "c3": 13450,
                "c4": 14123,
                "c5": 781245123,
                "c6": 651238977,
                "c7": 419283746512,
                "c8": 283746512983,
            },
            {
                "id": 1,
                "c1": None,
                "c2": 64,
                "c3": 12876,
                "c4": 13532,
                "c5": 963218731,
                "c6": 786452310,
                "c7": 367192837461,
                "c8": 265928374652,
            },
        ]


class aggtst_un_int_arg_min_where(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_arg_min_where AS SELECT
                      ARG_MIN(c1, c2) FILTER(WHERE c4>13450) AS f_c1,
                      ARG_MIN(c2, c1) FILTER(WHERE c4>13450) AS f_c2,
                      ARG_MIN(c3, c4) FILTER(WHERE c4>13450) AS f_c3,
                      ARG_MIN(c4, c3) FILTER(WHERE c4>13450) AS f_c4,
                      ARG_MIN(c5, c6) FILTER(WHERE c4>13450) AS f_c5,
                      ARG_MIN(c6, c5) FILTER(WHERE c4>13450) AS f_c6,
                      ARG_MIN(c7, c8) FILTER(WHERE c4>13450) AS f_c7,
                      ARG_MIN(c8, c7) FILTER(WHERE c4>13450) AS f_c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "f_c1": None,
                "f_c2": 64,
                "f_c3": 12876,
                "f_c4": 13532,
                "f_c5": 781245123,
                "f_c6": 786452310,
                "f_c7": 367192837461,
                "f_c8": 265928374652,
            }
        ]


class aggtst_un_int_arg_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_arg_min_where_gby AS SELECT
                      id,
                      ARG_MIN(c1, c2) FILTER(WHERE c4>13450) AS f_c1,
                      ARG_MIN(c2, c1) FILTER(WHERE c4>13450) AS f_c2,
                      ARG_MIN(c3, c4) FILTER(WHERE c4>13450) AS f_c3,
                      ARG_MIN(c4, c3) FILTER(WHERE c4>13450) AS f_c4,
                      ARG_MIN(c5, c6) FILTER(WHERE c4>13450) AS f_c5,
                      ARG_MIN(c6, c5) FILTER(WHERE c4>13450) AS f_c6,
                      ARG_MIN(c7, c8) FILTER(WHERE c4>13450) AS f_c7,
                      ARG_MIN(c8, c7) FILTER(WHERE c4>13450) AS f_c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "f_c1": None,
                "f_c2": 64,
                "f_c3": 13450,
                "f_c4": 14123,
                "f_c5": 781245123,
                "f_c6": 651238977,
                "f_c7": 419283746512,
                "f_c8": 283746512983,
            },
            {
                "id": 1,
                "f_c1": None,
                "f_c2": 64,
                "f_c3": 12876,
                "f_c4": 13532,
                "f_c5": 963218731,
                "f_c6": 786452310,
                "f_c7": 367192837461,
                "f_c8": 265928374652,
            },
        ]
