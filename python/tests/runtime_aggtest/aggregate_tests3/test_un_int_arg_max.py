from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_un_int_arg_max(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_arg_max_value AS SELECT
                      ARG_MAX(c1, c2) AS c1,
                      ARG_MAX(c2, c1) AS c2,
                      ARG_MAX(c3, c4) AS c3,
                      ARG_MAX(c4, c3) AS c4,
                      ARG_MAX(c5, c6) AS c5,
                      ARG_MAX(c6, c5) AS c6,
                      ARG_MAX(c7, c8) AS c7,
                      ARG_MAX(c8, c7) AS c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "c1": 72,
                "c2": 64,
                "c3": None,
                "c4": 15342,
                "c5": 709123456,
                "c6": 749321014,
                "c7": None,
                "c8": 283746512983,
            }
        ]


class aggtst_un_int_max_gby(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_arg_max_gby AS SELECT
                      id,
                      ARG_MAX(c1, c2) AS c1,
                      ARG_MAX(c2, c1) AS c2,
                      ARG_MAX(c3, c4) AS c3,
                      ARG_MAX(c4, c3) AS c4,
                      ARG_MAX(c5, c6) AS c5,
                      ARG_MAX(c6, c5) AS c6,
                      ARG_MAX(c7, c8) AS c7,
                      ARG_MAX(c8, c7) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 72,
                "c2": 64,
                "c3": None,
                "c4": 14123,
                "c5": 812347981,
                "c6": 698123417,
                "c7": None,
                "c8": 283746512983,
            },
            {
                "id": 1,
                "c1": 61,
                "c2": 64,
                "c3": 14257,
                "c4": 15342,
                "c5": 709123456,
                "c6": 749321014,
                "c7": None,
                "c8": 265928374652,
            },
        ]


class aggtst_un_int_max_distinct(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_arg_max_distinct AS SELECT
                      ARG_MAX(DISTINCT c1, c2) AS c1,
                      ARG_MAX(DISTINCT c2, c1) AS c2,
                      ARG_MAX(DISTINCT c3, c4) AS c3,
                      ARG_MAX(DISTINCT c4, c3) AS c4,
                      ARG_MAX(DISTINCT c5, c6) AS c5,
                      ARG_MAX(DISTINCT c6, c5) AS c6,
                      ARG_MAX(DISTINCT c7, c8) AS c7,
                      ARG_MAX(DISTINCT c8, c7) AS c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "c1": 72,
                "c2": 64,
                "c3": None,
                "c4": 15342,
                "c5": 709123456,
                "c6": 749321014,
                "c7": None,
                "c8": 283746512983,
            }
        ]


class aggtst_un_int_arg_max_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_arg_max_distinct_gby AS SELECT
                      id,
                      ARG_MAX(DISTINCT c1, c2) AS c1,
                      ARG_MAX(DISTINCT c2, c1) AS c2,
                      ARG_MAX(DISTINCT c3, c4) AS c3,
                      ARG_MAX(DISTINCT c4, c3) AS c4,
                      ARG_MAX(DISTINCT c5, c6) AS c5,
                      ARG_MAX(DISTINCT c6, c5) AS c6,
                      ARG_MAX(DISTINCT c7, c8) AS c7,
                      ARG_MAX(DISTINCT c8, c7) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 72,
                "c2": 64,
                "c3": None,
                "c4": 14123,
                "c5": 812347981,
                "c6": 698123417,
                "c7": None,
                "c8": 283746512983,
            },
            {
                "id": 1,
                "c1": 61,
                "c2": 64,
                "c3": 14257,
                "c4": 15342,
                "c5": 709123456,
                "c6": 749321014,
                "c7": None,
                "c8": 265928374652,
            },
        ]


class aggtst_un_int_arg_max_where(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_arg_max_where AS SELECT
                      ARG_MAX(c1, c2) FILTER(WHERE c4>13450) AS f_c1,
                      ARG_MAX(c2, c1) FILTER(WHERE c4>13450) AS f_c2,
                      ARG_MAX(c3, c4) FILTER(WHERE c4>13450) AS f_c3,
                      ARG_MAX(c4, c3) FILTER(WHERE c4>13450) AS f_c4,
                      ARG_MAX(c5, c6) FILTER(WHERE c4>13450) AS f_c5,
                      ARG_MAX(c6, c5) FILTER(WHERE c4>13450) AS f_c6,
                      ARG_MAX(c7, c8) FILTER(WHERE c4>13450) AS f_c7,
                      ARG_MAX(c8, c7) FILTER(WHERE c4>13450) AS f_c8
                      FROM un_int_tbl"""
        self.data = [
            {
                "f_c1": 72,
                "f_c2": 64,
                "f_c3": None,
                "f_c4": 15342,
                "f_c5": 709123456,
                "f_c6": 749321014,
                "f_c7": None,
                "f_c8": 283746512983,
            }
        ]


class aggtst_un_int_arg_max_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_arg_max_where_gby AS SELECT
                      id,
                      ARG_MAX(c1, c2) FILTER(WHERE c4>13450) AS f_c1,
                      ARG_MAX(c2, c1) FILTER(WHERE c4>13450) AS f_c2,
                      ARG_MAX(c3, c4) FILTER(WHERE c4>13450) AS f_c3,
                      ARG_MAX(c4, c3) FILTER(WHERE c4>13450) AS f_c4,
                      ARG_MAX(c5, c6) FILTER(WHERE c4>13450) AS f_c5,
                      ARG_MAX(c6, c5) FILTER(WHERE c4>13450) AS f_c6,
                      ARG_MAX(c7, c8) FILTER(WHERE c4>13450) AS f_c7,
                      ARG_MAX(c8, c7) FILTER(WHERE c4>13450) AS f_c8
                      FROM un_int_tbl
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "f_c1": 72,
                "f_c2": 64,
                "f_c3": None,
                "f_c4": 14123,
                "f_c5": 812347981,
                "f_c6": 698123417,
                "f_c7": None,
                "f_c8": 283746512983,
            },
            {
                "id": 1,
                "f_c1": 61,
                "f_c2": 64,
                "f_c3": 14257,
                "f_c4": 15342,
                "f_c5": 709123456,
                "f_c6": 749321014,
                "f_c7": None,
                "f_c8": 265928374652,
            },
        ]
