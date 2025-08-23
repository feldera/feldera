from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_un_int_stddev_pop(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_stddev_pop AS SELECT
                      STDDEV_POP(c1) AS c1,
                      STDDEV_POP(c2) AS c2,
                      STDDEV_POP(c3) AS c3,
                      STDDEV_POP(c4) AS c4,
                      STDDEV_POP(c5) AS c5,
                      STDDEV_POP(c6) AS c6,
                      STDDEV_POP(c7) AS c7,
                      STDDEV_POP(c8) AS c8
                      FROM un_int_tbl1"""
        self.data = [
            {
                "c1": 1,
                "c2": 1,
                "c3": 62,
                "c4": 55,
                "c5": 111803,
                "c6": 111803,
                "c7": 500000,
                "c8": 1081194,
            }
        ]


class aggtst_un_int_stddev_pop_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_stddev_pop_gby AS SELECT
                      id,
                      STDDEV_POP(c1) AS c1,
                      STDDEV_POP(c2) AS c2,
                      STDDEV_POP(c3) AS c3,
                      STDDEV_POP(c4) AS c4,
                      STDDEV_POP(c5) AS c5,
                      STDDEV_POP(c6) AS c6,
                      STDDEV_POP(c7) AS c7,
                      STDDEV_POP(c8) AS c8
                      FROM un_int_tbl1
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 1,
                "c2": 1,
                "c3": 0,
                "c4": 25,
                "c5": 100000,
                "c6": 100000,
                "c7": 0,
                "c8": 944444,
            },
            {
                "id": 1,
                "c1": 1,
                "c2": 1,
                "c3": 25,
                "c4": 25,
                "c5": 100000,
                "c6": 100000,
                "c7": 0,
                "c8": 1000000,
            },
        ]


class aggtst_un_int_stddev_pop_distinct(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_stddev_pop_min_distinct AS SELECT
                      STDDEV_POP(DISTINCT c1) AS c1,
                      STDDEV_POP(DISTINCT c2) AS c2,
                      STDDEV_POP(DISTINCT c3) AS c3,
                      STDDEV_POP(DISTINCT c4) AS c4,
                      STDDEV_POP(DISTINCT c5) AS c5,
                      STDDEV_POP(DISTINCT c6) AS c6,
                      STDDEV_POP(DISTINCT c7) AS c7,
                      STDDEV_POP(DISTINCT c8) AS c8
                      FROM un_int_tbl1"""
        self.data = [
            {
                "c1": 1,
                "c2": 1,
                "c3": 62,
                "c4": 55,
                "c5": 111803,
                "c6": 111803,
                "c7": 500000,
                "c8": 1081194,
            }
        ]


class aggtst_un_int_stddev_pop_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_stddev_pop_distinct_gby AS SELECT
                      id,
                      STDDEV_POP(DISTINCT c1) AS c1,
                      STDDEV_POP(DISTINCT c2) AS c2,
                      STDDEV_POP(DISTINCT c3) AS c3,
                      STDDEV_POP(DISTINCT c4) AS c4,
                      STDDEV_POP(DISTINCT c5) AS c5,
                      STDDEV_POP(DISTINCT c6) AS c6,
                      STDDEV_POP(DISTINCT c7) AS c7,
                      STDDEV_POP(DISTINCT c8) AS c8
                      FROM un_int_tbl1
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 1,
                "c2": 1,
                "c3": 0,
                "c4": 25,
                "c5": 100000,
                "c6": 100000,
                "c7": 0,
                "c8": 944444,
            },
            {
                "id": 1,
                "c1": 1,
                "c2": 1,
                "c3": 25,
                "c4": 25,
                "c5": 100000,
                "c6": 100000,
                "c7": 0,
                "c8": 1000000,
            },
        ]


class aggtst_un_int_stddev_pop_where(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_stddev_pop_where AS SELECT
                      STDDEV_POP(c1) FILTER(WHERE c4>1200) AS f_c1,
                      STDDEV_POP(c2) FILTER(WHERE c4>1200) AS f_c2,
                      STDDEV_POP(c3) FILTER(WHERE c4>1200) AS f_c3,
                      STDDEV_POP(c4) FILTER(WHERE c4>1200) AS f_c4,
                      STDDEV_POP(c5) FILTER(WHERE c4>1200) AS f_c5,
                      STDDEV_POP(c6) FILTER(WHERE c4>1200) AS f_c6,
                      STDDEV_POP(c7) FILTER(WHERE c4>1200) AS f_c7,
                      STDDEV_POP(c8) FILTER(WHERE c4>1200) AS f_c8
                      FROM un_int_tbl1"""
        self.data = [
            {
                "f_c1": 1,
                "f_c2": 1,
                "f_c3": 62,
                "f_c4": 40,
                "f_c5": 81649,
                "f_c6": 81649,
                "f_c7": 500000,
                "f_c8": 816496,
            }
        ]


class aggtst_un_int_stddev_pop_where_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_stddev_pop_where_gby AS SELECT
                      id,
                      STDDEV_POP(c1) FILTER(WHERE c4>1200) AS f_c1,
                      STDDEV_POP(c2) FILTER(WHERE c4>1200) AS f_c2,
                      STDDEV_POP(c3) FILTER(WHERE c4>1200) AS f_c3,
                      STDDEV_POP(c4) FILTER(WHERE c4>1200) AS f_c4,
                      STDDEV_POP(c5) FILTER(WHERE c4>1200) AS f_c5,
                      STDDEV_POP(c6) FILTER(WHERE c4>1200) AS f_c6,
                      STDDEV_POP(c7) FILTER(WHERE c4>1200) AS f_c7,
                      STDDEV_POP(c8) FILTER(WHERE c4>1200) AS f_c8
                      FROM un_int_tbl1
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "f_c1": 0,
                "f_c2": 0,
                "f_c3": 0,
                "f_c4": 0,
                "f_c5": 0,
                "f_c6": 0,
                "f_c7": 0,
                "f_c8": 0,
            },
            {
                "id": 1,
                "f_c1": 1,
                "f_c2": 1,
                "f_c3": 25,
                "f_c4": 25,
                "f_c5": 100000,
                "f_c6": 100000,
                "f_c7": 0,
                "f_c8": 1000000,
            },
        ]
