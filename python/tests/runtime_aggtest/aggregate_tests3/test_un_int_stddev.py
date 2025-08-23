from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_un_int_stddev(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_stddev_value AS SELECT
                      STDDEV_SAMP(c1) AS c1,
                      STDDEV_SAMP(c2) AS c2,
                      STDDEV_SAMP(c3) AS c3,
                      STDDEV_SAMP(c4) AS c4,
                      STDDEV_SAMP(c5) AS c5,
                      STDDEV_SAMP(c6) AS c6,
                      STDDEV_SAMP(c7) AS c7,
                      STDDEV_SAMP(c8) AS c8
                      FROM un_int_tbl1"""
        self.data = [
            {
                "c1": 1,
                "c2": 1,
                "c3": 76,
                "c4": 64,
                "c5": 129099,
                "c6": 129099,
                "c7": 707106,
                "c8": 1248455,
            }
        ]


class aggtst_un_int_stddev_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_stddev_gby AS SELECT
                      id,
                      STDDEV_SAMP(c1) AS c1,
                      STDDEV_SAMP(c2) AS c2,
                      STDDEV_SAMP(c3) AS c3,
                      STDDEV_SAMP(c4) AS c4,
                      STDDEV_SAMP(c5) AS c5,
                      STDDEV_SAMP(c6) AS c6,
                      STDDEV_SAMP(c7) AS c7,
                      STDDEV_SAMP(c8) AS c8
                      FROM un_int_tbl1
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 1,
                "c2": 1,
                "c3": None,
                "c4": 35,
                "c5": 141421,
                "c6": 141421,
                "c7": None,
                "c8": 1335646,
            },
            {
                "id": 1,
                "c1": 1,
                "c2": 1,
                "c3": 35,
                "c4": 35,
                "c5": 141421,
                "c6": 141421,
                "c7": None,
                "c8": 1414213,
            },
        ]


class aggtst_un_int_stddev_distinct(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_stddev_min_distinct AS SELECT
                      STDDEV_SAMP(DISTINCT c1) AS c1,
                      STDDEV_SAMP(DISTINCT c2) AS c2,
                      STDDEV_SAMP(DISTINCT c3) AS c3,
                      STDDEV_SAMP(DISTINCT c4) AS c4,
                      STDDEV_SAMP(DISTINCT c5) AS c5,
                      STDDEV_SAMP(DISTINCT c6) AS c6,
                      STDDEV_SAMP(DISTINCT c7) AS c7,
                      STDDEV_SAMP(DISTINCT c8) AS c8
                      FROM un_int_tbl1"""
        self.data = [
            {
                "c1": 1,
                "c2": 1,
                "c3": 76,
                "c4": 64,
                "c5": 129099,
                "c6": 129099,
                "c7": 707106,
                "c8": 1248455,
            }
        ]


class aggtst_un_int_stddev_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW un_int_stddev_distinct_gby AS SELECT
                      id,
                      STDDEV_SAMP(DISTINCT c1) AS c1,
                      STDDEV_SAMP(DISTINCT c2) AS c2,
                      STDDEV_SAMP(DISTINCT c3) AS c3,
                      STDDEV_SAMP(DISTINCT c4) AS c4,
                      STDDEV_SAMP(DISTINCT c5) AS c5,
                      STDDEV_SAMP(DISTINCT c6) AS c6,
                      STDDEV_SAMP(DISTINCT c7) AS c7,
                      STDDEV_SAMP(DISTINCT c8) AS c8
                      FROM un_int_tbl1
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "c1": 1,
                "c2": 1,
                "c3": None,
                "c4": 35,
                "c5": 141421,
                "c6": 141421,
                "c7": None,
                "c8": 1335646,
            },
            {
                "id": 1,
                "c1": 1,
                "c2": 1,
                "c3": 35,
                "c4": 35,
                "c5": 141421,
                "c6": 141421,
                "c7": None,
                "c8": 1414213,
            },
        ]


class aggtst_un_int_stddev_where(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_stddev_where AS SELECT
                      STDDEV_SAMP(c1) FILTER(WHERE c4>1200) AS f_c1,
                      STDDEV_SAMP(c2) FILTER(WHERE c4>1200) AS f_c2,
                      STDDEV_SAMP(c3) FILTER(WHERE c4>1200) AS f_c3,
                      STDDEV_SAMP(c4) FILTER(WHERE c4>1200) AS f_c4,
                      STDDEV_SAMP(c5) FILTER(WHERE c4>1200) AS f_c5,
                      STDDEV_SAMP(c6) FILTER(WHERE c4>1200) AS f_c6,
                      STDDEV_SAMP(c7) FILTER(WHERE c4>1200) AS f_c7,
                      STDDEV_SAMP(c8) FILTER(WHERE c4>1200) AS f_c8
                      FROM un_int_tbl1"""
        self.data = [
            {
                "f_c1": 1,
                "f_c2": 1,
                "f_c3": 76,
                "f_c4": 50,
                "f_c5": 100000,
                "f_c6": 100000,
                "f_c7": 707106,
                "f_c8": 1000000,
            }
        ]


class aggtst_un_int_stddev_where_gby(TstView):
    def __init__(self):
        # Validated on MySQL
        self.sql = """CREATE MATERIALIZED VIEW un_int_stddev_where_gby AS SELECT
                      id,
                      STDDEV_SAMP(c1) FILTER(WHERE c4>1200) AS f_c1,
                      STDDEV_SAMP(c2) FILTER(WHERE c4>1200) AS f_c2,
                      STDDEV_SAMP(c3) FILTER(WHERE c4>1200) AS f_c3,
                      STDDEV_SAMP(c4) FILTER(WHERE c4>1200) AS f_c4,
                      STDDEV_SAMP(c5) FILTER(WHERE c4>1200) AS f_c5,
                      STDDEV_SAMP(c6) FILTER(WHERE c4>1200) AS f_c6,
                      STDDEV_SAMP(c7) FILTER(WHERE c4>1200) AS f_c7,
                      STDDEV_SAMP(c8) FILTER(WHERE c4>1200) AS f_c8
                      FROM un_int_tbl1
                      GROUP BY id"""
        self.data = [
            {
                "id": 0,
                "f_c1": None,
                "f_c2": None,
                "f_c3": None,
                "f_c4": None,
                "f_c5": None,
                "f_c6": None,
                "f_c7": None,
                "f_c8": None,
            },
            {
                "id": 1,
                "f_c1": 1,
                "f_c2": 1,
                "f_c3": 35,
                "f_c4": 35,
                "f_c5": 141421,
                "f_c6": 141421,
                "f_c7": None,
                "f_c8": 1414213,
            },
        ]
