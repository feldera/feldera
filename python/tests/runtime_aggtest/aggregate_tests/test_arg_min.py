from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_arg_min(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": 4, "c2": 2, "c3": 3, "c4": 2, "c5": 2, "c6": 1, "c7": 3, "c8": 2}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min AS SELECT
                      ARG_MIN(c1, c1) AS c1, ARG_MIN(c2, c2) AS c2, ARG_MIN(c3, c3) AS c3, ARG_MIN(c4, c4) AS c4, ARG_MIN(c5, c5) AS c5, ARG_MIN(c6, c6) AS c6, ARG_MIN(c7, c7) AS c7, ARG_MIN(c8, c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_arg_min_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": 3, "c3": 3, "c4": 2, "c5": 2, "c6": 1, "c7": 4, "c8": 3}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_diff AS SELECT
                      ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2, ARG_MIN(c3, c4) AS c3, ARG_MIN(c4, c3) AS c4, ARG_MIN(c5, c6) AS c5, ARG_MIN(c6, c5) AS c6, ARG_MIN(c7, c8) AS c7, ARG_MIN(c8, c7) AS c8
                      FROM int0_tbl"""


class aggtst_int_arg_min_diff1(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": 7, "c2": -3, "c3": 0, "c4": 2, "c5": 2, "c6": 0, "c7": 6, "c8": -2}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_diff1 AS SELECT
                      ARG_MIN(c1+c2, c2-c1) AS c1, ARG_MIN(c2-c1, c1+c2) AS c2, ARG_MIN(c3%c4, c4%c3) AS c3, ARG_MIN(c4%c3, c3%c4) AS c4, ARG_MIN(c5*c6, c6/c5) AS c5, ARG_MIN(c6/c5, c5*c6) AS c6, ARG_MIN(c7+c8, c8-c7) AS c7, ARG_MIN(c8-c7, c7+c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_arg_min_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 2,
                "c5": 3,
                "c6": 4,
                "c7": 3,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 3,
                "c3": 4,
                "c4": 2,
                "c5": 2,
                "c6": 1,
                "c7": 4,
                "c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_gby AS SELECT
                      id, ARG_MIN(c1, c1) AS c1, ARG_MIN(c2, c2) AS c2, ARG_MIN(c3, c3) AS c3, ARG_MIN(c4, c4) AS c4, ARG_MIN(c5, c5) AS c5, ARG_MIN(c6, c6) AS c6, ARG_MIN(c7, c7) AS c7, ARG_MIN(c8, c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_arg_min_gby_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": 2,
                "c3": 3,
                "c4": 2,
                "c5": 3,
                "c6": 4,
                "c7": 3,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 3,
                "c3": 6,
                "c4": 6,
                "c5": 2,
                "c6": 1,
                "c7": 4,
                "c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_gby_diff AS SELECT
                      id, ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2, ARG_MIN(c3, c4) AS c3, ARG_MIN(c4, c3) AS c4, ARG_MIN(c5, c6) AS c5, ARG_MIN(c6, c5) AS c6, ARG_MIN(c7, c8) AS c7, ARG_MIN(c8, c7) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_arg_min_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": 4, "c2": 2, "c3": 3, "c4": 2, "c5": 2, "c6": 1, "c7": 3, "c8": 2}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_distinct AS SELECT
                      ARG_MIN(DISTINCT c1, c1) AS c1, ARG_MIN(DISTINCT c2, c2) AS c2, ARG_MIN(DISTINCT c3, c3) AS c3, ARG_MIN(DISTINCT c4, c4) AS c4, ARG_MIN(DISTINCT c5, c5) AS c5, ARG_MIN(DISTINCT c6, c6) AS c6, ARG_MIN(DISTINCT c7, c7) AS c7, ARG_MIN(DISTINCT c8, c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_arg_min_distinct_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": 3, "c3": 3, "c4": 2, "c5": 2, "c6": 1, "c7": 4, "c8": 3}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_distinct_diff AS SELECT
                      ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2, ARG_MIN(DISTINCT c3, c4) AS c3, ARG_MIN(DISTINCT c4, c3) AS c4, ARG_MIN(DISTINCT c5, c6) AS c5, ARG_MIN(DISTINCT c6, c5) AS c6, ARG_MIN(DISTINCT c7, c8) AS c7, ARG_MIN(DISTINCT c8, c7) AS c8
                      FROM int0_tbl"""


class aggtst_int_arg_min_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 2,
                "c5": 3,
                "c6": 4,
                "c7": 3,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 3,
                "c3": 4,
                "c4": 2,
                "c5": 2,
                "c6": 1,
                "c7": 4,
                "c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_distinct_gby AS SELECT
                      id, ARG_MIN(DISTINCT c1, c1) AS c1, ARG_MIN(DISTINCT c2, c2) AS c2, ARG_MIN(DISTINCT c3, c3) AS c3, ARG_MIN(DISTINCT c4, c4) AS c4, ARG_MIN(DISTINCT c5, c5) AS c5, ARG_MIN(DISTINCT c6, c6) AS c6, ARG_MIN(DISTINCT c7, c7) AS c7, ARG_MIN(DISTINCT c8, c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_arg_min_distinct_diff_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": 2,
                "c3": 3,
                "c4": 2,
                "c5": 3,
                "c6": 4,
                "c7": 3,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 3,
                "c3": 6,
                "c4": 6,
                "c5": 2,
                "c6": 1,
                "c7": 4,
                "c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_distinct_gby_diff AS SELECT
                      id, ARG_MIN(DISTINCT c1, c2) AS c1, ARG_MIN(DISTINCT c2, c1) AS c2, ARG_MIN(DISTINCT c3, c4) AS c3, ARG_MIN(DISTINCT c4, c3) AS c4, ARG_MIN(DISTINCT c5, c6) AS c5, ARG_MIN(DISTINCT c6, c5) AS c6, ARG_MIN(DISTINCT c7, c8) AS c7, ARG_MIN(DISTINCT c8, c7) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_arg_min_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "f_c1": 5,
                "f_c2": 2,
                "f_c3": 3,
                "f_c4": 2,
                "f_c5": 2,
                "f_c6": 1,
                "f_c7": 3,
                "f_c8": 3,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_where AS SELECT
                      ARG_MIN(c1, c1) FILTER(WHERE c8>2) AS f_c1, ARG_MIN(c2, c2) FILTER(WHERE c8>2) AS f_c2, ARG_MIN(c3, c3) FILTER(WHERE c8>2) AS f_c3, ARG_MIN(c4, c4) FILTER(WHERE c8>2) AS f_c4, ARG_MIN(c5, c5) FILTER(WHERE c8>2) AS f_c5, ARG_MIN(c6, c6) FILTER(WHERE c8>2) AS f_c6, ARG_MIN(c7, c7) FILTER(WHERE c8>2) AS f_c7, ARG_MIN(c8, c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl"""


class aggtst_int_arg_min_where_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "f_c1": None,
                "f_c2": 2,
                "f_c3": 3,
                "f_c4": 2,
                "f_c5": 2,
                "f_c6": 1,
                "f_c7": 3,
                "f_c8": 3,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_where_diff AS SELECT
                      ARG_MIN(c1, c2) FILTER(WHERE c8>2) AS f_c1, ARG_MIN(c2, c1) FILTER(WHERE c8>2) AS f_c2, ARG_MIN(c3, c4) FILTER(WHERE c8>2) AS f_c3, ARG_MIN(c4, c3) FILTER(WHERE c8>2) AS f_c4, ARG_MIN(c5, c6) FILTER(WHERE c8>2) AS f_c5, ARG_MIN(c6, c5) FILTER(WHERE c8>2) AS f_c6, ARG_MIN(c7, c8) FILTER(WHERE c8>2) AS f_c7, ARG_MIN(c8, c7) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl"""


class aggtst_int_arg_min_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "f_c1": 5,
                "f_c2": 2,
                "f_c3": 3,
                "f_c4": 2,
                "f_c5": 3,
                "f_c6": 4,
                "f_c7": 3,
                "f_c8": 3,
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
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_where_gby AS SELECT
                      id, ARG_MIN(c1, c1) FILTER(WHERE c8>2) AS f_c1, ARG_MIN(c2, c2) FILTER(WHERE c8>2) AS f_c2, ARG_MIN(c3, c3) FILTER(WHERE c8>2) AS f_c3, ARG_MIN(c4, c4) FILTER(WHERE c8>2) AS f_c4, ARG_MIN(c5, c5) FILTER(WHERE c8>2) AS f_c5, ARG_MIN(c6, c6) FILTER(WHERE c8>2) AS f_c6, ARG_MIN(c7, c7) FILTER(WHERE c8>2) AS f_c7, ARG_MIN(c8, c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_arg_min_where_gby_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "f_c1": None,
                "f_c2": 2,
                "f_c3": 3,
                "f_c4": 2,
                "f_c5": 3,
                "f_c6": 4,
                "f_c7": 3,
                "f_c8": 3,
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
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_where_gby_diff AS SELECT
                      id, ARG_MIN(c1, c2) FILTER(WHERE c8>2) AS f_c1, ARG_MIN(c2, c1) FILTER(WHERE c8>2) AS f_c2, ARG_MIN(c3, c4) FILTER(WHERE c8>2) AS f_c3, ARG_MIN(c4, c3) FILTER(WHERE c8>2) AS f_c4, ARG_MIN(c5, c6) FILTER(WHERE c8>2) AS f_c5, ARG_MIN(c6, c5) FILTER(WHERE c8>2) AS f_c6, ARG_MIN(c7, c8) FILTER(WHERE c8>2) AS f_c7, ARG_MIN(c8, c7) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_arg_min_where_gby_diff1(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "f_c1": 7,
                "f_c2": -3,
                "f_c3": 1,
                "f_c4": 2,
                "f_c5": 12,
                "f_c6": 1,
                "f_c7": 6,
                "f_c8": 0,
            },
            {
                "id": 1,
                "f_c1": None,
                "f_c2": None,
                "f_c3": 0,
                "f_c4": 2,
                "f_c5": 2,
                "f_c6": 0,
                "f_c7": None,
                "f_c8": None,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_min_where_gby_diff1 AS SELECT
                      id,
                      ARG_MIN(c1+c2, c2-c1) FILTER(WHERE c8>2) AS f_c1,
                      ARG_MIN(c2-c1, c1+c2) FILTER(WHERE c8>2) AS f_c2,
                      ARG_MIN(c3%c4, c4%c3) FILTER(WHERE c8>2) AS f_c3,
                      ARG_MIN(c4%c3, c3%c4) FILTER(WHERE c8>2) AS f_c4,
                      ARG_MIN(c5*c6, c6/c5) FILTER(WHERE c8>2) AS f_c5,
                      ARG_MIN(c6/c5, c5*c6) FILTER(WHERE c8>2) AS f_c6,
                      ARG_MIN(c7+c8, c8-c7) FILTER(WHERE c8>2) AS f_c7,
                      ARG_MIN(c8-c7, c7+c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""
