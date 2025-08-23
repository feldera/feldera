from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_int_arg_max(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": 5, "c2": 5, "c3": 6, "c4": 6, "c5": 5, "c6": 6, "c7": 4, "c8": 8}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max AS SELECT
                      ARG_MAX(c1, c1) AS c1, ARG_MAX(c2, c2) AS c2, ARG_MAX(c3, c3) AS c3, ARG_MAX(c4, c4) AS c4, ARG_MAX(c5, c5) AS c5, ARG_MAX(c6, c6) AS c6, ARG_MAX(c7, c7) AS c7, ARG_MAX(c8, c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_arg_max_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": None,
                "c2": 2,
                "c3": 4,
                "c4": 2,
                "c5": 5,
                "c6": 6,
                "c7": None,
                "c8": 2,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max_diff AS SELECT
                      ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2, ARG_MAX(c3, c4) AS c3, ARG_MAX(c4, c3) AS c4, ARG_MAX(c5, c6) AS c5, ARG_MAX(c6, c5) AS c6, ARG_MAX(c7, c8) AS c7, ARG_MAX(c8, c7) AS c8
                      FROM int0_tbl"""


class aggtst_int_arg_max_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 4,
                "c5": 5,
                "c6": 6,
                "c7": 3,
                "c8": 8,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 5,
                "c3": 6,
                "c4": 6,
                "c5": 2,
                "c6": 3,
                "c7": 4,
                "c8": 5,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max_gby AS SELECT
                      id, ARG_MAX(c1, c1) AS c1, ARG_MAX(c2, c2) AS c2, ARG_MAX(c3, c3) AS c3, ARG_MAX(c4, c4) AS c4, ARG_MAX(c5, c5) AS c5, ARG_MAX(c6, c6) AS c6, ARG_MAX(c7, c7) AS c7, ARG_MAX(c8, c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_arg_max_gby_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": None,
                "c4": 2,
                "c5": 5,
                "c6": 6,
                "c7": None,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": None,
                "c2": 3,
                "c3": 4,
                "c4": 2,
                "c5": 2,
                "c6": 3,
                "c7": None,
                "c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max_gby_diff AS SELECT
                      id, ARG_MAX(c1, c2) AS c1, ARG_MAX(c2, c1) AS c2, ARG_MAX(c3, c4) AS c3, ARG_MAX(c4, c3) AS c4, ARG_MAX(c5, c6) AS c5, ARG_MAX(c6, c5) AS c6, ARG_MAX(c7, c8) AS c7, ARG_MAX(c8, c7) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_arg_max_gby_diff1(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": 7,
                "c2": -3,
                "c3": 1,
                "c4": 2,
                "c5": 30,
                "c6": 1,
                "c7": 6,
                "c8": 0,
            },
            {
                "id": 1,
                "c1": 7,
                "c2": -1,
                "c3": 4,
                "c4": 2,
                "c5": 6,
                "c6": 1,
                "c7": 6,
                "c8": -2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_gby_max_diff1 AS SELECT
                      id, ARG_MAX(c1+c2, c2-c1) AS c1, ARG_MAX(c2-c1, c1+c2) AS c2, ARG_MAX(c3%c4, c4%c3) AS c3, ARG_MAX(c4%c3, c3%c4) AS c4, ARG_MAX(c5*c6, c6/c5) AS c5, ARG_MAX(c6/c5, c5*c6) AS c6, ARG_MAX(c7+c8, c8-c7) AS c7, ARG_MAX(c8-c7, c7+c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_arg_max_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": 5, "c2": 5, "c3": 6, "c4": 6, "c5": 5, "c6": 6, "c7": 4, "c8": 8}
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max_distinct AS SELECT
                      ARG_MAX(DISTINCT c1, c1) AS c1, ARG_MAX(DISTINCT c2, c2) AS c2, ARG_MAX(DISTINCT c3, c3) AS c3, ARG_MAX(DISTINCT c4, c4) AS c4, ARG_MAX(DISTINCT c5, c5) AS c5, ARG_MAX(DISTINCT c6, c6) AS c6, ARG_MAX(DISTINCT c7, c7) AS c7, ARG_MAX(DISTINCT c8, c8) AS c8
                      FROM int0_tbl"""


class aggtst_int_arg_max_distinct_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": None,
                "c2": 2,
                "c3": 4,
                "c4": 2,
                "c5": 5,
                "c6": 6,
                "c7": None,
                "c8": 2,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max_distinct_diff AS SELECT
                      ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2, ARG_MAX(DISTINCT c3, c4) AS c3, ARG_MAX(DISTINCT c4, c3) AS c4, ARG_MAX(DISTINCT c5, c6) AS c5, ARG_MAX(DISTINCT c6, c5) AS c6, ARG_MAX(DISTINCT c7, c8) AS c7, ARG_MAX(DISTINCT c8, c7) AS c8
                      FROM int0_tbl"""


class aggtst_int_arg_max_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": 3,
                "c4": 4,
                "c5": 5,
                "c6": 6,
                "c7": 3,
                "c8": 8,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 5,
                "c3": 6,
                "c4": 6,
                "c5": 2,
                "c6": 3,
                "c7": 4,
                "c8": 5,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max_distinct_gby AS SELECT
                      id, ARG_MAX(DISTINCT c1, c1) AS c1, ARG_MAX(DISTINCT c2, c2) AS c2, ARG_MAX(DISTINCT c3, c3) AS c3, ARG_MAX(DISTINCT c4, c4) AS c4, ARG_MAX(DISTINCT c5, c5) AS c5, ARG_MAX(DISTINCT c6, c6) AS c6, ARG_MAX(DISTINCT c7, c7) AS c7, ARG_MAX(DISTINCT c8, c8) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_arg_max_distinct_gby_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": None,
                "c4": 2,
                "c5": 5,
                "c6": 6,
                "c7": None,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": None,
                "c2": 3,
                "c3": 4,
                "c4": 2,
                "c5": 2,
                "c6": 3,
                "c7": None,
                "c8": 2,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max_distinct_gby_diff AS SELECT
                      id, ARG_MAX(DISTINCT c1, c2) AS c1, ARG_MAX(DISTINCT c2, c1) AS c2, ARG_MAX(DISTINCT c3, c4) AS c3, ARG_MAX(DISTINCT c4, c3) AS c4, ARG_MAX(DISTINCT c5, c6) AS c5, ARG_MAX(DISTINCT c6, c5) AS c6, ARG_MAX(DISTINCT c7, c8) AS c7, ARG_MAX(DISTINCT c8, c7) AS c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_arg_max_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "f_c1": 5,
                "f_c2": 5,
                "f_c3": 6,
                "f_c4": 4,
                "f_c5": 5,
                "f_c6": 6,
                "f_c7": 3,
                "f_c8": 8,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max_where AS SELECT
                      ARG_MAX(c1, c1) FILTER(WHERE c8>2) AS f_c1, ARG_MAX(c2, c2) FILTER(WHERE c8>2) AS f_c2, ARG_MAX(c3, c3) FILTER(WHERE c8>2) AS f_c3, ARG_MAX(c4, c4) FILTER(WHERE c8>2) AS f_c4, ARG_MAX(c5, c5) FILTER(WHERE c8>2) AS f_c5, ARG_MAX(c6, c6) FILTER(WHERE c8>2) AS f_c6, ARG_MAX(c7, c7) FILTER(WHERE c8>2) AS f_c7, ARG_MAX(c8, c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl"""


class aggtst_int_arg_max_where_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "f_c1": None,
                "f_c2": 2,
                "f_c3": None,
                "f_c4": 2,
                "f_c5": 5,
                "f_c6": 6,
                "f_c7": None,
                "f_c8": 3,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max_where_diff AS SELECT
                      ARG_MAX(c1, c2) FILTER(WHERE c8>2) AS f_c1, ARG_MAX(c2, c1) FILTER(WHERE c8>2) AS f_c2, ARG_MAX(c3, c4) FILTER(WHERE c8>2) AS f_c3, ARG_MAX(c4, c3) FILTER(WHERE c8>2) AS f_c4, ARG_MAX(c5, c6) FILTER(WHERE c8>2) AS f_c5, ARG_MAX(c6, c5) FILTER(WHERE c8>2) AS f_c6, ARG_MAX(c7, c8) FILTER(WHERE c8>2) AS f_c7, ARG_MAX(c8, c7) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl"""


class aggtst_int_arg_max_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "f_c1": 5,
                "f_c2": 2,
                "f_c3": 3,
                "f_c4": 4,
                "f_c5": 5,
                "f_c6": 6,
                "f_c7": 3,
                "f_c8": 8,
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
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max_where_gby AS SELECT
                      id, ARG_MAX(c1, c1) FILTER(WHERE c8>2) AS f_c1, ARG_MAX(c2, c2) FILTER(WHERE c8>2) AS f_c2, ARG_MAX(c3, c3) FILTER(WHERE c8>2) AS f_c3, ARG_MAX(c4, c4) FILTER(WHERE c8>2) AS f_c4, ARG_MAX(c5, c5) FILTER(WHERE c8>2) AS f_c5, ARG_MAX(c6, c6) FILTER(WHERE c8>2) AS f_c6, ARG_MAX(c7, c7) FILTER(WHERE c8>2) AS f_c7, ARG_MAX(c8, c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""


class aggtst_int_arg_max_where_diff1(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "f_c1": 7,
                "f_c2": -3,
                "f_c3": 1,
                "f_c4": 2,
                "f_c5": 30,
                "f_c6": 1,
                "f_c7": 6,
                "f_c8": 0,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max_where_diff1 AS SELECT
                      ARG_MAX(c1+c2, c2-c1) FILTER(WHERE c8>2) AS f_c1,
                      ARG_MAX(c2-c1, c1+c2) FILTER(WHERE c8>2) AS f_c2,
                      ARG_MAX(c3%c4, c4%c3) FILTER(WHERE c8>2) AS f_c3,
                      ARG_MAX(c4%c3, c3%c4) FILTER(WHERE c8>2) AS f_c4,
                      ARG_MAX(c5*c6, c6/c5) FILTER(WHERE c8>2) AS f_c5,
                      ARG_MAX(c6/c5, c5*c6) FILTER(WHERE c8>2) AS f_c6,
                      ARG_MAX(c7+c8, c8-c7) FILTER(WHERE c8>2) AS f_c7,
                      ARG_MAX(c8-c7, c7+c8) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl"""


class aggtst_int_arg_max_where_diff_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "f_c1": 5,
                "f_c2": 2,
                "f_c3": None,
                "f_c4": 2,
                "f_c5": 5,
                "f_c6": 6,
                "f_c7": None,
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
        self.sql = """CREATE MATERIALIZED VIEW int_arg_max_where_diff_gby AS SELECT
                      id, ARG_MAX(c1, c2) FILTER(WHERE c8>2) AS f_c1, ARG_MAX(c2, c1) FILTER(WHERE c8>2) AS f_c2, ARG_MAX(c3, c4) FILTER(WHERE c8>2) AS f_c3, ARG_MAX(c4, c3) FILTER(WHERE c8>2) AS f_c4, ARG_MAX(c5, c6) FILTER(WHERE c8>2) AS f_c5, ARG_MAX(c6, c5) FILTER(WHERE c8>2) AS f_c6, ARG_MAX(c7, c8) FILTER(WHERE c8>2) AS f_c7, ARG_MAX(c8, c7) FILTER(WHERE c8>2) AS f_c8
                      FROM int0_tbl
                      GROUP BY id"""
