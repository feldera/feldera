from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_charn_argmin_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": None, "c2": "variabl"}]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmin AS SELECT
                      ARG_MIN(f_c1, f_c2) AS c1, ARG_MIN(f_c2, f_c1) AS c2
                      FROM atbl_charn"""


class aggtst_charn_argmin_value_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "hello  exampl ", "c2": "variabl@abc   "}]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmin_diff AS SELECT
                      ARG_MIN(f_c1||f_c2, f_c2||f_c1) AS c1, ARG_MIN(f_c2||f_c1, f_c1||f_c2) AS c2
                      FROM atbl_charn"""


class aggtst_charn_argmin_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None, "c2": "fred   "},
            {"id": 1, "c1": "hello  ", "c2": "variabl"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmin_gby AS SELECT
                      id, ARG_MIN(f_c1, f_c2) AS c1, ARG_MIN(f_c2, f_c1) AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_argmin_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": None, "c2": "variabl"}]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmin_distinct AS SELECT
                      ARG_MIN(DISTINCT f_c1, f_c2) AS c1, ARG_MIN(DISTINCT f_c2, f_c1) AS c2
                      FROM atbl_charn"""


class aggtst_charn_argmin_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None, "c2": "fred   "},
            {"id": 1, "c1": "hello  ", "c2": "variabl"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmin_distinct_gby AS SELECT
                      id, ARG_MIN(DISTINCT f_c1, f_c2) AS c1, ARG_MIN(DISTINCT f_c2, f_c1) AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_argmin_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "hello  ", "c2": "variabl"}]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmin_where AS SELECT
                      ARG_MIN(f_c1, f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c1, ARG_MIN(f_c2, f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c2
                      FROM atbl_charn"""


class aggtst_charn_argmin_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hello  ", "c2": "fred   "},
            {"id": 1, "c1": "hello  ", "c2": "variabl"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmin_where_gby AS SELECT
                      id, ARG_MIN(f_c1, f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c1, ARG_MIN(f_c2, f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_argmin_where_gby_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hello  fred   ", "c2": "fred   hello  "},
            {"id": 1, "c1": "hello  exampl ", "c2": "exampl hello  "},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmin_where_gby_diff AS SELECT
                      id, ARG_MIN(f_c1||f_c2, f_c2||f_c1) FILTER(WHERE f_c1 LIKE '%hello  %') AS c1, ARG_MIN(f_c2||f_c1, f_c1||f_c2) FILTER(WHERE f_c1 LIKE '%hello  %') AS c2
                      FROM atbl_charn
                      GROUP BY id"""
