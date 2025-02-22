from tests.aggregate_tests.aggtst_base import TstView


class aggtst_charn_argmax_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "@abc   ", "c2": "fred   "}]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmax AS SELECT
                      ARG_MAX(f_c1, f_c2) AS c1, ARG_MAX(f_c2, f_c1) AS c2
                      FROM atbl_charn"""


class aggtst_charn_argmax_value_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "@abc   variabl", "c2": "fred   hello  "}]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmax_diff AS SELECT
                      ARG_MAX(f_c1||f_c2, f_c2||f_c1) AS c1, ARG_MAX(f_c2||f_c1, f_c1||f_c2) AS c2
                      FROM atbl_charn"""


class aggtst_charn_argmax_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hello  ", "c2": "fred   "},
            {"id": 1, "c1": "@abc   ", "c2": "exampl "},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmax_gby AS SELECT
                      id, ARG_MAX(f_c1, f_c2) AS c1, ARG_MAX(f_c2, f_c1) AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_argmax_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "@abc   ", "c2": "fred   "}]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmax_distinct AS SELECT
                      ARG_MAX(DISTINCT f_c1, f_c2) AS c1, ARG_MAX(DISTINCT f_c2, f_c1) AS c2
                      FROM atbl_charn"""


class aggtst_charn_argmax_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hello  ", "c2": "fred   "},
            {"id": 1, "c1": "@abc   ", "c2": "exampl "},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmax_distinct_gby AS SELECT
                      id, ARG_MAX(DISTINCT f_c1, f_c2) AS c1, ARG_MAX(DISTINCT f_c2, f_c1) AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_argmax_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "@abc   ", "c2": "fred   "}]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmax_where AS SELECT
                      ARG_MAX(f_c1, f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c1, ARG_MAX(f_c2, f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c2
                      FROM atbl_charn"""


class aggtst_charn_argmax_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hello  ", "c2": "fred   "},
            {"id": 1, "c1": "@abc   ", "c2": "exampl "},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmax_where_gby AS SELECT
                      id, ARG_MAX(f_c1, f_c2) FILTER(WHERE f_c1 != '%hello  %') AS c1, ARG_MAX(f_c2, f_c1) FILTER(WHERE f_c1 != '%hello  %') AS c2
                      FROM atbl_charn
                      GROUP BY id"""


class aggtst_charn_argmax_where_gby_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hello  fred   ", "c2": "fred   hello  "},
            {"id": 1, "c1": "hello  exampl ", "c2": "exampl hello  "},
        ]
        self.sql = """CREATE MATERIALIZED VIEW charn_argmax_where_gby_diff AS SELECT
                      id, ARG_MAX(f_c1||f_c2, f_c2||f_c1) FILTER(WHERE f_c1 LIKE '%hello  %') AS c1, ARG_MAX(f_c2||f_c1, f_c1||f_c2) FILTER(WHERE f_c1 LIKE '%hello  %') AS c2
                      FROM atbl_charn
                      GROUP BY id"""
