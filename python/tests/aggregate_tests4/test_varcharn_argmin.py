from tests.aggregate_tests.aggtst_base import TstView


class aggtst_varcharn_argmin_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": None, "c2": "varia"}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_argmin AS SELECT
                      ARG_MIN(f_c1, f_c2) AS c1, ARG_MIN(f_c2, f_c1) AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_argmin_value_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "helloexamp", "c2": "varia@abc"}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_argmin_diff AS SELECT
                      ARG_MIN(f_c1||f_c2, f_c2||f_c1) AS c1, ARG_MIN(f_c2||f_c1, f_c1||f_c2) AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_argmin_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None, "c2": "fred"},
            {"id": 1, "c1": "hello", "c2": "varia"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_argmin_gby AS SELECT
                      id, ARG_MIN(f_c1, f_c2) AS c1, ARG_MIN(f_c2, f_c1) AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""


class aggtst_varcharn_argmin_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": None, "c2": "varia"}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_argmin_distinct AS SELECT
                      ARG_MIN(DISTINCT f_c1, f_c2) AS c1, ARG_MIN(DISTINCT f_c2, f_c1) AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_argmin_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None, "c2": "fred"},
            {"id": 1, "c1": "hello", "c2": "varia"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_argmin_distinct_gby AS SELECT
                      id, ARG_MIN(DISTINCT f_c1, f_c2) AS c1, ARG_MIN(DISTINCT f_c2, f_c1) AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""


class aggtst_varcharn_argmin_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": "hello", "c2": "examp"}]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_argmin_where AS SELECT
                      ARG_MIN(f_c1, f_c2) FILTER(WHERE len(f_c1)>4) AS c1, ARG_MIN(f_c2, f_c1) FILTER(WHERE len(f_c1)>4) AS c2
                      FROM atbl_varcharn"""


class aggtst_varcharn_argmin_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hello", "c2": "fred"},
            {"id": 1, "c1": "hello", "c2": "examp"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_argmin_where_gby AS SELECT
                      id, ARG_MIN(f_c1, f_c2) FILTER(WHERE len(f_c1)>4) AS c1, ARG_MIN(f_c2, f_c1) FILTER(WHERE len(f_c1)>4) AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""


class aggtst_varcharn_argmin_where_gby_diff(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": "hellofred", "c2": "fredhello"},
            {"id": 1, "c1": "helloexamp", "c2": "examphello"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW varcharn_argmin_where_gby_diff AS SELECT
                      id, ARG_MIN(f_c1||f_c2, f_c2||f_c1) FILTER(WHERE len(f_c1)>4) AS c1, ARG_MIN(f_c2||f_c1, f_c1||f_c2) FILTER(WHERE len(f_c1)>4) AS c2
                      FROM atbl_varcharn
                      GROUP BY id"""
