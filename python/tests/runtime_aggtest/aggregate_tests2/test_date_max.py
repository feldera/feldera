from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_date_max_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "2024-12-05", "c2": "2024-12-05"}]
        self.sql = """CREATE MATERIALIZED VIEW date_max AS SELECT
                      MAX(c1) AS c1, MAX(c2) AS c2
                      FROM date_tbl"""


class aggtst_date_max_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "2020-06-21", "c2": "2024-12-05"},
            {"id": 1, "c1": "2024-12-05", "c2": "2023-02-26"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_max_gby AS SELECT
                      id, MAX(c1) AS c1, MAX(c2) AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_max_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "2024-12-05", "c2": "2024-12-05"}]
        self.sql = """CREATE MATERIALIZED VIEW date_max_distinct AS SELECT
                      MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM date_tbl"""


class aggtst_date_max_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "2020-06-21", "c2": "2024-12-05"},
            {"id": 1, "c1": "2024-12-05", "c2": "2023-02-26"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_max_distinct_gby AS SELECT
                      id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_max_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": "2024-12-05", "f_c2": "2023-02-26"}]
        self.sql = """CREATE MATERIALIZED VIEW date_max_where AS SELECT
                      MAX(c1) FILTER (WHERE c1 > '2014-11-05') AS f_c1, MAX(c2) FILTER (WHERE c1 > '2014-11-05') AS f_c2
                      FROM date_tbl"""


class aggtst_date_max_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": "2020-06-21", "f_c2": None},
            {"id": 1, "f_c1": "2024-12-05", "f_c2": "2023-02-26"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_max_where_gby AS SELECT
                      id, MAX(c1) FILTER (WHERE c1 > '2014-11-05') AS f_c1, MAX(c2) FILTER (WHERE c1 > '2014-11-05') AS f_c2
                      FROM date_tbl
                      GROUP BY id"""
