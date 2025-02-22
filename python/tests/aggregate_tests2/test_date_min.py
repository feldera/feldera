from tests.aggregate_tests.aggtst_base import TstView


class aggtst_date_min_value(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "1969-03-17", "c2": "2014-11-05"}]
        self.sql = """CREATE MATERIALIZED VIEW date_min AS SELECT
                      MIN(c1) AS c1, MIN(c2) AS c2
                      FROM date_tbl"""


class aggtst_date_min_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "2014-11-05", "c2": "2024-12-05"},
            {"id": 1, "c1": "1969-03-17", "c2": "2014-11-05"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_min_gby AS SELECT
                      id, MIN(c1) AS c1, MIN(c2) AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_min_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"c1": "1969-03-17", "c2": "2014-11-05"}]
        self.sql = """CREATE MATERIALIZED VIEW date_min_distinct AS SELECT
                      MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM date_tbl"""


class aggtst_date_min_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "2014-11-05", "c2": "2024-12-05"},
            {"id": 1, "c1": "1969-03-17", "c2": "2014-11-05"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_min_distinct_gby AS SELECT
                      id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2
                      FROM date_tbl
                      GROUP BY id"""


class aggtst_date_min_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{"f_c1": "2020-06-21", "f_c2": "2014-11-05"}]
        self.sql = """CREATE MATERIALIZED VIEW date_min_where AS SELECT
                      MIN(c1) FILTER (WHERE c1 > '2014-11-05') AS f_c1, MIN(c2) FILTER (WHERE c1 > '2014-11-05') AS f_c2
                      FROM date_tbl"""


class aggtst_date_min_where_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "f_c1": "2020-06-21", "f_c2": None},
            {"id": 1, "f_c1": "2020-06-21", "f_c2": "2014-11-05"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_min_where_gby AS SELECT
                      id, MIN(c1) FILTER (WHERE c1 > '2014-11-05') AS f_c1, MIN(c2) FILTER (WHERE c1 > '2014-11-05') AS f_c2
                      FROM date_tbl
                      GROUP BY id"""
