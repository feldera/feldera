from .aggtst_base import TstView


class aggtst_varchar_every(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'c1': True, 'c2': False}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_every AS SELECT
                      EVERY(c1 != '%hello%') AS c1, EVERY(c2 LIKE '%a%') AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_every_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'id': 0, 'c1': True, 'c2': False}, {'id': 1, 'c1': True, 'c2': True}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_every_gby AS SELECT
                      id, EVERY(c1 != '%hello%') AS c1, EVERY(c2 LIKE '%a%') AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_every_distinct(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'c1': True, 'c2': False}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_every_distinct AS SELECT
                      EVERY(DISTINCT c1 != '%hello%') AS c1, EVERY(DISTINCT c2 LIKE '%a%') AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_every_distinct_gby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'id': 0, 'c1': True, 'c2': False}, {'id': 1, 'c1': True, 'c2': True}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_every_distinct_gby AS SELECT
                      id, EVERY(DISTINCT c1 != '%hello%') AS c1, EVERY(DISTINCT c2 LIKE '%a%') AS c2
                      FROM varchar_tbl
                      GROUP BY id"""


class aggtst_varchar_every_where(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'c1': True, 'c2': False}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_every_where AS SELECT
                      EVERY(c1 != '%hello%') FILTER(WHERE len(c2)=4) AS c1, EVERY(c2 LIKE '%a%') FILTER(WHERE len(c2)=4) AS c2
                      FROM varchar_tbl"""


class aggtst_varchar_every_where_groupby(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'id': 0, 'c1': True, 'c2': False}, {'id': 1, 'c1': None, 'c2': None}]
        self.sql = """CREATE MATERIALIZED VIEW varchar_every_where_gby AS SELECT
                      id, EVERY(c1 != '%hello%') FILTER(WHERE len(c2)=4) AS c1, EVERY(c2 LIKE '%a%') FILTER(WHERE len(c2)=4) AS c2
                      FROM varchar_tbl
                      GROUP BY id"""