from .test_base import TestView

class test_int_stddev_pop(TestView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'c1': 0, 'c2': 0, 'c3': 0, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': 0, 'c8': 3}]
        self.sql = '''CREATE VIEW int_stddev_pop AS SELECT
                      STDDEV_POP(c1) AS c1, STDDEV_POP(c2) AS c2, STDDEV_POP(c3) AS c3, STDDEV_POP(c4) AS c4, STDDEV_POP(c5) AS c5, STDDEV_POP(c6) AS c6, STDDEV_POP(c7) AS c7, STDDEV_POP(c8) AS c8
                      FROM stddev_tbl'''

class test_int_stddev_pop_groupby(TestView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'id': 0, 'c1': 0, 'c2': 0, 'c3': 0, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': 0, 'c8': 2},
                     {'id': 1, 'c1': 0, 'c2': 1, 'c3': 1, 'c4': 2, 'c5': 0, 'c6': 1, 'c7': 0, 'c8': 1}]
        self.sql = '''CREATE VIEW int_stddev_pop_gby AS SELECT
                      id, STDDEV_POP(c1) AS c1, STDDEV_POP(c2) AS c2, STDDEV_POP(c3) AS c3, STDDEV_POP(c4) AS c4, STDDEV_POP(c5) AS c5, STDDEV_POP(c6) AS c6, STDDEV_POP(c7) AS c7, STDDEV_POP(c8) AS c8
                      FROM int0_tbl
                      GROUP BY id'''

class test_int_stddev_pop_distinct(TestView):
    def __init__(self):
        # Validated on Postgres
        self.data =  [{'c1': 0, 'c2': 1, 'c3': 1, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': 0, 'c8': 2}]
        self.sql = '''CREATE VIEW int_stddev_pop_distinct AS SELECT
                      STDDEV_POP(DISTINCT c1) AS c1, STDDEV_POP(DISTINCT c2) AS c2, STDDEV_POP(DISTINCT c3) AS c3, STDDEV_POP(DISTINCT c4) AS c4, STDDEV_POP(DISTINCT c5) AS c5, STDDEV_POP(DISTINCT c6) AS c6, STDDEV_POP(DISTINCT c7) AS c7, STDDEV_POP(DISTINCT c8) AS c8
                      FROM int0_tbl'''

class test_int_stddev_pop_distinct_groupby(TestView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'id': 0, 'c1': 0, 'c2': 0, 'c3': 0, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': 0, 'c8': 2},
                     {'id': 1, 'c1': 0, 'c2': 1, 'c3': 1, 'c4': 2, 'c5': 0, 'c6': 1, 'c7': 0, 'c8': 1}]
        self.sql = '''CREATE VIEW int_stddev_pop_distinct_gby AS SELECT
                      id, STDDEV_POP(DISTINCT c1) AS c1, STDDEV_POP(DISTINCT c2) AS c2, STDDEV_POP(DISTINCT c3) AS c3, STDDEV_POP(DISTINCT c4) AS c4, STDDEV_POP(DISTINCT c5) AS c5, STDDEV_POP(DISTINCT c6) AS c6, STDDEV_POP(DISTINCT c7) AS c7, STDDEV_POP(DISTINCT c8) AS c8
                      FROM int0_tbl
                      GROUP BY id'''

class test_int_stddev_pop_where(TestView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'c1': 0, 'c2': 1, 'c3': 1, 'c4': 1, 'c5': 1, 'c6': 2, 'c7': 0, 'c8': 2}]
        self.sql = '''CREATE VIEW int_stddev_pop_where AS SELECT
                      STDDEV_POP(c1) FILTER (WHERE c8>2) AS c1, STDDEV_POP(c2) FILTER (WHERE c8>2) AS c2, STDDEV_POP(c3) FILTER (WHERE c8>2) AS c3, STDDEV_POP(c4) FILTER (WHERE c8>2) AS c4, STDDEV_POP(c5) FILTER (WHERE c8>2) AS c5, STDDEV_POP(c6) FILTER (WHERE c8>2) AS c6, STDDEV_POP(c7) FILTER (WHERE c8>2) AS c7, STDDEV_POP(c8) FILTER (WHERE c8>2) AS c8
                      FROM int0_tbl'''

class test_int_stddev_pop_where_groupby(TestView):
    def __init__(self):
        # Validated on Postgres
        self.data = [{'id': 0, 'c1': 0, 'c2': 0, 'c3': 0, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': 0, 'c8': 2},
                     {'id': 1, 'c1': None, 'c2': 0, 'c3': 0, 'c4': 0, 'c5': 0, 'c6': 0, 'c7': None, 'c8': 0}]
        self.sql = '''CREATE VIEW int_stddev_pop_where_gby AS SELECT
                      id, STDDEV_POP(c1) FILTER (WHERE c8>2) AS c1, STDDEV_POP(c2) FILTER (WHERE c8>2) AS c2, STDDEV_POP(c3) FILTER (WHERE c8>2) AS c3, STDDEV_POP(c4) FILTER (WHERE c8>2) AS c4, STDDEV_POP(c5) FILTER (WHERE c8>2) AS c5, STDDEV_POP(c6) FILTER (WHERE c8>2) AS c6, STDDEV_POP(c7) FILTER (WHERE c8>2) AS c7, STDDEV_POP(c8) FILTER (WHERE c8>2) AS c8
                      FROM int0_tbl
                      GROUP BY id'''