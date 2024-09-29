from .test_base import TestView

class test_int_every(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW int_every_value AS SELECT
                      EVERY(c1 = 4) AS c1, EVERY(c2 > 1) AS c2, EVERY(c3>3) AS c3, EVERY(c4>1) AS c4, EVERY(c5>1) AS c5, EVERY(c6 % 2 = 1) AS c6, EVERY(c7>2) AS c7, EVERY(c8>2) AS c8
                      FROM int0_tbl'''
        self.data = [{'c1': False, 'c2': True, 'c3': False, 'c4': True, 'c5': True, 'c6': False, 'c7': True, 'c8': False}]

class test_int_every_gby(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW int_every_gby AS SELECT
                      id, EVERY(c1 = 4) AS c1, EVERY(c2 > 1) AS c2, EVERY(c3>3) AS c3, EVERY(c4>1) AS c4, EVERY(c5>1) AS c5, EVERY(c6 % 2 = 1) AS c6, EVERY(c7>2) AS c7, EVERY(c8>2) AS c8
                      FROM int0_tbl
                      GROUP BY id'''
        self.data = [{'id': 0, 'c1': False, 'c2': True, 'c3': False, 'c4': True, 'c5': True, 'c6': False, 'c7': True, 'c8': True},
                     {'id': 1, 'c1': True, 'c2': True, 'c3': True, 'c4': True, 'c5': True, 'c6': True, 'c7': True, 'c8': False}]

class test_int_every_distinct(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW int_every_distinct AS SELECT
                      EVERY(DISTINCT(c1 = 4)) AS c1, EVERY(DISTINCT(c2 > 1)) AS c2, EVERY(DISTINCT(c3>3)) AS c3, EVERY(DISTINCT(c4>1)) AS c4, EVERY(DISTINCT(c5>1)) AS c5, EVERY(DISTINCT(c6 % 2 = 1)) AS c6, EVERY(DISTINCT(c7>2)) AS c7, EVERY(DISTINCT(c8>2)) AS c8
                      FROM int0_tbl'''
        self.data = [{'c1': False, 'c2': True, 'c3': False, 'c4': True, 'c5': True, 'c6': False, 'c7': True, 'c8': False}]

class test_int_every_distinct_gby(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW int_every_distinct_gby AS SELECT
                      id, EVERY(DISTINCT(c1 = 4)) AS c1, EVERY(DISTINCT(c2 > 1)) AS c2, EVERY(DISTINCT(c3>3)) AS c3, EVERY(DISTINCT(c4>1)) AS c4, EVERY(DISTINCT(c5>1)) AS c5, EVERY(DISTINCT(c6 % 2 = 1)) AS c6, EVERY(DISTINCT(c7>2)) AS c7, EVERY(DISTINCT(c8>2)) AS c8
                      FROM int0_tbl
                      GROUP BY id'''
        self.data = [{'id': 0, 'c1': False, 'c2': True, 'c3': False, 'c4': True, 'c5': True, 'c6': False, 'c7': True, 'c8': True},
                     {'id': 1, 'c1': True, 'c2': True, 'c3': True, 'c4': True, 'c5': True, 'c6': True, 'c7': True, 'c8': False}]

class test_int_every_where(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW int_every_where AS SELECT
                      EVERY(c1 = 4) FILTER (WHERE c1 > 0) AS c1, EVERY(c2 > 1) FILTER (WHERE c1 > 0) AS c2, EVERY(c3 > 3) FILTER (WHERE c1 > 0) AS c3, EVERY(c4 > 1) FILTER (WHERE c1 > 0) AS c4, EVERY(c5 > 1) FILTER (WHERE c1 > 0) AS c5, EVERY(c6 % 2 = 1) FILTER (WHERE c1 > 0) AS c6, EVERY(c7 > 2) FILTER (WHERE c1 > 0) AS c7, EVERY(c8 > 2) FILTER (WHERE c1 > 0) AS c8
                      FROM int0_tbl'''
        self.data = [{'c1': False, 'c2': True, 'c3': True, 'c4': True, 'c5': True, 'c6': False, 'c7': True, 'c8': False}]

class test_int_every_where_gby(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW int_every_where_gby AS SELECT
                      id, EVERY(c1 = 4) FILTER (WHERE c1 > 0) AS c1, EVERY(c2 > 1) FILTER (WHERE c1 > 0) AS c2, EVERY(c3 > 3) FILTER (WHERE c1 > 0) AS c3, EVERY(c4 > 1) FILTER (WHERE c1 > 0) AS c4, EVERY(c5 > 1) FILTER (WHERE c1 > 0) AS c5, EVERY(c6 % 2 = 1) FILTER (WHERE c1 > 0) AS c6, EVERY(c7 > 2) FILTER (WHERE c1 > 0) AS c7, EVERY(c8 > 2) FILTER (WHERE c1 > 0) AS c8
                      FROM int0_tbl
                      GROUP BY id'''
        self.data = [{'id': 0, 'c1': False, 'c2': True, 'c3': None, 'c4': True, 'c5': True, 'c6': False, 'c7': None, 'c8': True},
                     {'id': 1, 'c1': True, 'c2': True, 'c3': True, 'c4': True, 'c5': True, 'c6': True, 'c7': True, 'c8': False}]