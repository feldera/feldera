from test_base import TestView

class test_int_avg(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW int_avg AS SELECT
                      AVG(c1) AS c1, AVG(c2) AS c2
                      FROM int_tbl'''
        self.data = [{'c1': 6, 'c2': 14}]

class test_int_avg_gby(TestView):
    def __init__(self):
        self.sql = '''CREATE VIEW int_avg_gby AS SELECT
                      id, AVG(c1) AS c1, AVG(c2) AS c2
                      FROM int_tbl
                      GROUP BY id'''
        self.data = [{'id': 0, 'c1': 1, 'c2': 11},
                     {'id': 1, 'c1': 11, 'c2': 22}]
