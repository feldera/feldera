from test_base import TestTable, TestView
from decimal import Decimal

class test_avg_table(TestTable):
    def __init__(self):
        self.sql = '''CREATE TABLE agg_tbl(
                      id INT, c1 DECIMAL(6,2), c2 DECIMAL(6,2) NOT NULL
                      )'''
        self.data = [{"id": 0, "c1": 1111.52, "c2": 2231.90},
                     {"id": 0, "c1": None, "c2": 3802.71},
                     {"id": 1, "c1": 5681.08, "c2": 7689.88},
                     {"id": 1, "c1": 5681.08, "c2": 7335.88}]

class test_avg_value(TestView):
    def __init__(self):
        self.sql = '''CREATE VIEW avg_value AS
                      SELECT AVG(c1) AS c1, AVG(c2) AS c2
                      FROM agg_tbl'''
        self.data = [{'c1': Decimal('4157.89'), 'c2': Decimal('5265.09'), 'insert_delete': 1}]

class test_avg_gby(TestView):
    def __init__(self):
        self.sql = '''CREATE VIEW test_avg_gby AS SELECT
                      id, AVG(c1) AS c1, AVG(c2) AS c2
                      FROM agg_tbl
                      GROUP BY id'''
        self.data = [{'id': 0, 'c1': Decimal('1111.52'), 'c2': Decimal('3017.30'), 'insert_delete': 1},
                     {'id': 1, 'c1': Decimal('5681.08'), 'c2': Decimal('7512.88'), 'insert_delete': 1}]

