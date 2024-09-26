from .test_base import TestTable, TestView

class test_bit_OR(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 5, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 7, 'c6': 7, 'c7': 4, 'c8': 10}]
        self.sql = '''CREATE VIEW bit_or_view AS SELECT
                      BIT_OR(c1) AS c1, BIT_OR(c2) AS c2, BIT_OR(c3) AS c3, BIT_OR(c4) AS c4, BIT_OR(c5) AS c5, BIT_OR(c6) AS c6, BIT_OR(c7) AS c7, BIT_OR(c8) AS c8
                      FROM bit_table'''

class test_bit_or_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 5, 'c2': 11, 'c3': 10, 'c4': 22, 'c5': 13, 'c6': 14, 'c7': 20, 'c8': 13},
                     {'id': 1, 'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2}]
        self.sql = '''CREATE VIEW bit_or_gbt AS
                      SELECT id, BIT_OR(c1) AS c1, BIT_OR(c2) AS c2, BIT_OR(c3) AS c3, BIT_OR(c4) AS c4, BIT_OR(c5) AS c5, BIT_OR(c6) AS c6, BIT_OR(c7) AS c7, BIT_OR(c8) AS c8
                      FROM bit_table0
                      GROUP BY id'''

class test_bit_or_distinct(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 5, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 7, 'c6': 7, 'c7': 4, 'c8': 10}]
        self.sql = '''CREATE VIEW bit_or_distinct AS SELECT
                      BIT_OR(DISTINCT c1) AS c1, BIT_OR(DISTINCT c2) AS c2, BIT_OR(DISTINCT c3) AS c3, BIT_OR(DISTINCT c4) AS c4, BIT_OR(DISTINCT c5) AS c5, BIT_OR(DISTINCT c6) AS c6, BIT_OR(DISTINCT c7) AS c7, BIT_OR(DISTINCT c8) AS c8
                      FROM bit_table'''

class test_bit_or_distinct_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 5, 'c2': 2, 'c3': 30, 'c4': 14, 'c5': 5, 'c6': 62, 'c7': 70, 'c8': 26},
                     {'id': 1, 'c1': 5, 'c2': 3, 'c3': 4, 'c4': 15, 'c5': 51, 'c6': 7, 'c7': 76, 'c8': 2}]
        self.sql = '''CREATE VIEW bit_or_distinct_gby AS
                      SELECT id, BIT_OR(DISTINCT c1) AS c1, BIT_OR(DISTINCT c2) AS c2, BIT_OR(DISTINCT c3) AS c3, BIT_OR(DISTINCT c4) AS c4, BIT_OR(DISTINCT c5) AS c5, BIT_OR(DISTINCT c6) AS c6, BIT_OR(DISTINCT c7) AS c7, BIT_OR(DISTINCT c8) AS c8
                      FROM bit_table1
                      GROUP BY id'''

class test_bit_or_where(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 4, 'c2': 3, 'c3': 4.0, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4.0, 'c8': 2},
                     {'c1': 5, 'c2': 2, 'c3': None, 'c4': 4, 'c5': 5, 'c6': 6, 'c7': None, 'c8': 8}]
        self.sql = '''CREATE VIEW bit_or_where AS
                      WITH bit_or_val AS(
                      SELECT BIT_OR(c1) AS bit_or_c1, BIT_OR(c2) AS bit_or_c2, BIT_OR(c3) AS bit_or_c3, BIT_OR(c4) AS bit_or_c4, BIT_OR(c5) AS bit_or_c5, BIT_OR(c6) AS bit_or_c6, BIT_OR(c7) AS bit_or_c7, BIT_OR(c8) AS bit_or_c8
                      FROM bit_table)
                      SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                      FROM bit_table t
                      WHERE (t.c8) < (SELECT bit_or_c8 FROM bit_or_val)'''

class test_bit_or_where_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'bit_or_c1': 5, 'bit_or_c2': 11, 'bit_or_c3': 10, 'bit_or_c4': 22, 'bit_or_c5': 13, 'bit_or_c6': 14, 'bit_or_c7': 20, 'bit_or_c8': 13},
                     {'id': 1, 'bit_or_c1': 4, 'bit_or_c2': 3, 'bit_or_c3': 4, 'bit_or_c4': 6, 'bit_or_c5': 2, 'bit_or_c6': 3, 'bit_or_c7': 4, 'bit_or_c8': 2}]
        self.sql =  '''CREATE VIEW bit_or_where_gby AS
                       WITH bit_or_val AS (
                            SELECT BIT_OR(c1) AS bit_or_c1, BIT_OR(c2) AS bit_or_c2, BIT_OR(c3) AS bit_or_c3, BIT_OR(c4) AS bit_or_c4, BIT_OR(c5) AS bit_or_c5, BIT_OR(c6) AS bit_or_c6, BIT_OR(c7) AS bit_or_c7, BIT_OR(c8) AS bit_or_c8
                            FROM bit_table0)
                       SELECT t.id, BIT_OR(t.c1) AS bit_or_c1, BIT_OR(t.c2) AS bit_or_c2, BIT_OR(t.c3) AS bit_or_c3, BIT_OR(t.c4) AS bit_or_c4,  BIT_OR(t.c5) AS bit_or_c5, BIT_OR(t.c6) AS bit_or_c6, BIT_OR(t.c7) AS bit_or_c7, BIT_OR(t.c8) AS bit_or_c8
                       FROM bit_table0 t
                       WHERE (t.c8) < (SELECT bit_or_c8 FROM bit_or_val)
                       GROUP BY t.id'''
