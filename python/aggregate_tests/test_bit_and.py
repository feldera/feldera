from .test_base import TestView

class test_bit_and(TestView):
    def __init__(self):
        self.data = [{'c1': 4, 'c2': 2, 'c3': 4, 'c4': 4, 'c5': 0, 'c6': 2, 'c7': 4, 'c8': 0}]
        self.sql = '''CREATE VIEW bit_and AS SELECT
                      BIT_AND(c1) AS c1, BIT_AND(c2) AS c2, BIT_AND(c3) AS c3, BIT_AND(c4) AS c4, BIT_AND(c5) AS c5, BIT_AND(c6) AS c6, BIT_AND(c7) AS c7, BIT_AND(c8) AS c8
                      FROM bit_table'''

class test_bit_and_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data =  [{'id': 0, 'c1': 5, 'c2': 0, 'c3': 10, 'c4': 0, 'c5': 0, 'c6': 2, 'c7': 20, 'c8': 0},
                      {'id': 1, 'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2}]
        self.sql = '''CREATE VIEW bit_and_groupby AS SELECT
                      id, BIT_AND(c1) AS c1, BIT_AND(c2) AS c2, BIT_AND(c3) AS c3, BIT_AND(c4) AS c4, BIT_AND(c5) AS c5, BIT_AND(c6) AS c6, BIT_AND(c7) AS c7, BIT_AND(c8) AS c8
                      FROM bit_table0
                      GROUP BY id'''

class test_bit_and_distinct(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 4, 'c2': 2, 'c3': 4, 'c4': 4, 'c5': 0, 'c6': 2, 'c7': 4, 'c8': 0}]
        self.sql = '''CREATE VIEW bit_and_distinct AS SELECT
                      BIT_AND(DISTINCT c1) AS c1, BIT_AND(DISTINCT c2) AS c2, BIT_AND(DISTINCT c3) AS c3, BIT_AND(DISTINCT c4) AS c4, BIT_AND(DISTINCT c5) AS c5, BIT_AND(DISTINCT c6) AS c6, BIT_AND(DISTINCT c7) AS c7, BIT_AND(DISTINCT c8) AS c8
                      FROM bit_table'''

class test_bit_and_distinct_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 4, 'c2': 2, 'c3': 30, 'c4': 4, 'c5': 5, 'c6': 4, 'c7': 70, 'c8': 0},
                     {'id': 1, 'c1': 4, 'c2': 3, 'c3': 4, 'c4': 0, 'c5': 2, 'c6': 2, 'c7': 0, 'c8': 2}]
        self.sql = '''CREATE VIEW bit_and_distinct_groupby AS SELECT
                      id, BIT_AND(DISTINCT c1) AS c1, BIT_AND(DISTINCT c2) AS c2, BIT_AND(DISTINCT c3) AS c3, BIT_AND(DISTINCT c4) AS c4, BIT_AND(DISTINCT c5) AS c5, BIT_AND(DISTINCT c6) AS c6, BIT_AND(DISTINCT c7) AS c7, BIT_AND(DISTINCT c8) AS c8
                      FROM bit_table1
                      GROUP BY id'''

class test_bit_and_where(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2},
                     {'c1': 5, 'c2': 2, 'c3': None, 'c4': 4, 'c5': 5, 'c6': 6, 'c7': None, 'c8': 8}]
        self.sql = '''CREATE VIEW bit_and_where AS
                       WITH bit_and_val AS(
                       SELECT BIT_AND(c1) AS bit_and_c1, BIT_AND(c2) AS bit_and_c2, BIT_AND(c3) AS bit_and_c3, BIT_AND(c4) AS bit_and_c4, BIT_AND(c5) AS bit_and_c5, BIT_AND(c6) AS bit_and_c6, BIT_AND(c7) AS bit_and_c7, BIT_AND(c8) AS bit_and_c8
                       FROM bit_table)
                       SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                       FROM bit_table t
                       WHERE (t.c8) > (SELECT bit_and_c8 FROM bit_and_val)'''

class test_bit_and_where_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'bit_and_c1': 5, 'bit_and_c2': 0, 'bit_and_c3': 10, 'bit_and_c4': 0, 'bit_and_c5': 0, 'bit_and_c6': 2, 'bit_and_c7': 20, 'bit_and_c8': 0},
                     {'id': 1, 'bit_and_c1': 4, 'bit_and_c2': 3, 'bit_and_c3': 4, 'bit_and_c4': 6, 'bit_and_c5': 2, 'bit_and_c6': 3, 'bit_and_c7': 4, 'bit_and_c8': 2}]
        self.sql = '''CREATE VIEW bit_and_where_groupby AS
                      WITH bit_and_val AS (
                           SELECT BIT_AND(c1) AS bit_and_c1, BIT_AND(c2) AS bit_and_c2, BIT_AND(c3) AS bit_and_c3, BIT_AND(c4) AS bit_and_c4, BIT_AND(c5) AS bit_and_c5, BIT_AND(c6) AS bit_and_c6, BIT_AND(c7) AS bit_and_c7, BIT_AND(c8) AS bit_and_c8
                           FROM bit_table2)
                      SELECT t.id, BIT_AND(t.c1) AS bit_and_c1, BIT_AND(t.c2) AS bit_and_c2, BIT_AND(t.c3) AS bit_and_c3, BIT_AND(t.c4) AS bit_and_c4,  BIT_AND(t.c5) AS bit_and_c5, BIT_AND(t.c6) AS bit_and_c6, BIT_AND(t.c7) AS bit_and_c7, BIT_AND(t.c8) AS bit_and_c8
                      FROM bit_table2 t
                      WHERE (t.c8) > (SELECT bit_and_c8 FROM bit_and_val)
                      GROUP BY t.id'''

