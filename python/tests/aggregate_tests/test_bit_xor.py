from .test_base import TestTable, TestView

class test_bit_xor(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 1, 'c2': 1, 'c3': 4, 'c4': 2, 'c5': 7, 'c6': 5, 'c7': 4, 'c8': 10}]
        self.sql = '''CREATE VIEW bit_xor_view AS SELECT
                      BIT_XOR(c1) AS c1, BIT_XOR(c2) AS c2, BIT_XOR(c3) AS c3, BIT_XOR(c4) AS c4, BIT_XOR(c5) AS c5, BIT_XOR(c6) AS c6, BIT_XOR(c7) AS c7, BIT_XOR(c8) AS c8
                      FROM bit_table'''

class test_bit_xor_Groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 0, 'c2': 11, 'c3': 10, 'c4': 22, 'c5': 13, 'c6': 12, 'c7': 20, 'c8': 13},
                     {'id': 1, 'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2}]
        self.sql = '''CREATE VIEW bit_xor_gby AS SELECT
                      id, BIT_XOR(c1) AS c1, BIT_XOR(c2) AS c2, BIT_XOR(c3) AS c3, BIT_XOR(c4) AS c4, BIT_XOR(c5) AS c5, BIT_XOR(c6) AS c6, BIT_XOR(c7) AS c7, BIT_XOR(c8) AS c8
                      FROM bit_table0
                      GROUP BY id'''

class test_bit_xor_distinct(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 1, 'c2': 1, 'c3': 4, 'c4': 2, 'c5': 7, 'c6': 5, 'c7': 4, 'c8': 10}]
        table_name = "bit_xor_distinct"
        self.sql = '''CREATE VIEW bit_xor_distinct AS SELECT
                      BIT_XOR(DISTINCT c1) AS c1, BIT_XOR(DISTINCT c2) AS c2, BIT_XOR(DISTINCT c3) AS c3, BIT_XOR(DISTINCT c4) AS c4, BIT_XOR(DISTINCT c5) AS c5, BIT_XOR(DISTINCT c6) AS c6, BIT_XOR(DISTINCT c7) AS c7, BIT_XOR(DISTINCT c8) AS c8
                      FROM bit_table'''

class test_bit_xor_distinct_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 1, 'c2': 2, 'c3': 30, 'c4': 10, 'c5': 5, 'c6': 58, 'c7': 70, 'c8': 26},
                     {'id': 1, 'c1': 1, 'c2': 3, 'c3': 4, 'c4': 15, 'c5': 49, 'c6': 5, 'c7': 76, 'c8': 2}]
        self.sql = '''CREATE VIEW bit_xor_distinct_gby AS SELECT
                      id, BIT_XOR(DISTINCT c1) AS c1, BIT_XOR(DISTINCT c2) AS c2, BIT_XOR(DISTINCT c3) AS c3, BIT_XOR(DISTINCT c4) AS c4, BIT_XOR(DISTINCT c5) AS c5, BIT_XOR(DISTINCT c6) AS c6, BIT_XOR(DISTINCT c7) AS c7, BIT_XOR(DISTINCT c8) AS c8
                      FROM bit_table1
                      GROUP BY id'''

class test_bit_xor_where(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 4, 'c2': 3, 'c3': 4.0, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4.0, 'c8': 2},
                     {'c1': 5, 'c2': 2, 'c3': None, 'c4': 4, 'c5': 5, 'c6': 6, 'c7': None, 'c8': 8}]
        self.sql = '''CREATE VIEW bit_xor_where AS
                      WITH bit_xor_val AS(
                            SELECT
                                BIT_XOR(c1) AS bit_xor_c1, BIT_XOR(c2) AS bit_xor_c2, BIT_XOR(c3) AS bit_xor_c3, BIT_XOR(c4) AS bit_xor_c4, BIT_XOR(c5) AS bit_xor_c5, BIT_XOR(c6) AS bit_xor_c6, BIT_XOR(c7) AS bit_xor_c7, BIT_XOR(c8) AS bit_xor_c8
                            FROM bit_table)
                      SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                      FROM bit_table t
                      WHERE (t.c8) < (SELECT bit_xor_c8 FROM bit_xor_val)'''

class test_bit_xor_where_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data =[{'id': 0, 'bit_xor_c1': 0, 'bit_xor_c2': 11, 'bit_xor_c3': 10, 'bit_xor_c4': 22, 'bit_xor_c5': 13, 'bit_or_c6': 12, 'bit_xor_c7': 20, 'bit_xor_c8': 13},
                    {'id': 1, 'bit_xor_c1': 4, 'bit_xor_c2': 3, 'bit_xor_c3': 4, 'bit_xor_c4': 6, 'bit_xor_c5': 2, 'bit_or_c6': 3, 'bit_xor_c7': 4, 'bit_xor_c8': 2}]
        self.sql = '''CREATE VIEW bit_xor_where_gby AS
                      WITH bit_xor_val AS (
                            SELECT BIT_XOR(c1) AS bit_xor_c1, BIT_XOR(c2) AS bit_xor_c2, BIT_XOR(c3) AS bit_xor_c3, BIT_XOR(c4) AS bit_xor_c4, BIT_XOR(c5) AS bit_xor_c5, BIT_XOR(c6) AS bit_xor_c6, BIT_XOR(c7) AS bit_xor_c7, BIT_XOR(c8) AS bit_xor_c8
                            FROM bit_table0)
                      SELECT t.id, BIT_XOR(t.c1) AS bit_xor_c1, BIT_XOR(t.c2) AS bit_xor_c2, BIT_XOR(t.c3) AS bit_xor_c3, BIT_XOR(t.c4) AS bit_xor_c4,  BIT_XOR(t.c5) AS bit_xor_c5, BIT_XOR(t.c6) AS bit_or_c6, BIT_OR(t.c7) AS bit_xor_c7, BIT_XOR(t.c8) AS bit_xor_c8
                      FROM bit_table0 t
                      WHERE (t.c8) < (SELECT bit_xor_c8 FROM bit_xor_val)
                      GROUP BY t.id'''
