from test_base import TestTable, TestView

class test_max_table(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2},
                     {"id" : 0, "c1": None, "c2": 2, "c3": 3, "c4": 2, "c5": 3, "c6": 4, "c7": 3, "c8": 3},
                     {"id" : 1, "c1": None, "c2": 5, "c3": 6, "c4": 2, "c5": 2, "c6": 1, "c7": None, "c8": 5}]
        self.sql = '''CREATE TABLE max_table(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_max(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 5, 'c2': 5, 'c3': 6, 'c4': 6, 'c5': 5, 'c6': 6, 'c7': 4, 'c8': 8}]
        self.sql = '''CREATE VIEW max_view AS SELECT
                      MAX(c1) AS c1, MAX(c2) AS c2, MAX(c3) AS c3, MAX(c4) AS c4, MAX(c5) AS c5, MAX(c6) AS c6, MAX(c7) AS c7, MAX(c8) AS c8
                      FROM max_table'''

class test_max_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 5, 'c2': 2, 'c3': 3, 'c4': 4, 'c5': 5, 'c6': 6, 'c7': 3, 'c8': 8},
                     {'id': 1, 'c1': 4, 'c2': 5, 'c3': 6, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 5}]
        self.sql = '''CREATE VIEW max_gby AS SELECT
                      id, MAX(c1) AS c1, MAX(c2) AS c2, MAX(c3) AS c3, MAX(c4) AS c4, MAX(c5) AS c5, MAX(c6) AS c6, MAX(c7) AS c7, MAX(c8) AS c8
                      FROM max_table
                      GROUP BY id'''

class test_max_distinct(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 5, 'c2': 5, 'c3': 6, 'c4': 6, 'c5': 5, 'c6': 6, 'c7': 4, 'c8': 8}]
        self.sql = '''CREATE VIEW max_distinct AS SELECT
                      MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2, MAX(DISTINCT c3) AS c3, MAX(DISTINCT c4) AS c4, MAX(DISTINCT c5) AS c5, MAX(DISTINCT c6) AS c6, MAX(DISTINCT c7) AS c7, MAX(DISTINCT c8) AS c8
                      FROM max_table'''

class test_max_distinct_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 5, 'c2': 2, 'c3': 3, 'c4': 4, 'c5': 5, 'c6': 6, 'c7': 3, 'c8': 8},
                         {'id': 1, 'c1': 4, 'c2': 5, 'c3': 6, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 5}]
        self.sql = '''CREATE VIEW max_distinct_gbt AS SELECT
                      id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2, MAX(DISTINCT c3) AS c3, MAX(DISTINCT c4) AS c4, MAX(DISTINCT c5) AS c5, MAX(DISTINCT c6) AS c6, MAX(DISTINCT c7) AS c7, MAX(DISTINCT c8) AS c8
                      FROM max_table
                      GROUP BY id'''

class test_max_where(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2},
                     {'c1': 5, 'c2': 2, 'c3': None, 'c4': 4, 'c5': 5, 'c6': 6, 'c7': None, 'c8': 8}]
        self.sql = '''CREATE VIEW max_where AS
                      WITH max_val AS(
                         SELECT MAX(c1) AS max_c1, MAX(c2) AS max_c2, MAX(c3) AS max_c3, MAX(c4) AS max_c4, MAX(c5) AS max_c5, MAX(c6) AS max_c6, MAX(c7) AS max_c7, MAX(c8) AS max_c8
                         FROM max_table)
                      SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                      FROM max_table t
                      WHERE (t.c4+T.c5) > (SELECT max_c4 FROM max_val)'''

class test_max_Where_Groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'max_c1': 5, 'max_c2': 2, 'max_c3': None, 'max_c4': 4, 'max_c5': 5, 'max_c6': 6, 'max_c7': None, 'max_c8': 8},
                     {'id': 1, 'max_c1': 4, 'max_c2': 3, 'max_c3': 4, 'max_c4': 6, 'max_c5': 2, 'max_c6': 3, 'max_c7': 4, 'max_c8': 2}]
        self.sql = '''CREATE VIEW max_where_gby AS
                      WITH max_val AS (
                          SELECT MAX(c1) AS max_c1, MAX(c2) AS max_c2, MAX(c3) AS max_c3, MAX(c4) AS max_c4, MAX(c5) AS max_c5, MAX(c6) AS max_c6, MAX(c7) AS max_c7, MAX(c8) AS max_c8
                          FROM max_table)
                      SELECT t.id, MAX(t.c1) AS max_c1, MAX(t.c2) AS max_c2, MAX(t.c3) AS max_c3, MAX(t.c4) AS max_c4,  MAX(t.c5) AS max_c5, MAX(t.c6) AS max_c6, MAX(t.c7) AS max_c7, MAX(t.c8) AS max_c8
                      FROM max_table t
                      WHERE (t.c4 + t.c5) > (SELECT max_c4 FROM max_val)
                      GROUP BY t.id'''
