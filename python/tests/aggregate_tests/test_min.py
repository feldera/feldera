from test_base import TestTable, TestView

class test_min_table(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                    {"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2},
                    {"id" : 0, "c1": None, "c2": 2, "c3": 3, "c4": 2, "c5": 3, "c6": 4, "c7": 3, "c8": 3},
                    {"id" : 1, "c1": None, "c2": 5, "c3": 6, "c4": 2, "c5": 2, "c6": 1, "c7": None, "c8": 5}]
        self.sql = '''CREATE TABLE min_table(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_min(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 4, 'c2': 2, 'c3': 3, 'c4': 2, 'c5': 2, 'c6': 1, 'c7': 3, 'c8': 2}]
        self.sql = '''CREATE VIEW min_view AS SELECT
                      MIN(c1) AS c1, MIN(c2) AS c2, MIN(c3) AS c3, MIN(c4) AS c4, MIN(c5) AS c5, MIN(c6) AS c6, MIN(c7) AS c7, MIN(c8) AS c8
                      FROM min_table'''

class test_min_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 5, 'c2': 2, 'c3': 3, 'c4': 2, 'c5': 3, 'c6': 4, 'c7': 3, 'c8': 3},
                     {'id': 1, 'c1': 4, 'c2': 3, 'c3': 4, 'c4': 2, 'c5': 2, 'c6': 1, 'c7': 4, 'c8': 2}]
        self.sql = '''CREATE VIEW min_gby AS SELECT
                      id, MIN(c1) AS c1, MIN(c2) AS c2, MIN(c3) AS c3, MIN(c4) AS c4, MIN(c5) AS c5, MIN(c6) AS c6, MIN(c7) AS c7, MIN(c8) AS c8
                      FROM min_table
                      GROUP BY id'''

class test_min_distinct(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 4, 'c2': 2, 'c3': 3, 'c4': 2, 'c5': 2, 'c6': 1, 'c7': 3, 'c8': 2}]
        self.sql = '''CREATE VIEW min_distinct AS SELECT
                      MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2, MIN(DISTINCT c3) AS c3, MIN(DISTINCT c4) AS c4, MIN(DISTINCT c5) AS c5, MIN(DISTINCT c6) AS c6, MIN(DISTINCT c7) AS c7, MIN(DISTINCT c8) AS c8
                      FROM min_table'''

class test_min_distinct_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 5, 'c2': 2, 'c3': 3, 'c4': 2, 'c5': 3, 'c6': 4, 'c7': 3, 'c8': 3},
                     {'id': 1, 'c1': 4, 'c2': 3, 'c3': 4, 'c4': 2, 'c5': 2, 'c6': 1, 'c7': 4, 'c8': 2}]
        self.sql = '''CREATE VIEW min_distinct_gby AS
                      SELECT id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2, MIN(DISTINCT c3) AS c3, MIN(DISTINCT c4) AS c4, MIN(DISTINCT c5) AS c5, MIN(DISTINCT c6) AS c6, MIN(DISTINCT c7) AS c7, MIN(DISTINCT c8) AS c8
                      FROM min_table
                      GROUP BY id'''

class test_min_where(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2}]
        self.sql = '''CREATE VIEW min_where AS SELECT
                      MIN(c1) AS c1, MIN(c2) AS c2, MIN(c3) AS c3, MIN(c4) AS c4, MIN(c5) AS c5, MIN(c6) AS c6, MIN(c7) AS c7, MIN(c8) AS c8
                      FROM min_table WHERE c1 is NOT NULl AND c3 is NOT NULL'''

class test_min_where_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 1, 'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2}]
        self.sql = '''CREATE VIEW min_where_gby AS SELECT
                      id, MIN(c1) AS c1, MIN(c2) AS c2, MIN(c3) AS c3, MIN(c4) AS c4, MIN(c5) AS c5, MIN(c6) AS c6, MIN(c7) AS c7, MIN(c8) AS c8
                      FROM min_table
                      WHERE c1 is NOT NULl AND c3 is NOT NULL
                      GROUP BY id'''
