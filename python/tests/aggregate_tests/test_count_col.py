from test_base import TestTable, TestView

class test_count_col_table(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2},
                     {"id" :0 ,"c1": 4, "c2": 2, "c3": 30, "c4": 14, "c5": None, "c6": 60, "c7": 70, "c8": 18},
                     {"id": 1,"c1": 5, "c2": 3, "c3": None, "c4": 9, "c5": 51, "c6": 6, "c7": 72, "c8": 2}]
        self.sql = '''CREATE TABLE count_col_table(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_count_Col(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 4, 'c2': 4, 'c3': 2, 'c4': 4, 'c5': 3, 'c6': 4, 'c7': 3, 'c8': 4}]
        self.sql = '''CREATE VIEW count_col AS SELECT
                      COUNT(c1) AS c1, COUNT(c2) AS c2, COUNT(c3) AS c3, COUNT(c4) AS c4, COUNT(c5) AS c5, COUNT(c6) AS c6, COUNT(c7) AS c7, COUNT(c8) AS c8
                      FROM count_col_table'''

class test_count_col_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 2, 'c2': 2, 'c3': 1, 'c4': 2, 'c5': 1, 'c6': 2, 'c7': 1, 'c8': 2},
                     {'id': 1, 'c1': 2, 'c2': 2, 'c3': 1, 'c4': 2, 'c5': 2, 'c6': 2, 'c7': 2, 'c8': 2}]
        self.sql = '''CREATE VIEW count_col_gby AS SELECT
                      id, COUNT(c1) AS c1, COUNT(c2) AS c2, COUNT(c3) AS c3, COUNT(c4) AS c4, COUNT(c5) AS c5, COUNT(c6) AS c6, COUNT(c7) AS c7, COUNT(c8) AS c8
                      FROM count_col_table
                      GROUP BY id'''

class test_count_col_distinct(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 2, 'c2': 2, 'c3': 2, 'c4': 4, 'c5': 3, 'c6': 3, 'c7': 3, 'c8': 3}]
        self.sql = '''CREATE VIEW count_col_distinct AS SELECT
                      COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2, COUNT(DISTINCT c3) AS c3, COUNT(DISTINCT c4) AS c4, COUNT(DISTINCT c5) AS c5, COUNT(DISTINCT c6) AS c6, COUNT(DISTINCT c7) AS c7, COUNT(DISTINCT c8) AS c8
                      FROM count_col_table'''

class test_count_col_distinct_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 2, 'c2': 1, 'c3': 1, 'c4': 2, 'c5': 1, 'c6': 2, 'c7': 1, 'c8': 2},
                     {'id': 1, 'c1': 2, 'c2': 1, 'c3': 1, 'c4': 2, 'c5': 2, 'c6': 2, 'c7': 2, 'c8': 1}]
        self.sql = '''CREATE VIEW count_col_distinct_gby AS SELECT
                      id, COUNT(DISTINCT c1) AS c1, COUNT(DISTINCT c2) AS c2, COUNT(DISTINCT c3) AS c3, COUNT(DISTINCT c4) AS c4, COUNT(DISTINCT c5) AS c5, COUNT(DISTINCT c6) AS c6, COUNT(DISTINCT c7) AS c7, COUNT(DISTINCT c8) AS c8
                      FROM count_col_table
                      GROUP BY id'''

class test_count_col_where(TestView):
    def __init__(self):
        # validated using postgres
        self.data =  [{'c1': 1, 'c2': 1, 'c3': 1, 'c4': 1, 'c5': 0, 'c6': 1, 'c7': 1, 'c8': 1}]
        self.sql = '''CREATE VIEW count_col_where AS SELECT
                      COUNT(c1) AS c1, COUNT(c2) AS c2, COUNT(c3) AS c3, COUNT(c4) AS c4, COUNT(c5) AS c5, COUNT(c6) AS c6, COUNT(c7) AS c7, COUNT(c8) AS c8
                      FROM count_col_table
                      WHERE c3>4'''

class test_count_col_where_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 1, 'c2': 1, 'c3': 1, 'c4': 1, 'c5': 0, 'c6': 1, 'c7': 1, 'c8': 1},
                         {'id': 1, 'c1': 2, 'c2': 2, 'c3': 1, 'c4': 2, 'c5': 2, 'c6': 2, 'c7': 2, 'c8': 2}]
        self.sql = '''CREATE VIEW count_col_where_gbt AS SELECT
                      id, COUNT(c1) AS c1, COUNT(c2) AS c2, COUNT(c3) AS c3, COUNT(c4) AS c4, COUNT(c5) AS c5, COUNT(c6) AS c6, COUNT(c7) AS c7, COUNT(c8) AS c8
                      FROM count_col_table
                      WHERE c4>4
                      GROUP BY id'''
