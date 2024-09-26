from .test_base import TestTable, TestView

class test_avg_table(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 0, "c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80},
                     {"id": 1, "c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 61, "c7": 72, "c8": 88}]
        self.sql = '''CREATE TABLE avg_table(
                      id INT,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_avg_table1(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 0, "c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
                     {"id": 1, "c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
                     {"id": 1, "c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        self.sql = '''CREATE TABLE avg_table1(
                      id INT,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_avg(TestView):
    def __init__(self):
        self.data = [{"c1": 6, "c2": 14, "c3" : 16, "c4" : 29, "c5" : 28, "c6" : 42, "c7": 71, "c8": 58}]
        self.sql = '''CREATE VIEW avg_view AS SELECT
                      AVG(c1) AS c1,AVG(c2) AS c2,AVG(c3) AS c3,AVG(c4) AS c4,AVG(c5) AS c5,AVG(c6) AS c6,AVG(c7) AS c7,AVG(c8) AS c8
                      FROM avg_table'''

class test_avg_groupby(TestView):
    def __init__(self):
        # checked manually
        self.data = [{"id": 0, "c1": 1, "c2": 11, "c3": 16, "c4": 22, "c5": 5, "c6": 33, "c7": 70, "c8": 44},
                     {"id" : 1, "c1": 11, "c2": 22, "c3" : None, "c4" : 45, "c5" : 51, "c6" : 61, "c7":72, "c8": 88}]
        self.sql = '''CREATE VIEW avg_gby AS SELECT
                      id,AVG(c1) AS c1, AVG(c2) AS c2, AVG(c3) AS c3, AVG(c4) AS c4, AVG(c5) AS c5, AVG(c6) AS c6, AVG(c7) AS c7, AVG(c8) AS c8
                      FROM avg_table
                      GROUP BY id'''

class test_avg_distinct(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{"c1": 8, "c2": 15, "c3" : 25, "c4" : 27, "c5" : 30, "c6" : 52, "c7": 57, "c8": 54}]
        self.sql = '''CREATE VIEW avg_distinct AS
                      SELECT AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2, AVG(DISTINCT c3) AS c3, AVG(DISTINCT c4) AS c4, AVG(DISTINCT c5) AS c5, AVG(DISTINCT c6) AS c6, AVG(DISTINCT c7) AS c7, AVG(DISTINCT c8) AS c8
                      FROM avg_table1'''

class test_avg_distinct_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{"id": 0, "c1": 1, "c2": 2, "c3": 16, "c4": 4, "c5": 5, "c6": 33, "c7": 70, "c8": 8},
                     {"id" : 1, "c1": 12, "c2": 21, "c3" : 44, "c4" : 38, "c5" : 42, "c6" : 48, "c7":50, "c8": 77}]
        self.sql = '''CREATE VIEW avg_distinct_gby AS
                      SELECT id, AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2, AVG(DISTINCT c3) AS c3, AVG(DISTINCT c4) AS c4, AVG(DISTINCT c5) AS c5, AVG(DISTINCT c6) AS c6, AVG(DISTINCT c7) AS c7, AVG(DISTINCT c8) AS c8
                      FROM avg_table1
                      GROUP BY id'''

class test_avg_where(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{"c1": 14, "c2": 21, "c3" : 44, "c4" : 32, "c5" : 34, "c6" : 90, "c7": 29, "c8": 67}]
        self.sql = '''CREATE VIEW avg_where AS
                      SELECT AVG(c1) AS c1, AVG(c2) AS c2, AVG(c3) AS c3, AVG(c4) AS c4, AVG(c5) AS c5, AVG(c6) AS c6, AVG(c7) AS c7, AVG(c8) AS c8
                      FROM avg_table1
                      WHERE c1 is NOT NULl AND c3 is NOT NULL AND c5 is NOT NULL AND c7 is NOT NULL;'''

class test_avg_where_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{"id": 1, "c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        self.sql = '''CREATE VIEW avg_where_gby AS SELECT
                      id, AVG(c1) AS c1, AVG(c2) AS c2, AVG(c3) AS c3, AVG(c4) AS c4, AVG(c5) AS c5, AVG(c6) AS c6, AVG(c7) AS c7, AVG(c8) AS c8
                      FROM avg_table1
                      WHERE c1 is NOT NULl AND c3 is NOT NULL AND c5 is NOT NULL AND c7 is NOT NULL
                      GROUP BY id'''
