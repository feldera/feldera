from test_base import TestTable, TestView

class test_integers_table(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 0, "c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80},
                     {"id": 1, "c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 61, "c7": 72, "c8": 88}]
        self.sql = '''CREATE TABLE integers_table(
                      id INT,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_integers_table1(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 0, "c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80},
                     {"id": 1, "c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 61, "c7": 72, "c8": 88},
                     {"id": 1, "c1": 23, "c2": 26, "c3": None, "c4": 9, "c5": 3, "c6": 20, "c7": 43, "c8": 3}]
        self.sql = '''CREATE TABLE integers_table1(
                      id INT,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_integers_table2(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 0, "c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80},
                     {"id": 1, "c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 61, "c7": 72, "c8": 88},
                     {"id": 1, "c1": 27, "c2": 55, "c3": 37, "c4": 84, "c5": 11, "c6": 7, "c7": 6, "c8": 8}]
        self.sql = '''CREATE TABLE integers_table2(
                      id INT,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_sum(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{"c1": 12, "c2": 44, "c3": 33, "c4": 89, "c5": 56, "c6": 127, "c7": 142, "c8": 176}]
        self.sql = '''CREATE VIEW integers_sum_view AS SELECT
                      SUM(c1) AS c1, SUM(c2) AS c2, SUM(c3) AS c3, SUM(c4) AS c4, SUM(c5) AS c5, SUM(c6) AS c6, SUM(c7) AS c7, SUM(c8) AS c8
                      FROM integers_table'''

class test_sum_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{"id": 0, "c1": 1, "c2": 22, "c3": 33, "c4": 44, "c5": 5, "c6": 66, "c7": 70, "c8": 88},
                         {"id": 1, "c1": 34, "c2": 48, "c3": None, "c4": 54, "c5": 54, "c6": 81, "c7": 115, "c8": 91}]
        self.sql = '''CREATE VIEW integers_sum_gby AS
                      SELECT id, SUM(c1) AS c1, SUM(c2) AS c2, SUM(c3) AS c3, SUM(c4) AS c4, SUM(c5) AS c5, SUM(c6) AS c6, SUM(c7) AS c7, SUM(c8) AS c8
                      FROM integers_table1
                      GROUP BY id'''

class test_sum_distinct(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{"c1": 12, "c2": 44, "c3": 33, "c4": 89, "c5": 56, "c6": 127, "c7": 142, "c8": 176}]
        self.sql = '''CREATE VIEW integers_sum_distinct AS SELECT
                      SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2, SUM(DISTINCT c3) AS c3, SUM(DISTINCT c4) AS c4, SUM(DISTINCT c5) AS c5, SUM(DISTINCT c6) AS c6, SUM(DISTINCT c7) AS c7, SUM(DISTINCT c8) AS c8
                      FROM integers_table'''

class test_sum_distinct_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{"id": 0, "c1": 1, "c2": 22, "c3": 33, "c4": 44, "c5": 5, "c6": 66, "c7": 70, "c8": 88},
                     {"id": 1, "c1": 38, "c2": 77, "c3": 37, "c4": 129, "c5": 62, "c6": 68, "c7": 78, "c8": 96}]
        self.sql = '''CREATE VIEW integers_sum_distinct_gby AS SELECT
                      id, SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2, SUM(DISTINCT c3) AS c3, SUM(DISTINCT c4) AS c4, SUM(DISTINCT c5) AS c5, SUM(DISTINCT c6) AS c6, SUM(DISTINCT c7) AS c7, SUM(DISTINCT c8) AS c8
                      FROM integers_table2
                      GROUP BY id'''

class test_sum_where(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{"c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}]
        self.sql = '''CREATE VIEW integers_sum_where AS SELECT
                      SUM(c1) AS c1, SUM(c2) AS c2, SUM(c3) AS c3, SUM(c4) AS c4, SUM(c5) AS c5, SUM(c6) AS c6, SUM(c7) AS c7, SUM(c8) AS c8
                      FROM integers_table
                      WHERE c1 is NOT NULl AND c3 is NOT NULL'''

class test_sum_where_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{"id": 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}]
        self.sql = '''CREATE VIEW integers_sum_where_gby AS
                      SELECT id, SUM(c1) AS c1, SUM(c2) AS c2, SUM(c3) AS c3, SUM(c4) AS c4, SUM(c5) AS c5, SUM(c6) AS c6, SUM(c7) AS c7, SUM(c8) AS c8
                      FROM integers_table
                      WHERE c1 is NOT NULl AND c3 is NOT NULL
                      GROUP BY id'''
