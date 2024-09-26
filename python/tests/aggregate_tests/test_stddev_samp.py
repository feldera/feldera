from .test_base import TestTable, TestView

class test_stddev_table(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2},
                     {"id" : 1, "c1": None, "c2": 2, "c3": 3, "c4": 2, "c5": 3, "c6": 6, "c7": 3, "c8": 2}]
        self.sql = '''CREATE TABLE stddev_table(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_sttdev(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 1, 'c2': 1, 'c3': None, 'c4': 1, 'c5': 2, 'c6': 2, 'c7': None, 'c8': 4}]
        self.sql = '''CREATE VIEW stddev_view AS SELECT
                      STDDEV_SAMP(c1) AS c1, STDDEV_SAMP(c2) AS c2, STDDEV_SAMP(c3) AS c3, STDDEV_SAMP(c4) AS c4, STDDEV_SAMP(c5) AS c5, STDDEV_SAMP(c6) AS c6, STDDEV_SAMP(c7) AS c7, STDDEV_SAMP(c8) AS c8
                      FROM pop_table'''

class test_stddev_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': None, 'c2': 0, 'c3': None, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': None, 'c8': 3},
                         {'id': 1, 'c1': None, 'c2': 1, 'c3': 1, 'c4': 2, 'c5': 0, 'c6': 1, 'c7': None, 'c8': 2}]
        self.sql = '''CREATE VIEW stddev_samp_gby AS SELECT
                      id, STDDEV_SAMP(c1) AS c1, STDDEV_SAMP(c2) AS c2, STDDEV_SAMP(c3) AS c3, STDDEV_SAMP(c4) AS c4, STDDEV_SAMP(c5) AS c5, STDDEV_SAMP(c6) AS c6, STDDEV_SAMP(c7) AS c7, STDDEV_SAMP(c8) AS c8
                      FROM pop_table1
                      GROUP BY id'''

class test_stddev_distinct(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 1, 'c2': 1, 'c3': 1, 'c4': 2, 'c5': 1, 'c6': 2, 'c7': 1, 'c8': 4}]
        self.sql = '''CREATE VIEW stddev_samp_distinct AS SELECT
                      STDDEV_SAMP(DISTINCT c1) AS c1, STDDEV_SAMP(DISTINCT c2) AS c2, STDDEV_SAMP(DISTINCT c3) AS c3, STDDEV_SAMP(DISTINCT c4) AS c4, STDDEV_SAMP(DISTINCT c5) AS c5, STDDEV_SAMP(DISTINCT c6) AS c6, STDDEV_SAMP(DISTINCT c7) AS c7, STDDEV_SAMP(DISTINCT c8) AS c8
                      FROM stddev_table'''

class test_stddev_distinct_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{"id": 0, "c1": None, "c2": None, "c3": None, "c4": 1, "c5": 1, "c6": 1, "c7": None, "c8": 3},
                     {"id": 1, "c1": None, "c2": 1, "c3": 1, "c4": 2, "c5": None, "c6": 1, "c7": None, "c8": 2}]
        self.sql = '''CREATE VIEW stddev_samp_distinct_gby AS SELECT
                      id, STDDEV_SAMP(DISTINCT c1) AS c1, STDDEV_SAMP(DISTINCT c2) AS c2, STDDEV_SAMP(DISTINCT c3) AS c3, STDDEV_SAMP(DISTINCT c4) AS c4, STDDEV_SAMP(DISTINCT c5) AS c5, STDDEV_SAMP(DISTINCT c6) AS c6, STDDEV_SAMP(DISTINCT c7) AS c7, STDDEV_SAMP(DISTINCT c8) AS c8
                      FROM pop_table1
                      GROUP BY id'''

class test_stddev_where(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{ "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}]
        self.sql = '''CREATE VIEW stddev_where AS
                      WITH stddev_val AS(
                            SELECT STDDEV_SAMP(c1) AS stddev_c1, STDDEV_SAMP(c2) AS stddev_c2, STDDEV_SAMP(c3) AS stddev_c3, STDDEV_SAMP(c4) AS stddev_c4, STDDEV_SAMP(c5) AS stddev_c5, STDDEV_SAMP(c6) AS stddev_c6, STDDEV_SAMP(c7) AS stddev_c7, STDDEV_SAMP(c8) AS stddev_c8
                            FROM pop_table)
                      SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                      FROM pop_table t
                      WHERE (t.c8) > (SELECT stddev_c8 FROM stddev_val)'''

class xtest_stddev_where_groupby(TestView):
    # This test is disabled, since it overflows at runtime.
    # But the test is wrong anyway
    def __init__(self):
        # validated using postgres
        self.data = [{"id": 0, "stddev_c1": None, "stddev_c2": 0, "stddev_c3":None, "stddev_c4": 1, "stddev_c5": 1, "stddev_c6": 1, "stddev_c7": None, "stddev_c8": 3},
                     {"id": 1, "stddev_c1": None, "stddev_c2": None, "stddev_c3":None, "stddev_c4": None, "stddev_c5": None, "stddev_c6": None, "stddev_c7": None, "stddev_c8": None}]
        self.sql = '''CREATE VIEW stddev_where_gby AS
                      WITH stddev_val AS (
                            SELECT
                                STDDEV_SAMP(c1) AS stddev_c1, STDDEV_SAMP(c2) AS stddev_c2, STDDEV_SAMP(c3) AS stddev_c3, STDDEV_SAMP(c4) AS stddev_c4, STDDEV_SAMP(c5) AS stddev_c5, STDDEV_SAMP(c6) AS stddev_c6, STDDEV_SAMP(c7) AS stddev_c7, STDDEV_SAMP(c8) AS stddev_c8
                            FROM pop_table1)
                      SELECT t.id, STDDEV_SAMP(t.c1) AS stddev_c1, STDDEV_SAMP(t.c2) AS stddev_c2, STDDEV_SAMP(t.c3) AS stddev_c3, STDDEV_SAMP(t.c4) AS stddev_c4,  STDDEV_SAMP(t.c5) AS stddev_c5, STDDEV_SAMP(t.c6) AS stddev_c6, STDDEV_SAMP(t.c7) AS stddev_c7, STDDEV_SAMP(t.c8) AS stddev_c8
                      FROM pop_table1 t
                      WHERE (t.c8) > (SELECT stddev_c8 FROM stddev_val)
                      GROUP BY t.id'''
