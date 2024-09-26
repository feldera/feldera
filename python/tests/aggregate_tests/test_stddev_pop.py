from test_base import TestTable, TestView

class test_pop_table(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2}]
        self.sql = '''CREATE TABLE pop_table(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_pop_table1(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2},
                     {"id" : 0, "c1": None, "c2": 2, "c3": 3, "c4": 2, "c5": 3, "c6": 4, "c7": 3, "c8": 3},
                     {"id" : 1, "c1": None, "c2": 5, "c3": 6, "c4": 2, "c5": 2, "c6": 1, "c7": None, "c8": 5}]
        self.sql = '''CREATE TABLE pop_table1(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_pop_table2(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2},
                     {"id" :0 ,"c1": 4, "c2": 2, "c3": 30, "c4": 14, "c5": None, "c6": 60, "c7": 70, "c8": 18},
                     {"id": 1,"c1": 5, "c2": 3, "c3": None, "c4": 9, "c5": 51, "c6": 6, "c7": 72, "c8": 2}]
        self.sql = '''CREATE TABLE pop_table2(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_pop_table3(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2},
                     {"id" : 0, "c1": 0, "c2": 2, "c3": 0, "c4": 18, "c5": 0, "c6": 10, "c7": 0, "c8": 5}]
        self.sql = '''CREATE TABLE pop_table3(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_sttdev_pop(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': 0, 'c2': 0, 'c3': 0, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': 0, 'c8': 3}]
        self.sql = '''CREATE VIEW stddev_pop AS SELECT
                      STDDEV_POP(c1) AS c1, STDDEV_POP(c2) AS c2, STDDEV_POP(c3) AS c3, STDDEV_POP(c4) AS c4, STDDEV_POP(c5) AS c5, STDDEV_POP(c6) AS c6, STDDEV_POP(c7) AS c7, STDDEV_POP(c8) AS c8
                      FROM pop_table'''

class test_stddev_pop_groupby(TestView):
    def __init__(self):
        pipeline_name = "test_stddev_pop_groupby"
        # validated using postgres
        self.data = [{'id': 0, 'c1': 0, 'c2': 0, 'c3': 0, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': 0, 'c8': 2},
                     {'id': 1, 'c1': 0, 'c2': 1, 'c3': 1, 'c4': 2, 'c5': 0, 'c6': 1, 'c7': 0, 'c8': 1}]
        self.sql = '''CREATE VIEW stddev_pop_gby AS SELECT
                      id, STDDEV_POP(c1) AS c1, STDDEV_POP(c2) AS c2, STDDEV_POP(c3) AS c3, STDDEV_POP(c4) AS c4, STDDEV_POP(c5) AS c5, STDDEV_POP(c6) AS c6, STDDEV_POP(c7) AS c7, STDDEV_POP(c8) AS c8
                      FROM pop_table1
                      GROUP BY id'''

class test_stddev_pop_distinct(TestView):
    def __init__(self):
        new_data = [{"id" : 1, "c1": None, "c2": 2, "c3": 3, "c4": 2, "c5": 3, "c6": 6, "c7": 3, "c8": 2}]
        # validated using postgres
        self.data = [{'c1': 0, 'c2': 0, 'c3': 0, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': 0, 'c8': 3}]
        self.sql = '''CREATE VIEW stddev_pop_distinct AS SELECT
                      STDDEV_POP(DISTINCT c1) AS c1, STDDEV_POP(DISTINCT c2) AS c2, STDDEV_POP(DISTINCT c3) AS c3, STDDEV_POP(DISTINCT c4) AS c4, STDDEV_POP(DISTINCT c5) AS c5, STDDEV_POP(DISTINCT c6) AS c6, STDDEV_POP(DISTINCT c7) AS c7, STDDEV_POP(DISTINCT c8) AS c8
                      FROM pop_table'''

class test_stddev_pop_distinct_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': 0, 'c2': 0, 'c3': 0, 'c4': 5, 'c5': 0, 'c6': 27, 'c7': 0, 'c8': 5},
                     {'id': 1, 'c1': 0, 'c2': 0, 'c3': 0, 'c4': 1, 'c5': 24, 'c6': 1, 'c7': 34, 'c8': 0}]
        self.sql = '''CREATE VIEW stddev_pop_distinct_gby AS SELECT
                      id, STDDEV_POP(DISTINCT c1) AS c1, STDDEV_POP(DISTINCT c2) AS c2, STDDEV_POP(DISTINCT c3) AS c3, STDDEV_POP(DISTINCT c4) AS c4, STDDEV_POP(DISTINCT c5) AS c5, STDDEV_POP(DISTINCT c6) AS c6, STDDEV_POP(DISTINCT c7) AS c7, STDDEV_POP(DISTINCT c8) AS c8
                      FROM pop_table2
                      GROUP BY id'''

class test_stddev_pop_where(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{ "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}]
        self.sql = '''CREATE VIEW stddev_pop_view AS
                       WITH stddev_val AS(
                            SELECT STDDEV_POP(c1) AS stddev_c1, STDDEV_POP(c2) AS stddev_c2, STDDEV_POP(c3) AS stddev_c3, STDDEV_POP(c4) AS stddev_c4, STDDEV_POP(c5) AS stddev_c5, STDDEV_POP(c6) AS stddev_c6, STDDEV_POP(c7) AS stddev_c7, STDDEV_POP(c8) AS stddev_c8
                            FROM pop_table)
                       SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                       FROM pop_table t
                       WHERE (t.c8) > (SELECT stddev_c8 FROM stddev_val)'''

class test_stddev_pop_where_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'stddev_pop_c1': 2, 'stddev_pop_c2': 0, 'stddev_pop_c3': 0, 'stddev_pop_c4': 7, 'stddev_pop_c5': 2, 'stddev_pop_c6': 2, 'stddev_pop_c7': 0, 'stddev_pop_c8': 1}]
        self.sql = '''CREATE VIEW stddev_pop_where_gby AS
                      WITH stddev_pop_val AS (
                            SELECT STDDEV_POP(c1) AS stddev_pop_c1, STDDEV_POP(c2) AS stddev_pop_c2, STDDEV_POP(c3) AS stddev_pop_c3, STDDEV_POP(c4) AS stddev_pop_c4, STDDEV_POP(c5) AS stddev_pop_c5, STDDEV_POP(c6) AS stddev_pop_c6, STDDEV_POP(c7) AS stddev_pop_c7, STDDEV_POP(c8) AS stddev_pop_c8
                            FROM pop_table3)
                      SELECT t.id, STDDEV_POP(t.c1) AS stddev_pop_c1, STDDEV_POP(t.c2) AS stddev_pop_c2, STDDEV_POP(t.c3) AS stddev_pop_c3, STDDEV_POP(t.c4) AS stddev_pop_c4,  STDDEV_POP(t.c5) AS stddev_pop_c5,STDDEV_POP(t.c6) AS stddev_pop_c6,STDDEV_POP(t.c7) AS stddev_pop_c7, STDDEV_POP(t.c8) AS stddev_pop_c8
                      FROM pop_table3 t
                      WHERE (t.c8) > (SELECT stddev_pop_c8 FROM stddev_pop_val)
                      GROUP BY t.id'''
