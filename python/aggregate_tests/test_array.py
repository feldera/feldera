from .test_base import TestTable, TestView

class test_array_table(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2},
                     {"id": 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                     {"id": 1,"c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80}]
        self.sql = """CREATE TABLE array_tbl(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)"""

class test_array_agg_value(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': [None, 1, 4, 5], 'c2': [20, 2, 3, 2], 'c3': [30, 3, 4, None], 'c4': [40, 4, 6, 4], 'c5': [None, 5, 2, 5], 'c6': [60, 6, 3, 6], 'c7': [70, None, 4, None], 'c8': [80, 8, 2, 8]}]
        self.sql = '''CREATE VIEW array_agg AS SELECT
                      ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3, ARRAY_AGG(c4) AS c4, ARRAY_AGG(c5) AS c5, ARRAY_AGG(c6) AS c6, ARRAY_AGG(c7) AS c7, ARRAY_AGG(c8) AS c8
                      FROM array_tbl'''

class test_array_agg_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': [1, 5], 'c2': [2, 2], 'c3': [3, None], 'c4': [4, 4], 'c5': [5, 5], 'c6': [6, 6], 'c7': [None, None], 'c8': [8, 8]},
                     {'id': 1, 'c1': [None, 4], 'c2': [20, 3], 'c3': [30, 4], 'c4': [40, 6], 'c5': [None, 2], 'c6': [60, 3], 'c7': [70, 4], 'c8': [80, 2]}]
        self.sql = '''CREATE VIEW array_agg_view_gby AS SELECT
                      id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3, ARRAY_AGG(c4) AS c4, ARRAY_AGG(c5) AS c5, ARRAY_AGG(c6) AS c6, ARRAY_AGG(c7) AS c7, ARRAY_AGG(c8) AS c8
                      FROM array_tbl
                      GROUP BY id'''

class test_array_agg_distinct(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'c1': [None, 1, 4, 5], 'c2': [2, 3, 20], 'c3': [None, 3, 4, 30], 'c4': [4, 6, 40], 'c5': [None, 2, 5], 'c6': [3, 6, 60], 'c7': [None, 4, 70], 'c8': [2, 8, 80]}]
        self.sql = '''CREATE VIEW array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1,ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3, ARRAY_AGG(DISTINCT c4) AS c4, ARRAY_AGG(DISTINCT c5) AS c5, ARRAY_AGG(DISTINCT c6) AS c6, ARRAY_AGG(DISTINCT c7) AS c7, ARRAY_AGG(DISTINCT c8) AS c8
                      FROM array_tbl'''

class test_array_agg_distinct_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'c1': [1, 5], 'c2': [2], 'c3': [None, 3], 'c4': [4], 'c5': [5], 'c6': [6], 'c7': [None], 'c8': [8]},
                     {'id': 1, 'c1': [None, 4], 'c2': [3, 20], 'c3': [4, 30], 'c4': [6, 40], 'c5': [None, 2], 'c6': [3, 60], 'c7': [4, 70], 'c8': [2, 80]}]
        table_name = "array_agg_distinct_groupby"
        self.sql = '''CREATE VIEW array_agg_distinct_groupby AS SELECT
                      id, ARRAY_AGG(DISTINCT c1) AS c1,ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3, ARRAY_AGG(DISTINCT c4) AS c4, ARRAY_AGG(DISTINCT c5) AS c5, ARRAY_AGG(DISTINCT c6) AS c6, ARRAY_AGG(DISTINCT c7) AS c7, ARRAY_AGG(DISTINCT c8) AS c8
                      FROM array_tbl
                      GROUP BY id'''

class test_array_agg_where(TestView):
    def __init__(self):
        pipeline_name = "test_array_agg_where"
        # validated using postgres
        self.data = [{'array_agg_c1': [1, 5], 'array_agg_c2': [2, 2], 'array_agg_c3': [3, None], 'array_agg_c4': [4, 4], 'array_agg_c5': [5, 5], 'array_agg_c6': [6, 6], 'array_agg_c7': [None, None], 'array_agg_c8': [8, 8]}]
        self.sql = '''CREATE VIEW array_agg_where AS
                      WITH array_agg_val AS(
                      SELECT
                      ARRAY_AGG(c1) AS array_agg_c1, ARRAY_AGG(c2) AS array_agg_c2, ARRAY_AGG(c3) AS array_agg_c3, ARRAY_AGG(c4) AS array_agg_c4, ARRAY_AGG(c5) AS array_agg_c5, ARRAY_AGG(c6) AS array_agg_c6, ARRAY_AGG(c7) AS array_agg_c7, ARRAY_AGG(c8) AS array_agg_c8
                      FROM array_tbl)
                      SELECT  ARRAY_AGG(t.c1) AS array_agg_c1, ARRAY_AGG(t.c2) AS array_agg_c2, ARRAY_AGG(t.c3) AS array_agg_c3, ARRAY_AGG(t.c4) AS array_agg_c4, ARRAY_AGG(t.c5) AS array_agg_c5, ARRAY_AGG(t.c6) AS array_agg_c6, ARRAY_AGG(t.c7) AS array_agg_c7, ARRAY_AGG(t.c8) AS array_agg_c8
                      FROM array_tbl t
                      WHERE (t.c4+t.c5) > 8'''

class test_array_agg_where_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'id': 0, 'array_agg_c1': [1, 5], 'array_agg_c2': [2, 2], 'array_agg_c3': [3, None], 'array_agg_c4': [4, 4], 'array_agg_c5': [5, 5], 'array_agg_c6': [6, 6], 'array_agg_c7': [None, None], 'array_agg_c8': [8, 8]},
                     {'id': 1, 'array_agg_c1': [4], 'array_agg_c2': [3], 'array_agg_c3': [4], 'array_agg_c4': [6], 'array_agg_c5': [2], 'array_agg_c6': [3], 'array_agg_c7': [4], 'array_agg_c8': [2]}]
        self.sql = '''CREATE VIEW array_agg_view AS
                      WITH array_agg_val AS(
                      SELECT
                      ARRAY_AGG(c1) AS array_agg_c1, ARRAY_AGG(c2) AS array_agg_c2, ARRAY_AGG(c3) AS array_agg_c3, ARRAY_AGG(c4) AS array_agg_c4, ARRAY_AGG(c5) AS array_agg_c5, ARRAY_AGG(c6) AS array_agg_c6, ARRAY_AGG(c7) AS array_agg_c7, ARRAY_AGG(c8) AS array_agg_c8
                      FROM array_tbl)
                      SELECT  t.id, ARRAY_AGG(t.c1) AS array_agg_c1, ARRAY_AGG(t.c2) AS array_agg_c2, ARRAY_AGG(t.c3) AS array_agg_c3, ARRAY_AGG(t.c4) AS array_agg_c4, ARRAY_AGG(t.c5) AS array_agg_c5, ARRAY_AGG(t.c6) AS array_agg_c6, ARRAY_AGG(t.c7) AS array_agg_c7, ARRAY_AGG(t.c8) AS array_agg_c8
                      FROM array_tbl t
                      WHERE (t.c2) < 20
                      GROUP BY t.id'''
