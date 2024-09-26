from test_base import TestTable, TestView

class test_every_table(TestTable):
    def __init__(self):
        self.data = [{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
                    {"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2},
                    {"id" :0 ,"c1": 4, "c2": 2, "c3": 30, "c4": 14, "c5": None, "c6": 60, "c7": 70, "c8": 18},
                    {"id": 1,"c1": 5, "c2": 3, "c3": None, "c4": 9, "c5": 51, "c6": 6, "c7": 72, "c8": 2}]
        self.sql = '''CREATE TABLE every_table(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)'''

class test_every(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'every': True}]
        self.sql = '''CREATE VIEW every_view AS SELECT EVERY(c4>3) AS every FROM every_table'''

class test_every_groupby(TestView):
    def __init__(self):
        pipeline_name = "test_every_groupby"
        # validated using postgres
        self.data = [{'every': True}, {'every': True}]
        self.sql = '''CREATE VIEW every_view_gby AS SELECT EVERY(c4>3) AS every FROM every_table GROUP BY id'''

class test_every_where(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'every': False}]
        self.sql = '''CREATE VIEW every_view_where AS SELECT EVERY(c4=4) AS every FROM every_table WHERE c6=6'''

class test_every_where_groupby(TestView):
    def __init__(self):
        # validated using postgres
        self.data = [{'every': False}, {'every': True}]
        self.sql = '''CREATE VIEW every_view_where_gby AS SELECT EVERY(c4>4) AS every FROM every_table WHERE c6>3 GROUP BY id'''
