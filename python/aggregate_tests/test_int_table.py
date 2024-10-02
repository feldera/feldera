from .test_base import TestTable

class test_int_table(TestTable):
    """Define the table used by some integer tests"""
    def __init__(self):
        self.sql = '''CREATE TABLE int_tbl(
                      id INT, c1 INT, c2 INT NOT NULL)'''
        self.data =  [{"id": 0, "c1": None, "c2": 20},
                      {"id": 1, "c1": 11, "c2": 22},
                      {"id": 0, "c1": 1, "c2": 2}]
