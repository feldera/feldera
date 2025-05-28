from tests.aggregate_tests.aggtst_base import TstView, TstTable


class orderby_tbl(TstTable):
    def __init__(self):
        self.sql = """CREATE TABLE tbl(
                      c3 INT)"""
        self.data = [
            {"c3": 3},
            {"c3": 2},
            {"c3": None},
            {"c3": 2},
        ]


class orderby_v(TstView):
    def __init__(self):
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW v AS
                       SELECT c3 FROM tbl
                       ORDER BY c3 ASC NULLS LAST
                       LIMIT 3"""


class orderby_tbl1(TstTable):
    def __init__(self):
        self.sql = """CREATE TABLE tbl1(
                      name VARCHAR)"""
        self.data = [{"name": "ferris"}, {"name": "fred"}, {"c3": None}]


class orderby_v1(TstView):
    def __init__(self):
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW v1 AS
                       SELECT name FROM tbl1
                       ORDER BY name DESC"""


# Negative Tests
# Ignore(produces intended error): sqlite3.OperationalError: table users has 3 columns but 2 values were supplied
class ignore_orderby_broken_data_mismatch(TstTable):
    def __init__(self):
        super().__init__()
        self.sql = "CREATE TABLE users (id INT, name VARCHAR, age INT)"
        self.data = [
            {"id": 1, "name": "ferris", "age": 1},  # Correct
            {"id": 2, "name": "fred"},  # 'age' is missing
        ]
