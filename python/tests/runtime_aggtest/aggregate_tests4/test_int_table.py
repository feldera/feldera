from tests.runtime_aggtest.aggtst_base import TstTable


class aggtst_int_table(TstTable):
    """Define the table used by some integer tests"""

    def __init__(self):
        self.sql = """CREATE TABLE int_tbl(
                      id INT, c1 INT, c2 INT NOT NULL)"""
        self.data = [
            {"id": 0, "c1": None, "c2": 20},
            {"id": 1, "c1": 11, "c2": 22},
            {"id": 0, "c1": 1, "c2": 2},
        ]


class aggtst_int0_table(TstTable):
    def __init__(self):
        self.sql = """CREATE TABLE int0_tbl(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)"""

        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": None,
                "c4": 4,
                "c5": 5,
                "c6": 6,
                "c7": None,
                "c8": 8,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 3,
                "c3": 4,
                "c4": 6,
                "c5": 2,
                "c6": 3,
                "c7": 4,
                "c8": 2,
            },
            {
                "id": 0,
                "c1": None,
                "c2": 2,
                "c3": 3,
                "c4": 2,
                "c5": 3,
                "c6": 4,
                "c7": 3,
                "c8": 3,
            },
            {
                "id": 1,
                "c1": None,
                "c2": 5,
                "c3": 6,
                "c4": 2,
                "c5": 2,
                "c6": 1,
                "c7": None,
                "c8": 5,
            },
        ]


class aggtst_int_stddev_table(TstTable):
    def __init__(self):
        self.sql = """CREATE TABLE stddev_tbl(
                      id INT NOT NULL,
                      c1 TINYINT,
                      c2 TINYINT NOT NULL,
                      c3 INT2,
                      c4 INT2 NOT NULL,
                      c5 INT,
                      c6 INT NOT NULL,
                      c7 BIGINT,
                      c8 BIGINT NOT NULL)"""

        self.data = [
            {
                "id": 0,
                "c1": 5,
                "c2": 2,
                "c3": None,
                "c4": 4,
                "c5": 5,
                "c6": 6,
                "c7": None,
                "c8": 8,
            },
            {
                "id": 1,
                "c1": 4,
                "c2": 3,
                "c3": 4,
                "c4": 6,
                "c5": 2,
                "c6": 3,
                "c7": 4,
                "c8": 2,
            },
        ]
