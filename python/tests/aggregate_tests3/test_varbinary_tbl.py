from tests.aggregate_tests.aggtst_base import TstTable


class aggtst_varbinary_table(TstTable):
    """Define the table used by the varbinary tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varbinary_tbl(
                      id INT,
                      c1 VARBINARY,
                      c2 VARBINARY NULL)"""
        self.data = [
            {"id": 0, "c1": [12, 22, 32], "c2": None},
            {"id": 0, "c1": [23, 56, 33, 21], "c2": [55, 66, 77, 88]},
            {"id": 1, "c1": [23, 56, 33, 21], "c2": [99, 20, 31, 77]},
            {"id": 1, "c1": [49, 43, 84, 29, 11], "c2": [32, 34]},
        ]

        # Equivalent SQL for Postgres

        # CREATE TABLE varbinary_tbl (
        #     id INT,
        #     c1 BYTEA,
        #     c2 BYTEA NULL
        # );

        # INSERT INTO varbinary_tbl (id, c1, c2) VALUES
        #     (0, '\x0c1620', NULL),
        #     (0, '\x17382115', '\x37424d58'),
        #     (1, '\x17382115', '\x63141f4d'),
        #     (1, '\x312b541d0b', '\x2022');
