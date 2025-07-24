from tests.aggregate_tests.aggtst_base import TstTable


class aggtst_binary_table(TstTable):
    """Define the table used by the binary tests"""

    def __init__(self):
        self.sql = """CREATE TABLE binary_tbl(
                      id INT,
                      c1 BINARY(4),
                      c2 BINARY(4) NULL)"""
        self.data = [
            {"id": 0, "c1": [12, 22, 32], "c2": None},
            {"id": 0, "c1": [23, 56, 33, 21], "c2": [55, 66, 77, 88]},
            {"id": 1, "c1": [23, 56, 33, 21], "c2": [99, 20, 31, 77]},
            {"id": 1, "c1": [49, 43, 84, 29], "c2": [32, 34, 22, 12]},
        ]

        # Equivalent SQL for Postgres

        # CREATE TABLE binary_tbl (
        #     id INT,
        #     c1 BYTEA,
        #     c2 BYTEA NULL
        # );

        # INSERT INTO binary_tbl (id, c1, c2) VALUES
        #     (0, '\x0c1620', NULL),
        #     (0, '\x17382115', '\x37424d58'),
        #     (1, '\x17382115', '\x63141f4d'),
        #     (1, '\x312b541d', '\x2022160c');
