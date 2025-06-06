from tests.aggregate_tests.aggtst_base import TstTable


class orderby_tbl_int_varchar(TstTable):
    """Define the table used by the order by/limit tests with INT and VARCHAR values"""

    def __init__(self):
        self.sql = """CREATE TABLE orderby_tbl_int_varchar(
                      c1 INT,
                      c2 VARCHAR)"""
        self.data = [
            {"c1": 3, "c2": "hello"},
            {"c1": 2, "c2": "bye bye, friend!!"},
            {"c1": None, "c2": "see you later"},
            {"c1": 2, "c2": None},
            {"c1": 1, "c2": "ferris says ciao"},
            {"c1": 14, "c2": "meet you @ 5"},
            {"c1": 3, "c2": None},
            {"c1": 6, "c2": "see you"},
            {"c1": 1, "c2": "hello"},
            {"c1": None, "c2": "fred says konichiwa"},
            {"c1": 4, "c2": "see you"},
        ]
