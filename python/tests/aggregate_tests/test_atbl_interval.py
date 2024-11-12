from .aggtst_base import TstTable, TstView


class aggtst_interval_table(TstTable):
    """Define the table used by the view atbl_interval"""

    def __init__(self):
        self.sql = """CREATE TABLE interval_tbl(
                      id INT NOT NULL,
                      c1 TIMESTAMP,
                      c2 TIMESTAMP,
                      c3 TIMESTAMP)"""

        self.data = [
            {
                "id": 0,
                "c1": "2014-11-05 08:27:00",
                "c2": "2024-12-05 12:45:00",
                "c3": "2020-03-14 15:41:00",
            },
            {
                "id": 0,
                "c1": "2020-06-21 14:00:00",
                "c2": "1970-01-01 14:33:00",
                "c3": "2023-04-19 11:25:00",
            },
            {
                "id": 1,
                "c1": "1969-03-17 07:01:00",
                "c2": "2015-09-07 01:20:00",
                "c3": "1987-04-29 02:14:00",
            },
            {
                "id": 1,
                "c1": "2020-06-21 14:00:00",
                "c2": "2023-02-26 18:00:00",
                "c3": "2021-11-03 06:33:00",
            },
            {
                "id": 1,
                "c1": "2024-12-05 09:15:00",
                "c2": "2014-11-05 16:30:00",
                "c3": "2017-08-25 16:15:00",
            },
        ]


class aggtst_atbl_interval(TstView):
    """Define the view used by interval tests as input"""

    def __init__(self):
        # No data needed for a local view
        self.data = []  # Data is not required for local views

        self.sql = """CREATE LOCAL VIEW atbl_interval AS SELECT
                      id,
                      (c1 - c2)MONTH AS c1_minus_c2,
                      (c2 - c1)MONTH AS c2_minus_c1,
                      (c1 - c3)MONTH AS c1_minus_c3,
                      (c3 - c1)MONTH AS c3_minus_c1,
                      (c2 - c3)MONTH AS c2_minus_c3,
                      (c3 - c2)MONTH AS c3_minus_c2
                      FROM interval_tbl"""
