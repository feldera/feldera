from tests.aggregate_tests.aggtst_base import TstView, TstTable


class orderby_tbl(TstTable):
    """Define the table used by the order by/limit tests"""

    def __init__(self):
        self.sql = """CREATE TABLE orderby_tbl(
                      c1 INT)"""
        self.data = [
            {"c1": 3},
            {"c1": 2},
            {"c1": None},
            {"c1": 2},
            {"c1": 1},
            {"c1": 14},
            {"c1": 3},
            {"c1": 6},
            {"c1": 1},
            {"c1": None},
            {"c1": 4},
        ]


class orderby_asc(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {"c1": 14},
            {"c1": 1},
            {"c1": 1},
            {"c1": 2},
            {"c1": 2},
            {"c1": 3},
            {"c1": 3},
            {"c1": 4},
            {"c1": 6},
            {"c1": None},
            {"c1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc AS
                       SELECT c1
                       FROM orderby_tbl
                       ORDER BY c1 ASC"""


class orderby_asc_limit1(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [{"c1": None}]
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_limit1 AS
                       SELECT c1
                       FROM orderby_tbl
                       ORDER BY c1 ASC
                       LIMIT 1"""


class orderby_asc_limit2(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [{"c1": None}, {"c1": None}]
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_limit2 AS
                       SELECT c1
                       FROM orderby_tbl
                       ORDER BY c1 ASC
                       LIMIT 2"""


class orderby_asc_limit3(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [{"c1": 1}, {"c1": None}, {"c1": None}]
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_limit3 AS
                       SELECT c1
                       FROM orderby_tbl
                       ORDER BY c1 ASC
                       LIMIT 3"""


class orderby_asc_nulls_last(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": 1}, {"c1": 1}, {"c1": 2}]
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_nulls_last AS
                       SELECT c1
                       FROM orderby_tbl
                       ORDER BY c1 ASC
                       NULLS LAST
                       LIMIT 3"""


class orderby_desc(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [
            {"c1": 1},
            {"c1": 1},
            {"c1": 14},
            {"c1": 2},
            {"c1": 2},
            {"c1": 3},
            {"c1": 3},
            {"c1": 4},
            {"c1": 6},
            {"c1": None},
            {"c1": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc AS
                       SELECT c1
                       FROM orderby_tbl
                       ORDER BY c1 DESC"""


class orderby_desc_limit1(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [{"c1": 14}]
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_limit1 AS
                       SELECT c1
                       FROM orderby_tbl
                       ORDER BY c1 DESC
                       LIMIT 1"""


class orderby_desc_limit2(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [{"c1": 14}, {"c1": 6}]
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_limit2 AS
                       SELECT c1
                       FROM orderby_tbl
                       ORDER BY c1 DESC
                       LIMIT 2"""


class orderby_desc_limit3(TstView):
    def __init__(self):
        # Validated on MySQL
        self.data = [{"c1": 14}, {"c1": 6}, {"c1": 4}]
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_limit3 AS
                       SELECT c1
                       FROM orderby_tbl
                       ORDER BY c1 DESC
                       LIMIT 3"""


class orderby_desc_nulls_first(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": 14}, {"c1": None}, {"c1": None}]
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_nulls_first AS
                       SELECT c1
                       FROM orderby_tbl
                       ORDER BY c1 DESC
                       NULLS FIRST
                       LIMIT 3"""


class orderby_nulls_first(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": 1}, {"c1": 1}, {"c1": 2}, {"c1": None}, {"c1": None}]
        self.sql = """CREATE MATERIALIZED VIEW orderby_nulls_first AS
                       SELECT c1
                       FROM orderby_tbl
                       ORDER BY c1 NULLS FIRST
                       LIMIT 5"""


class orderby_nulls_last(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": 14},
            {"c1": 1},
            {"c1": 1},
            {"c1": 2},
            {"c1": 2},
            {"c1": 3},
            {"c1": 3},
            {"c1": 4},
            {"c1": 6},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_nulls_last AS
                       SELECT c1
                       FROM orderby_tbl
                       ORDER BY c1 NULLS LAST
                       LIMIT 9"""
