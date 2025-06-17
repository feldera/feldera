from tests.aggregate_tests.aggtst_base import TstView


class orderby_asc_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_int AS
                       SELECT c1
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c1 ASC"""


class orderby_asc_limit3_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_limit3_int AS
                       SELECT c1
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c1 ASC
                       LIMIT 3"""


class orderby_asc_nulls_last_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_nulls_last_int AS
                       SELECT c1
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c1 ASC
                       NULLS LAST
                       LIMIT 3"""


class orderby_asc_no_nulls_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_no_nulls_int AS
                       SELECT c1
                       FROM orderby_tbl_sqlite_int_varchar
                       WHERE c1 IS NOT NULL
                       ORDER BY c1 ASC"""


class orderby_desc_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_int AS
                       SELECT c1
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c1 DESC"""


class orderby_desc_limit3_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_limit3_int AS
                       SELECT c1
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c1 DESC
                       LIMIT 3"""


class orderby_desc_nulls_first_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_nulls_first_int AS
                       SELECT c1
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c1 DESC
                       NULLS FIRST
                       LIMIT 3"""


class orderby_nulls_first_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_nulls_first_int AS
                       SELECT c1
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c1 NULLS FIRST
                       LIMIT 3"""


class orderby_desc_no_nulls_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_no_nulls_int AS
                       SELECT c1
                       FROM orderby_tbl_sqlite_int_varchar
                       WHERE c1 IS NOT NULL
                       ORDER BY c1 DESC"""


class orderby_nulls_last_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_nulls_last_int AS
                       SELECT c1
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c1 NULLS LAST
                       LIMIT 9"""
