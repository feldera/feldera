from tests.aggregate_tests.aggtst_base import TstView


class orderby_asc_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_varchar AS
                       SELECT c2
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c2 ASC"""


class orderby_asc_limit3_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_limit3_varchar AS
                       SELECT c2
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c2 ASC
                       LIMIT 3"""


class orderby_asc_nulls_last_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_nulls_last_varchar AS
                       SELECT c2
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c2 ASC
                       NULLS LAST
                       LIMIT 3"""


class orderby_asc_no_nulls_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_no_nulls_varchar AS
                       SELECT c2
                       FROM orderby_tbl_sqlite_int_varchar
                       WHERE c2 IS NOT NULL
                       ORDER BY c1 ASC"""


class orderby_desc_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_varchar AS
                       SELECT c2
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c2 DESC"""


class orderby_desc_limit3_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_limit3_varchar AS
                       SELECT c2
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c2 DESC
                       LIMIT 3"""


class orderby_desc_nulls_first_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_nulls_first_varchar AS
                       SELECT c2
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c2 DESC
                       NULLS FIRST
                       LIMIT 3"""


class orderby_nulls_first_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_nulls_first_varchar AS
                       SELECT c2
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c2 NULLS FIRST
                       LIMIT 3"""


class orderby_desc_no_nulls_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_no_nulls_varchar AS
                       SELECT c2
                       FROM orderby_tbl_sqlite_int_varchar
                       WHERE c2 IS NOT NULL
                       ORDER BY c1 DESC"""


class orderby_nulls_last_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_nulls_last_varchar AS
                       SELECT c2
                       FROM orderby_tbl_sqlite_int_varchar
                       ORDER BY c2 NULLS LAST
                       LIMIT 9"""
