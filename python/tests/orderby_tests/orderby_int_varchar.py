from tests.aggregate_tests.aggtst_base import TstView


class orderby_asc_int_asc_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_int_asc_varchar AS
                       SELECT c1, c2
                       FROM orderby_tbl_int_varchar
                       ORDER BY c1 ASC, c2 ASC
                       LIMIT 3"""
        # Returns the following:
        # [{'c1': None, 'c2': 'fred says konichiwa'}, {'c1': None, 'c2': 'see you later'}, {'c1': 1, 'c2': 'ferris says ciao'}]


class orderby_asc_int_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_int_varchar AS
                       SELECT c1, c2
                       FROM orderby_tbl_int_varchar
                       ORDER BY c1 ASC
                       LIMIT 3"""
        # Returns the following:
        # [{'c1': None, 'c2': 'see you later'}, {'c1': None, 'c2': 'fred says konichiwa'}, {'c1': 1, 'c2': 'ferris says ciao'}]


class orderby_asc_varchar_asc_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_varchar_asc_int AS
                       SELECT c2, c1
                       FROM orderby_tbl_int_varchar
                       ORDER BY c2 ASC, c1 ASC
                       LIMIT 7"""
        # Returns the following:
        # [{'c2': None, 'c1': 2}, {'c2': None, 'c1': 3}, {'c2': 'bye bye, friend!!', 'c1': 2}, {'c2': 'ferris says ciao', 'c1': 1}, {'c2': 'fred says konichiwa', 'c1': None}, {'c2': 'hello', 'c1': 1}, {'c2': 'hello', 'c1': 3}]


class orderby_asc_varchar_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_varchar_int AS
                       SELECT c2, c1
                       FROM orderby_tbl_int_varchar
                       ORDER BY c2 ASC
                       LIMIT 7"""
        # Returns the following:
        # [{'c2': None, 'c1': 2}, {'c2': None, 'c1': 3}, {'c2': 'bye bye, friend!!', 'c1': 2}, {'c2': 'ferris says ciao', 'c1': 1}, {'c2': 'fred says konichiwa', 'c1': None}, {'c2': 'hello', 'c1': 3}, {'c2': 'hello', 'c1': 1}]


class orderby_asc_no_nulls_int_and_var(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_asc_no_nulls_int_and_var AS
                       SELECT c1, c2
                       FROM orderby_tbl_int_varchar
                       WHERE c1 IS NOT NULL AND c2 IS NOT NULL
                       ORDER BY c1 ASC, c2 ASC
                       LIMIT 3"""


class orderby_desc_int_desc_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_int_desc_varchar AS
                       SELECT c1, c2
                       FROM orderby_tbl_int_varchar
                       ORDER BY c1 DESC, c2 DESC
                       LIMIT 3"""


class orderby_desc_nulls_first_var_and_int(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_nulls_first_var_and_int AS
                       SELECT c2, c1
                       FROM orderby_tbl_int_varchar
                       ORDER BY c2 DESC NULLS FIRST, c1 DESC NULLS FIRST
                       LIMIT 3"""


class orderby_nulls_first_int_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_nulls_first_int_varchar AS
                       SELECT c1, c2
                       FROM orderby_tbl_int_varchar
                       ORDER BY c1 NULLS FIRST, c2 NULLS FIRST
                       LIMIT 5"""


class orderby_desc_no_nulls_int_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_desc_no_nulls_int_and_varchar AS
                       SELECT c1, c2
                       FROM orderby_tbl_int_varchar
                       WHERE c1 IS NOT NULL AND c2 IS NOT NULL
                       ORDER BY c1 DESC, c2 DESC"""


class orderby_nulls_last_int_varchar(TstView):
    def __init__(self):
        # Validated on SQLite
        self.data = []
        self.sql = """CREATE MATERIALIZED VIEW orderby_nulls_last_int_varchar AS
                       SELECT c1, c2
                       FROM orderby_tbl_int_varchar
                       ORDER BY c1 NULLS LAST, c2 NULLS LAST
                       LIMIT 9"""
