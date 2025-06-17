from tests.aggregate_tests.aggtst_base import TstView


class orderby_binary_timestamp1(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp1 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC, c2 ASC
                       LIMIT 3"""


class orderby_binary_timestamp2(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp2 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC, c2 ASC NULLS FIRST
                       LIMIT 3"""


class orderby_binary_timestamp3(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp3 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC, c2 ASC NULLS LAST
                       LIMIT 3"""


class orderby_binary_timestamp4(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp4 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC NULLS FIRST, c2 ASC
                       LIMIT 3"""


class orderby_binary_timestamp5(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp5 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC NULLS FIRST, c2 ASC NULLS FIRST
                       LIMIT 3"""


class orderby_binary_timestamp6(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp6 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC NULLS FIRST, c2 ASC NULLS LAST
                       LIMIT 3"""


class orderby_binary_timestamp7(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": "0c1620", "c2": "1987-06-05T06:43:00"},
            {"c1": "0c1620", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp7 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC NULLS LAST, c2 ASC
                       LIMIT 3"""


class orderby_binary_timestamp8(TstView):
    def __init__(self):
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": "0c1620", "c2": "1987-06-05T06:43:00"},
            {"c1": "0c1620", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp8 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC NULLS LAST, c2 ASC NULLS FIRST
                       LIMIT 3"""


class orderby_binary_timestamp9(TstView):
    def __init__(self):
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": "0c1620", "c2": "1987-06-05T06:43:00"},
            {"c1": "0c1620", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp9 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC NULLS LAST, c2 ASC NULLS LAST
                       LIMIT 3"""


class orderby_binary_timestamp10(TstView):
    def __init__(self):
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp10 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC, c2 DESC
                       LIMIT 3"""


class orderby_binary_timestamp11(TstView):
    def __init__(self):
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp11 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC, c2 DESC NULLS FIRST
                       LIMIT 3"""


class orderby_binary_timestamp12(TstView):
    def __init__(self):
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp12 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC, c2 DESC NULLS LAST
                       LIMIT 3"""


class orderby_binary_timestamp13(TstView):
    def __init__(self):
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp13 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC NULLS FIRST, c2 DESC
                       LIMIT 3"""


class orderby_binary_timestamp14(TstView):
    def __init__(self):
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp14 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC NULLS FIRST, c2 DESC NULLS FIRST
                       LIMIT 3"""


class orderby_binary_timestamp15(TstView):
    def __init__(self):
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp15 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC NULLS FIRST, c2 DESC NULLS LAST LIMIT 3"""


class orderby_binary_timestamp16(TstView):
    def __init__(self):
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": "0c1620", "c2": "1987-06-05T06:43:00"},
            {"c1": "0c1620", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp16 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC NULLS LAST, c2 DESC LIMIT 3"""


class orderby_binary_timestamp17(TstView):
    def __init__(self):
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": "0c1620", "c2": "1987-06-05T06:43:00"},
            {"c1": "0c1620", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp17 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC NULLS LAST, c2 DESC NULLS FIRST LIMIT 3"""


class orderby_binary_timestamp18(TstView):
    def __init__(self):
        self.data = [
            {"c1": "0a0c1c0e", "c2": "2007-12-15T20:20:00"},
            {"c1": "0c1620", "c2": "1987-06-05T06:43:00"},
            {"c1": "0c1620", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp18 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 ASC NULLS LAST, c2 DESC NULLS LAST LIMIT 3"""


class orderby_binary_timestamp19(TstView):
    def __init__(self):
        self.data = [
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp19 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC, c2 ASC LIMIT 3"""


class orderby_binary_timestamp20(TstView):
    def __init__(self):
        self.data = [
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp20 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC, c2 ASC NULLS FIRST LIMIT 3"""


class orderby_binary_timestamp21(TstView):
    def __init__(self):
        self.data = [
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp21 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC, c2 ASC NULLS LAST LIMIT 3"""


class orderby_binary_timestamp22(TstView):
    def __init__(self):
        self.data = [
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp22 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC NULLS FIRST, c2 ASC LIMIT 3"""


class orderby_binary_timestamp23(TstView):
    def __init__(self):
        self.data = [
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp23 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC NULLS FIRST, c2 ASC NULLS FIRST LIMIT 3"""


class orderby_binary_timestamp24(TstView):
    def __init__(self):
        self.data = [
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp24 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC NULLS FIRST, c2 ASC NULLS LAST LIMIT 3"""


class orderby_binary_timestamp25(TstView):
    def __init__(self):
        self.data = [
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp25 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC NULLS LAST, c2 ASC LIMIT 3"""


class orderby_binary_timestamp26(TstView):
    def __init__(self):
        self.data = [
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp26 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC NULLS LAST, c2 ASC NULLS FIRST LIMIT 3"""


class orderby_binary_timestamp27(TstView):
    def __init__(self):
        self.data = [
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp27 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC NULLS LAST, c2 ASC NULLS LAST LIMIT 3"""


class orderby_binary_timestamp28(TstView):
    def __init__(self):
        self.data = [
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp28 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC, c2 DESC LIMIT 3"""


class orderby_binary_timestamp29(TstView):
    def __init__(self):
        self.data = [
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp29 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC, c2 DESC NULLS FIRST LIMIT 3"""


class orderby_binary_timestamp30(TstView):
    def __init__(self):
        self.data = [
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp30 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC, c2 DESC NULLS LAST LIMIT 3"""


class orderby_binary_timestamp31(TstView):
    def __init__(self):
        self.data = [
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp31 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC NULLS FIRST, c2 DESC LIMIT 3"""


class orderby_binary_timestamp32(TstView):
    def __init__(self):
        self.data = [
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp32 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC NULLS FIRST, c2 DESC NULLS FIRST LIMIT 3"""


class orderby_binary_timestamp33(TstView):
    def __init__(self):
        self.data = [
            {"c1": None, "c2": "1965-12-11T17:22:00"},
            {"c1": None, "c2": "2020-06-21T14:00:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp33 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC NULLS FIRST, c2 DESC NULLS LAST LIMIT 3"""


class orderby_binary_timestamp34(TstView):
    def __init__(self):
        self.data = [
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp34 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC NULLS LAST, c2 DESC LIMIT 3"""


class orderby_binary_timestamp35(TstView):
    def __init__(self):
        self.data = [
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp35 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC NULLS LAST, c2 DESC NULLS FIRST LIMIT 3"""


class orderby_binary_timestamp36(TstView):
    def __init__(self):
        self.data = [
            {"c1": "312b541d", "c2": "2014-11-15T23:45:00"},
            {"c1": "37424d58", "c2": "2024-12-05T12:45:00"},
            {"c1": "63141f4d", "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_binary_timestamp36 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_binary_ts
                       ORDER BY c1 DESC NULLS LAST, c2 DESC NULLS LAST LIMIT 3"""
