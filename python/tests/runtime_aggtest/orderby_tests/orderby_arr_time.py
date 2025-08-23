from tests.runtime_aggtest.aggtst_base import TstView


class orderby_arr_time1(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time1 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC, c2 ASC
                       LIMIT 3"""


class orderby_arr_time2(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time2 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC, c2 ASC NULLS FIRST
                       LIMIT 3"""


class orderby_arr_time3(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time3 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC, c2 ASC NULLS LAST
                       LIMIT 3"""


class orderby_arr_time4(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time4 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC NULLS FIRST, c2 ASC
                       LIMIT 3"""


class orderby_arr_time5(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time5 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC NULLS FIRST, c2 ASC NULLS FIRST
                       LIMIT 3"""


class orderby_arr_time6(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time6 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC NULLS FIRST, c2 ASC NULLS LAST
                       LIMIT 3"""


class orderby_arr_time7(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time7 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC NULLS LAST, c2 ASC
                       LIMIT 3"""


class orderby_arr_time8(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time8 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC NULLS LAST, c2 ASC NULLS FIRST
                       LIMIT 3"""


class orderby_arr_time9(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time9 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC NULLS LAST, c2 ASC NULLS LAST
                       LIMIT 3"""


class orderby_arr_time10(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time10 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC, c2 DESC
                       LIMIT 3"""


class orderby_arr_time11(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time11 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC, c2 DESC NULLS FIRST
                       LIMIT 3"""


class orderby_arr_time12(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time12 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC, c2 DESC NULLS LAST
                       LIMIT 3"""


class orderby_arr_time13(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time13 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC NULLS FIRST, c2 DESC
                       LIMIT 3"""


class orderby_arr_time14(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time14 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC NULLS FIRST, c2 DESC NULLS FIRST
                       LIMIT 3"""


class orderby_arr_time15(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time15 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC NULLS FIRST, c2 DESC NULLS LAST
                       LIMIT 3"""


class orderby_arr_time16(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time16 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC NULLS LAST, c2 DESC
                       LIMIT 3"""


class orderby_arr_time17(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time17 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC NULLS LAST, c2 DESC NULLS FIRST
                       LIMIT 3"""


class orderby_arr_time18(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [11, 56, 33, 21], "c2": "14:00:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
            {"c1": [12, 22, 32], "c2": "06:43:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time18 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 ASC NULLS LAST, c2 DESC NULLS LAST
                       LIMIT 3"""


class orderby_arr_time19(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time19 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC, c2 ASC
                       LIMIT 3"""


class orderby_arr_time20(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time20 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC, c2 ASC NULLS FIRST
                       LIMIT 3"""


class orderby_arr_time21(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time21 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC, c2 ASC NULLS LAST
                       LIMIT 3"""


class orderby_arr_time22(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time22 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC NULLS FIRST, c2 ASC
                       LIMIT 3"""


class orderby_arr_time23(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time23 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC NULLS FIRST, c2 ASC NULLS FIRST
                       LIMIT 3"""


class orderby_arr_time24(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time24 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC NULLS FIRST, c2 ASC NULLS LAST
                       LIMIT 3"""


class orderby_arr_time25(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time25 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC NULLS LAST, c2 ASC
                       LIMIT 3"""


class orderby_arr_time26(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time26 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC NULLS LAST, c2 ASC NULLS FIRST
                       LIMIT 3"""


class orderby_arr_time27(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time27 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC NULLS LAST, c2 ASC NULLS LAST
                       LIMIT 3"""


class orderby_arr_time28(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time28 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC, c2 DESC
                       LIMIT 3"""


class orderby_arr_time29(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time29 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC, c2 DESC NULLS FIRST
                       LIMIT 3"""


class orderby_arr_time30(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time30 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC, c2 DESC NULLS LAST
                       LIMIT 3"""


class orderby_arr_time31(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time31 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC NULLS FIRST, c2 DESC
                       LIMIT 3"""


class orderby_arr_time32(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time32 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC NULLS FIRST, c2 DESC NULLS FIRST
                       LIMIT 3"""


class orderby_arr_time33(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": None, "c2": "08:27:00"},
            {"c1": None, "c2": "17:22:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time33 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC NULLS FIRST, c2 DESC NULLS LAST
                       LIMIT 3"""


class orderby_arr_time34(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time34 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC NULLS LAST, c2 DESC
                       LIMIT 3"""


class orderby_arr_time35(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time35 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC NULLS LAST, c2 DESC NULLS FIRST
                       LIMIT 3"""


class orderby_arr_time36(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": [49, 43, 84, 29], "c2": "23:45:00"},
            {"c1": [55, 66, 77, 88], "c2": "12:45:00"},
            {"c1": [99, 20, 31, 77], "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW orderby_arr_time36 AS
                       SELECT c1, c2
                       FROM orderby_tbl_manual_arr_time
                       ORDER BY c1 DESC NULLS LAST, c2 DESC NULLS LAST
                       LIMIT 3"""
