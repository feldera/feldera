from tests.runtime_aggtest.aggtst_base import TstView
from decimal import Decimal


class asof_test1(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_intt": 10, "t2_intt": 5},
            {"id": 2, "t1_intt": 15, "t2_intt": None},
            {"id": 3, "t1_intt": 20, "t2_intt": None},
            {"id": 4, "t1_intt": 25, "t2_intt": 12},
            {"id": 5, "t1_intt": None, "t2_intt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test1 AS SELECT
                        t1.id, t1.intt AS t1_intt, t2.intt AS t2_intt
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt >= t2.intt )
                        ON t1.id = t2.id;"""


class asof_test2(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_str": "apple", "t2_str": None},
            {"id": 2, "t1_str": "cat", "t2_str": None},
            {"id": 3, "t1_str": "dog", "t2_str": "ciao"},
            {"id": 4, "t1_str": "firefly", "t2_str": "c you!"},
            {"id": 5, "t1_str": None, "t2_str": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test2 AS SELECT
                        t1.id, t1.str AS t1_str, t2.str AS t2_str
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION ( t1.str >= t2.str )
                        ON t1.id = t2.id;"""


class asof_test3(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_decimall": Decimal("-1111.52"), "t2_decimall": None},
            {
                "id": 2,
                "t1_decimall": Decimal("-0.52"),
                "t2_decimall": Decimal("-256.25"),
            },
            {"id": 3, "t1_decimall": Decimal("-123.45"), "t2_decimall": None},
            {"id": 4, "t1_decimall": Decimal("0.00"), "t2_decimall": None},
            {"id": 5, "t1_decimall": None, "t2_decimall": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test3 AS SELECT
                        t1.id, t1.decimall AS t1_decimall, t2.decimall AS t2_decimall
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION ( t1.decimall >= t2.decimall )
                        ON t1.id = t2.id;"""


class asof_test4(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_reall": Decimal("-57681.18"), "t2_reall": None},
            {"id": 2, "t1_reall": Decimal("2.56"), "t2_reall": Decimal("-0.1234567")},
            {"id": 3, "t1_reall": Decimal("0.5"), "t2_reall": Decimal("-987.0")},
            {"id": 4, "t1_reall": Decimal("0.0"), "t2_reall": None},
            {"id": 5, "t1_reall": None, "t2_reall": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test4 AS SELECT
                        t1.id, t1.reall AS t1_reall, t2.reall AS t2_reall
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION ( t1.reall >= t2.reall )
                        ON t1.id = t2.id;"""


class asof_test5(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_dbl": Decimal("-38.2711234601246"), "t2_dbl": None},
            {"id": 2, "t1_dbl": Decimal("-0.82711234601246"), "t2_dbl": None},
            {"id": 3, "t1_dbl": Decimal("0.125"), "t2_dbl": Decimal("-999.9999999")},
            {"id": 4, "t1_dbl": Decimal("0.0"), "t2_dbl": None},
            {"id": 5, "t1_dbl": None, "t2_dbl": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test5 AS SELECT
                        t1.id, t1.dbl AS t1_dbl, t2.dbl AS t2_dbl
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION ( t1.dbl >= t2.dbl)
                        ON t1.id = t2.id;"""


class asof_test6(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_booll": False, "t2_booll": None},
            {"id": 2, "t1_booll": True, "t2_booll": False},
            {"id": 3, "t1_booll": False, "t2_booll": None},
            {"id": 4, "t1_booll": True, "t2_booll": False},
            {"id": 5, "t1_booll": None, "t2_booll": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test6 AS SELECT
                        t1.id, t1.booll AS t1_booll, t2.booll AS t2_booll
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION ( t1.booll >= t2.booll)
                        ON t1.id = t2.id;"""


class asof_test7(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_bin": "0a1620", "t2_bin": None},
            {"id": 2, "t1_bin": "10172c", "t2_bin": "0f3716"},
            {"id": 3, "t1_bin": "11172c", "t2_bin": "0c1037"},
            {"id": 4, "t1_bin": "16172c", "t2_bin": None},
            {"id": 5, "t1_bin": None, "t2_bin": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test7 AS SELECT
                        t1.id, t1.bin AS t1_bin, t2.bin AS t2_bin
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION ( t1.bin >= t2.bin)
                        ON t1.id = t2.id;"""


class asof_test8(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_tme": "13:23:44.456", "t2_tme": None},
            {"id": 2, "t1_tme": "19:23:44.456", "t2_tme": None},
            {"id": 3, "t1_tme": "01:23:44.456", "t2_tme": "00:23:44.456"},
            {"id": 4, "t1_tme": "23:23:44.456", "t2_tme": "22:23:44.456"},
            {"id": 5, "t1_tme": None, "t2_tme": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test8 AS SELECT
                        t1.id, t1.tme AS t1_tme, t2.tme AS t2_tme
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION ( t1.tme >= t2.tme)
                        ON t1.id = t2.id;"""


class asof_test9(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_tmestmp": "2000-06-21T14:23:44.123", "t2_tmestmp": None},
            {"id": 2, "t1_tmestmp": "2019-06-21T14:23:44.123", "t2_tmestmp": None},
            {
                "id": 3,
                "t1_tmestmp": "1978-06-21T14:23:44.123",
                "t2_tmestmp": "1977-06-21T14:23:44.123",
            },
            {
                "id": 4,
                "t1_tmestmp": "2002-06-21T14:23:44.123",
                "t2_tmestmp": "2001-06-21T14:23:44.123",
            },
            {"id": 5, "t1_tmestmp": None, "t2_tmestmp": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test9 AS SELECT
                        t1.id, t1.tmestmp AS t1_tmestmp, t2.tmestmp AS t2_tmestmp
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION ( t1.tmestmp >= t2.tmestmp)
                        ON t1.id = t2.id;"""


class asof_test10(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_datee": "2000-06-21", "t2_datee": None},
            {"id": 2, "t1_datee": "2019-06-21", "t2_datee": None},
            {"id": 3, "t1_datee": "1978-06-21", "t2_datee": "1977-06-21"},
            {"id": 4, "t1_datee": "2002-06-21", "t2_datee": "2001-06-21"},
            {"id": 5, "t1_datee": None, "t2_datee": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test10 AS SELECT
                        t1.id, t1.datee AS t1_datee, t2.datee AS t2_datee
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION ( t1.datee >= t2.datee)
                        ON t1.id = t2.id;"""


class asof_test11(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {
                "id": 1,
                "t1_uuidd": "a3b0c442-98fc-1c14-9af7-4c2b95f9c16a",
                "t2_uuidd": None,
            },
            {
                "id": 2,
                "t1_uuidd": "e3b0c442-98fc-1c14-9af7-4c2b95f9c16a",
                "t2_uuidd": "a3b0c442-98fc-1c14-9af7-4c2b95f9c16a",
            },
            {
                "id": 3,
                "t1_uuidd": "a9b8c7d6-e5f4-3210-9999-abcdefabcdef",
                "t2_uuidd": None,
            },
            {
                "id": 4,
                "t1_uuidd": "ffffffff-ffff-ffff-ffff-ffffffffffff",
                "t2_uuidd": "efffffff-ffff-ffff-ffff-ffffffffffff",
            },
            {"id": 5, "t1_uuidd": None, "t2_uuidd": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test11 AS SELECT
                        t1.id, t1.uuidd AS t1_uuidd, t2.uuidd AS t2_uuidd
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION ( t1.uuidd >= t2.uuidd)
                        ON t1.id = t2.id;"""


class asof_test12(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_arr": ["-0.14", "friends", "See you!"], "t2_arr": None},
            {
                "id": 2,
                "t1_arr": ["42", "sample", "-1.1", "2022-03-03", "yes"],
                "t2_arr": ["12", "sample", "-1.1", "2022-03-03", "yes"],
            },
            {"id": 3, "t1_arr": ["hello", "123", "0.0", None], "t2_arr": None},
            {
                "id": 4,
                "t1_arr": ["end", "2099", "12", "31"],
                "t2_arr": ["and", "2099", "12", "31"],
            },
            {"id": 5, "t1_arr": None, "t2_arr": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test12 AS SELECT
                        t1.id, t1.arr AS t1_arr, t2.arr AS t2_arr
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION ( t1.arr >= t2.arr)
                        ON t1.id = t2.id;"""


class asof_test13(TstView):
    def __init__(self):
        # Validated on DuckDB
        self.data = [
            {"id": 1, "t1_mapp": {"a": 15, "b": None}, "t2_mapp": None},
            {"id": 2, "t1_mapp": {"a": 3, "b": 9}, "t2_mapp": {"a": 1, "b": 9}},
            {"id": 3, "t1_mapp": {"a": 11, "b": 22}, "t2_mapp": None},
            {"id": 4, "t1_mapp": {"a": 200, "b": 200}, "t2_mapp": {"a": 100, "b": 200}},
            {"id": 5, "t1_mapp": None, "t2_mapp": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_test13 AS SELECT
                        t1.id, t1.mapp AS t1_mapp, t2.mapp AS t2_mapp
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION ( t1.mapp >= t2.mapp)
                        ON t1.id = t2.id;"""
