from tests.runtime_aggtest.aggtst_base import TstView


class asof_test_illarg1(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW asof_illarg1 AS SELECT
                        t1.id, t1.intt AS t1_intt, t2.intt AS t2_intt
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt = t2.intt )
                        ON t1.id = t2.id;"""
        self.expected_error = " ASOF JOIN MATCH_CONDITION must be a comparison between columns from the two inputs"


class asof_test_illarg2(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW asof_illarg2 AS SELECT
                        t1.id, t1.intt AS t1_intt, t2.intt AS t2_intt
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt <= t2.intt )
                        ON t1.id = t2.id;"""
        self.expected_error = "Currently the only MATCH_CONDITION comparison supported by ASOF joins is 'leftCol >= rightCol'"


class asof_test_illarg3(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW asof_illarg3 AS SELECT
                        t1.id, t1.intt AS t1_intt, t2.intt AS t2_intt
                        FROM asof_tbl1 t1
                        RIGHT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt >= t2.intt )
                        ON t1.id = t2.id;"""
        self.expected_error = "error parsing sql"


class asof_test_illarg4(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW asof_illarg4 AS SELECT
                        t1.id, t1.intt AS t1_intt, t2.intt AS t2_intt
                        FROM asof_tbl1 t1
                        ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt >= t2.intt )
                        ON t1.id = t2.id;"""
        self.expected_error = "Currently only left asof joins are supported."


class asof_test_illarg5(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg5 AS
                        WITH combined AS (SELECT
                        t1.id AS t1_id, t1.intt AS t1_intt
                        FROM asof_tbl1 t1)
                        SELECT
                        c.t1_id, c.t1_intt
                        FROM combined c
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t2.intt >= c.t1_intt)
                        ON c.t1_id = t2.id;"""
        self.expected_error = "currently the only match_condition comparison supported by asof joins is 'leftcol >= rightcol'"


class asof_test_illarg6(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW illarg6 AS SELECT
                        t1.id,
                        t2.intt AS t2_int
                        FROM asof_tbl1 AS t1
                        LEFT ASOF JOIN asof_tbl2 AS t2
                        MATCH_CONDITION (t2.intt = (SELECT t2_sub.intt))
                        ON t1.id = t2.id;"""
        self.expected_error = "table 't2_sub' not found"


class asof_on_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW asof_on_illegal AS SELECT
                        t1.id, t1.intt AS t1_intt, t2.intt AS t2_intt
                        FROM asof_tbl1 t1
                        LEFT ASOF JOIN asof_tbl2 t2
                        MATCH_CONDITION (t1.intt >= t2.intt )
                        ON t1.id != t2.id;"""
        self.expected_error = (
            "ASOF JOIN condition must be a conjunction of equality comparisons"
        )
