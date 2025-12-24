from tests.runtime_aggtest.aggtst_base import TstView


class asof_multi_asof_joins(TstView):
    def __init__(self):
        # Validated on DUCKDB
        self.data = [
            {"id": 1, "t1_intt": 10, "t2_intt": None},
            {"id": 2, "t1_intt": 15, "t2_intt": None},
            {"id": 3, "t1_intt": 20, "t2_intt": None},
            {"id": 4, "t1_intt": 25, "t2_intt": None},
            {"id": 5, "t1_intt": None, "t2_intt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_multi_asof_joins AS SELECT
                      t1.id, t1.intt AS t1_intt, t2.intt AS t2_intt
                      FROM asof_tbl1 t1
                      LEFT ASOF JOIN asof_tbl2 t2
                      MATCH_CONDITION (t1.intt >= t2.intt)
                      ON t1.id = t2.id
                      AND t1.decimall = t2.decimall
                      LEFT ASOF JOIN asof_tbl3 t3
                      MATCH_CONDITION (t2.intt >= t3.intt)
                      ON t2.id = t3.id
                      AND t2.booll = t3.booll;"""


class asof_cmpx_on(TstView):
    def __init__(self):
        # Validated on DUCKDB
        self.data = [
            {"id": 1, "intt": 10},
            {"id": 2, "intt": 15},
            {"id": 3, "intt": 20},
            {"id": 4, "intt": 25},
            {"id": 5, "intt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_cmpx_on AS SELECT
                      t1.id, t1.intt
                      FROM asof_tbl1 t1
                      LEFT ASOF JOIN asof_tbl2 t2
                      MATCH_CONDITION (t1.intt >= t2.intt)
                      ON t1.arr = t2.arr
                      AND t1.mapp = t2.mapp;"""


class asof_cmpx_agg_on(TstView):
    def __init__(self):
        # Validated on DUCKDB
        self.data = [
            {"id": 1, "t1_sum": 10, "t2_sum": None},
            {"id": 2, "t1_sum": 15, "t2_sum": None},
            {"id": 3, "t1_sum": 20, "t2_sum": None},
            {"id": 4, "t1_sum": 25, "t2_sum": None},
            {"id": 5, "t1_sum": None, "t2_sum": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_cmpx_agg_on AS
                        WITH t1_agg AS (
                            SELECT id, SUM(intt) AS sum_intt
                            FROM asof_tbl1
                            GROUP BY id
                        ),
                        t2_agg AS (
                            SELECT id, SUM(intt) AS sum_intt
                            FROM asof_tbl2
                            GROUP BY id
                        )
                        SELECT t1_agg.id, t1_agg.sum_intt AS t1_sum, t2_agg.sum_intt AS t2_sum
                        FROM t1_agg
                        LEFT ASOF JOIN t2_agg
                        MATCH_CONDITION (t1_agg.sum_intt >= t2_agg.sum_intt)
                        ON t1_agg.sum_intt = t2_agg.sum_intt;"""


class asof_same_match_on_condition(TstView):
    def __init__(self):
        # Validated on DUCKDB
        self.data = [
            {"intt": 10},
            {"intt": 15},
            {"intt": 20},
            {"intt": 25},
            {"intt": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_same_match_on_condition AS
                      WITH t1_desc AS (
                        SELECT intt FROM asof_tbl1 ORDER BY intt DESC
                      )
                      SELECT t1.intt
                      FROM t1_desc t1
                      LEFT ASOF JOIN asof_tbl2 t2
                      MATCH_CONDITION (t1.intt >= t2.intt)
                      ON t1.intt = t2.intt;"""


class asof_computed_int_on(TstView):
    def __init__(self):
        # Validated on DUCKDB
        self.data = [
            {"id": 1, "t1_intt": 10, "interval_yr": 20},
            {"id": 2, "t1_intt": 15, "interval_yr": 2},
            {"id": 3, "t1_intt": 20, "interval_yr": -1},
            {"id": 4, "t1_intt": 25, "interval_yr": -1},
            {"id": 5, "t1_intt": None, "interval_yr": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_computed_int_on AS
                      WITH joined AS (
                        SELECT t1.id,
                               t1.intt AS t1_intt,
                               t2.intt AS t2_intt,
                               TIMESTAMPDIFF(YEAR, t1.tmestmp, t2.tmestmp) AS interval_yr
                        FROM asof_tbl1 t1
                        JOIN asof_tbl2 t2
                        ON t1.id = t2.id
                      )
                      SELECT j.id, j.t1_intt, interval_yr
                      FROM joined j
                      LEFT ASOF JOIN asof_tbl3 t3
                      MATCH_CONDITION (j.interval_yr>= t3.intt)
                      ON j.id = t3.id
                      AND j.t1_intt = t3.intt;"""


class asof_null_on_multiasof(TstView):
    def __init__(self):
        # Validated on DUCKDB
        self.data = [{"id": 5, "t1_intt": None, "t2_intt": None}]
        self.sql = """CREATE MATERIALIZED VIEW asof_null_on_multiasof AS SELECT
                      t1.id, t1.intt AS t1_intt, t2.intt AS t2_intt
                      FROM asof_tbl1 t1
                      LEFT ASOF JOIN asof_tbl2 t2
                      MATCH_CONDITION (t1.intt >= t2.intt)
                      ON t1.bin = t2.bin
                      AND t1.decimall = t2.decimall
                      LEFT ASOF JOIN asof_tbl3 t3
                      MATCH_CONDITION (t2.intt >= t3.intt)
                      ON t2.uuidd = t3.uuidd
                      AND t2.booll = t3.booll
                      WHERE t1.id = 5;"""


class asof_decimal_match_dbl_on(TstView):
    def __init__(self):
        # Validated on DUCKDB
        self.data = [
            {"id": 1, "t2_decimall": None},
            {"id": 2, "t2_decimall": None},
            {"id": 3, "t2_decimall": None},
            {"id": 4, "t2_decimall": None},
            {"id": 5, "t2_decimall": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW asof_decimal_match_dbl_on AS SELECT
                      t1.id, t2.decimall AS t2_decimall
                      FROM asof_tbl1 t1
                      LEFT ASOF JOIN asof_tbl2 t2
                      MATCH_CONDITION (t1.decimall >= t2.decimall)
                      ON t1.dbl = t2.dbl;"""
