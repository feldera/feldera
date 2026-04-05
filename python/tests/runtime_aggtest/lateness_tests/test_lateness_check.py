from tests.runtime_aggtest.aggtst_base import TstView, TstTable


"""The following tests verify that lateness propagates through supported operations using emit-final annotation.
The tables are intentionally left empty; the tests only verify that the SQL compiles, not that the results are correct.
They also ensure that tables/views with no data do not produce JSON NULL errors."""


class lateness_lateness_tbl(TstTable):
    """Define the table used by the lateness tests"""

    def __init__(self):
        self.sql = """CREATE TABLE purchase (
                        ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
                        amount BIGINT,
                        value BIGINT LATENESS 5
                    ) WITH (
                        'append_only' = 'true'
                    )"""
        self.data = []


class lateness_lateness_check(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW daily_total_final
                    WITH ('emit_final' = 'd')
                    AS
                    SELECT
                        TIMESTAMP_TRUNC(ts, DAY) AS d,
                        SUM(amount) AS total
                    FROM purchase
                    GROUP BY TIMESTAMP_TRUNC(ts, DAY)"""


class lateness_tumble_start(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW tumble_start
                    WITH ('emit_final' = 'w_start')
                    AS
                    SELECT
                        TUMBLE_START(ts, INTERVAL '1' DAY) AS w_start,
                        SUM(value) AS total
                    FROM purchase
                    GROUP BY TUMBLE(ts, INTERVAL '1' DAY)"""


class lateness_tumble_end(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW tumble_end
                    WITH ('emit_final' = 'w_end')
                    AS
                    SELECT
                        TUMBLE_END(ts, INTERVAL '1' DAY) AS w_end,
                        SUM(value) AS total
                    FROM purchase
                    GROUP BY TUMBLE(ts, INTERVAL '1' DAY)"""


class lateness_rolling_sum_illegal(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW rolling_sum_illegal
                    AS
                    SELECT
                        ts,
                        SUM(value) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_sum
                    FROM purchase"""
        self.expected_error = "Not yet implemented"


class lateness_interval_add(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW interval_add
                    WITH ('emit_final' = 'shifted_ts')
                    AS
                    SELECT
                        ts + INTERVAL '1' HOUR AS shifted_ts,
                        amount
                    FROM purchase"""


class lateness_interval_sub(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW interval_sub
                    WITH ('emit_final' = 'shifted_ts')
                    AS
                    SELECT
                        ts - INTERVAL '1' HOUR AS shifted_ts,
                        amount
                    FROM purchase"""


class lateness_shift_trunc(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW shift_trunc
                    WITH ('emit_final' = 'd')
                    AS
                    SELECT
                        TIMESTAMP_TRUNC(ts_shift, DAY) AS d,
                        SUM(amount)
                    FROM (
                        SELECT ts + INTERVAL '2' HOUR AS ts_shift, amount
                        FROM purchase
                    )
                    GROUP BY TIMESTAMP_TRUNC(ts_shift, DAY)"""


class lateness_trunc_tumble(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW trunc_tumble
                    WITH ('emit_final' = 'w_start')
                    AS
                    SELECT
                        TUMBLE_START(d, INTERVAL '1' DAY) AS w_start,
                        COUNT(*)
                    FROM (
                        SELECT TIMESTAMP_TRUNC(ts, HOUR) AS d
                        FROM purchase
                    )
                    GROUP BY TUMBLE(d, INTERVAL '1' DAY)"""


class lateness_timestampadd(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW ts_add
                    WITH ('emit_final' = 'ts_added')
                    AS
                    SELECT
                        TIMESTAMPADD(HOUR, 1, ts) AS ts_added,
                        amount
                    FROM purchase"""


class lateness_hop(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW hop
                    WITH ('emit_final' = 'window_start')
                    AS
                    SELECT
                        window_start,
                        SUM(value) AS total
                    FROM TABLE(
                        HOP(
                            TABLE purchase,
                            DESCRIPTOR(ts),
                            INTERVAL '12' HOUR,
                            INTERVAL '1' DAY
                        )
                    )
                    GROUP BY window_start"""


class lateness_projection(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW projection
                    WITH ('emit_final' = 't')
                    AS
                    SELECT ts AS t
                    FROM purchase"""


class lateness_deep_chain(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW deep_chain
                    WITH ('emit_final' = 'd2')
                    AS
                    SELECT
                        TIMESTAMP_TRUNC(w_start, DAY) AS d2,
                        COUNT(*)
                    FROM (
                        SELECT
                            TUMBLE_START(d, INTERVAL '1' DAY) AS w_start
                        FROM (
                            SELECT
                                TIMESTAMP_TRUNC(ts + INTERVAL '3' HOUR, HOUR) AS d
                            FROM purchase
                        )
                        GROUP BY TUMBLE(d, INTERVAL '1' DAY)
                    )
                    GROUP BY TIMESTAMP_TRUNC(w_start, DAY)"""


class lateness_deep_v1(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW deep_v1 AS
                    SELECT ts + INTERVAL '1' HOUR AS t1, amount
                    FROM purchase"""


class lateness_deep_v2(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW deep_v2 AS
                    SELECT TIMESTAMP_TRUNC(t1, HOUR) AS t2, amount
                    FROM deep_v1"""


class lateness_deep_v3(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW deep_v3 AS
                    SELECT
                        TUMBLE_START(t2, INTERVAL '1' DAY) AS w_start,
                        SUM(amount) AS total
                    FROM deep_v2
                    GROUP BY TUMBLE(t2, INTERVAL '1' DAY)"""


class lateness_deep_v4(TstView):
    def __init__(self):
        self.sql = """CREATE MATERIALIZED VIEW deep_v4
                    WITH ('emit_final' = 'd')
                    AS
                    SELECT
                        TIMESTAMP_TRUNC(w_start, DAY) AS d,
                        SUM(total)
                    FROM deep_v3
                    GROUP BY TIMESTAMP_TRUNC(w_start, DAY)"""
