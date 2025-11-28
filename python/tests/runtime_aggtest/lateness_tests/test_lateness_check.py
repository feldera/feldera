from tests.runtime_aggtest.aggtst_base import TstView, TstTable


# The following LATENESS test verifies that the feature works as expected,
# And also that the tables/views with no data provided do not produce JSON NULL errors.
class lateness_lateness_tbl(TstTable):
    """Define the table used by the lateness tests"""

    def __init__(self):
        self.sql = """CREATE TABLE purchase (
                        ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
                        amount BIGINT
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
