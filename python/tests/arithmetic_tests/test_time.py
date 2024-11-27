from tests.aggregate_tests.aggtst_base import TstView


class arithtst_time_minus_time(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW time_minus_time AS SELECT
                      id,
                      (c1-c2)SECOND AS c1_minus_c2
                      FROM time_tbl"""


class arithtst_time_minus_time_seconds(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'c1_minus_c2_seconds': 20700},
            {'id': 1, 'c1_minus_c2_seconds': -21600}
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_minus_time_seconds AS SELECT
                      id, 
                      CAST((c1_minus_c2) AS BIGINT) AS c1_minus_c2_seconds
                      FROM time_minus_time"""

# Equivalent SQL for Postgres
# CREATE TABLE time_sub_time AS
# SELECT
#     id,
#     (c1 - c2) AS c1_minus_c2_days
# FROM time_tbl;
#
# SELECT
#     id,
#     EXTRACT(EPOCH FROM c1_minus_c2_days) AS c1_minus_c2_seconds
# FROM time_sub_time;


class arithtst_time_minus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'c1': '18:20:00', 'c2': '08:45:00'},
            {'id': 1, 'c1': '07:50:00', 'c2': '10:00:00'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_minus_interval AS SELECT
                      id,
                      c1 - INTERVAL '10' MINUTES AS c1,
                      c2 - INTERVAL '4' HOUR AS c2
                      FROM time_tbl"""


class arithtst_time_plus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {'id': 0, 'c1': '18:40:00', 'c2': '16:45:00'},
            {'id': 1, 'c1': '08:10:00', 'c2': '18:00:00'}
        ]
        self.sql = """CREATE MATERIALIZED VIEW time_plus_interval AS SELECT
                      id,
                      c1 + INTERVAL '10' MINUTES AS c1,
                      c2 + INTERVAL '4' HOUR AS c2
                      FROM time_tbl"""


# Equivalent SQL for Postgres
# SELECT
#     (c1 + INTERVAL '10 minute') AS c1,
#     (c2 + INTERVAL '4 hour') AS c2
# FROM time_tbl;