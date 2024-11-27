from tests.aggregate_tests.aggtst_base import TstView


class arithtst_timestamp_minus_timestamp(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW timestamp_minus_timestamp AS SELECT
                      id,
                      (c1-c2)SECOND AS c1_minus_c2
                      FROM timestamp_tbl"""


class arithtst_timestamp_minus_timestamp_seconds(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1_minus_c2_seconds": 160342920},
            {"id": 1, "c1_minus_c2_seconds": -84686400},
            {"id": 2, "c1_minus_c2_seconds": 332907420},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_minus_timestamp_seconds AS SELECT
                      id,
                      CAST((c1_minus_c2) AS BIGINT) AS c1_minus_c2_seconds
                      FROM timestamp_minus_timestamp"""


# Equivalent SQL for Postgres
# CREATE TABLE timestamp_minus_timestamp AS
# SELECT
#     id,
#     c1 - c2 AS c1_minus_c2_days
# FROM timestamp_tbl;
#
# SELECT
#     id,
#     EXTRACT(EPOCH FROM c1_minus_c2_days) AS c1_minus_c2_seconds
# FROM timestamp_minus_timestamp ;


class arithtst_timestamp_minus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "2019-11-05T08:27:00", "c2": "2013-11-05T12:45:00"},
            {"id": 1, "c1": "2020-05-22T14:00:00", "c2": "2022-02-26T18:00:00"},
            {"id": 2, "c1": "1959-05-22T11:32:00", "c2": "1947-12-03T09:15:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_minus_interval AS SELECT
                      id,
                      c1 - INTERVAL '2592000' SECOND AS c1,
                      c2 - INTERVAL '31536000' SECOND AS c2
                      FROM timestamp_tbl"""


class arithtst_timestamp_plus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "2020-01-04T08:27:00", "c2": "2015-11-05T12:45:00"},
            {"id": 1, "c1": "2020-07-21T14:00:00", "c2": "2024-02-26T18:00:00"},
            {"id": 2, "c1": "1959-07-21T11:32:00", "c2": "1949-12-02T09:15:00"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW timestamp_plus_interval AS SELECT
                      id,
                      c1 + INTERVAL '2592000' SECOND AS c1,
                      c2 + INTERVAL '31536000' SECOND AS c2
                      FROM timestamp_tbl"""


# Equivalent SQL for Postgres
# SELECT
#     (c1 + INTERVAL '2592000 second') AS c1,
#     (c2 + INTERVAL '31536000 second') AS c2
# FROM timestamp_tbl;
