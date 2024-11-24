from tests.aggregate_tests.aggtst_base import TstView


class arithtst_date_minus_date(TstView):
    def __init__(self):
        # Result validation is not required for local views
        self.data = []
        self.sql = """CREATE LOCAL VIEW date_minus_date AS SELECT
                      id,
                      (c1-c2)SECOND AS c1_minus_c2
                      FROM date_tbl"""


class arithtst_date_minus_date_seconds(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1_minus_c2_seconds": 318211200},
            {"id": 1, "c1_minus_c2_seconds": -84672000},
            {"id": 2, "c1_minus_c2_seconds": 648518400},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_minus_date_seconds AS SELECT
                      id,
                      CAST((c1_minus_c2) AS BIGINT) AS c1_minus_c2_seconds
                      FROM date_minus_date"""


# Equivalent SQL for Postgres
# CREATE TABLE date_sub_date AS
# SELECT
#     id,
#     (c1 - c2) AS c1_minus_c2_days
# FROM date_tbl;

# SELECT
#     id,
#     c1_minus_c2_days * 86400 AS c1_minus_c2_seconds  -- Convert days to seconds
# FROM date_sub_date;


class arithtst_date_minus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "2024-11-05", "c2": "2013-11-05"},
            {"id": 1, "c1": "2020-05-22", "c2": "2022-02-26"},
            {"id": 2, "c1": "1969-05-22", "c2": "1947-12-03"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_minus_interval AS SELECT
                      id,
                      c1 - INTERVAL '2592000' SECOND AS c1,
                      c2 - INTERVAL '31536000' SECOND AS c2
                      FROM date_tbl"""


class arithtst_date_plus_interval(TstView):
    def __init__(self):
        # Validated on Postgres
        self.data = [
            {"id": 0, "c1": "2024-12-06", "c2": "2015-11-05"},
            {"id": 1, "c1": "2020-06-22", "c2": "2024-02-26"},
            {"id": 2, "c1": "1969-06-22", "c2": "1949-12-02"},
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_plus_interval AS SELECT
                      id,
                      c1 + INTERVAL '86400' SECOND AS c1,
                      c2 + INTERVAL '31536000' SECOND AS c2
                      FROM date_tbl"""


# Equivalent SQL for Postgres
# SELECT
#     id,
#     (c1 + INTERVAL '86400 second') AS c1_with_seconds,
#     (c2 + INTERVAL '31536000 second') AS c2_with_seconds
# FROM date_tbl;
