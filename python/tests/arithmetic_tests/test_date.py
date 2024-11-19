from tests.aggregate_tests.aggtst_base import TstView


class arithtst_date_minus_date(TstView):
    def __init__(self):
        # Validated on Postgres
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
        ]
        self.sql = """CREATE MATERIALIZED VIEW date_minus_date_seconds AS SELECT
                      id,
                      TIMESTAMPDIFF(SECOND, d(), d() + c1_minus_c2) AS c1_minus_c2_seconds
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
