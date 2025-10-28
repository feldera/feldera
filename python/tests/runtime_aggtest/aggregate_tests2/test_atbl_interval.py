from tests.runtime_aggtest.aggtst_base import TstTable, TstView


class aggtst_interval_table(TstTable):
    """Define the table used by the view atbl_interval"""

    def __init__(self):
        self.sql = """CREATE FUNCTION d()
                      RETURNS TIMESTAMP NOT NULL AS
                      CAST('1970-01-01 00:00:00' AS TIMESTAMP);

                      CREATE TABLE interval_tbl(
                      id INT NOT NULL,
                      c1 TIMESTAMP,
                      c2 TIMESTAMP,
                      c3 TIMESTAMP)"""

        self.data = [
            {
                "id": 0,
                "c1": "2014-11-05 08:27:00",
                "c2": "2024-12-05 12:45:00",
                "c3": "2020-03-14 15:41:00",
            },
            {
                "id": 0,
                "c1": "2020-06-21 14:00:00",
                "c2": "1970-01-01 14:33:00",
                "c3": "2023-04-19 11:25:00",
            },
            {
                "id": 1,
                "c1": "1969-03-17 07:01:00",
                "c2": "2015-09-07 01:20:00",
                "c3": "1987-04-29 02:14:00",
            },
            {
                "id": 1,
                "c1": "2020-06-21 14:00:00",
                "c2": "1970-01-01 14:33:00",
                "c3": "2021-11-03 06:33:00",
            },
            {
                "id": 1,
                "c1": "2024-12-05 09:15:00",
                "c2": "2014-11-05 16:30:00",
                "c3": "2017-08-25 16:15:00",
            },
        ]


class aggtst_atbl_interval_seconds(TstView):
    """Define the view used by interval tests as input"""

    def __init__(self):
        # Result validation is not required for local views
        self.data = []

        self.sql = """CREATE LOCAL VIEW atbl_interval_seconds AS SELECT
                      id,
                      (c1 - c2)SECOND AS c1_minus_c2,
                      (c2 - c1)SECOND AS c2_minus_c1,
                      (c1 - c3)SECOND AS c1_minus_c3,
                      (c3 - c1)SECOND AS c3_minus_c1,
                      (c2 - c3)SECOND AS c2_minus_c3,
                      (c3 - c2)SECOND AS c3_minus_c2
                      FROM interval_tbl"""


class aggtst_atbl_interval_seconds_res(TstView):
    """Interval seconds converted to varchar"""

    def __init__(self):
        self.data = [
            {
                "id": 0,
                "f_c1": "+1592695620.000000",
                "f_c2": "-1592695620.000000",
                "f_c3": "-89155500.000000",
                "f_c4": "+89155500.000000",
                "f_c5": "-1681851120.000000",
                "f_c6": "+1681851120.000000",
            },
            {
                "id": 0,
                "f_c1": "-318226680.000000",
                "f_c2": "+318226680.000000",
                "f_c3": "-169024440.000000",
                "f_c4": "+169024440.000000",
                "f_c5": "+149202240.000000",
                "f_c6": "-149202240.000000",
            },
            {
                "id": 1,
                "f_c1": "+1592695620.000000",
                "f_c2": "-1592695620.000000",
                "f_c3": "-43173180.000000",
                "f_c4": "+43173180.000000",
                "f_c5": "-1635868800.000000",
                "f_c6": "+1635868800.000000",
            },
            {
                "id": 1,
                "f_c1": "+318185100.000000",
                "f_c2": "-318185100.000000",
                "f_c3": "+229712400.000000",
                "f_c4": "-229712400.000000",
                "f_c5": "-88472700.000000",
                "f_c6": "+88472700.000000",
            },
            {
                "id": 1,
                "f_c1": "-1466619540.000000",
                "f_c2": "+1466619540.000000",
                "f_c3": "-571691580.000000",
                "f_c4": "+571691580.000000",
                "f_c5": "+894927960.000000",
                "f_c6": "-894927960.000000",
            },
        ]

        self.sql = """CREATE MATERIALIZED VIEW atbl_interval_seconds_res AS SELECT
                      id,
                      CAST(c1_minus_c2 AS VARCHAR) AS f_c1,
                      CAST(c2_minus_c1 AS VARCHAR) AS f_c2,
                      CAST(c1_minus_c3 AS VARCHAR) AS f_c3,
                      CAST(c3_minus_c1 AS VARCHAR) AS f_c4,
                      CAST(c2_minus_c3 AS VARCHAR) AS f_c5,
                      CAST(c3_minus_c2 AS VARCHAR) AS f_c6
                      FROM atbl_interval_seconds"""


class aggtst_atbl_interval_months(TstView):
    """Define the view used by interval tests as input"""

    def __init__(self):
        # Result validation is not required for local views
        self.data = []

        self.sql = """CREATE LOCAL VIEW atbl_interval_months AS SELECT
                      id,
                      (c1 - c2)MONTH AS c1_minus_c2,
                      (c2 - c1)MONTH AS c2_minus_c1,
                      (c1 - c3)MONTH AS c1_minus_c3,
                      (c3 - c1)MONTH AS c3_minus_c1,
                      (c2 - c3)MONTH AS c2_minus_c3,
                      (c3 - c2)MONTH AS c3_minus_c2
                      FROM interval_tbl"""


class aggtst_atbl_interval_months_res(TstView):
    """Interval months converted to varchar"""

    def __init__(self):
        self.data = [
            {
                "id": 0,
                "f_c1": "+605",
                "f_c2": "-605",
                "f_c3": "-33",
                "f_c4": "+33",
                "f_c5": "-639",
                "f_c6": "+639",
            },
            {
                "id": 0,
                "f_c1": "-121",
                "f_c2": "+121",
                "f_c3": "-64",
                "f_c4": "+64",
                "f_c5": "+56",
                "f_c6": "-56",
            },
            {
                "id": 1,
                "f_c1": "+120",
                "f_c2": "-120",
                "f_c3": "+87",
                "f_c4": "-87",
                "f_c5": "-33",
                "f_c6": "+33",
            },
            {
                "id": 1,
                "f_c1": "+605",
                "f_c2": "-605",
                "f_c3": "-16",
                "f_c4": "+16",
                "f_c5": "-622",
                "f_c6": "+622",
            },
            {
                "id": 1,
                "f_c1": "-557",
                "f_c2": "+557",
                "f_c3": "-217",
                "f_c4": "+217",
                "f_c5": "+340",
                "f_c6": "-340",
            },
        ]

        self.sql = """CREATE MATERIALIZED VIEW atbl_interval_months_res AS SELECT
                      id,
                      CAST(c1_minus_c2 AS VARCHAR) AS f_c1,
                      CAST(c2_minus_c1 AS VARCHAR) AS f_c2,
                      CAST(c1_minus_c3 AS VARCHAR) AS f_c3,
                      CAST(c3_minus_c1 AS VARCHAR) AS f_c4,
                      CAST(c2_minus_c3 AS VARCHAR) AS f_c5,
                      CAST(c3_minus_c2 AS VARCHAR) AS f_c6
                      FROM atbl_interval_months"""


# Equivalent SQL for Postgres

# CREATE TABLE interval_tbl (
#     id INT,
#     c1 TIMESTAMPTZ NOT NULL,
#     c2 TIMESTAMPTZ,
#     c3 TIMESTAMPTZ
# );

# INSERT INTO interval_tbl (id, c1, c2, c3) VALUES
# (0, '2014-11-05 08:27:00+00', '2024-12-05 12:45:00+00', '2020-03-14 15:41:00+00'),
# (0, '2020-06-21 14:00:00+00', '1970-01-01 14:33:00+00', '2023-04-19 11:25:00+00'),
# (1, '1969-03-17 07:01:00+00', '2015-09-07 01:20:00+00', '1987-04-29 02:14:00+00'),
# (1, '2020-06-21 14:00:00+00', '1970-01-01 14:33:00+00', '2021-11-03 06:33:00+00'),
# (1, '2024-12-05 09:15:00+00', '2014-11-05 16:30:00+00', '2017-08-25 16:15:00+00');

# CREATE TABLE atbl_interval AS
# SELECT
#     id,
#     (c1 - c2) AS c1_minus_c2,
#     (c2 - c1) AS c2_minus_c1,
#     (c1 - c3) AS c1_minus_c3,
#     (c3 - c1) AS c3_minus_c1,
#     (c2 - c3) AS c2_minus_c3,
#     (c3 - c2) AS c3_minus_c2
# FROM interval_tbl;

# CREATE TABLE agg_view AS
# SELECT
#     aggregate(c1_minus_c2) AS f_c1,
#     aggregate(c2_minus_c1) AS f_c2,
#     aggregate(c1_minus_c3) AS f_c3,
#     aggregate(c3_minus_c1) AS f_c4,
#     aggregate(c2_minus_c3) AS f_c5,
#     aggregate(c3_minus_c2) AS f_c6
# FROM atbl_interval;

# For interval seconds:
# SELECT
#     EXTRACT(EPOCH FROM f_c1) AS m_c1_seconds,
#     EXTRACT(EPOCH FROM f_c2) AS m_c2_seconds,
#     EXTRACT(EPOCH FROM f_c3) AS m_c3_seconds,
#     EXTRACT(EPOCH FROM f_c4) AS m_c4_seconds,
#     EXTRACT(EPOCH FROM f_c5) AS m_c5_seconds,
#     EXTRACT(EPOCH FROM f_c6) AS m_c6_seconds
# FROM agg_view;

# For interval months:
# SELECT
#   EXTRACT(YEAR FROM f_c1) * 12 + EXTRACT(MONTH FROM f_c1) AS f_c1_months,
#   EXTRACT(YEAR FROM f_c2) * 12 + EXTRACT(MONTH FROM f_c2) AS f_c2_months,
#   EXTRACT(YEAR FROM f_c3) * 12 + EXTRACT(MONTH FROM f_c3) AS f_c3_months,
#   EXTRACT(YEAR FROM f_c4) * 12 + EXTRACT(MONTH FROM f_c4) AS f_c4_months,
#   EXTRACT(YEAR FROM f_c5) * 12 + EXTRACT(MONTH FROM f_c5) AS f_c5_months,
#   EXTRACT(YEAR FROM f_c6) * 12 + EXTRACT(MONTH FROM f_c6) AS f_c6_months
# FROM agg_view;
