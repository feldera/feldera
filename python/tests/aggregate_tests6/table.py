from tests.aggregate_tests.aggtst_base import TstTable, TstView


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
                      c3 TIMESTAMP)WITH ('append_only' = 'true')"""

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

# SELECT
#     EXTRACT(EPOCH FROM f_c1) AS m_c1_seconds,
#     EXTRACT(EPOCH FROM f_c2) AS m_c2_seconds,
#     EXTRACT(EPOCH FROM f_c3) AS m_c3_seconds,
#     EXTRACT(EPOCH FROM f_c4) AS m_c4_seconds,
#     EXTRACT(EPOCH FROM f_c5) AS m_c5_seconds,
#     EXTRACT(EPOCH FROM f_c6) AS m_c6_seconds
# FROM agg_view;


class aggtst_binary_table(TstTable):
    """Define the table used by the binary tests"""

    def __init__(self):
        self.sql = """CREATE TABLE binary_tbl(
                      id INT,
                      c1 BINARY(4),
                      c2 BINARY(4) NULL)WITH ('append_only' = 'true')"""
        self.data = [
            {"id": 0, "c1": [12, 22, 32], "c2": None},
            {"id": 0, "c1": [23, 56, 33, 21], "c2": [55, 66, 77, 88]},
            {"id": 1, "c1": [23, 56, 33, 21], "c2": [99, 20, 31, 77]},
            {"id": 1, "c1": [49, 43, 84, 29], "c2": [32, 34, 22, 12]},
        ]

        # Equivalent SQL for Postgres

        # CREATE TABLE binary_tbl (
        #     id INT,
        #     c1 BYTEA,
        #     c2 BYTEA NULL
        # );

        # INSERT INTO binary_tbl (id, c1, c2) VALUES
        #     (0, '\x0c1620', NULL),
        #     (0, '\x17382115', '\x37424d58'),
        #     (1, '\x17382115', '\x63141f4d'),
        #     (1, '\x312b541d', '\x2022160c');


class aggtst_unsigned_int_table(TstTable):
    def __init__(self):
        self.sql = """CREATE TABLE un_int_tbl(
                      id INT NOT NULL,
                      c1 TINYINT UNSIGNED,
                      c2 TINYINT UNSIGNED NOT NULL,
                      c3 SMALLINT UNSIGNED,
                      c4 SMALLINT UNSIGNED NOT NULL,
                      c5 INT UNSIGNED,
                      c6 INT UNSIGNED NOT NULL,
                      c7 BIGINT UNSIGNED,
                      c8 BIGINT UNSIGNED NOT NULL)WITH ('append_only' = 'true')"""

        self.data = [
            {
                "id": 0,
                "c1": 72,
                "c2": 64,
                "c3": None,
                "c4": 16002,
                "c5": 781245123,
                "c6": 651238977,
                "c7": None,
                "c8": 284792878783,
            },
            {
                "id": 1,
                "c1": 61,
                "c2": 64,
                "c3": 14257,
                "c4": 15342,
                "c5": 963218731,
                "c6": 749321014,
                "c7": 367192837461,
                "c8": 265928374652,
            },
            {
                "id": 0,
                "c1": None,
                "c2": 45,
                "c3": 13450,
                "c4": 14123,
                "c5": 812347981,
                "c6": 698123417,
                "c7": 419283746512,
                "c8": 283746512983,
            },
            {
                "id": 1,
                "c1": None,
                "c2": 48,
                "c3": 12876,
                "c4": 13532,
                "c5": 709123456,
                "c6": 786452310,
                "c7": None,
                "c8": 274839201928,
            },
        ]


class aggtst_varbinary_table(TstTable):
    """Define the table used by the varbinary tests"""

    def __init__(self):
        self.sql = """CREATE TABLE varbinary_tbl(
                      id INT,
                      c1 VARBINARY,
                      c2 VARBINARY NULL)WITH ('append_only' = 'true')"""
        self.data = [
            {"id": 0, "c1": [12, 22, 32], "c2": None},
            {"id": 0, "c1": [23, 56, 33, 21], "c2": [55, 66, 77, 88]},
            {"id": 1, "c1": [23, 56, 33, 21], "c2": [99, 20, 31, 77]},
            {"id": 1, "c1": [49, 43, 84, 29, 11], "c2": [32, 34]},
        ]

        # Equivalent SQL for Postgres

        # CREATE TABLE varbinary_tbl (
        #     id INT,
        #     c1 BYTEA,
        #     c2 BYTEA NULL
        # );

        # INSERT INTO varbinary_tbl (id, c1, c2) VALUES
        #     (0, '\x0c1620', NULL),
        #     (0, '\x17382115', '\x37424d58'),
        #     (1, '\x17382115', '\x63141f4d'),
        #     (1, '\x312b541d0b', '\x2022');


class aggtst_array_tbl(TstTable):
    """Define the table used by the array tests"""

    def __init__(self):
        self.sql = """CREATE TABLE array_tbl(
                      id INT,
                      c1 INT ARRAY NOT NULL,
                      c2 INT ARRAY,
                      c3 MAP<VARCHAR, INT> ARRAY)WITH ('append_only' = 'true')"""
        self.data = [
            {"id": 0, "c1": [12, 22], "c2": None, "c3": [{"a": 5, "b": 66}]},
            {"id": 0, "c1": [23, 56, 16], "c2": [55, 66, None], "c3": [{"c": 2}]},
            {"id": 1, "c1": [23, 56, 16], "c2": [99], "c3": None},
            {"id": 1, "c1": [49], "c2": [32, 34, 22, 12], "c3": [{"x": 1}]},
        ]


class aggtst_map_tbl(TstTable):
    """Define the table used by the MAP tests"""

    def __init__(self):
        self.sql = """CREATE TABLE map_tbl(
                      id INT,
                      c1 MAP<VARCHAR, INT> NOT NULL,
                      c2 MAP<VARCHAR, INT>)WITH ('append_only' = 'true')"""
        self.data = [
            {"id": 0, "c1": {"a": 75, "b": 66}, "c2": None},
            {"id": 0, "c1": {"q": 11, "v": 66}, "c2": {"q": 22}},
            {"id": 1, "c1": {"x": 8, "y": 6}, "c2": {"i": 5, "j": 66}},
            {"id": 1, "c1": {"f": 45, "h": 66}, "c2": {"f": 1}},
            {"id": 1, "c1": {"q": 11, "v": 66}, "c2": {"q": 11, "v": 66, "x": None}},
        ]


class aggtst_row_tbl(TstTable):
    """Define the table used by the ROW tests"""

    def __init__(self):
        self.sql = """CREATE TABLE row_tbl(
                      id INT,
                      c1 INT NOT NULL,
                      c2 VARCHAR,
                      c3 VARCHAR)WITH ('append_only' = 'true')"""
        self.data = [
            {"id": 0, "c1": 4, "c2": None, "c3": "adios"},
            {"id": 0, "c1": 3, "c2": "ola", "c3": "ciao"},
            {"id": 1, "c1": 7, "c2": "hi", "c3": "hiya"},
            {"id": 1, "c1": 2, "c2": "elo", "c3": "ciao"},
            {"id": 1, "c1": 2, "c2": "elo", "c3": "ciao"},
        ]
