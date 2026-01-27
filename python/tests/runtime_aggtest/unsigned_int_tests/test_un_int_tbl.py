from tests.runtime_aggtest.aggtst_base import TstTable, TstView


class un_int_datagen_tbl(TstTable):
    """Define the table used by unsigned integer tests that use the datagen connector to generate data"""

    def __init__(self):
        self.sql = """CREATE TABLE un_int_datagen_tbl (
            id INT,
            tiny_int TINYINT UNSIGNED,
            small_int SMALLINT UNSIGNED,
            intt INTEGER UNSIGNED,
            big_int BIGINT UNSIGNED,
            tiny_int1 TINYINT UNSIGNED,
            small_int1 SMALLINT UNSIGNED,
            intt1 INTEGER UNSIGNED,
            big_int1 BIGINT UNSIGNED
        ) WITH (
            'connectors' = '[{
                "transport": {
                    "name": "datagen",
                    "config": {
                        "seed": 12345,
                        "plan": [{
                            "limit": 1,
                            "fields": {
                                "id": { "strategy": "increment"},
                                "tiny_int": { "strategy": "uniform", "range": [0, 255] },
                                "small_int": { "strategy": "uniform", "range": [256, 65535] },
                                "intt": { "strategy": "uniform", "range": [65536, 4294967295] },
                                "big_int": { "strategy": "uniform", "range": [4294967296, 9223372036854775807] },
                                "tiny_int1": { "strategy": "uniform", "range": [0, 255] },
                                "small_int1": { "strategy": "uniform", "range": [256, 65535] },
                                "intt1": { "strategy": "uniform", "range": [65536, 4294967295] },
                                "big_int1": { "strategy": "uniform", "range": [4294967296, 9223372036854775807] }
                            }
                        }]
                    }
                }
            }]'
        );
        """
        self.data = []


class un_int_datagen_view(TstView):
    def __init__(self):
        self.data = [
            {
                "id": 0,
                "tiny_int": 9,
                "small_int": 10192,
                "intt": 1146171806,
                "big_int": 6996571239486628517,
                "tiny_int1": 84,
                "small_int1": 15522,
                "intt1": 1457925796,
                "big_int1": 3352245520498693749,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_datagen_view AS SELECT
                      *
                      FROM un_int_datagen_tbl"""


class un_int_datagen_tbl1(TstTable):
    """Define the table used by some unsigned integer tests"""

    def __init__(self):
        self.sql = """CREATE TABLE un_int_datagen_tbl1(
                      id INT,
                      tiny_int TINYINT UNSIGNED,
                      small_int SMALLINT UNSIGNED,
                      intt INTEGER UNSIGNED,
                      big_int BIGINT UNSIGNED
                      ) with (
                      'connectors' = '[{
                        "transport": {
                          "name": "datagen",
                          "config": {
                            "plan": [{"limit": 2}]
                          }
                        }
                      }]'
                    );"""
        self.data = []


class un_int_datagen_view1(TstView):
    def __init__(self):
        self.data = [
            {"id": 0, "tiny_int": 0, "small_int": 0, "intt": 0, "big_int": 0},
            {"id": 1, "tiny_int": 1, "small_int": 1, "intt": 1, "big_int": 1},
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_datagen_view1 AS SELECT
                      *
                      FROM un_int_datagen_tbl1"""
