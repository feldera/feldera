from tests.aggregate_tests.aggtst_base import TstView


# CONCAT function
class illarg_concat_legal(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"str": "hello hello "}]
        self.sql = """CREATE MATERIALIZED VIEW concat_legal AS SELECT
                      CONCAT(str, str) AS str
                      FROM illegal_tbl"""


class illarg_concat_wrong_type(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "intt": "-12-12",
                "decimall": "-1111.52-1111.52",
                "reall": "-57681.18-57681.18",
                "dbl": "-38.2711234601246-38.2711234601246",
                "booll": "TRUETRUE",
                "tmestmp": "2020-06-21 14:23:442020-06-21 14:23:44",
                "uuidd": "42b8fec7-c7a3-4531-9611-4bde80f9cb4c42b8fec7-c7a3-4531-9611-4bde80f9cb4c",
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW concat_wrong_type AS SELECT
                      CONCAT(intt, intt) AS intt,
                      CONCAT(decimall, decimall) AS decimall,
                      CONCAT(reall, reall) AS reall,
                      CONCAT(dbl, dbl) AS dbl,
                      CONCAT(booll, booll) AS booll,
                      CONCAT(tmestmp, tmestmp) AS tmestmp,
                      CONCAT(uuidd, uuidd) AS uuidd
                      FROM illegal_tbl"""


# Negative Test
class illarg_concat_illegal(TstView):
    def __init__(self):
        # checked manually
        self.sql = """CREATE MATERIALIZED VIEW concat_illegal AS SELECT
                      CONCAT(bin, bin) AS bin
                      FROM illegal_tbl"""
        self.expected_error = "Not yet implemented"
