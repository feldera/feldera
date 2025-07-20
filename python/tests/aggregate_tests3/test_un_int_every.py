from tests.aggregate_tests.aggtst_base import TstView


class aggtst_un_int_every(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": False,
                "c2": False,
                "c3": False,
                "c4": False,
                "c5": False,
                "c6": False,
                "c7": False,
                "c8": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_every AS SELECT
                      EVERY(c1 > 61) AS c1,
                      EVERY(c2 > 45) AS c2,
                      EVERY(c3 > 12876) AS c3,
                      EVERY(c4 > 13532) AS c4,
                      EVERY(c5 > 709123456) AS c5,
                      EVERY(c6 > 651238977) AS c6,
                      EVERY(c7 > 367192837461) AS c7,
                      EVERY(c8 > 265928374652) AS c8
                      FROM un_int_tbl"""


class aggtst_un_int_every_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": True,
                "c2": False,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": False,
                "c7": True,
                "c8": True,
            },
            {
                "id": 1,
                "c1": False,
                "c2": True,
                "c3": False,
                "c4": False,
                "c5": False,
                "c6": True,
                "c7": False,
                "c8": False,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_every_gby AS SELECT
                      id,
                      EVERY(c1 > 61) AS c1,
                      EVERY(c2 > 45) AS c2,
                      EVERY(c3 > 12876) AS c3,
                      EVERY(c4 > 13532) AS c4,
                      EVERY(c5 > 709123456) AS c5,
                      EVERY(c6 > 651238977) AS c6,
                      EVERY(c7 > 367192837461) AS c7,
                      EVERY(c8 > 265928374652) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""


class aggtst_un_int_every_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": False,
                "c2": False,
                "c3": False,
                "c4": False,
                "c5": False,
                "c6": False,
                "c7": False,
                "c8": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_every_distinct AS SELECT
                      EVERY(DISTINCT c1 > 61) AS c1,
                      EVERY(DISTINCT c2 > 45) AS c2,
                      EVERY(DISTINCT c3 > 12876) AS c3,
                      EVERY(DISTINCT c4 > 13532) AS c4,
                      EVERY(DISTINCT c5 > 709123456) AS c5,
                      EVERY(DISTINCT c6 > 651238977) AS c6,
                      EVERY(DISTINCT c7 > 367192837461) AS c7,
                      EVERY(DISTINCT c8 > 265928374652) AS c8
                      FROM un_int_tbl"""


class aggtst_un_int_every_distinct_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": True,
                "c2": False,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": False,
                "c7": True,
                "c8": True,
            },
            {
                "id": 1,
                "c1": False,
                "c2": True,
                "c3": False,
                "c4": False,
                "c5": False,
                "c6": True,
                "c7": False,
                "c8": False,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_every_distinct_gby AS SELECT
                      id,
                      EVERY(DISTINCT c1 > 61) AS c1,
                      EVERY(DISTINCT c2 > 45) AS c2,
                      EVERY(DISTINCT c3 > 12876) AS c3,
                      EVERY(DISTINCT c4 > 13532) AS c4,
                      EVERY(DISTINCT c5 > 709123456) AS c5,
                      EVERY(DISTINCT c6 > 651238977) AS c6,
                      EVERY(DISTINCT c7 > 367192837461) AS c7,
                      EVERY(DISTINCT c8 > 265928374652) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""


class aggtst_un_int_every_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": False,
                "c2": False,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": True,
                "c7": False,
                "c8": False,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_every_where AS SELECT
                      EVERY(c1 > 61) FILTER(WHERE c7 IS NOT NULL) AS c1,
                      EVERY(c2 > 45) FILTER(WHERE c7 IS NOT NULL) AS c2,
                      EVERY(c3 > 12876) FILTER(WHERE c7 IS NOT NULL) AS c3,
                      EVERY(c4 > 13532) FILTER(WHERE c7 IS NOT NULL) AS c4,
                      EVERY(c5 > 709123456) FILTER(WHERE c7 IS NOT NULL) AS c5,
                      EVERY(c6 > 651238977) FILTER(WHERE c7 IS NOT NULL) AS c6,
                      EVERY(c7 > 367192837461) FILTER(WHERE c7 IS NOT NULL) AS c7,
                      EVERY(c8 > 265928374652) FILTER(WHERE c7 IS NOT NULL) AS c8
                      FROM un_int_tbl"""


class aggtst_un_int_every_where_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": None,
                "c2": False,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": True,
                "c7": True,
                "c8": True,
            },
            {
                "id": 1,
                "c1": False,
                "c2": True,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": True,
                "c7": False,
                "c8": False,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_every_where_gby AS SELECT
                      id,
                      EVERY(c1 > 61) FILTER(WHERE c7 IS NOT NULL) AS c1,
                      EVERY(c2 > 45) FILTER(WHERE c7 IS NOT NULL) AS c2,
                      EVERY(c3 > 12876) FILTER(WHERE c7 IS NOT NULL) AS c3,
                      EVERY(c4 > 13532) FILTER(WHERE c7 IS NOT NULL) AS c4,
                      EVERY(c5 > 709123456) FILTER(WHERE c7 IS NOT NULL) AS c5,
                      EVERY(c6 > 651238977) FILTER(WHERE c7 IS NOT NULL) AS c6,
                      EVERY(c7 > 367192837461) FILTER(WHERE c7 IS NOT NULL) AS c7,
                      EVERY(c8 > 265928374652) FILTER(WHERE c7 IS NOT NULL) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
