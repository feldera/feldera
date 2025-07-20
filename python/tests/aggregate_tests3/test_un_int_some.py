from tests.aggregate_tests.aggtst_base import TstView


class aggtst_un_int_some(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": True,
                "c2": True,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": True,
                "c7": True,
                "c8": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_some AS SELECT
                      SOME(c1 > 61) AS c1,
                      SOME(c2 > 45) AS c2,
                      SOME(c3 > 12876) AS c3,
                      SOME(c4 > 13532) AS c4,
                      SOME(c5 > 709123456) AS c5,
                      SOME(c6 > 651238977) AS c6,
                      SOME(c7 > 367192837461) AS c7,
                      SOME(c8 > 265928374652) AS c8
                      FROM un_int_tbl"""


class aggtst_un_int_some_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": True,
                "c2": True,
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
                "c8": True,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_some_gby AS SELECT
                      id,
                      SOME(c1 > 61) AS c1,
                      SOME(c2 > 45) AS c2,
                      SOME(c3 > 12876) AS c3,
                      SOME(c4 > 13532) AS c4,
                      SOME(c5 > 709123456) AS c5,
                      SOME(c6 > 651238977) AS c6,
                      SOME(c7 > 367192837461) AS c7,
                      SOME(c8 > 265928374652) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""


class aggtst_un_int_some_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": True,
                "c2": True,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": True,
                "c7": True,
                "c8": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_some_distinct AS SELECT
                      SOME(DISTINCT c1 > 61) AS c1,
                      SOME(DISTINCT c2 > 45) AS c2,
                      SOME(DISTINCT c3 > 12876) AS c3,
                      SOME(DISTINCT c4 > 13532) AS c4,
                      SOME(DISTINCT c5 > 709123456) AS c5,
                      SOME(DISTINCT c6 > 651238977) AS c6,
                      SOME(DISTINCT c7 > 367192837461) AS c7,
                      SOME(DISTINCT c8 > 265928374652) AS c8
                      FROM un_int_tbl"""


class aggtst_un_int_some_distinct_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": True,
                "c2": True,
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
                "c8": True,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_some_distinct_gby AS SELECT
                      id,
                      SOME(DISTINCT c1 > 61) AS c1,
                      SOME(DISTINCT c2 > 45) AS c2,
                      SOME(DISTINCT c3 > 12876) AS c3,
                      SOME(DISTINCT c4 > 13532) AS c4,
                      SOME(DISTINCT c5 > 709123456) AS c5,
                      SOME(DISTINCT c6 > 651238977) AS c6,
                      SOME(DISTINCT c7 > 367192837461) AS c7,
                      SOME(DISTINCT c8 > 265928374652) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""


class aggtst_un_int_some_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": False,
                "c2": True,
                "c3": True,
                "c4": True,
                "c5": True,
                "c6": True,
                "c7": True,
                "c8": True,
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_some_where AS SELECT
                      SOME(c1 > 61) FILTER(WHERE c7 IS NOT NULL) AS c1,
                      SOME(c2 > 45) FILTER(WHERE c7 IS NOT NULL) AS c2,
                      SOME(c3 > 12876) FILTER(WHERE c7 IS NOT NULL) AS c3,
                      SOME(c4 > 13532) FILTER(WHERE c7 IS NOT NULL) AS c4,
                      SOME(c5 > 709123456) FILTER(WHERE c7 IS NOT NULL) AS c5,
                      SOME(c6 > 651238977) FILTER(WHERE c7 IS NOT NULL) AS c6,
                      SOME(c7 > 367192837461) FILTER(WHERE c7 IS NOT NULL) AS c7,
                      SOME(c8 > 265928374652) FILTER(WHERE c7 IS NOT NULL) AS c8
                      FROM un_int_tbl"""


class aggtst_un_int_some_where_groupby(TstView):
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
        self.sql = """CREATE MATERIALIZED VIEW un_int_some_where_gby AS SELECT
                      id,
                      SOME(c1 > 61) FILTER(WHERE c7 IS NOT NULL) AS c1,
                      SOME(c2 > 45) FILTER(WHERE c7 IS NOT NULL) AS c2,
                      SOME(c3 > 12876) FILTER(WHERE c7 IS NOT NULL) AS c3,
                      SOME(c4 > 13532) FILTER(WHERE c7 IS NOT NULL) AS c4,
                      SOME(c5 > 709123456) FILTER(WHERE c7 IS NOT NULL) AS c5,
                      SOME(c6 > 651238977) FILTER(WHERE c7 IS NOT NULL) AS c6,
                      SOME(c7 > 367192837461) FILTER(WHERE c7 IS NOT NULL) AS c7,
                      SOME(c8 > 265928374652) FILTER(WHERE c7 IS NOT NULL) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
