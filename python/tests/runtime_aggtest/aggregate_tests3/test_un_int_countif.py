from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_un_int_countif(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": 1, "c2": 3, "c3": 2, "c4": 3, "c5": 3, "c6": 3, "c7": 1, "c8": 3}
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_countif AS SELECT
                      COUNTIF(c1 > 61) AS c1,
                      COUNTIF(c2 > 45) AS c2,
                      COUNTIF(c3 > 12876) AS c3,
                      COUNTIF(c4 > 13532) AS c4,
                      COUNTIF(c5 > 709123456) AS c5,
                      COUNTIF(c6 > 651238977) AS c6,
                      COUNTIF(c7 > 367192837461) AS c7,
                      COUNTIF(c8 > 265928374652) AS c8
                      FROM un_int_tbl"""


class aggtst_un_int_countif_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": 1,
                "c2": 1,
                "c3": 1,
                "c4": 2,
                "c5": 2,
                "c6": 1,
                "c7": 1,
                "c8": 2,
            },
            {
                "id": 1,
                "c1": 0,
                "c2": 2,
                "c3": 1,
                "c4": 1,
                "c5": 1,
                "c6": 2,
                "c7": 0,
                "c8": 1,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_countif_gby AS SELECT
                      id,
                      COUNTIF(c1 > 61) AS c1,
                      COUNTIF(c2 > 45) AS c2,
                      COUNTIF(c3 > 12876) AS c3,
                      COUNTIF(c4 > 13532) AS c4,
                      COUNTIF(c5 > 709123456) AS c5,
                      COUNTIF(c6 > 651238977) AS c6,
                      COUNTIF(c7 > 367192837461) AS c7,
                      COUNTIF(c8 > 265928374652) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""


class aggtst_un_int_countif_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": 1, "c2": 3, "c3": 2, "c4": 3, "c5": 3, "c6": 3, "c7": 1, "c8": 3}
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_countif_distinct AS SELECT
                      COUNTIF(DISTINCT c1 > 61) AS c1,
                      COUNTIF(DISTINCT c2 > 45) AS c2,
                      COUNTIF(DISTINCT c3 > 12876) AS c3,
                      COUNTIF(DISTINCT c4 > 13532) AS c4,
                      COUNTIF(DISTINCT c5 > 709123456) AS c5,
                      COUNTIF(DISTINCT c6 > 651238977) AS c6,
                      COUNTIF(DISTINCT c7 > 367192837461) AS c7,
                      COUNTIF(DISTINCT c8 > 265928374652) AS c8
                      FROM un_int_tbl"""


class aggtst_un_int_countif_distinct_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": 1,
                "c2": 1,
                "c3": 1,
                "c4": 2,
                "c5": 2,
                "c6": 1,
                "c7": 1,
                "c8": 2,
            },
            {
                "id": 1,
                "c1": 0,
                "c2": 2,
                "c3": 1,
                "c4": 1,
                "c5": 1,
                "c6": 2,
                "c7": 0,
                "c8": 1,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_countif_distinct_gby AS SELECT
                      id,
                      COUNTIF(DISTINCT c1 > 61) AS c1,
                      COUNTIF(DISTINCT c2 > 45) AS c2,
                      COUNTIF(DISTINCT c3 > 12876) AS c3,
                      COUNTIF(DISTINCT c4 > 13532) AS c4,
                      COUNTIF(DISTINCT c5 > 709123456) AS c5,
                      COUNTIF(DISTINCT c6 > 651238977) AS c6,
                      COUNTIF(DISTINCT c7 > 367192837461) AS c7,
                      COUNTIF(DISTINCT c8 > 265928374652) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""


class aggtst_un_int_countif_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"c1": 0, "c2": 1, "c3": 2, "c4": 2, "c5": 2, "c6": 2, "c7": 1, "c8": 1}
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_countif_where AS SELECT
                      COUNTIF(c1 > 61) FILTER(WHERE c7 IS NOT NULL) AS c1,
                      COUNTIF(c2 > 45) FILTER(WHERE c7 IS NOT NULL) AS c2,
                      COUNTIF(c3 > 12876) FILTER(WHERE c7 IS NOT NULL) AS c3,
                      COUNTIF(c4 > 13532) FILTER(WHERE c7 IS NOT NULL) AS c4,
                      COUNTIF(c5 > 709123456) FILTER(WHERE c7 IS NOT NULL) AS c5,
                      COUNTIF(c6 > 651238977) FILTER(WHERE c7 IS NOT NULL) AS c6,
                      COUNTIF(c7 > 367192837461) FILTER(WHERE c7 IS NOT NULL) AS c7,
                      COUNTIF(c8 > 265928374652) FILTER(WHERE c7 IS NOT NULL) AS c8
                      FROM un_int_tbl"""


class aggtst_un_int_countif_where_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": 0,
                "c2": 0,
                "c3": 1,
                "c4": 1,
                "c5": 1,
                "c6": 1,
                "c7": 1,
                "c8": 1,
            },
            {
                "id": 1,
                "c1": 0,
                "c2": 1,
                "c3": 1,
                "c4": 1,
                "c5": 1,
                "c6": 1,
                "c7": 0,
                "c8": 0,
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_countif_where_gby AS SELECT
                      id,
                      COUNTIF(c1 > 61) FILTER(WHERE c7 IS NOT NULL) AS c1,
                      COUNTIF(c2 > 45) FILTER(WHERE c7 IS NOT NULL) AS c2,
                      COUNTIF(c3 > 12876) FILTER(WHERE c7 IS NOT NULL) AS c3,
                      COUNTIF(c4 > 13532) FILTER(WHERE c7 IS NOT NULL) AS c4,
                      COUNTIF(c5 > 709123456) FILTER(WHERE c7 IS NOT NULL) AS c5,
                      COUNTIF(c6 > 651238977) FILTER(WHERE c7 IS NOT NULL) AS c6,
                      COUNTIF(c7 > 367192837461) FILTER(WHERE c7 IS NOT NULL) AS c7,
                      COUNTIF(c8 > 265928374652) FILTER(WHERE c7 IS NOT NULL) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""
