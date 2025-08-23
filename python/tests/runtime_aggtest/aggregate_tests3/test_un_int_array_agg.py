from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_un_int_array_agg_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": [None, None, 61, 72],
                "c2": [45, 48, 64, 64],
                "c3": [13450, 12876, 14257, None],
                "c4": [14123, 13532, 15342, 16002],
                "c5": [812347981, 709123456, 963218731, 781245123],
                "c6": [698123417, 786452310, 749321014, 651238977],
                "c7": [419283746512, None, 367192837461, None],
                "c8": [283746512983, 274839201928, 265928374652, 284792878783],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_array_agg AS SELECT
                      ARRAY_AGG(c1) AS c1,
                      ARRAY_AGG(c2) AS c2,
                      ARRAY_AGG(c3) AS c3,
                      ARRAY_AGG(c4) AS c4,
                      ARRAY_AGG(c5) AS c5,
                      ARRAY_AGG(c6) AS c6,
                      ARRAY_AGG(c7) AS c7,
                      ARRAY_AGG(c8) AS c8
                      FROM un_int_tbl"""


class aggtst_un_int_array_agg_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": [None, 72],
                "c2": [45, 64],
                "c3": [13450, None],
                "c4": [14123, 16002],
                "c5": [812347981, 781245123],
                "c6": [698123417, 651238977],
                "c7": [419283746512, None],
                "c8": [283746512983, 284792878783],
            },
            {
                "id": 1,
                "c1": [None, 61],
                "c2": [48, 64],
                "c3": [12876, 14257],
                "c4": [13532, 15342],
                "c5": [709123456, 963218731],
                "c6": [786452310, 749321014],
                "c7": [None, 367192837461],
                "c8": [274839201928, 265928374652],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_array_agg_gby AS SELECT
                      id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3, ARRAY_AGG(c4) AS c4, ARRAY_AGG(c5) AS c5, ARRAY_AGG(c6) AS c6, ARRAY_AGG(c7) AS c7, ARRAY_AGG(c8) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""


class aggtst_un_int_array_agg_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "c1": [None, 61, 72],
                "c2": [45, 48, 64],
                "c3": [None, 12876, 13450, 14257],
                "c4": [13532, 14123, 15342, 16002],
                "c5": [709123456, 781245123, 812347981, 963218731],
                "c6": [651238977, 698123417, 749321014, 786452310],
                "c7": [None, 367192837461, 419283746512],
                "c8": [265928374652, 274839201928, 283746512983, 284792878783],
            }
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_array_agg_distinct AS SELECT
                      ARRAY_AGG(DISTINCT c1) AS c1,ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3, ARRAY_AGG(DISTINCT c4) AS c4, ARRAY_AGG(DISTINCT c5) AS c5, ARRAY_AGG(DISTINCT c6) AS c6, ARRAY_AGG(DISTINCT c7) AS c7, ARRAY_AGG(DISTINCT c8) AS c8
                      FROM un_int_tbl"""


class aggtst_un_int_array_agg_distinct_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "c1": [None, 72],
                "c2": [45, 64],
                "c3": [None, 13450],
                "c4": [14123, 16002],
                "c5": [781245123, 812347981],
                "c6": [651238977, 698123417],
                "c7": [None, 419283746512],
                "c8": [283746512983, 284792878783],
            },
            {
                "id": 1,
                "c1": [None, 61],
                "c2": [48, 64],
                "c3": [12876, 14257],
                "c4": [13532, 15342],
                "c5": [709123456, 963218731],
                "c6": [749321014, 786452310],
                "c7": [None, 367192837461],
                "c8": [265928374652, 274839201928],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_array_agg_distinct_groupby AS SELECT
                      id, ARRAY_AGG(DISTINCT c1) AS c1,ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3, ARRAY_AGG(DISTINCT c4) AS c4, ARRAY_AGG(DISTINCT c5) AS c5, ARRAY_AGG(DISTINCT c6) AS c6, ARRAY_AGG(DISTINCT c7) AS c7, ARRAY_AGG(DISTINCT c8) AS c8
                      FROM un_int_tbl
                      GROUP BY id"""


class aggtst_un_int_array_agg_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "f_c1": [None, 61, 72],
                "f_c2": [48, 64, 64],
                "f_c3": [12876, 14257, None],
                "f_c4": [13532, 15342, 16002],
                "f_c5": [709123456, 963218731, 781245123],
                "f_c6": [786452310, 749321014, 651238977],
                "f_c7": [None, 367192837461, None],
                "f_c8": [274839201928, 265928374652, 284792878783],
            }
        ]

        self.sql = """CREATE MATERIALIZED VIEW un_int_array_agg_where AS SELECT
                      ARRAY_AGG(c1) FILTER(WHERE c2 > 45) AS f_c1,
                      ARRAY_AGG(c2) FILTER(WHERE c2 > 45) AS f_c2,
                      ARRAY_AGG(c3) FILTER(WHERE c2 > 45) AS f_c3,
                      ARRAY_AGG(c4) FILTER(WHERE c2 > 45) AS f_c4,
                      ARRAY_AGG(c5) FILTER(WHERE c2 > 45) AS f_c5,
                      ARRAY_AGG(c6) FILTER(WHERE c2 > 45) AS f_c6,
                      ARRAY_AGG(c7) FILTER(WHERE c2 > 45) AS f_c7,
                      ARRAY_AGG(c8) FILTER(WHERE c2 > 45) AS f_c8
                      FROM un_int_tbl """


class aggtst_un_int_array_agg_where_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {
                "id": 0,
                "f_c1": [72],
                "f_c2": [64],
                "f_c3": [None],
                "f_c4": [16002],
                "f_c5": [781245123],
                "f_c6": [651238977],
                "f_c7": [None],
                "f_c8": [284792878783],
            },
            {
                "id": 1,
                "f_c1": [None, 61],
                "f_c2": [48, 64],
                "f_c3": [12876, 14257],
                "f_c4": [13532, 15342],
                "f_c5": [709123456, 963218731],
                "f_c6": [786452310, 749321014],
                "f_c7": [None, 367192837461],
                "f_c8": [274839201928, 265928374652],
            },
        ]
        self.sql = """CREATE MATERIALIZED VIEW un_int_array_agg_where_gby AS SELECT
                      id,
                      ARRAY_AGG(c1) FILTER(WHERE c2 > 45) AS f_c1,
                      ARRAY_AGG(c2) FILTER(WHERE c2 > 45) AS f_c2,
                      ARRAY_AGG(c3) FILTER(WHERE c2 > 45) AS f_c3,
                      ARRAY_AGG(c4) FILTER(WHERE c2 > 45) AS f_c4,
                      ARRAY_AGG(c5) FILTER(WHERE c2 > 45) AS f_c5,
                      ARRAY_AGG(c6) FILTER(WHERE c2 > 45) AS f_c6,
                      ARRAY_AGG(c7) FILTER(WHERE c2 > 45) AS f_c7,
                      ARRAY_AGG(c8) FILTER(WHERE c2 > 45) AS f_c8
                      FROM un_int_tbl
                      GROUP BY id"""
