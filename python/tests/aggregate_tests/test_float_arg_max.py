from .aggtst_base import TstView
from decimal import Decimal


# REAL tests (considering precision of 7 digits)
class aggtst_real_arg_max_value(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": Decimal("57681.18"), "c2": Decimal("873315.8")}]
        self.sql = """CREATE MATERIALIZED VIEW real_arg_max AS SELECT
                      RF(ARG_MAX(c1, c2)) AS c1,
                      RF(ARG_MAX(c2, c1)) AS c2
                      FROM real_tbl"""


class aggtst_real_arg_max_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None, "c2": Decimal("-38.27112")},
            {"id": 1, "c1": Decimal("57681.18"), "c2": Decimal("873315.8")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW real_arg_max_gby AS SELECT
                      id,
                      RF(ARG_MAX(c1, c2)) AS c1,
                      RF(ARG_MAX(c2, c1)) AS c2
                      FROM real_tbl
                      GROUP BY id"""


class aggtst_real_arg_max_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": Decimal("57681.18"), "c2": Decimal("873315.8")}]
        self.sql = """CREATE MATERIALIZED VIEW real_arg_max_distinct AS SELECT
                      RF(ARG_MAX(DISTINCT c1, c2)) AS c1,
                      RF(ARG_MAX(DISTINCT c2, c1)) AS c2
                      FROM real_tbl"""


class aggtst_real_arg_max_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": None, "c2": Decimal("-38.27112")},
            {"id": 1, "c1": Decimal("57681.18"), "c2": Decimal("873315.8")},
        ]
        self.sql = """CREATE MATERIALIZED VIEW real_arg_max_distinct_gby AS SELECT
                      id,
                      RF(ARG_MAX(DISTINCT c1, c2)) AS c1,
                      RF(ARG_MAX(DISTINCT c2, c1)) AS c2
                      FROM real_tbl
                      GROUP BY id"""


class aggtst_real_arg_max_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": Decimal("57681.18"), "c2": Decimal("-38.27112")}]
        self.sql = """CREATE MATERIALIZED VIEW real_arg_max_where AS SELECT
                      RF(ARG_MAX(c1, c2) FILTER(WHERE c1 > c2)) AS c1,
                      RF(ARG_MAX(c2, c1) FILTER(WHERE c1 > c2)) AS c2
                      FROM real_tbl"""


class aggtst_real_arg_max_where_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": Decimal("57681.18"), "c2": Decimal("-38.27112")},
            {"id": 1, "c1": None, "c2": None},
        ]
        self.sql = """CREATE MATERIALIZED VIEW real_arg_max_where_gby AS SELECT
                      id,
                      RF(ARG_MAX(c1, c2) FILTER(WHERE c1 > c2)) AS c1,
                      RF(ARG_MAX(c2, c1) FILTER(WHERE c1 > c2)) AS c2
                      FROM real_tbl
                      GROUP BY id"""