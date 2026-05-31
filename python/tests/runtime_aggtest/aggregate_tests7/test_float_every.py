from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_float_every(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": False, "c2": True, "c3": False, "c4": True}]
        self.sql = """
        CREATE MATERIALIZED VIEW float_every AS
        SELECT
            EVERY(c1 IS NOT NULL) AS c1,
            EVERY(c2 IS NOT NULL) AS c2,
            EVERY(c3 IS NOT NULL) AS c3,
            EVERY(c4 IS NOT NULL) AS c4
        FROM float_tbl
        """


class aggtst_float_every_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": False, "c2": True, "c3": True, "c4": True},
            {"id": 1, "c1": True, "c2": True, "c3": False, "c4": True},
        ]
        self.sql = """
        CREATE MATERIALIZED VIEW float_every_gby AS
        SELECT
            id,
            EVERY(c1 IS NOT NULL) AS c1,
            EVERY(c2 IS NOT NULL) AS c2,
            EVERY(c3 IS NOT NULL) AS c3,
            EVERY(c4 IS NOT NULL) AS c4
        FROM float_tbl
        GROUP BY id
        """


class aggtst_float_every_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": False, "c2": True, "c3": False, "c4": True}]
        self.sql = """
        CREATE MATERIALIZED VIEW float_every_distinct AS
        SELECT
            EVERY(DISTINCT c1 IS NOT NULL) AS c1,
            EVERY(DISTINCT c2 IS NOT NULL) AS c2,
            EVERY(DISTINCT c3 IS NOT NULL) AS c3,
            EVERY(DISTINCT c4 IS NOT NULL) AS c4
        FROM float_tbl
        """


class aggtst_float_every_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": False, "c2": True, "c3": True, "c4": True},
            {"id": 1, "c1": True, "c2": True, "c3": False, "c4": True},
        ]
        self.sql = """
        CREATE MATERIALIZED VIEW float_every_distinct_gby AS
        SELECT
            id,
            EVERY(DISTINCT c1 IS NOT NULL) AS c1,
            EVERY(DISTINCT c2 IS NOT NULL) AS c2,
            EVERY(DISTINCT c3 IS NOT NULL) AS c3,
            EVERY(DISTINCT c4 IS NOT NULL) AS c4
        FROM float_tbl
        GROUP BY id
        """


class aggtst_float_every_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": True, "c2": True, "c3": True, "c4": True}]
        self.sql = """
        CREATE MATERIALIZED VIEW float_every_where AS
        SELECT
            EVERY(c1 IS NOT NULL) FILTER(WHERE c2 < 2) AS c1,
            EVERY(c2 IS NOT NULL) FILTER(WHERE c2 < 2) AS c2,
            EVERY(c3 IS NOT NULL) FILTER(WHERE c2 < 2) AS c3,
            EVERY(c4 IS NOT NULL) FILTER(WHERE c2 < 2) AS c4
        FROM float_tbl
        """


class aggtst_float_every_where_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": True, "c2": True, "c3": True, "c4": True},
            {"id": 1, "c1": True, "c2": True, "c3": True, "c4": True},
        ]

        self.sql = """
        CREATE MATERIALIZED VIEW float_every_where_gby AS
        SELECT
            id,
            EVERY(c1 IS NOT NULL) FILTER(WHERE c2 < 2) AS c1,
            EVERY(c2 IS NOT NULL) FILTER(WHERE c2 < 2) AS c2,
            EVERY(c3 IS NOT NULL) FILTER(WHERE c2 < 2) AS c3,
            EVERY(c4 IS NOT NULL) FILTER(WHERE c2 < 2) AS c4
        FROM float_tbl
        GROUP BY id
        """
