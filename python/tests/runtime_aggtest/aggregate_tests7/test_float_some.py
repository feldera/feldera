from tests.runtime_aggtest.aggtst_base import TstView


class aggtst_float_some(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": True, "c2": False, "c3": True, "c4": False}]
        self.sql = """
        CREATE MATERIALIZED VIEW float_some AS
        SELECT
            SOME(c1 IS NULL) AS c1,
            SOME(c2 IS NULL) AS c2,
            SOME(c3 IS NULL) AS c3,
            SOME(c4 IS NULL) AS c4
        FROM float_tbl
        """


class aggtst_float_some_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": True, "c2": False, "c3": False, "c4": False},
            {"id": 1, "c1": False, "c2": False, "c3": True, "c4": False},
        ]
        self.sql = """
        CREATE MATERIALIZED VIEW float_some_gby AS
        SELECT
            id,
            SOME(c1 IS NULL) AS c1,
            SOME(c2 IS NULL) AS c2,
            SOME(c3 IS NULL) AS c3,
            SOME(c4 IS NULL) AS c4
        FROM float_tbl
        GROUP BY id
        """


class aggtst_float_some_distinct(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": True, "c2": False, "c3": True, "c4": False}]
        self.sql = """
        CREATE MATERIALIZED VIEW float_some_distinct AS
        SELECT
            SOME(DISTINCT c1 IS NULL) AS c1,
            SOME(DISTINCT c2 IS NULL) AS c2,
            SOME(DISTINCT c3 IS NULL) AS c3,
            SOME(DISTINCT c4 IS NULL) AS c4
        FROM float_tbl
        """


class aggtst_float_some_distinct_gby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": True, "c2": False, "c3": False, "c4": False},
            {"id": 1, "c1": False, "c2": False, "c3": True, "c4": False},
        ]
        self.sql = """
        CREATE MATERIALIZED VIEW float_some_distinct_gby AS
        SELECT
            id,
            SOME(DISTINCT c1 IS NULL) AS c1,
            SOME(DISTINCT c2 IS NULL) AS c2,
            SOME(DISTINCT c3 IS NULL) AS c3,
            SOME(DISTINCT c4 IS NULL) AS c4
        FROM float_tbl
        GROUP BY id
        """


class aggtst_float_some_where(TstView):
    def __init__(self):
        # checked manually
        self.data = [{"c1": False, "c2": False, "c3": False, "c4": False}]
        self.sql = """
        CREATE MATERIALIZED VIEW float_some_where AS
        SELECT
            SOME(c1 IS NULL) FILTER(WHERE c2 < 2) AS c1,
            SOME(c2 IS NULL) FILTER(WHERE c2 < 2) AS c2,
            SOME(c3 IS NULL) FILTER(WHERE c2 < 2) AS c3,
            SOME(c4 IS NULL) FILTER(WHERE c2 < 2) AS c4
        FROM float_tbl
        """


class aggtst_float_some_where_groupby(TstView):
    def __init__(self):
        # checked manually
        self.data = [
            {"id": 0, "c1": False, "c2": False, "c3": False, "c4": False},
            {"id": 1, "c1": False, "c2": False, "c3": False, "c4": False},
        ]
        self.sql = """
        CREATE MATERIALIZED VIEW float_some_where_gby AS
        SELECT
            id,
            SOME(c1 IS NULL) FILTER(WHERE c2 < 2) AS c1,
            SOME(c2 IS NULL) FILTER(WHERE c2 < 2) AS c2,
            SOME(c3 IS NULL) FILTER(WHERE c2 < 2) AS c3,
            SOME(c4 IS NULL) FILTER(WHERE c2 < 2) AS c4
        FROM float_tbl
        GROUP BY id
        """
