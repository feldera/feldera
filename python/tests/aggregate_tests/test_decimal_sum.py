from .test_base import TestView
from decimal import Decimal

class test_decimal_sum(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW decimal_sum AS SELECT
                      SUM(c1) AS c1, SUM(c2) AS c2
                      FROM decimal_tbl'''
        self.data = [{'c1': Decimal('12473.68'), 'c2': Decimal('21060.37')}]

class test_decimal_sum_gby(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW decimal_sum_gby AS SELECT
                      id, SUM(c1) AS c1, SUM(c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id'''
        self.data = [{'id': 0, 'c1': Decimal('1111.52'), 'c2': Decimal('6034.61')},
                     {'id': 1, 'c1': Decimal('11362.16'), 'c2': Decimal('15025.76')}]

class test_decimal_sum_distinct(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW decimal_sum_distinct AS SELECT
                      SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2
                      FROM decimal_tbl'''
        self.data = [{'c1': Decimal('6792.60'), 'c2': Decimal('21060.37')}]

class test_decimal_sum_distinct_gby(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW decimal_sum_distinct_gby AS SELECT
                      id, SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2
                      FROM decimal_tbl
                      GROUP BY id'''
        self.data = [{'id': 0, 'c1': Decimal('1111.52'), 'c2': Decimal('6034.61')},
                     {'id': 1,'c1': Decimal('5681.08'), 'c2': Decimal('15025.76')}]

class test_decimal_sum_where(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW decimal_sum_where AS SELECT
                           SUM(c1) FILTER(WHERE c2>2231.90) AS f_c1, SUM(c2) FILTER(WHERE c2>2231.90) AS f_c2
                        FROM decimal_tbl'''
        self.data = [{'f_c1': Decimal('11362.16'), 'f_c2': Decimal('18828.47')}]

class test_decimal_sum_where_gby(TestView):
    def __init__(self):
        # Validated on Postgres
        self.sql = '''CREATE VIEW decimal_sum_where_gby AS SELECT
                      id, SUM(c1) FILTER(WHERE c2>2231.90) AS f_c1, SUM(c2) FILTER(WHERE c2>2231.90) AS f_c2
                      FROM decimal_tbl
                      GROUP BY id'''
        self.data = [{'id': 0, 'f_c1': None, 'f_c2': Decimal('3802.71')},
                     {'id': 1, 'f_c1': Decimal('11362.16'), 'f_c2': Decimal('15025.76')}]