from tests.refactored_tests.test_base_class import Base
import unittest
from decimal import Decimal

class DecimalAggregates0(Base):
    def setUp(self) -> None:
        super().setUp()
        print("Setting up all the test cases...")
        self.pipeline_name = "test_aggregates"
        ddl = '''CREATE TABLE agg_tbl(
                        id INT, c1 DECIMAL(6,2), c2 DECIMAL(6,2) NOT NULL);'''
        self.set_tables(ddl)
        data_list = [{"insert": {"id": 0, "c1": 1111.52, "c2": 2231.90}},
                     {"insert": {"id": 0, "c1": None, "c2": 3802.71}},
                     {"insert": {"id": 1, "c1": 5681.08, "c2": 7689.88}},
                     {"insert": {"id": 1, "c1": 5681.08, "c2": 7335.88}}]
        self.set_data('agg_tbl', data_list)

    # average tests

    def test_avg_value(self):
        print("Running test_avg_value...")
        test_name = 'test_avg_value'
        view_name = 'avg_view_value'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                            AVG(c1) AS c1, AVG(c2) AS c2
                         FROM agg_tbl;''')
        expected_data = [{'c1': Decimal('4157.89'), 'c2': Decimal('5265.09'), 'insert_delete': 1}]
        self.set_expected_output(test_name, view_name, expected_data)

    def test_avg_gby(self):
        print("Running test_avg_gby...")
        test_name = 'test_avg_gby'
        view_name = 'avg_view_gby'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                            id, AVG(c1) AS c1, AVG(c2) AS c2
                         FROM agg_tbl
                         GROUP BY id;''')
        expected_data = [{'id': 0, 'c1': Decimal('1111.52'), 'c2': Decimal('3017.30'), 'insert_delete': 1},
                         {'id': 1, 'c1': Decimal('5681.08'), 'c2': Decimal('7512.88'), 'insert_delete': 1}]
        self.set_expected_output(test_name, view_name, expected_data)

    def test_avg_distinct(self):
        print("Running test_distinct...")
        test_name = 'test_avg_distinct'
        view_name = 'avg_view_distinct'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                            AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2
                         FROM agg_tbl;''')
        expected_data = [{'c1': Decimal('3396.30'), 'c2': Decimal('5265.09'), 'insert_delete': 1}]
        self.set_expected_output(test_name, view_name, expected_data)

    def test_avg_distinct_gby(self):
        print("Running test_distinct_gby...")
        test_name = 'test_avg_distinct_gby'
        view_name = 'avg_view_distinct_gby'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                            id, AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2
                         FROM agg_tbl
                         GROUP BY id;''')
        expected_data = [{'id':0,'c1': Decimal('1111.52'), 'c2': Decimal('3017.30'), 'insert_delete': 1},
                         {'id':1,'c1': Decimal('5681.08'), 'c2': Decimal('7512.88'), 'insert_delete': 1}]
        self.set_expected_output(test_name, view_name, expected_data)

    def test_avg_where(self):
        print("Running test_avg_where...")
        test_name = 'test_avg_where'
        view_name = 'avg_view_where'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                            AVG(c1) FILTER(WHERE c2>2231.90) AS f_c1, AVG(c2) FILTER(WHERE c2>2231.90) AS f_c2
                         FROM agg_tbl;''')
        expected_data = [{'f_c1': Decimal('5681.08'), 'f_c2': Decimal('6276.15'), 'insert_delete': 1}]
        self.set_expected_output(test_name, view_name, expected_data)

    def test_avg_where_gby(self):
        print("Running test_avg_where_gby...")
        test_name = 'test_avg_where_gby'
        view_name = 'avg_view_where_gby'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                            id, AVG(c1) FILTER(WHERE c2>2231.90) AS f_c1, AVG(c2) FILTER(WHERE c2>2231.90) AS f_c2
                         FROM agg_tbl
                         GROUP BY id;''')
        expected_data = [{'id': 0, 'f_c1': None, 'f_c2': Decimal('3802.71'), 'insert_delete': 1},
                         {'id': 1, 'f_c1': Decimal('5681.08'), 'f_c2': Decimal('7512.88'), 'insert_delete': 1}]
        self.set_expected_output(test_name, view_name, expected_data)

    # sum tests

    def test_sum_value(self):
        print("Running test_sum_value...")
        test_name = 'test_sum_value'
        view_name = 'sum_view_value'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                            SUM(c1) AS c1, SUM(c2) AS c2
                         FROM agg_tbl;''')
        expected_data = [{'c1': Decimal('12473.68'), 'c2': Decimal('21060.37'), 'insert_delete': 1}]
        self.set_expected_output(test_name, view_name, expected_data)

    def test_sum_gby(self):
        print("Running test_sum_gby...")
        test_name = 'test_sum_gby'
        view_name = 'sum_view_gby'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                            id, SUM(c1) AS c1, SUM(c2) AS c2
                         FROM agg_tbl
                         GROUP BY id;''')
        expected_data = [{'id': 0, 'c1': Decimal('1111.52'), 'c2': Decimal('6034.61'), 'insert_delete': 1},
                         {'id': 1, 'c1': Decimal('11362.16'), 'c2': Decimal('15025.76'), 'insert_delete': 1}]
        self.set_expected_output(test_name, view_name, expected_data)

    def test_sum_distinct(self):
        print("Running test_sum_distinct...")
        test_name = 'test_sum_distinct'
        view_name = 'sum_view_distinct'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                            SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2
                         FROM agg_tbl;''')
        expected_data = [{'c1': Decimal('6792.60'), 'c2': Decimal('21060.37'), 'insert_delete': 1}]
        self.set_expected_output(test_name, view_name, expected_data)

    def test_sum_distinct_gby(self):
        print("Running test_sum_distinct_gby...")
        test_name = 'test_sum_distinct_gby'
        view_name = 'sum_view_distinct_gby'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                            id, SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2
                         FROM agg_tbl
                         GROUP BY id;''')
        expected_data = [{'id': 0, 'c1': Decimal('1111.52'), 'c2': Decimal('6034.61'), 'insert_delete': 1},
                        {'id': 1,'c1': Decimal('5681.08'), 'c2': Decimal('15025.76'), 'insert_delete': 1}]
        self.set_expected_output(test_name, view_name, expected_data)

    def test_sum_where(self):
        print("Running test_sum_where..")
        test_name = 'test_sum_where'
        view_name = 'sum_view_where'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                           SUM(c1) FILTER(WHERE c2>2231.90) AS f_c1, SUM(c2) FILTER(WHERE c2>2231.90) AS f_c2
                        FROM agg_tbl;''')
        expected_data = [{'f_c1': Decimal('11362.16'), 'f_c2': Decimal('18828.47'), 'insert_delete': 1}]
        self.set_expected_output(test_name, view_name, expected_data)

    def test_sum_where_gby(self):
        print("Running test_sum_where_gby..")
        test_name = 'test_sum_where_gby'
        view_name = 'sum_view_wheregby'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                           id, SUM(c1) FILTER(WHERE c2>2231.90) AS f_c1, SUM(c2) FILTER(WHERE c2>2231.90) AS f_c2
                        FROM agg_tbl
                        GROUP BY id;''')
        expected_data = [{'id': 0, 'f_c1': None, 'f_c2': Decimal('3802.71'), 'insert_delete': 1},
                         {'id': 1, 'f_c1': Decimal('11362.16'), 'f_c2': Decimal('15025.76'), 'insert_delete': 1}]
        self.set_expected_output(test_name, view_name, expected_data)

    def testall(self):
        self.run_tests()

if __name__ == "__main__":
    unittest.main()