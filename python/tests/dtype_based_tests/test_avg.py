import unittest
from decimal import Decimal
from tests.dtype_based_tests.test_base_class import TestAggregatesBase

#decimal type
class Avg_Decimal(TestAggregatesBase):
    def setUp(self) -> None:
        self.data = [{"insert": {"id": 0, "c1": 1111.52, "c2": 2231.90}},
                     {"insert": {"id": 0, "c1": None, "c2": 3802.71}},
                     {"insert": {"id": 1, "c1": 5681.08, "c2": 7689.88}},
                     {"insert": {"id": 1, "c1": 5681.08, "c2": 7335.88}}]
        self.table_name = "agg_avg"
        self.sql = f'''CREATE TABLE {self.table_name}(
                        id INT, c1 DECIMAL(6,2), c2 DECIMAL(6,2) NOT NULL);'''
        self.view_name = "avg_view"

    @unittest.skip("temporarily disabled; use ad hoc query API to check the results reliably")
    def test_avg_value(self):
        pipeline_name = "test_avg_value"
        # validated using postgres
        expected_data =  [{'c1': Decimal('4157.89'), 'c2': Decimal('5265.09')}]
        view_query = f'''CREATE VIEW avg_view AS SELECT
                            AVG(c1) AS c1,AVG(c2) AS c2
                         FROM {self.table_name}'''
        self.execute_query(pipeline_name, expected_data, view_query)

    def test_avg_groupby(self):
        pipeline_name = "test_avg_groupby"
        # validated using postgres
        expected_data = [{'id': 0, 'c1': Decimal('1111.52'), 'c2': Decimal('3017.30')},
                         {'id': 1, 'c1': Decimal('5681.08'), 'c2': Decimal('7512.88')}]
        view_query = f'''CREATE VIEW {self.view_name} AS SELECT
                            id, AVG(c1) AS c1, AVG(c2) AS c2
                         FROM {self.table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, view_query)

    @unittest.skip("temporarily disabled; use ad hoc query API to check the results reliably")
    def test_avg_distinct(self):
        pipeline_name = "test_avg_distinct"
        # validated using postgres
        expected_data = [{'c1': Decimal('3396.30'), 'c2': Decimal('5265.09')}]
        view_query = f'''CREATE VIEW {self.view_name} AS SELECT
                            AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2
                        FROM {self.table_name}'''
        self.execute_query(pipeline_name, expected_data, view_query)

    def test_avg_distinct_groupby(self):
        pipeline_name = "test_avg_distinct_groupby"
        # validated using postgres
        expected_data = [{'id': 0, 'c1': Decimal('1111.52'), 'c2': Decimal('3017.30')},
                         {'id': 1, 'c1': Decimal('5681.08'), 'c2': Decimal('7512.88')}]
        view_query = f'''CREATE VIEW {self.view_name} AS SELECT
                            id, AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2
                        FROM {self.table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, view_query)

    @unittest.skip("temporarily disabled; use ad hoc query API to check the results reliably")
    def test_avg_where(self):
        pipeline_name = "test_avg_where"
        # validated using postgres
        expected_data = [{'f_c1': Decimal('5681.08'), 'f_c2': Decimal('6276.15')}]
        view_query = f'''CREATE VIEW {self.view_name} AS SELECT
                            AVG(c1) FILTER(WHERE c2>2231.90) AS f_c1, AVG(c2) FILTER(WHERE c2>2231.90) AS f_c2
                        FROM {self.table_name};'''
        self.execute_query(pipeline_name, expected_data, view_query)

    def test_avg_where_groupy(self):
        pipeline_name = "test_avg_where_groupby"
        # validated using postgres
        expected_data = [{'id': 0, 'f_c1': None, 'f_c2': Decimal('3802.71')},
                         {'id': 1, 'f_c1': Decimal('5681.08'), 'f_c2': Decimal('7512.88')}]
        view_query = f'''CREATE VIEW {self.view_name} AS SELECT
                            id, AVG(c1) FILTER(WHERE c2>2231.90) AS f_c1, AVG(c2) FILTER(WHERE c2>2231.90) AS f_c2
                        FROM {self.table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, view_query)

if __name__ == '__main__':
    unittest.main()
