import unittest
from tests.dtype_based_tests.test_base_class import TestAggregatesBase
from decimal import Decimal

#decimal type
@unittest.skip("temporarily disabled due to a rounding error")
class Sum_Decimal(TestAggregatesBase):
    def setUp(self) -> None:
        self.data = [{"insert": {"id": 0, "c1": 1111.52, "c2": 2231.90}},
                     {"insert": {"id": 0, "c1": None, "c2": 3802.71}},
                     {"insert": {"id": 1, "c1": 5681.08, "c2": 7689.88}}]
        self.table_name = "agg_sum"
        self.sql = f'''CREATE TABLE {self.table_name}(
                        id INT, c1 DECIMAL(6,2), c2 DECIMAL(6,2) NOT NULL);'''
        self.view_name = "sum_view"
        
    def test_sum_value(self):
        pipeline_name = "test_sum_value"
        # validated using postgres
        expected_data = [{'c1': Decimal('6792.60'), 'c2': Decimal('13724.49')}]
        view_query = f'''CREATE VIEW {self.view_name} AS SELECT
                            SUM(c1) AS c1, SUM(c2) AS c2
                         FROM {self.table_name};'''
        self.execute_query(pipeline_name, expected_data, view_query)

    def test_sum_groupby(self):
        pipeline_name = "test_sum_groupby"
        # validated using postgres
        expected_data = [{'id': 0, 'c1': Decimal('1111.52'), 'c2': Decimal('6034.61')},
                         {'id': 1, 'c1': Decimal('5681.08'), 'c2': Decimal('7689.88')}]
        view_query = f'''CREATE VIEW {self.view_name} AS SELECT
                            id, SUM(c1) AS c1, SUM(c2) AS c2
                         FROM {self.table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, view_query)

    def test_sum_distinct(self):
        pipeline_name = "test_sum_distinct"
        # validated using postgres
        new_data = [
            {"id": 1, "c1": 5681.08, "c2": 7335.88}]
        self.add_data(new_data)
        expected_data = [{'c1': Decimal('6792.60'), 'c2': Decimal('21060.37')}]
        view_query = f'''CREATE VIEW {self.view_name} AS SELECT
                            SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2
                        FROM {self.table_name}'''
        self.execute_query(pipeline_name, expected_data, view_query)

    def test_sum_distinct_groupby(self):
        pipeline_name = "test_sum_distinct_groupby"
        # validated using postgres
        new_data = [
            {"id": 1, "c1": 5681.08, "c2": 7335.88}]
        self.add_data(new_data)
        expected_data =[{'id': 0, 'c1': Decimal('1111.52'), 'c2': Decimal('6034.61')}, 
                        {'id': 1,'c1': Decimal('5681.08'), 'c2': Decimal('15025.76')}]
        view_query = f'''CREATE VIEW {self.view_name} AS SELECT
                            id, SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2
                        FROM {self.table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, view_query)

    def test_sum_where(self):
        pipeline_name = "test_sum_where"
        # validated using postgres
        expected_data = [{'f_c1': Decimal('5681.08'), 'f_c2': Decimal('11492.59')}]
        view_query = f'''CREATE VIEW {self.view_name} AS SELECT
                            SUM(c1) FILTER(WHERE c2>2231.90) AS f_c1, SUM(c2) FILTER(WHERE c2>2231.90) AS f_c2
                        FROM {self.table_name};'''
        self.execute_query(pipeline_name, expected_data, view_query)
    
    def test_sum_where_groupby(self):
        pipeline_name = "test_sum_where_groupby"
        # validated using postgres
        new_data = [
            {"id": 1, "c1": 5681.08, "c2": 7335.88}]
        self.add_data(new_data)
        expected_data = [{'id': 0, 'f_c1': None, 'f_c2': Decimal('3802.71')}, 
                         {'id': 1, 'f_c1': Decimal('11362.16'), 'f_c2': Decimal('15025.76')}]
        view_query = f'''CREATE VIEW {self.view_name} AS SELECT
                            id, SUM(c1) FILTER(WHERE c2>2231.90) AS f_c1, SUM(c2) FILTER(WHERE c2>2231.90) AS f_c2
                        FROM {self.table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, view_query)

if __name__ == '__main__':
    unittest.main()
