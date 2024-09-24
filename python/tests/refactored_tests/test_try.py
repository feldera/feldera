from tests.refactored_tests.test_base_class import Base
import unittest
from decimal import Decimal

class Avg0(Base):     
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
        
    def testall(self):
        self.run_tests()

if __name__ == '__main__':
    unittest.main()
        