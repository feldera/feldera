# issue: https://github.com/feldera/feldera/issues/2535

import unittest
from tests.sql_aggregates.base import Base


class Avg(Base):
    def setUp(self) -> None:
        super().setUp()
        print("Setting up Avg test case...")
        self.pipeline_name = "test_avg"
        ddl = '''CREATE TABLE avg_tbl(
                        id INT, c1 INT, c2 INT NOT NULL);'''
        self.set_tables(ddl)
        data_list = [
            {"id": 0, "c1": None, "c2": 20},
            {"id": 1, "c1": 11, "c2": 22}
        ]
        self.set_data('avg_tbl', data_list)

    def test_avg_value(self):
        print("Running test_avg_value...")
        test_name = 'test_avg_value'
        view_name = 'avg_view_value'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                            AVG(c1) AS c1, AVG(c2) AS c2
                         FROM avg_tbl;''')
        additional_data = [
            {"id": 0, "c1": 1, "c2": 2}
        ]
        expected_data = [{'c1': 4, 'c2': 11, 'insert_delete': 1}]
        self.add_data('avg_tbl', additional_data, test_name)
        self.set_expected_output(test_name, view_name, expected_data)

    def test_avg_gby(self):
        print("Running test_avg_gby...")
        test_name = 'test_avg_gby'
        view_name = 'avg_view_gby'
        self.register_view(f'''CREATE VIEW {view_name} AS SELECT
                            id, AVG(c1) AS c1, AVG(c2) AS c2
                         FROM avg_tbl
                         GROUP BY id;''')
        additional_data = [
            {"id": 0, "c1": 1, "c2": 2}
        ]
        expected_data = [{'id': 0, 'c1': 1, 'c2': 11, 'insert_delete': 1},
                         {'id': 1, 'c1': 11, 'c2': 22, 'insert_delete': 1}]
        self.add_data('avg_tbl', additional_data, test_name)
        self.set_expected_output(test_name, view_name, expected_data)

    def testall(self):
        self.run_tests()


if __name__ == "__main__":
    unittest.main()
