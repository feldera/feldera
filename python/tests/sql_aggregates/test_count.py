import unittest
from feldera import PipelineBuilder, Pipeline
from tests import TEST_CLIENT

class TestAggregatesBase(unittest.TestCase):
    def setUp(self) -> None:
        self.data = [{"insert":{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}},
                    {"insert":{"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2}},
                    {"insert":{"id" :0 ,"c1": 4, "c2": 2, "c3": 30, "c4": 14, "c5": None, "c6": 60, "c7": 70, "c8": 18}},
                    {"insert":{"id": 1,"c1": 5, "c2": 3, "c3": None, "c4": 9, "c5": 51, "c6": 6, "c7": 72, "c8": 2}}]
        return super().setUp()

    def execute_query(self, pipeline_name, expected_data, table_name, view_query):
        sql = f'''CREATE TABLE {table_name}(
                    id INT NOT NULL, c1 TINYINT, c2 TINYINT NOT NULL, c3 INT2, c4 INT2 NOT NULL, c5 INT, c6 INT NOT NULL,c7 BIGINT,c8 BIGINT NOT NULL);''' + view_query
        pipeline = PipelineBuilder(TEST_CLIENT, f'{pipeline_name}', sql=sql).create_or_replace()
        out = pipeline.listen('count_view')
        pipeline.start()
        pipeline.input_json(table_name, self.data, update_format="insert_delete")
        pipeline.wait_for_completion(True)
        out_data = out.to_dict()
        print(out_data)
        for datum in expected_data:
            datum.update({"insert_delete": 1})
        assert expected_data == out_data
        pipeline.delete()

    def add_data(self, new_data, delete: bool = False):
        key = "delete" if delete else "insert"
        for datum in new_data:
            self.data.append({key: datum})

@unittest.skip("temporarily disabled; use ad hoc query API to check the results reliably")
class Count(TestAggregatesBase):
    def test_count_value(self):
        pipeline_name = "test_count"
        # validated using postgres
        expected_data = [{'count': 4}]
        table_name = "count"
        view_query = f'''CREATE VIEW count_view AS SELECT COUNT(*) AS count FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class Count_Groupby(TestAggregatesBase):
    def test_count_groupby(self):
        pipeline_name = "test_count_groupby"
        # validated using postgres
        expected_data = [{'count': 2}, {'count': 2}]
        table_name = "count_groupby"
        view_query = f'''CREATE VIEW count_view AS SELECT COUNT(*) AS count FROM {table_name} GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

@unittest.skip("temporarily disabled; use ad hoc query API to check the results reliably")
class Count_Where(TestAggregatesBase):
    def test_count_where(self):
        pipeline_name = "test_count_where"
        # validated using postgres
        expected_data = [{'count': 1}]
        table_name = "count_where"
        view_query = f'''CREATE VIEW count_view AS SELECT COUNT(*) AS count FROM {table_name} WHERE c3>4;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class Count_Where_Groupby(TestAggregatesBase):
    def test_count_where_groupby(self):
        pipeline_name = "test_count_where_groupby"
        # validated using postgres
        expected_data = [{'count': 1}, {'count': 2}]
        table_name = "count_where_groupby"
        view_query = f'''CREATE VIEW count_view AS SELECT COUNT(*) AS count FROM {table_name} WHERE c4>4 GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

if __name__ == '__main__':
    unittest.main()