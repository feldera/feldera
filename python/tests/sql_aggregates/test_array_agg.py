import unittest
from feldera import PipelineBuilder, Pipeline
from tests import TEST_CLIENT

class TestAggregatesBase(unittest.TestCase):
    def setUp(self) -> None:
        self.data = [{"insert":{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}},
                       {"insert":{"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2}},
                       {"insert":{"id": 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}},
                       {"insert":{"id": 1,"c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80}}]       
        return super().setUp()
      
    def execute_query(self, pipeline_name, expected_data, table_name, view_query):
        sql = f'''CREATE TABLE {table_name}(
                    id INT NOT NULL, c1 TINYINT, c2 TINYINT NOT NULL, c3 INT2, c4 INT2 NOT NULL, c5 INT, c6 INT NOT NULL,c7 BIGINT,c8 BIGINT NOT NULL);''' + view_query
        pipeline = PipelineBuilder(TEST_CLIENT, f'{pipeline_name}', sql=sql).create_or_replace()
        out = pipeline.listen('array_agg_view')
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
            
class Array_AGG(TestAggregatesBase):
    def test_array_agg_value(self):
        pipeline_name = "test_array_agg_value"
        # validated using postgres
        expected_data = [{'c1': [None, 1, 4, 5], 'c2': [20, 2, 3, 2], 'c3': [30, 3, 4, None], 'c4': [40, 4, 6, 4], 'c5': [None, 5, 2, 5], 'c6': [60, 6, 3, 6], 'c7': [70, None, 4, None], 'c8': [80, 8, 2, 8]}]
        table_name = "array_agg_value"
        view_query = f'''CREATE VIEW array_agg_view AS SELECT
                            ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3, ARRAY_AGG(c4) AS c4, ARRAY_AGG(c5) AS c5, ARRAY_AGG(c6) AS c6, ARRAY_AGG(c7) AS c7, ARRAY_AGG(c8) AS c8
                         FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Array_AGG_Groupby(TestAggregatesBase):
    def test_array_agg_groupby(self):
        pipeline_name = "test_array_agg_groupby"
        # validated using postgres
        expected_data = [{'id': 0, 'c1': [1, 5], 'c2': [2, 2], 'c3': [3, None], 'c4': [4, 4], 'c5': [5, 5], 'c6': [6, 6], 'c7': [None, None], 'c8': [8, 8]}, 
                         {'id': 1, 'c1': [None, 4], 'c2': [20, 3], 'c3': [30, 4], 'c4': [40, 6], 'c5': [None, 2], 'c6': [60, 3], 'c7': [70, 4], 'c8': [80, 2]}]
        table_name = "array_agg_groupby"
        view_query = f'''CREATE VIEW array_agg_view AS SELECT
                            id, ARRAY_AGG(c1) AS c1, ARRAY_AGG(c2) AS c2, ARRAY_AGG(c3) AS c3, ARRAY_AGG(c4) AS c4, ARRAY_AGG(c5) AS c5, ARRAY_AGG(c6) AS c6, ARRAY_AGG(c7) AS c7, ARRAY_AGG(c8) AS c8
                         FROM {table_name}
                         GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class Array_AGG_Distinct(TestAggregatesBase):
    def test_array_agg_distinct(self):
        pipeline_name = "test_array_agg_distinct"
        # validated using postgres
        expected_data = [{'c1': [None, 1, 4, 5], 'c2': [2, 3, 20], 'c3': [None, 3, 4, 30], 'c4': [4, 6, 40], 'c5': [None, 2, 5], 'c6': [3, 6, 60], 'c7': [None, 4, 70], 'c8': [2, 8, 80]}]
        table_name = "array_agg_distinct"
        view_query = f'''CREATE VIEW array_agg_view AS SELECT
                            ARRAY_AGG(DISTINCT c1) AS c1,ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3, ARRAY_AGG(DISTINCT c4) AS c4, ARRAY_AGG(DISTINCT c5) AS c5, ARRAY_AGG(DISTINCT c6) AS c6, ARRAY_AGG(DISTINCT c7) AS c7, ARRAY_AGG(DISTINCT c8) AS c8
                         FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Array_AGG_Distinct_Groupby(TestAggregatesBase):
    def test_array_agg_distinct_groupby(self):
        pipeline_name = "test_array_agg_distinct_groupby"
        # validated using postgres
        expected_data = [{'id': 0, 'c1': [1, 5], 'c2': [2], 'c3': [None, 3], 'c4': [4], 'c5': [5], 'c6': [6], 'c7': [None], 'c8': [8], 'insert_delete': 1}, {'id': 1, 'c1': [None, 4], 'c2': [3, 20], 'c3': [4, 30], 'c4': [6, 40], 'c5': [None, 2], 'c6': [3, 60], 'c7': [4, 70], 'c8': [2, 80]}]
        table_name = "array_agg_distinct_groupby"
        view_query = f'''CREATE VIEW array_agg_view AS SELECT
                            id, ARRAY_AGG(DISTINCT c1) AS c1,ARRAY_AGG(DISTINCT c2) AS c2, ARRAY_AGG(DISTINCT c3) AS c3, ARRAY_AGG(DISTINCT c4) AS c4, ARRAY_AGG(DISTINCT c5) AS c5, ARRAY_AGG(DISTINCT c6) AS c6, ARRAY_AGG(DISTINCT c7) AS c7, ARRAY_AGG(DISTINCT c8) AS c8
                         FROM {table_name}
                         GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Array_AGG_Where(TestAggregatesBase):
    def test_array_agg_where(self):
        pipeline_name = "test_array_agg_where"
        # validated using postgres
        expected_data = [{'array_agg_c1': [1, 5], 'array_agg_c2': [2, 2], 'array_agg_c3': [3, None], 'array_agg_c4': [4, 4], 'array_agg_c5': [5, 5], 'array_agg_c6': [6, 6], 'array_agg_c7': [None, None], 'array_agg_c8': [8, 8]}]
        table_name = "array_agg_where"
        view_query = f'''CREATE VIEW array_agg_view AS
                        WITH array_agg_val AS(
                            SELECT
                                ARRAY_AGG(c1) AS array_agg_c1, ARRAY_AGG(c2) AS array_agg_c2, ARRAY_AGG(c3) AS array_agg_c3, ARRAY_AGG(c4) AS array_agg_c4, ARRAY_AGG(c5) AS array_agg_c5, ARRAY_AGG(c6) AS array_agg_c6, ARRAY_AGG(c7) AS array_agg_c7, ARRAY_AGG(c8) AS array_agg_c8
                            FROM {table_name})
                            SELECT  ARRAY_AGG(t.c1) AS array_agg_c1, ARRAY_AGG(t.c2) AS array_agg_c2, ARRAY_AGG(t.c3) AS array_agg_c3, ARRAY_AGG(t.c4) AS array_agg_c4, ARRAY_AGG(t.c5) AS array_agg_c5, ARRAY_AGG(t.c6) AS array_agg_c6, ARRAY_AGG(t.c7) AS array_agg_c7, ARRAY_AGG(t.c8) AS array_agg_c8
                            FROM {table_name} t
                            WHERE (t.c4+t.c5) > 8;''' 
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Array_AGG_Where_Groupby(TestAggregatesBase):
    def test_array_agg_where_groupby(self):
        pipeline_name = "test_array_agg_where_groupby"
        # validated using postgres
        expected_data = [{'id': 0, 'array_agg_c1': [1, 5], 'array_agg_c2': [2, 2], 'array_agg_c3': [3, None], 'array_agg_c4': [4, 4], 'array_agg_c5': [5, 5], 'array_agg_c6': [6, 6], 'array_agg_c7': [None, None], 'array_agg_c8': [8, 8]}, 
                         {'id': 1, 'array_agg_c1': [4], 'array_agg_c2': [3], 'array_agg_c3': [4], 'array_agg_c4': [6], 'array_agg_c5': [2], 'array_agg_c6': [3], 'array_agg_c7': [4], 'array_agg_c8': [2]}]
        table_name = "array_agg_where_groupby"
        view_query = f'''CREATE VIEW array_agg_view AS
                        WITH array_agg_val AS(
                            SELECT
                                ARRAY_AGG(c1) AS array_agg_c1, ARRAY_AGG(c2) AS array_agg_c2, ARRAY_AGG(c3) AS array_agg_c3, ARRAY_AGG(c4) AS array_agg_c4, ARRAY_AGG(c5) AS array_agg_c5, ARRAY_AGG(c6) AS array_agg_c6, ARRAY_AGG(c7) AS array_agg_c7, ARRAY_AGG(c8) AS array_agg_c8
                            FROM {table_name})
                            SELECT  t.id, ARRAY_AGG(t.c1) AS array_agg_c1, ARRAY_AGG(t.c2) AS array_agg_c2, ARRAY_AGG(t.c3) AS array_agg_c3, ARRAY_AGG(t.c4) AS array_agg_c4, ARRAY_AGG(t.c5) AS array_agg_c5, ARRAY_AGG(t.c6) AS array_agg_c6, ARRAY_AGG(t.c7) AS array_agg_c7, ARRAY_AGG(t.c8) AS array_agg_c8
                            FROM {table_name} t
                            WHERE (t.c2) < 20
                            GROUP BY t.id;''' 
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

if __name__ == '__main__':
    unittest.main()