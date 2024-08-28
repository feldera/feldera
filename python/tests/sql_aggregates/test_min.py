import unittest
from feldera import PipelineBuilder, Pipeline
from tests import TEST_CLIENT

class TestAggregatesBase(unittest.TestCase):
    def setUp(self) -> None:
        self.data = [{"insert":{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}},
                    {"insert":{"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2}},
                    {"insert":{"id" : 0, "c1": None, "c2": 2, "c3": 3, "c4": 2, "c5": 3, "c6": 4, "c7": 3, "c8": 3}},
                    {"insert":{"id" : 1, "c1": None, "c2": 5, "c3": 6, "c4": 2, "c5": 2, "c6": 1, "c7": None, "c8": 5}}]       
        return super().setUp()
      
    def execute_query(self, pipeline_name, expected_data, table_name, view_query):
        sql = f'''CREATE TABLE {table_name}(
                    id INT NOT NULL, c1 TINYINT, c2 TINYINT NOT NULL, c3 INT2, c4 INT2 NOT NULL, c5 INT, c6 INT NOT NULL,c7 BIGINT,c8 BIGINT NOT NULL);''' + view_query
        pipeline = PipelineBuilder(TEST_CLIENT, f'{pipeline_name}', sql=sql).create_or_replace()
        out = pipeline.listen('min_view')
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
            
class Min(TestAggregatesBase):
    def test_min_value(self):
        pipeline_name = "test_min_value"
        # validated using postgres
        expected_data = [{'c1': 4, 'c2': 2, 'c3': 3, 'c4': 2, 'c5': 2, 'c6': 1, 'c7': 3, 'c8': 2}]
        table_name = "min_value"
        view_query = f'''CREATE VIEW min_view AS SELECT
                            MIN(c1) AS c1, MIN(c2) AS c2, MIN(c3) AS c3, MIN(c4) AS c4, MIN(c5) AS c5, MIN(c6) AS c6, MIN(c7) AS c7, MIN(c8) AS c8
                         FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class Min_Groupby(TestAggregatesBase):
    def test_min_groupby(self):
        pipeline_name = "test_min_groupby"
        # validated using postgres
        expected_data = [{'id': 0, 'c1': 5, 'c2': 2, 'c3': 3, 'c4': 2, 'c5': 3, 'c6': 4, 'c7': 3, 'c8': 3}, 
                         {'id': 1, 'c1': 4, 'c2': 3, 'c3': 4, 'c4': 2, 'c5': 2, 'c6': 1, 'c7': 4, 'c8': 2}]
        table_name = "min_groupby"
        view_query = f'''CREATE VIEW min_view AS SELECT
                            id, MIN(c1) AS c1, MIN(c2) AS c2, MIN(c3) AS c3, MIN(c4) AS c4, MIN(c5) AS c5, MIN(c6) AS c6, MIN(c7) AS c7, MIN(c8) AS c8
                         FROM {table_name}
                         GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Min_Distinct(TestAggregatesBase):
    def test_min_distinct(self):
        pipeline_name = "test_min_distinct"
        # validated using postgres
        expected_data = [{'c1': 4, 'c2': 2, 'c3': 3, 'c4': 2, 'c5': 2, 'c6': 1, 'c7': 3, 'c8': 2}]
        table_name = "min_distinct"
        view_query = f'''CREATE VIEW min_view AS SELECT
                            MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2, MIN(DISTINCT c3) AS c3, MIN(DISTINCT c4) AS c4, MIN(DISTINCT c5) AS c5, MIN(DISTINCT c6) AS c6, MIN(DISTINCT c7) AS c7, MIN(DISTINCT c8) AS c8
                        FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Min_Distinct_Groupby(TestAggregatesBase):
    def test_min_distinct_groupby(self):
        pipeline_name = "test_min_distinct_groupby"
        # validated using postgres
        expected_data = [{'id': 0, 'c1': 5, 'c2': 2, 'c3': 3, 'c4': 2, 'c5': 3, 'c6': 4, 'c7': 3, 'c8': 3},
                         {'id': 1, 'c1': 4, 'c2': 3, 'c3': 4, 'c4': 2, 'c5': 2, 'c6': 1, 'c7': 4, 'c8': 2}]
        table_name = "min_distinct_groupby"
        view_query = f'''CREATE VIEW min_view AS SELECT
                            id, MIN(DISTINCT c1) AS c1, MIN(DISTINCT c2) AS c2, MIN(DISTINCT c3) AS c3, MIN(DISTINCT c4) AS c4, MIN(DISTINCT c5) AS c5, MIN(DISTINCT c6) AS c6, MIN(DISTINCT c7) AS c7, MIN(DISTINCT c8) AS c8
                        FROM {table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Min_Where(TestAggregatesBase):
    def test_min_where0(self):
        pipeline_name = "test_min_where0"
        # validated using postgres
        expected_data = [{'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2}]
        table_name = "min_where0"
        view_query = f'''CREATE VIEW min_view AS SELECT
                            MIN(c1) AS c1, MIN(c2) AS c2, MIN(c3) AS c3, MIN(c4) AS c4, MIN(c5) AS c5, MIN(c6) AS c6, MIN(c7) AS c7, MIN(c8) AS c8
                        FROM {table_name}
                        WHERE 
                            c1 is NOT NULl AND c3 is NOT NULL;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
    def test_min_where1(self):
        pipeline_name = "test_min_where1"
        # validated using postgres
        expected_data = [{'c1': None, 'c2': 2, 'c3': 3.0, 'c4': 2, 'c5': 3, 'c6': 4, 'c7': 3.0, 'c8': 3}, 
                         {'c1': None, 'c2': 5, 'c3': 6.0, 'c4': 2, 'c5': 2, 'c6': 1, 'c7': None, 'c8': 5}, 
                         {'c1': 4.0, 'c2': 3, 'c3': 4.0, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4.0, 'c8': 2},
                         {'c1': 5.0, 'c2': 2, 'c3': None, 'c4': 4, 'c5': 5, 'c6': 6, 'c7': None, 'c8': 8}]
        table_name = "min_where1"
        view_query = f'''CREATE VIEW min_view AS
                        WITH min_val AS(
                            SELECT
                                MIN(c1) AS min_c1, MIN(c2) AS min_c2, MIN(c3) AS min_c3, MIN(c4) AS min_c4, MIN(c5) AS min_c5, MIN(c6) AS min_c6, MIN(c7) AS min_c7, MIN(c8) AS min_c8
                            FROM {table_name})
                            SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                            FROM {table_name} t
                            WHERE (t.c4+T.c5) > (SELECT min_c4 FROM min_val);'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Min_Where_Groupby(TestAggregatesBase):
    def test_min_where_groupby0(self):
        pipeline_name = "test_min_where_groupby0"
        # validated using postgres
        expected_data = [{'id': 1, 'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2}]
        table_name = "min_where_groupby0"
        view_query = f'''CREATE VIEW min_view AS SELECT
                            id, MIN(c1) AS c1, MIN(c2) AS c2, MIN(c3) AS c3, MIN(c4) AS c4, MIN(c5) AS c5, MIN(c6) AS c6, MIN(c7) AS c7, MIN(c8) AS c8
                        FROM {table_name}
                        WHERE 
                            c1 is NOT NULl AND c3 is NOT NULL
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

    def test_min_where_groupby1(self):
        pipeline_name = "test_min_where_groupby1"
        # validated using postgres
        expected_data = [{'id': 0, 'min_c1': 5, 'min_c2': 2, 'min_c3': 3, 'min_c4': 2, 'min_c5': 3, 'min_c6': 4, 'min_c7': 3, 'min_c8': 3}, 
                         {'id': 1, 'min_c1': 4, 'min_c2': 3, 'min_c3': 4, 'min_c4': 2, 'min_c5': 2, 'min_c6': 1, 'min_c7': 4, 'min_c8': 2}]
        table_name = "min_where_groupby1"
        view_query = (f'''CREATE VIEW min_view AS
                         WITH min_val AS (
                            SELECT
                                 MIN(c1) AS min_c1, MIN(c2) AS min_c2, MIN(c3) AS min_c3, MIN(c4) AS min_c4, MIN(c5) AS min_c5, MIN(c6) AS min_c6, MIN(c7) AS min_c7, MIN(c8) AS min_c8
                            FROM {table_name})
                            SELECT
                                t.id, MIN(t.c1) AS min_c1, MIN(t.c2) AS min_c2, MIN(t.c3) AS min_c3, MIN(t.c4) AS min_c4,  MIN(t.c5) AS min_c5, MIN(t.c6) AS min_c6, MIN(t.c7) AS min_c7, MIN(t.c8) AS min_c8
                            FROM {table_name} t
                            WHERE (t.c4 + t.c5) > (SELECT min_c4 FROM min_val)
                            GROUP BY t.id;''')

        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
if __name__ == '__main__':
    unittest.main()