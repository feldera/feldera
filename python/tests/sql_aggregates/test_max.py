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
        out = pipeline.listen('max_view')
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
            
class Max(TestAggregatesBase):
    def test_max_value(self):
        pipeline_name = "test_max_value"
        # validated using postgres
        expected_data = [{'c1': 5, 'c2': 5, 'c3': 6, 'c4': 6, 'c5': 5, 'c6': 6, 'c7': 4, 'c8': 8}]
        table_name = "max_value"
        view_query = f'''CREATE VIEW max_view AS SELECT
                            MAX(c1) AS c1, MAX(c2) AS c2, MAX(c3) AS c3, MAX(c4) AS c4, MAX(c5) AS c5, MAX(c6) AS c6, MAX(c7) AS c7, MAX(c8) AS c8
                         FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class Max_Groupby(TestAggregatesBase):
    def test_max_groupby(self):
        pipeline_name = "test_max_groubpy"
        # validated using postgres
        expected_data = [{'id': 0, 'c1': 5, 'c2': 2, 'c3': 3, 'c4': 4, 'c5': 5, 'c6': 6, 'c7': 3, 'c8': 8}, 
                         {'id': 1, 'c1': 4, 'c2': 5, 'c3': 6, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 5}]
        table_name = "max_groupby"
        view_query = f'''CREATE VIEW max_view AS SELECT
                            id, MAX(c1) AS c1, MAX(c2) AS c2, MAX(c3) AS c3, MAX(c4) AS c4, MAX(c5) AS c5, MAX(c6) AS c6, MAX(c7) AS c7, MAX(c8) AS c8
                         FROM {table_name}
                         GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Max_Distinct(TestAggregatesBase):
    def test_max_distinct(self):
        pipeline_name = "test_max_distinct"
        # validated using postgres
        expected_data = [{'c1': 5, 'c2': 5, 'c3': 6, 'c4': 6, 'c5': 5, 'c6': 6, 'c7': 4, 'c8': 8}]
        table_name = "max_distinct"
        view_query = f'''CREATE VIEW max_view AS SELECT
                            MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2, MAX(DISTINCT c3) AS c3, MAX(DISTINCT c4) AS c4, MAX(DISTINCT c5) AS c5, MAX(DISTINCT c6) AS c6, MAX(DISTINCT c7) AS c7, MAX(DISTINCT c8) AS c8
                        FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Max_Distinct_Groupby(TestAggregatesBase):
    def test_max_distinct_groupby(self):
        pipeline_name = "test_max_distinct_groupy"
        # validated using postgres
        expected_data = [{'id': 0, 'c1': 5, 'c2': 2, 'c3': 3, 'c4': 4, 'c5': 5, 'c6': 6, 'c7': 3, 'c8': 8}, 
                         {'id': 1, 'c1': 4, 'c2': 5, 'c3': 6, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 5}]
        table_name = "max_distinct_groupby"
        view_query = f'''CREATE VIEW max_view AS SELECT
                            id, MAX(DISTINCT c1) AS c1, MAX(DISTINCT c2) AS c2, MAX(DISTINCT c3) AS c3, MAX(DISTINCT c4) AS c4, MAX(DISTINCT c5) AS c5, MAX(DISTINCT c6) AS c6, MAX(DISTINCT c7) AS c7, MAX(DISTINCT c8) AS c8
                        FROM {table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Max_Where(TestAggregatesBase):
    def test_max_where0(self):
        pipeline_name = "test_max_where0"
        # validated using postgres
        expected_data = [{'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2}]
        table_name = "max_where0"
        view_query = f'''CREATE VIEW max_view AS SELECT
                            MAX(c1) AS c1, MAX(c2) AS c2, MAX(c3) AS c3, MAX(c4) AS c4, MAX(c5) AS c5, MAX(c6) AS c6, MAX(c7) AS c7, MAX(c8) AS c8
                        FROM {table_name}
                        WHERE 
                            c1 is NOT NULl AND c3 is NOT NULL;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
    def test_max_where1(self):
        pipeline_name = "test_max_where1"
        # validated using postgres
        expected_data = [{'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2}, 
                         {'c1': 5, 'c2': 2, 'c3': None, 'c4': 4, 'c5': 5, 'c6': 6, 'c7': None, 'c8': 8}]
        table_name = "max_where1"
        view_query = f'''CREATE VIEW max_view AS
                        WITH max_val AS(
                            SELECT
                                MAX(c1) AS max_c1, MAX(c2) AS max_c2, MAX(c3) AS max_c3, MAX(c4) AS max_c4, MAX(c5) AS max_c5, MAX(c6) AS max_c6, MAX(c7) AS max_c7, MAX(c8) AS max_c8
                            FROM {table_name})
                            SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                            FROM {table_name} t
                            WHERE (t.c4+T.c5) > (SELECT max_c4 FROM max_val);'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Max_Where_Groupby(TestAggregatesBase):
    def test_max_where_groupy0(self):
        pipeline_name = "test_max_where_groupby0"
        # validated using postgres
        expected_data = [{'id': 1, 'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2}]
        table_name = "max_where_groupby0"
        view_query = f'''CREATE VIEW max_view AS SELECT
                            id, MAX(c1) AS c1, MAX(c2) AS c2, MAX(c3) AS c3, MAX(c4) AS c4, MAX(c5) AS c5, MAX(c6) AS c6, MAX(c7) AS c7, MAX(c8) AS c8
                        FROM {table_name}
                        WHERE 
                            c1 is NOT NULl AND c3 is NOT NULL
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

    def test_max_where_groupby1(self):
        pipeline_name = "test_max_where_groupby1"
        # validated using postgres
        expected_data = [{'id': 0, 'max_c1': 5, 'max_c2': 2, 'max_c3': None, 'max_c4': 4, 'max_c5': 5, 'max_c6': 6, 'max_c7': None, 'max_c8': 8}, 
                         {'id': 1, 'max_c1': 4, 'max_c2': 3, 'max_c3': 4, 'max_c4': 6, 'max_c5': 2, 'max_c6': 3, 'max_c7': 4, 'max_c8': 2}]
        table_name = "min_where_groupby1"
        view_query = (f'''CREATE VIEW max_view AS
                         WITH max_val AS (
                            SELECT
                                 MAX(c1) AS max_c1, MAX(c2) AS max_c2, MAX(c3) AS max_c3, MAX(c4) AS max_c4, MAX(c5) AS max_c5, MAX(c6) AS max_c6, MAX(c7) AS max_c7, MAX(c8) AS max_c8
                            FROM {table_name})
                            SELECT
                                t.id, MAX(t.c1) AS max_c1, MAX(t.c2) AS max_c2, MAX(t.c3) AS max_c3, MAX(t.c4) AS max_c4,  MAX(t.c5) AS max_c5, MAX(t.c6) AS max_c6, MAX(t.c7) AS max_c7, MAX(t.c8) AS max_c8
                            FROM {table_name} t
                            WHERE (t.c4 + t.c5) > (SELECT max_c4 FROM max_val)
                            GROUP BY t.id;''')

        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
if __name__ == '__main__':
    unittest.main()