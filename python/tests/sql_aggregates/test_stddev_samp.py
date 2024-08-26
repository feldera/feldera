import unittest
from feldera import PipelineBuilder, Pipeline
from tests import TEST_CLIENT

class TestAggregatesBase(unittest.TestCase):
    def setUp(self) -> None:
        self.data = [{"insert":{"id": 0, "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}},
                       {"insert":{"id": 1,"c1": 4, "c2": 3, "c3": 4, "c4": 6, "c5": 2, "c6": 3, "c7": 4, "c8": 2}}]       
        return super().setUp()
      
    def execute_query(self, pipeline_name, expected_data, table_name, view_query):
        sql = f'''CREATE TABLE {table_name}(
                    id INT NOT NULL, c1 TINYINT, c2 TINYINT NOT NULL, c3 INT2, c4 INT2 NOT NULL, c5 INT, c6 INT NOT NULL,c7 BIGINT,c8 BIGINT NOT NULL);''' + view_query
        pipeline = PipelineBuilder(TEST_CLIENT, f'{pipeline_name}', sql=sql).create_or_replace()
        out = pipeline.listen('stddev_view')
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
            
class Sttdev(TestAggregatesBase):
    def test_stddev_value(self):
        pipeline_name = "test_sttdev_value"
        # validated using postgres
        expected_data = [{'c1': 1, 'c2': 1, 'c3': None, 'c4': 1, 'c5': 2, 'c6': 2, 'c7': None, 'c8': 4}]
        table_name = "stddev_value"
        view_query = f'''CREATE VIEW stddev_view AS SELECT
                            STDDEV_SAMP(c1) AS c1, STDDEV_SAMP(c2) AS c2, STDDEV_SAMP(c3) AS c3, STDDEV_SAMP(c4) AS c4, STDDEV_SAMP(c5) AS c5, STDDEV_SAMP(c6) AS c6, STDDEV_SAMP(c7) AS c7, STDDEV_SAMP(c8) AS c8
                         FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class Stddev_Groupby(TestAggregatesBase):
    def test_stddev_groupby(self):
        pipeline_name = "test_stddev_groupby"
        # validated using postgres
        new_data = [
            {"id" : 0, "c1": None, "c2": 2, "c3": 3, "c4": 2, "c5": 3, "c6": 4, "c7": 3, "c8": 3},
            {"id" : 1, "c1": None, "c2": 5, "c3": 6, "c4": 2, "c5": 2, "c6": 1, "c7": None, "c8": 5}]
        self.add_data(new_data)
        expected_data = [{'id': 0, 'c1': None, 'c2': 0, 'c3': None, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': None, 'c8': 3},
                         {'id': 1, 'c1': None, 'c2': 1, 'c3': 1, 'c4': 2, 'c5': 0, 'c6': 1, 'c7': None, 'c8': 2}]
        table_name = "stddev_groupby"
        view_query = f'''CREATE VIEW stddev_view AS SELECT
                            id, STDDEV_SAMP(c1) AS c1, STDDEV_SAMP(c2) AS c2, STDDEV_SAMP(c3) AS c3, STDDEV_SAMP(c4) AS c4, STDDEV_SAMP(c5) AS c5, STDDEV_SAMP(c6) AS c6, STDDEV_SAMP(c7) AS c7, STDDEV_SAMP(c8) AS c8
                         FROM {table_name}
                         GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)
        
class Stddev_Distinct(TestAggregatesBase):
    def test_stddev_distinct(self):
        pipeline_name ="test_stddev_distinct"
        new_data = [
            {"id" : 1, "c1": None, "c2": 2, "c3": 3, "c4": 2, "c5": 3, "c6": 6, "c7": 3, "c8": 2}]
        self.add_data(new_data)
        # validated using postgres
        expected_data = [{'c1': 1, 'c2': 1, 'c3': 1, 'c4': 2, 'c5': 1, 'c6': 2, 'c7': 1, 'c8': 4}]
        table_name = "stddev_distinct"
        view_query = f'''CREATE VIEW stddev_view AS SELECT
                            STDDEV_SAMP(DISTINCT c1) AS c1, STDDEV_SAMP(DISTINCT c2) AS c2, STDDEV_SAMP(DISTINCT c3) AS c3, STDDEV_SAMP(DISTINCT c4) AS c4, STDDEV_SAMP(DISTINCT c5) AS c5, STDDEV_SAMP(DISTINCT c6) AS c6, STDDEV_SAMP(DISTINCT c7) AS c7, STDDEV_SAMP(DISTINCT c8) AS c8
                        FROM {table_name}'''   
        self.execute_query(pipeline_name, expected_data, table_name, view_query) 

@unittest.skip("https://github.com/feldera/feldera/issues/2315")      
class Stddev_Distinct_Groupby(TestAggregatesBase):
    def test_stddev_distinct_groupby(self):
        pipeline_name = "test_stddev_distinct_groupby"
        # validated using postgres
        new_data = [
            {"id" : 0, "c1": None, "c2": 2, "c3": 3, "c4": 2, "c5": 3, "c6": 4, "c7": 3, "c8": 3},
            {"id" : 1, "c1": None, "c2": 5, "c3": 6, "c4": 2, "c5": 2, "c6": 1, "c7": None, "c8": 5}]
        self.add_data(new_data)
        expected_data = [{"id": 0, "c1": None, "c2": None, "c3": None, "c4": 1, "c5": 1, "c6": 1, "c7": None, "c8": 3},
                        {"id": 1, "c1": None, "c2": 1, "c3": 1, "c4": 2, "c5": None, "c6": 1, "c7": None, "c8": 2}]
        table_name = "stddev_distinct_groupby"
        view_query = f'''CREATE VIEW stddev_view AS SELECT
                            id, STDDEV_SAMP(DISTINCT c1) AS c1, STDDEV_SAMP(DISTINCT c2) AS c2, STDDEV_SAMP(DISTINCT c3) AS c3, STDDEV_SAMP(DISTINCT c4) AS c4, STDDEV_SAMP(DISTINCT c5) AS c5, STDDEV_SAMP(DISTINCT c6) AS c6, STDDEV_SAMP(DISTINCT c7) AS c7, STDDEV_SAMP(DISTINCT c8) AS c8
                        FROM {table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query) 
        
class Stddev_Where(TestAggregatesBase):        
    def test_stddev_where(self):
        pipeline_name ="test_stddev_where"
        # validated using postgres
        expected_data = [{ "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}]
        table_name = "stddev_where"
        view_query = f'''CREATE VIEW stddev_view AS
                        WITH stddev_val AS(
                            SELECT
                                STDDEV_SAMP(c1) AS stddev_c1, STDDEV_SAMP(c2) AS stddev_c2, STDDEV_SAMP(c3) AS stddev_c3, STDDEV_SAMP(c4) AS stddev_c4, STDDEV_SAMP(c5) AS stddev_c5, STDDEV_SAMP(c6) AS stddev_c6, STDDEV_SAMP(c7) AS stddev_c7, STDDEV_SAMP(c8) AS stddev_c8
                            FROM {table_name})
                            SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                            FROM {table_name} t
                            WHERE (t.c8) > (SELECT stddev_c8 FROM stddev_val);'''   
        self.execute_query(pipeline_name, expected_data, table_name, view_query) 

@unittest.skip("https://github.com/feldera/feldera/issues/2315")          
class Stddev_Where_Groupby(TestAggregatesBase):       
    def test_stddev_where_groupby(self):
        pipeline_name ="test_stddev_where_groupby"
        # validated using postgres
        new_data = [
            {"id" : 0, "c1": None, "c2": 2, "c3": 3, "c4": 2, "c5": 3, "c6": 4, "c7": 3, "c8": 3},
            {"id" : 1, "c1": None, "c2": 5, "c3": 6, "c4": 2, "c5": 2, "c6": 1, "c7": None, "c8": 5}]
        self.add_data(new_data)
        expected_data = [{"id": 0, "stddev_c1": None, "stddev_c2": 0, "stddev_c3":None, "stddev_c4": 1, "stddev_c5": 1, "stddev_c6": 1, "stddev_c7": None, "stddev_c8": 3},
                         {"id": 1, "stddev_c1": None, "stddev_c2": None, "stddev_c3":None, "stddev_c4": None, "stddev_c5": None, "stddev_c6": None, "stddev_c7": None, "stddev_c8": None}]
        table_name = "stddev_where_groupby"
        view_query = (f'''CREATE VIEW stddev_view AS
                         WITH stddev_val AS (
                            SELECT
                                STDDEV_SAMP(c1) AS stddev_c1, STDDEV_SAMP(c2) AS stddev_c2, STDDEV_SAMP(c3) AS stddev_c3, STDDEV_SAMP(c4) AS stddev_c4, STDDEV_SAMP(c5) AS stddev_c5, STDDEV_SAMP(c6) AS stddev_c6, STDDEV_SAMP(c7) AS stddev_c7, STDDEV_SAMP(c8) AS stddev_c8
                            FROM {table_name})
                            SELECT
                                t.id, STDDEV_SAMP(t.c1) AS stddev_c1, STDDEV_SAMP(t.c2) AS stddev_c2, STDDEV_SAMP(t.c3) AS stddev_c3, STDDEV_SAMP(t.c4) AS stddev_c4,  STDDEV_SAMP(t.c5) AS stddev_c5, STDDEV_SAMP(t.c6) AS stddev_c6, STDDEV_SAMP(t.c7) AS stddev_c7, STDDEV_SAMP(t.c8) AS stddev_c8
                            FROM {table_name} t
                            WHERE (t.c8) > (SELECT stddev_c8 FROM stddev_val)
                            GROUP BY t.id;''')  
                         
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

if __name__ == '__main__':
    unittest.main()