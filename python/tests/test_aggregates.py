import unittest
from feldera import SQLContext
from tests import TEST_CLIENT

class TestAggregatesBase(unittest.TestCase):
    def execute_query(self, data, expected_data, pipeline_name, table_name, view_name, view_query):
        sql = SQLContext(f'{pipeline_name}', TEST_CLIENT).get_or_create()
        
        sql.register_table_from_sql(f''' 
                                    CREATE TABLE {table_name}(
                                        id INT, c1 TINYINT, c2 TINYINT NOT NULL, c3 INT2, c4 INT2 NOT NULL, c5 INT, c6 INT NOT NULL,c7 BIGINT,c8 BIGINT NOT NULL);''')
        sql.register_materialized_view(f"{view_name}", f"{view_query}")
        
        out = sql.listen(f"{view_name}")
        sql.start()
        sql.input_json(f"{table_name}", data)
        sql.wait_for_completion(True)
        out_data = out.to_dict()
        print(out_data)
        for datum in expected_data:
            datum.update({"insert_delete": 1})
        assert expected_data == out_data

class Avg(TestAggregatesBase):
    def test_avg_value(self):
        pipeline_name = "test_avg_value"
        data = [
            {"c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
            {"c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80},
            {"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 61, "c7": 72, "c8": 88}]
        expected_data = [{"c1": 6, "c2": 14, "c3" : 16, "c4" : 29, "c5" : 28, "c6" : 42,	"c7":71, "c8": 58}]
        table_name = "avg_value"
        view_name = "avg_view_value"
        view_query = f'''SELECT
                            AVG(c1) AS c1,AVG(c2) AS c2,AVG(c3) AS c3,AVG(c4) AS c4,AVG(c5) AS c5,AVG(c6) AS c6,AVG(c7) AS c7,AVG(c8) AS c8
                        FROM {table_name}'''                
        self.execute_query(data, expected_data, pipeline_name, table_name, view_name, view_query)
        
class Avg_Groupby(TestAggregatesBase):
    def test_avg_groupby(self):
        pipeline_name = "test_avg_groupby"
        data = [
            {"id" : 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8 },
            {"id" : 0, "c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80},
            {"id" : 1,"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 61, "c7": 72, "c8": 88}]
        # checked manually
        expected_data = [
            {"id": 0, "c1": 1, "c2": 11, "c3": 16, "c4": 22, "c5": 5, "c6": 33, "c7": 70, "c8": 44},
            {"id" : 1, "c1": 11, "c2": 22, "c3" : None, "c4" : 45, "c5" : 51, "c6" : 61, "c7":72, "c8": 88}]
        # in id: 0, returns c3 : 16.0 && in id: 1, returns c3: nan
        table_name = "avg_groupby"
        view_name = "avg_view_groupby"
        view_query = f'''SELECT
                            id,AVG(c1) AS c1, AVG(c2) AS c2, AVG(c3) AS c3, AVG(c4) AS c4, AVG(c5) AS c5, AVG(c6) AS c6, AVG(c7) AS c7, AVG(c8) AS c8
                         FROM {table_name}
                         GROUP BY id;'''
        self.execute_query(data, expected_data, pipeline_name, table_name, view_name, view_query)

class Avg_Distinct(TestAggregatesBase):
    def test_avg_distinct(self):
        pipeline_name ="test_avg_distinct"
        data = [
            {"c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
            {"c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
            {"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        expected_data = [{"c1": 8, "c2": 15, "c3" : 25, "c4" : 27, "c5" : 30, "c6" : 52, "c7": 57, "c8": 54}]
        table_name = "avg_distinct"
        view_name = "avg_view_distinct"
        view_query = f'''SELECT
                            AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2, AVG(DISTINCT c3) AS c3, AVG(DISTINCT c4) AS c4, AVG(DISTINCT c5) AS c5, AVG(DISTINCT c6) AS c6, AVG(DISTINCT c7) AS c7, AVG(DISTINCT c8) AS c8
                        FROM {table_name}'''   
        self.execute_query(data, expected_data, pipeline_name, table_name, view_name, view_query)   

class Avg_Where(TestAggregatesBase):
    def test_avg_where(self):
        pipeline_name ="test_avg_where"
        data = [
            {"c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
            {"c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
            {"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        expected_data = [{"c1": 14, "c2": 21, "c3" : 44, "c4" : 32, "c5" : 34, "c6" : 90, "c7": 29, "c8": 67}]
        table_name = "avg_where"
        view_name = "avg_view_where"
        view_query = f'''SELECT
                            AVG(c1) AS c1, AVG(c2) AS c2, AVG(c3) AS c3, AVG(c4) AS c4, AVG(c5) AS c5, AVG(c6) AS c6, AVG(c7) AS c7, AVG(c8) AS c8
                        FROM {table_name}
                        WHERE 
                            c1 is NOT NULl AND c3 is NOT NULL AND c5 is NOT NULL AND c7 is NOT NULL;'''   
        self.execute_query(data, expected_data, pipeline_name, table_name, view_name, view_query)  
                   
if __name__ == '__main__':
    unittest.main()