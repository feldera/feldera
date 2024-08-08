import unittest
from feldera import SQLContext
from tests import TEST_CLIENT

class TestAggregatesBase(unittest.TestCase):
    def setUp(self) -> None:
        self.data = [{"insert":{"id": 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}},]       
        return super().setUp()
        
    def execute_query(self, expected_data, pipeline_name, table_name, view_name, view_query):
        sql = SQLContext(f'{pipeline_name}', TEST_CLIENT).get_or_create()
        sql.register_table_from_sql(f''' 
                                    CREATE TABLE {table_name}(
                                        id INT, c1 TINYINT, c2 TINYINT NOT NULL, c3 INT2, c4 INT2 NOT NULL, c5 INT, c6 INT NOT NULL,c7 BIGINT,c8 BIGINT NOT NULL);''')
        sql.register_materialized_view(f"{view_name}", f"{view_query}")
        out = sql.listen(f"{view_name}")
        sql.start()
        sql.input_json(f"{table_name}", self.data, update_format= "insert_delete")
        sql.wait_for_completion(True)
        out_data = out.to_dict()
        print(out_data)
        for datum in expected_data:
            datum.update({"insert_delete": 1})
        assert expected_data == out_data
        
    def add_data(self, new_data, delete: bool = False):
        key = "delete" if delete else "insert"
        for datum in new_data:
            self.data.append({key: datum})

class Avg(TestAggregatesBase):
    def test_avg_value(self):
        pipeline_name = "test_avg_value"
        # validated using postgres
        new_data = [
            {"c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80},
            {"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 61, "c7": 72, "c8": 88}]
        self.add_data(new_data)
        expected_data = [{"c1": 6, "c2": 14, "c3" : 16, "c4" : 29, "c5" : 28, "c6" : 42,	"c7":71, "c8": 58}]
        table_name = "avg_value"
        view_name = "avg_view_value"
        view_query = f'''SELECT
                            AVG(c1) AS c1,AVG(c2) AS c2,AVG(c3) AS c3,AVG(c4) AS c4,AVG(c5) AS c5,AVG(c6) AS c6,AVG(c7) AS c7,AVG(c8) AS c8
                        FROM {table_name}'''                
        self.execute_query(expected_data, pipeline_name, table_name, view_name, view_query)

@unittest.skip("https://github.com/feldera/feldera/issues/2142")
class Avg_Groupby(TestAggregatesBase):
    def test_avg_groupby(self):
        pipeline_name = "test_avg_groupby"
        new_data = [
            {"id" : 0, "c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80},
            {"id" : 1,"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 61, "c7": 72, "c8": 88}]
        self.add_data(new_data)
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
        self.execute_query(expected_data, pipeline_name, table_name, view_name, view_query)

class Avg_Distinct(TestAggregatesBase):
    def test_avg_distinct(self):
        pipeline_name ="test_avg_distinct"
        # validated using postgres
        new_data = [
            {"c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
            {"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        self.add_data(new_data)
        expected_data = [{"c1": 8, "c2": 15, "c3" : 25, "c4" : 27, "c5" : 30, "c6" : 52, "c7": 57, "c8": 54}]
        table_name = "avg_distinct"
        view_name = "avg_view_distinct"
        view_query = f'''SELECT
                            AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2, AVG(DISTINCT c3) AS c3, AVG(DISTINCT c4) AS c4, AVG(DISTINCT c5) AS c5, AVG(DISTINCT c6) AS c6, AVG(DISTINCT c7) AS c7, AVG(DISTINCT c8) AS c8
                        FROM {table_name}'''   
        self.execute_query(expected_data, pipeline_name, table_name, view_name, view_query)   

class Avg_Distinct_Groupby(TestAggregatesBase):
    def test_distinct_groupby(self):
        pipeline_name = "test_distinct_groupby"
        # validated using postgres
        new_data = [
            {"id" :0 ,"c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
            {"id": 1,"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"id": 1,"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        self.add_data(new_data)
        expected_data = [
            {"id": 0, "c1": 1, "c2": 2, "c3": 16, "c4": 4, "c5": 5, "c6": 33, "c7": 70, "c8": 8},
            {"id" : 1, "c1": 12, "c2": 21, "c3" : 44, "c4" : 38, "c5" : 42, "c6" : 48, "c7":50, "c8": 77}]
        table_name = "avg_distinct_groupby"
        view_name = "avg_view_distinct_groupby"
        view_query = f'''SELECT
                            id, AVG(DISTINCT c1) AS c1, AVG(DISTINCT c2) AS c2, AVG(DISTINCT c3) AS c3, AVG(DISTINCT c4) AS c4, AVG(DISTINCT c5) AS c5, AVG(DISTINCT c6) AS c6, AVG(DISTINCT c7) AS c7, AVG(DISTINCT c8) AS c8
                         FROM {table_name}
                         GROUP BY id;'''
        self.execute_query( expected_data, pipeline_name, table_name, view_name, view_query)  

class Avg_Where(TestAggregatesBase):
    def test_avg_where0(self):
        pipeline_name ="test_avg_where"
        # validated using postgres
        new_data = [
            {"c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
            {"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        self.add_data(new_data)
        expected_data = [{"c1": 14, "c2": 21, "c3" : 44, "c4" : 32, "c5" : 34, "c6" : 90, "c7": 29, "c8": 67}]
        table_name = "avg_where"
        view_name = "avg_view_where"
        view_query = f'''SELECT
                            AVG(c1) AS c1, AVG(c2) AS c2, AVG(c3) AS c3, AVG(c4) AS c4, AVG(c5) AS c5, AVG(c6) AS c6, AVG(c7) AS c7, AVG(c8) AS c8
                        FROM {table_name}
                        WHERE 
                            c1 is NOT NULl AND c3 is NOT NULL AND c5 is NOT NULL AND c7 is NOT NULL;'''   
        self.execute_query(expected_data, pipeline_name, table_name, view_name, view_query)
    
    @unittest.skip("https://github.com/feldera/feldera/issues/2142")
    def test_avg_where1(self):
        pipeline_name ="test_avg_where1"
        # checked manually
        new_data = [
            {"c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
            {"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        self.add_data(new_data)
        # in row: 1, returns c3 : 1nan && in row : 2, returns c3: 44.0
        expected_data = [
            {"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        table_name = "avg_where1"
        view_name = "avg_view_where1"
        view_query = f'''WITH avg_val AS(
                            SELECT
                                FLOOR(AVG(c1)) AS avg_c1, FLOOR(AVG(c2)) AS avg_c2, FLOOR(AVG(c3)) AS avg_c3, FLOOR(AVG(c4)) AS avg_c4, FLOOR(AVG(c5)) AS avg_c5, FLOOR(AVG(c6)) AS avg_c6, FLOOR(AVG(c7)) AS avg_c7, FLOOR(AVG(c8)) AS avg_c8
                            FROM {table_name})
                            SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                            FROM {table_name} t
                            WHERE t.c2 > (SELECT avg_c2 FROM avg_val);'''   
        self.execute_query(expected_data, pipeline_name, table_name, view_name, view_query)    

class Avg_Where_Groupby(TestAggregatesBase):
    def test_avg_where(self):
        pipeline_name ="test_avg_where_groupby"
        # validated using postgres
        new_data = [
            {"id": 0,"c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
            {"id": 1,"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"id" :1,"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        self.add_data(new_data)
        expected_data = [{"id": 1, "c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        table_name = "avg_where_groupby"
        view_name = "avg_view_where_groupby"
        view_query = f'''SELECT
                            id, AVG(c1) AS c1, AVG(c2) AS c2, AVG(c3) AS c3, AVG(c4) AS c4, AVG(c5) AS c5, AVG(c6) AS c6, AVG(c7) AS c7, AVG(c8) AS c8
                        FROM {table_name}
                        WHERE 
                            c1 is NOT NULl AND c3 is NOT NULL AND c5 is NOT NULL AND c7 is NOT NULL
                        GROUP BY id;'''   
        self.execute_query(expected_data, pipeline_name, table_name, view_name, view_query) 
        
    @unittest.skip("https://github.com/feldera/feldera/issues/2142")
    def test_avg_where1(self):
        pipeline_name ="test_avg_where_groupby1"
        # checked manually
        new_data = [
            {"id": 0,"c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
            {"id": 0,"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"id": 1,"c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
            {"id" :1,"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        self.add_data(new_data)
        # in id: 0, returns c3 : nan && in id: 1, returns c3: 44.0
        expected_data = [
            {"id": 0,"avg_c1": 11, "avg_c2": 22, "avg_c3": None, "avg_c4": 45, "avg_c5": 51, "avg_c6": 6, "avg_c7": 72, "avg_c8": 88},
            {"id" :1,"avg_c1": 14, "avg_c2": 21, "avg_c3": 44, "avg_c4": 32, "avg_c5": 34, "avg_c6": 90, "avg_c7": 29, "avg_c8": 67}]
        table_name = "avg_where_groupby1"
        view_name = "avg_view_where_groupby1"
        view_query = f'''WITH avg_val AS (
                            SELECT
                                FLOOR(AVG(c1)) AS avg_c1, FLOOR(AVG(c2)) AS avg_c2, FLOOR(AVG(c3)) AS avg_c3, FLOOR(AVG(c4)) AS avg_c4, FLOOR(AVG(c5)) AS avg_c5, FLOOR(AVG(c6)) AS avg_c6, FLOOR(AVG(c7)) AS avg_c7, FLOOR(AVG(c8)) AS avg_c8
                            FROM {table_name})
                            SELECT
                                t.id, AVG(t.c1) AS avg_c1, AVG(t.c2) AS avg_c2, AVG(t.c3) AS avg_c3, AVG(t.c4) AS avg_c4,  AVG(t.c5) AS avg_c5, AVG(t.c6) AS avg_c6, AVG(t.c7) AS avg_c7, AVG(t.c8) AS avg_c8
                            FROM {table_name} t
                            WHERE t.c4 > (SELECT avg_c4 FROM avg_val)
                            GROUP BY t.id;'''   
                         
        self.execute_query(expected_data, pipeline_name, table_name, view_name, view_query) 

class Avg_Fail0(TestAggregatesBase):
    # checks for assertion fail on incorrect expected output
    def test_avg_fail0(self):
        pipeline_name ="test_avg_fail0"
        new_data = [
            {"c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80},
            {"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 61, "c7": 72, "c8": 88}]
        self.add_data(new_data)
        expected_data = [{"c1": 1, "c2": 2, "c3" : 3, "c4" : 4, "c5" : 5, "c6" : 6,	"c7":7, "c8": 8}] 
        table_name = "avg_fail0"
        view_name = "avg_view_fail0"
        view_query = f'''SELECT
                            AVG(c1) AS c1,AVG(c2) AS c2,AVG(c3) AS c3,AVG(c4) AS c4,AVG(c5) AS c5,AVG(c6) AS c6,AVG(c7) AS c7,AVG(c8) AS c8
                        FROM {table_name}'''  
        with self.assertRaises(AssertionError):              
            self.execute_query(expected_data, pipeline_name, table_name, view_name, view_query)
    
if __name__ == '__main__':
    unittest.main()