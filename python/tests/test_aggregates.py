import unittest
from feldera import SQLContext
from tests import TEST_CLIENT

class TestAggregates(unittest.TestCase):
    def test_avg0_value(self):
        sql = SQLContext('test_avg0_value', TEST_CLIENT).get_or_create()
        
        sql.register_table_from_sql(''' 
                                    CREATE TABLE avg0_value(
                                        c1 TINYINT,
                                        c2 TINYINT NOT NULL,
                                        c3 INT2,
                                        c4 INT2 NOT NULL,
                                        c5 INT,
                                        c6 INT NOT NULL,
                                        c7 BIGINT,
                                        c8 BIGINT NOT NULL);
                                    ''')


        sql.register_materialized_view("avg_view_value", '''
                                       SELECT
                                            AVG(c1) AS c1,
                                            AVG(c2) AS c2,
                                            AVG(c3) AS c3,
                                            AVG(c4) AS c4,
                                            AVG(c5) AS c5,
                                            AVG(c6) AS c6,
                                            AVG(c7) AS c7,
                                            AVG(c8) AS c8
                                       FROM avg0_value''')
        data = [
            {"c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
            {"c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80},
            {"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 61, "c7": 72, "c8": 88}]
        
        out = sql.listen("avg_view_value")
        
        sql.start()

        sql.input_json("avg0_value", data)

        expected_data = [{"c1": 6, "c2": 14, "c3" : 16, "c4" : 29, "c5" : 28, "c6" : 42,	"c7":71, "c8": 58}]
        
        sql.wait_for_completion(True)
        
        out_data = out.to_dict()
        
        print(out_data)
        for datum in expected_data:
            datum.update({"insert_delete": 1})
        
        assert expected_data == out_data
        
    def test_avg0_value_groupby(self):
        sql = SQLContext('test_avg0_value_groupby', TEST_CLIENT).get_or_create()
        
        sql.register_table_from_sql(''' 
                                    CREATE TABLE avg0_value_groupby(
                                        id INT,
                                        c1 TINYINT,
                                        c2 TINYINT NOT NULL,
                                        c3 INT2,
                                        c4 INT2 NOT NULL,
                                        c5 INT,
                                        c6 INT NOT NULL,
                                        c7 BIGINT,
                                        c8 BIGINT NOT NULL);
                                    ''')


        sql.register_materialized_view("avg_view_value_groupby", '''
                                       SELECT
                                            id,
                                            AVG(c1) AS c1,
                                            AVG(c2) AS c2,
                                            AVG(c3) AS c3,
                                            AVG(c4) AS c4,
                                            AVG(c5) AS c5,
                                            AVG(c6) AS c6,
                                            AVG(c7) AS c7,
                                            AVG(c8) AS c8
                                       FROM avg0_value_groupby
                                       GROUP BY id;''')
        data = [
            {"id" : 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8 },
            {"id" : 0, "c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70, "c8": 80},
            {"id" : 1,"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 61, "c7": 72, "c8": 88}]
        
        out = sql.listen("avg_view_value_groupby")
        
        sql.start()

        sql.input_json("avg0_value_groupby", data)
        
        # checked manually
        expected_data = [
            {"id": 0, "c1": 1, "c2": 11, "c3": 16, "c4": 22, "c5": 5, "c6": 33, "c7": 70, "c8": 44},
            {"id" : 1, "c1": 11, "c2": 22, "c3" : None, "c4" : 45, "c5" : 51, "c6" : 61, "c7":72, "c8": 88}]
        
        sql.wait_for_completion(True)
        
        out_data = out.to_dict()
        print(type(out_data))  
        
        def check_dict_types(out_data):
            for i, item in enumerate(out_data):
                print(f"Dictionary {i}:")
                if isinstance(item, dict):
                    for key, value in item.items():
                        print(f"  Key: '{key}' has value '{value}' of type {type(value)}")
                else:
                    print(f"Item {i} is not a dictionary.")

        check_dict_types(data)
        
        # in id: 0, returns c3 : 16.0 && in id: 1, returns c3: nan
        
        print(out_data)
        
    
    def test_avg0_distinct(self):
        sql = SQLContext('test_avg0_distinct', TEST_CLIENT).get_or_create()
        
        sql.register_table_from_sql(''' 
                                    CREATE TABLE avg0_distinct(
                                        c1 TINYINT,
                                        c2 TINYINT NOT NULL,
                                        c3 INT2,
                                        c4 INT2 NOT NULL,
                                        c5 INT,
                                        c6 INT NOT NULL,
                                        c7 BIGINT,
                                        c8 BIGINT NOT NULL);
                                    ''')


        sql.register_materialized_view("avg_view_distinct", '''
                                       SELECT
                                            AVG(DISTINCT c1) AS c1,
                                            AVG(DISTINCT c2) AS c2,
                                            AVG(DISTINCT c3) AS c3,
                                            AVG(DISTINCT c4) AS c4,
                                            AVG(DISTINCT c5) AS c5,
                                            AVG(DISTINCT c6) AS c6,
                                            AVG(DISTINCT c7) AS c7,
                                            AVG(DISTINCT c8) AS c8
                                       FROM avg0_distinct''')
        data = [
            {"c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
            {"c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
            {"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        
        out = sql.listen("avg_view_distinct")
        
        sql.start()

        sql.input_json("avg0_distinct", data)
        
        expected_data = [{"c1": 9, "c2": 15, "c3" : 26, "c4" : 27, "c5" : 30, "c6" : 52, "c7": 57, "c8": 54}]

        sql.wait_for_completion(True)
        
        out_data = out.to_dict()
        
        for datum in expected_data:
            datum.update({"insert_delete": 1})
        
        assert expected_data == out_data
        
    def test_avg0_distinct_groupby(self):
        sql = SQLContext('test_avg0_distinct_groupby', TEST_CLIENT).get_or_create()
        
        sql.register_table_from_sql(''' 
                                    CREATE TABLE avg0_distinct_groupby(
                                        id INT,
                                        c1 TINYINT,
                                        c2 TINYINT NOT NULL,
                                        c3 INT2,
                                        c4 INT2 NOT NULL,
                                        c5 INT,
                                        c6 INT NOT NULL,
                                        c7 BIGINT,
                                        c8 BIGINT NOT NULL);
                                    ''')

        sql.register_materialized_view("avg_view_distinct_groupby", '''
                                       SELECT
                                            id,
                                            AVG(DISTINCT c1) AS c1,
                                            AVG(DISTINCT c2) AS c2,
                                            AVG(DISTINCT c3) AS c3,
                                            AVG(DISTINCT c4) AS c4,
                                            AVG(DISTINCT c5) AS c5,
                                            AVG(DISTINCT c6) AS c6,
                                            AVG(DISTINCT c7) AS c7,
                                            AVG(DISTINCT c8) AS c8
                                       FROM avg0_distinct_groupby
                                       GROUP BY id;''')
        data = [
            {"id" :0 ,"c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
            {"id" :0 ,"c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
            {"id": 1,"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"id": 1,"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        
        out = sql.listen("avg_view_distinct_groupby")
        
        sql.start()

        sql.input_json("avg0_distinct_groupby", data)
        
        expected_data = [
            {"id": 0, "c1": 1, "c2": 2, "c3": 16, "c4": 4, "c5": 5, "c6": 33, "c7": 70, "c8": 8},
            {"id" : 1, "c1": 12, "c2": 21, "c3" : 44, "c4" : 38, "c5" : 42, "c6" : 48, "c7":50, "c8": 77}]
        
        sql.wait_for_completion(True)
        
        out_data = out.to_dict()
        
        for datum in expected_data:
            datum.update({"insert_delete": 1})
        
        assert expected_data == out_data
        
    def test_avg0_where(self):
        sql = SQLContext('test_avg0_where', TEST_CLIENT).get_or_create()
        
        sql.register_table_from_sql(''' 
                                    CREATE TABLE avg0_where(
                                        c1 TINYINT,
                                        c2 TINYINT NOT NULL,
                                        c3 INT2,
                                        c4 INT2 NOT NULL,
                                        c5 INT,
                                        c6 INT NOT NULL,
                                        c7 BIGINT,
                                        c8 BIGINT NOT NULL);''')
        
        sql.register_materialized_view("avg_view_where", '''
                                       SELECT
                                            AVG(c1) AS c1,
                                            AVG(c2) AS c2,
                                            AVG(c3) AS c3,
                                            AVG(c4) AS c4,
                                            AVG(c5) AS c5,
                                            AVG(c6) AS c6,
                                            AVG(c7) AS c7,
                                            AVG(c8) AS c8
                                       FROM avg0_where
                                       WHERE 
                                            c1 is NOT NULl AND
                                            c3 is NOT NULL AND
                                            c5 is NOT NULL AND
                                            c7 is NOT NULL;''')
        data = [
            {"c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
            {"c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
            {"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        
        out = sql.listen("avg_view_where")
        
        sql.start()

        sql.input_json("avg0_where", data)
        
        expected_data = [{"c1": 14, "c2": 21, "c3" : 44, "c4" : 32, "c5" : 34, "c6" : 90, "c7": 29, "c8": 67}]

        sql.wait_for_completion(True)
        
        out_data = out.to_dict()
        
        for datum in expected_data:
            datum.update({"insert_delete": 1})
        
        assert expected_data == out_data
    
    def test_avg0_where_groupby(self):
        sql = SQLContext('test_avg0_where_groupby', TEST_CLIENT).get_or_create()
        
        sql.register_table_from_sql(''' 
                                    CREATE TABLE avg0_where_groupby(
                                        id INT,
                                        c1 TINYINT,
                                        c2 TINYINT NOT NULL,
                                        c3 INT2,
                                        c4 INT2 NOT NULL,
                                        c5 INT,
                                        c6 INT NOT NULL,
                                        c7 BIGINT,
                                        c8 BIGINT NOT NULL);''')
        
        sql.register_materialized_view("avg_view_where_groupby", '''
                                       SELECT
                                            id,
                                            AVG(c1) AS c1,
                                            AVG(c2) AS c2,
                                            AVG(c3) AS c3,
                                            AVG(c4) AS c4,
                                            AVG(c5) AS c5,
                                            AVG(c6) AS c6,
                                            AVG(c7) AS c7,
                                            AVG(c8) AS c8
                                       FROM avg0_where_groupby
                                       WHERE 
                                            c1 is NOT NULl AND
                                            c3 is NOT NULL AND
                                            c5 is NOT NULL AND
                                            c7 is NOT NULL
                                        GROUP BY id;''')
        data = [
            {"id": 0,"c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8},
            {"id": 0,"c1": None, "c2": 2, "c3": 30, "c4": 4, "c5": None, "c6": 60, "c7": 70, "c8": 8},
            {"id": 1,"c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 6, "c7": 72, "c8": 88},
            {"id" :1,"c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]
        
        out = sql.listen("avg_view_where_groupby")
        
        sql.start()

        sql.input_json("avg0_where_groupby", data)
        
        expected_data = [
            {"id": 1, "c1": 14, "c2": 21, "c3": 44, "c4": 32, "c5": 34, "c6": 90, "c7": 29, "c8": 67}]

        sql.wait_for_completion(True)
        
        out_data = out.to_dict()
        
        for datum in expected_data:
            datum.update({"insert_delete": 1})
        
        assert expected_data == out_data