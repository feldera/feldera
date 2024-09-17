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
        out = pipeline.listen('bit_or_view')
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
class Bit_OR(TestAggregatesBase):
    def test_bit_or_value(self):
        pipeline_name = "test_bit_or_value"
        # validated using postgres
        expected_data = [{'c1': 5, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 7, 'c6': 7, 'c7': 4, 'c8': 10}]
        table_name = "bit_or_value"
        view_query = f'''CREATE VIEW bit_or_view AS SELECT
                            BIT_OR(c1) AS c1, BIT_OR(c2) AS c2, BIT_OR(c3) AS c3, BIT_OR(c4) AS c4, BIT_OR(c5) AS c5, BIT_OR(c6) AS c6, BIT_OR(c7) AS c7, BIT_OR(c8) AS c8
                         FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class BIT_OR_Groupby(TestAggregatesBase):
    def test_bit_or_groupby(self):
        pipeline_name = "test_bit_or_groupby"
        # validated using postgres
        new_data = [
            {"id" : 0, "c1": 5, "c2": 9, "c3": 10, "c4": 18, "c5": 8, "c6": 10, "c7": 20, "c8": 5}]
        self.add_data(new_data)
        expected_data = [{'id': 0, 'c1': 5, 'c2': 11, 'c3': 10, 'c4': 22, 'c5': 13, 'c6': 14, 'c7': 20, 'c8': 13},
                         {'id': 1, 'c1': 4, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4, 'c8': 2}]
        table_name = "bit_or_groupby"
        view_query = f'''CREATE VIEW bit_or_view AS SELECT
                            id, BIT_OR(c1) AS c1, BIT_OR(c2) AS c2, BIT_OR(c3) AS c3, BIT_OR(c4) AS c4, BIT_OR(c5) AS c5, BIT_OR(c6) AS c6, BIT_OR(c7) AS c7, BIT_OR(c8) AS c8
                         FROM {table_name}
                         GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

@unittest.skip("temporarily disabled; use ad hoc query API to check the results reliably")
class Bit_OR_Distinct(TestAggregatesBase):
    def test_bit_or_distinct(self):
        pipeline_name ="test_bit_or_distinct"
        # validated using postgres
        expected_data = [{'c1': 5, 'c2': 3, 'c3': 4, 'c4': 6, 'c5': 7, 'c6': 7, 'c7': 4, 'c8': 10}]
        table_name = "bit_or_distinct"
        view_query = f'''CREATE VIEW bit_or_view AS SELECT
                            BIT_OR(DISTINCT c1) AS c1, BIT_OR(DISTINCT c2) AS c2, BIT_OR(DISTINCT c3) AS c3, BIT_OR(DISTINCT c4) AS c4, BIT_OR(DISTINCT c5) AS c5, BIT_OR(DISTINCT c6) AS c6, BIT_OR(DISTINCT c7) AS c7, BIT_OR(DISTINCT c8) AS c8
                        FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class Bit_OR_Distinct_Groupby(TestAggregatesBase):
    def test_bit_or_distinct_groupby(self):
        pipeline_name = "test_bit_or_distinct_groupby"
        # validated using postgres
        new_data = [
            {"id" :0 ,"c1": 4, "c2": 2, "c3": 30, "c4": 14, "c5": None, "c6": 60, "c7": 70, "c8": 18},
            {"id": 1,"c1": 5, "c2": 3, "c3": None, "c4": 9, "c5": 51, "c6": 6, "c7": 72, "c8": 2}]
        self.add_data(new_data)
        expected_data = [{'id': 0, 'c1': 5, 'c2': 2, 'c3': 30, 'c4': 14, 'c5': 5, 'c6': 62, 'c7': 70, 'c8': 26},
                         {'id': 1, 'c1': 5, 'c2': 3, 'c3': 4, 'c4': 15, 'c5': 51, 'c6': 7, 'c7': 76, 'c8': 2}]
        table_name = "bit_or_distinct_groupby"
        view_query = f'''CREATE VIEW bit_or_view AS SELECT
                            id, BIT_OR(DISTINCT c1) AS c1, BIT_OR(DISTINCT c2) AS c2, BIT_OR(DISTINCT c3) AS c3, BIT_OR(DISTINCT c4) AS c4, BIT_OR(DISTINCT c5) AS c5, BIT_OR(DISTINCT c6) AS c6, BIT_OR(DISTINCT c7) AS c7, BIT_OR(DISTINCT c8) AS c8
                        FROM {table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

@unittest.skip("temporarily disabled; use ad hoc query API to check the results reliably")
class Bit_OR_Where(TestAggregatesBase):
    def test_bit_or_where1(self):
        pipeline_name ="test_bit_or_where"
        # validated using postgres
        expected_data = [{'c1': 4, 'c2': 3, 'c3': 4.0, 'c4': 6, 'c5': 2, 'c6': 3, 'c7': 4.0, 'c8': 2},
                         {'c1': 5, 'c2': 2, 'c3': None, 'c4': 4, 'c5': 5, 'c6': 6, 'c7': None, 'c8': 8}]
        table_name = "bit_or_where"
        view_query = f'''CREATE VIEW bit_or_view AS
                        WITH bit_or_val AS(
                            SELECT
                                BIT_OR(c1) AS bit_or_c1, BIT_OR(c2) AS bit_or_c2, BIT_OR(c3) AS bit_or_c3, BIT_OR(c4) AS bit_or_c4, BIT_OR(c5) AS bit_or_c5, BIT_OR(c6) AS bit_or_c6, BIT_OR(c7) AS bit_or_c7, BIT_OR(c8) AS bit_or_c8
                            FROM {table_name})
                            SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                            FROM {table_name} t
                            WHERE (t.c8) < (SELECT bit_or_c8 FROM bit_or_val);'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class BIT_OR_Where_Groupby(TestAggregatesBase):
    def test_bit_or_where_groupby(self):
        pipeline_name ="test_bit_or_where_groupby"
        # validated using postgres
        new_data = [
            {"id" : 0, "c1": 5, "c2": 9, "c3": 10, "c4": 18, "c5": 8, "c6": 10, "c7": 20, "c8": 5}]
        self.add_data(new_data)
        expected_data = [{'id': 0, 'bit_or_c1': 5, 'bit_or_c2': 11, 'bit_or_c3': 10, 'bit_or_c4': 22, 'bit_or_c5': 13, 'bit_or_c6': 14, 'bit_or_c7': 20, 'bit_or_c8': 13},
                         {'id': 1, 'bit_or_c1': 4, 'bit_or_c2': 3, 'bit_or_c3': 4, 'bit_or_c4': 6, 'bit_or_c5': 2, 'bit_or_c6': 3, 'bit_or_c7': 4, 'bit_or_c8': 2}]
        table_name = "bit_or_where_groupby"
        view_query = (f'''CREATE VIEW bit_or_view AS
                         WITH bit_or_val AS (
                            SELECT
                                BIT_OR(c1) AS bit_or_c1, BIT_OR(c2) AS bit_or_c2, BIT_OR(c3) AS bit_or_c3, BIT_OR(c4) AS bit_or_c4, BIT_OR(c5) AS bit_or_c5, BIT_OR(c6) AS bit_or_c6, BIT_OR(c7) AS bit_or_c7, BIT_OR(c8) AS bit_or_c8
                            FROM {table_name})
                            SELECT
                                t.id, BIT_OR(t.c1) AS bit_or_c1, BIT_OR(t.c2) AS bit_or_c2, BIT_OR(t.c3) AS bit_or_c3, BIT_OR(t.c4) AS bit_or_c4,  BIT_OR(t.c5) AS bit_or_c5, BIT_OR(t.c6) AS bit_or_c6, BIT_OR(t.c7) AS bit_or_c7, BIT_OR(t.c8) AS bit_or_c8
                            FROM {table_name} t
                            WHERE (t.c8) < (SELECT bit_or_c8 FROM bit_or_val)
                            GROUP BY t.id;''')

        self.execute_query(pipeline_name, expected_data, table_name, view_query)

if __name__ == '__main__':
    unittest.main()