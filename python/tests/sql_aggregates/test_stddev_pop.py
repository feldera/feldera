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
        out = pipeline.listen('stddev_pop_view')
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
class Sttdev_POP(TestAggregatesBase):
    def test_stddev_pop_value(self):
        pipeline_name = "test_sttdev_pop_value"
        # validated using postgres
        expected_data = [{'c1': 0, 'c2': 0, 'c3': 0, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': 0, 'c8': 3}]
        table_name = "stddev_pop_value"
        view_query = f'''CREATE VIEW stddev_pop_view AS SELECT
                            STDDEV_POP(c1) AS c1, STDDEV_POP(c2) AS c2, STDDEV_POP(c3) AS c3, STDDEV_POP(c4) AS c4, STDDEV_POP(c5) AS c5, STDDEV_POP(c6) AS c6, STDDEV_POP(c7) AS c7, STDDEV_POP(c8) AS c8
                         FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class Stddev_POP_Groupby(TestAggregatesBase):
    def test_stddev_pop_groupby(self):
        pipeline_name = "test_stddev_pop_groupby"
        # validated using postgres
        new_data = [
            {"id" : 0, "c1": None, "c2": 2, "c3": 3, "c4": 2, "c5": 3, "c6": 4, "c7": 3, "c8": 3},
            {"id" : 1, "c1": None, "c2": 5, "c3": 6, "c4": 2, "c5": 2, "c6": 1, "c7": None, "c8": 5}]
        self.add_data(new_data)
        expected_data = [{'id': 0, 'c1': 0, 'c2': 0, 'c3': 0, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': 0, 'c8': 2},
                         {'id': 1, 'c1': 0, 'c2': 1, 'c3': 1, 'c4': 2, 'c5': 0, 'c6': 1, 'c7': 0, 'c8': 1}]
        table_name = "stddev_pop_groupby"
        view_query = f'''CREATE VIEW stddev_pop_view AS SELECT
                            id, STDDEV_POP(c1) AS c1, STDDEV_POP(c2) AS c2, STDDEV_POP(c3) AS c3, STDDEV_POP(c4) AS c4, STDDEV_POP(c5) AS c5, STDDEV_POP(c6) AS c6, STDDEV_POP(c7) AS c7, STDDEV_POP(c8) AS c8
                         FROM {table_name}
                         GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

@unittest.skip("temporarily disabled; use ad hoc query API to check the results reliably")
class Stddev_POP_Distinct(TestAggregatesBase):
    def test_stddev_pop_distinct(self):
        pipeline_name ="test_stddev_pop_distinct"
        new_data = [
            {"id" : 1, "c1": None, "c2": 2, "c3": 3, "c4": 2, "c5": 3, "c6": 6, "c7": 3, "c8": 2}]
        self.add_data(new_data)
        # validated using postgres
        expected_data = [{'c1': 0, 'c2': 0, 'c3': 0, 'c4': 1, 'c5': 1, 'c6': 1, 'c7': 0, 'c8': 3}]
        table_name = "stddev_pop_distinct"
        view_query = f'''CREATE VIEW stddev_pop_view AS SELECT
                            STDDEV_POP(DISTINCT c1) AS c1, STDDEV_POP(DISTINCT c2) AS c2, STDDEV_POP(DISTINCT c3) AS c3, STDDEV_POP(DISTINCT c4) AS c4, STDDEV_POP(DISTINCT c5) AS c5, STDDEV_POP(DISTINCT c6) AS c6, STDDEV_POP(DISTINCT c7) AS c7, STDDEV_POP(DISTINCT c8) AS c8
                        FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class Stddev_POP_Distinct_Groupby(TestAggregatesBase):
    def test_stddev_pop_distinct_groupby(self):
        pipeline_name = "test_stddev_pop_distinct_groupby"
        # validated using postgres
        new_data = [
            {"id" :0 ,"c1": 4, "c2": 2, "c3": 30, "c4": 14, "c5": None, "c6": 60, "c7": 70, "c8": 18},
            {"id": 1,"c1": 5, "c2": 3, "c3": None, "c4": 9, "c5": 51, "c6": 6, "c7": 72, "c8": 2}]
        self.add_data(new_data)
        expected_data = [
            {'id': 0, 'c1': 0, 'c2': 0, 'c3': 0, 'c4': 5, 'c5': 0, 'c6': 27, 'c7': 0, 'c8': 5},
            {'id': 1, 'c1': 0, 'c2': 0, 'c3': 0, 'c4': 1, 'c5': 24, 'c6': 1, 'c7': 34, 'c8': 0}]
        table_name = "stddev_pop_distinct_groupby"
        view_query = f'''CREATE VIEW stddev_pop_view AS SELECT
                            id, STDDEV_POP(DISTINCT c1) AS c1, STDDEV_POP(DISTINCT c2) AS c2, STDDEV_POP(DISTINCT c3) AS c3, STDDEV_POP(DISTINCT c4) AS c4, STDDEV_POP(DISTINCT c5) AS c5, STDDEV_POP(DISTINCT c6) AS c6, STDDEV_POP(DISTINCT c7) AS c7, STDDEV_POP(DISTINCT c8) AS c8
                        FROM {table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

@unittest.skip("temporarily disabled; use ad hoc query API to check the results reliably")
class Stddev_POP_Where(TestAggregatesBase):
    def test_stddev_pop_where(self):
        pipeline_name ="test_stddev_pop_where"
        # validated using postgres
        expected_data = [{ "c1": 5, "c2": 2, "c3": None, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}]
        table_name = "stddev_pop_where"
        view_query = f'''CREATE VIEW stddev_pop_view AS
                        WITH stddev_val AS(
                            SELECT
                                STDDEV_POP(c1) AS stddev_c1, STDDEV_POP(c2) AS stddev_c2, STDDEV_POP(c3) AS stddev_c3, STDDEV_POP(c4) AS stddev_c4, STDDEV_POP(c5) AS stddev_c5, STDDEV_POP(c6) AS stddev_c6, STDDEV_POP(c7) AS stddev_c7, STDDEV_POP(c8) AS stddev_c8
                            FROM {table_name})
                            SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                            FROM {table_name} t
                            WHERE (t.c8) > (SELECT stddev_c8 FROM stddev_val);'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class Stddev_POP_Where_Groupby(TestAggregatesBase):
    def test_stddev_pop_where_groupby(self):
        pipeline_name ="test_stddev_pop_where_groupby"
        # validated using postgres
        new_data = [
            {"id" : 0, "c1": 0, "c2": 2, "c3": 0, "c4": 18, "c5": 0, "c6": 10, "c7": 0, "c8": 5}]
        self.add_data(new_data)
        expected_data = [{'id': 0, 'stddev_pop_c1': 2, 'stddev_pop_c2': 0, 'stddev_pop_c3': 0, 'stddev_pop_c4': 7, 'stddev_pop_c5': 2, 'stddev_pop_c6': 2, 'stddev_pop_c7': 0, 'stddev_pop_c8': 1}]
        table_name = "stddev_pop_where_groupby1"
        view_query = (f'''CREATE VIEW stddev_pop_view AS
                         WITH stddev_pop_val AS (
                            SELECT
                                STDDEV_POP(c1) AS stddev_pop_c1, STDDEV_POP(c2) AS stddev_pop_c2, STDDEV_POP(c3) AS stddev_pop_c3, STDDEV_POP(c4) AS stddev_pop_c4, STDDEV_POP(c5) AS stddev_pop_c5, STDDEV_POP(c6) AS stddev_pop_c6, STDDEV_POP(c7) AS stddev_pop_c7, STDDEV_POP(c8) AS stddev_pop_c8
                            FROM {table_name})
                            SELECT
                                t.id, STDDEV_POP(t.c1) AS stddev_pop_c1, STDDEV_POP(t.c2) AS stddev_pop_c2, STDDEV_POP(t.c3) AS stddev_pop_c3, STDDEV_POP(t.c4) AS stddev_pop_c4,  STDDEV_POP(t.c5) AS stddev_pop_c5,STDDEV_POP(t.c6) AS stddev_pop_c6,STDDEV_POP(t.c7) AS stddev_pop_c7, STDDEV_POP(t.c8) AS stddev_pop_c8
                            FROM {table_name} t
                            WHERE (t.c8) > (SELECT stddev_pop_c8 FROM stddev_pop_val)
                            GROUP BY t.id;''')

        self.execute_query(pipeline_name, expected_data, table_name, view_query)

if __name__ == '__main__':
    unittest.main()