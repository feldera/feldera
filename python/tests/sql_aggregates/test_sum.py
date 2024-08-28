import unittest
from feldera import PipelineBuilder, Pipeline
from tests import TEST_CLIENT


class TestAggregatesBase(unittest.TestCase):
    def setUp(self) -> None:
        self.data = [{"insert": {"id": 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}},
                     {"insert": {"id": 0, "c1": None, "c2": 20, "c3": 30, "c4": 40, "c5": None, "c6": 60, "c7": 70,
                                 "c8": 80}},
                     {"insert": {"id": 1, "c1": 11, "c2": 22, "c3": None, "c4": 45, "c5": 51, "c6": 61, "c7": 72,
                                 "c8": 88}}]
        return super().setUp()

    def execute_query(self, pipeline_name, expected_data, table_name, view_query):
        sql = f'''CREATE TABLE {table_name}(
                    id INT, c1 TINYINT, c2 TINYINT NOT NULL, c3 INT2, c4 INT2 NOT NULL, c5 INT, c6 INT NOT NULL,c7 BIGINT,c8 BIGINT NOT NULL);''' + view_query
        pipeline = PipelineBuilder(TEST_CLIENT, f'{pipeline_name}', sql=sql).create_or_replace()
        out = pipeline.listen('sum_view')
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


class Sum(TestAggregatesBase):
    def test_sum_value(self):
        pipeline_name = "test_sum_value"
        # validated using postgres
        expected_data = [{"c1": 12, "c2": 44, "c3": 33, "c4": 89, "c5": 56, "c6": 127, "c7": 142, "c8": 176}]
        table_name = "sum_value"
        view_query = f'''CREATE VIEW sum_view AS SELECT
                            SUM(c1) AS c1, SUM(c2) AS c2, SUM(c3) AS c3, SUM(c4) AS c4, SUM(c5) AS c5, SUM(c6) AS c6, SUM(c7) AS c7, SUM(c8) AS c8
                         FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class Sum_Groupby(TestAggregatesBase):
    def test_sum_groupby(self):
        pipeline_name = "test_sum_groupby"
        # validated using postgres
        new_data = [
            {"id": 1, "c1": 23, "c2": 26, "c3": None, "c4": 9, "c5": 3, "c6": 20, "c7": 43, "c8": 3}]
        self.add_data(new_data)
        expected_data = [{"id": 0, "c1": 1, "c2": 22, "c3": 33, "c4": 44, "c5": 5, "c6": 66, "c7": 70, "c8": 88},
                         {"id": 1, "c1": 34, "c2": 48, "c3": None, "c4": 54, "c5": 54, "c6": 81, "c7": 115, "c8": 91}]
        table_name = "sum_groupby"
        view_query = f'''CREATE VIEW sum_view AS SELECT
                            id, SUM(c1) AS c1, SUM(c2) AS c2, SUM(c3) AS c3, SUM(c4) AS c4, SUM(c5) AS c5, SUM(c6) AS c6, SUM(c7) AS c7, SUM(c8) AS c8
                         FROM {table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)


class Sum_Distinct(TestAggregatesBase):
    def test_sum_distinct(self):
        pipeline_name = "test_sum_distinct"
        # validated using postgres
        expected_data = [{"c1": 12, "c2": 44, "c3": 33, "c4": 89, "c5": 56, "c6": 127, "c7": 142, "c8": 176}]
        table_name = "sum_distinct"
        view_query = f'''CREATE VIEW sum_view AS SELECT
                            SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2, SUM(DISTINCT c3) AS c3, SUM(DISTINCT c4) AS c4, SUM(DISTINCT c5) AS c5, SUM(DISTINCT c6) AS c6, SUM(DISTINCT c7) AS c7, SUM(DISTINCT c8) AS c8
                        FROM {table_name}'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)


class Sum_Distinct_Groupby(TestAggregatesBase):
    def test_sum_distinct_groupby(self):
        pipeline_name = "test_sum_distinct_groupby"
        # validated using postgres
        new_data = [
            {"id": 1, "c1": 27, "c2": 55, "c3": 37, "c4": 84, "c5": 11, "c6": 7, "c7": 6, "c8": 8}]
        self.add_data(new_data)
        expected_data = [
            {"id": 0, "c1": 1, "c2": 22, "c3": 33, "c4": 44, "c5": 5, "c6": 66, "c7": 70, "c8": 88},
            {"id": 1, "c1": 38, "c2": 77, "c3": 37, "c4": 129, "c5": 62, "c6": 68, "c7": 78, "c8": 96}]
        table_name = "sum_distinct_groupby"
        view_query = f'''CREATE VIEW sum_view AS SELECT
                            id, SUM(DISTINCT c1) AS c1, SUM(DISTINCT c2) AS c2, SUM(DISTINCT c3) AS c3, SUM(DISTINCT c4) AS c4, SUM(DISTINCT c5) AS c5, SUM(DISTINCT c6) AS c6, SUM(DISTINCT c7) AS c7, SUM(DISTINCT c8) AS c8
                         FROM {table_name}
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)


class Sum_Where(TestAggregatesBase):
    def test_sum_where0(self):
        pipeline_name = "test_sum_where"
        # validated using postgres
        expected_data = [{"c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}]
        table_name = "sum_where"
        view_query = f'''CREATE VIEW sum_view AS SELECT
                            SUM(c1) AS c1, SUM(c2) AS c2, SUM(c3) AS c3, SUM(c4) AS c4, SUM(c5) AS c5, SUM(c6) AS c6, SUM(c7) AS c7, SUM(c8) AS c8
                        FROM {table_name}
                        WHERE 
                            c1 is NOT NULl AND c3 is NOT NULL;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

    def test_sum_where1(self):
        pipeline_name = "test_sum_where1"
        # validated using postgres
        new_data = [
            {"id": 1, "c1": 33, "c2": 23, "c3": 301, "c4": 412, "c5": 333, "c6": 601, "c7": 710, "c8": 988}]
        self.add_data(new_data)
        expected_data = [
            {"c1": 33, "c2": 23, "c3": 301, "c4": 412, "c5": 333, "c6": 601, "c7": 710, "c8": 988}]
        table_name = "sum_where1"
        view_query = f'''CREATE VIEW sum_view AS
                        WITH sum_val AS(
                            SELECT
                                SUM(c1) AS sum_c1, SUM(c2) AS sum_c2, SUM(c3) AS sum_c3, SUM(c4) AS sum_c4, SUM(c5) AS sum_c5, SUM(c6) AS sum_c6, SUM(c7) AS sum_c7, SUM(c8) AS sum_c8
                            FROM {table_name})
                            SELECT t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, t.c8
                            FROM {table_name} t
                            WHERE (t.c4+T.c5) > (SELECT sum_c4 FROM sum_val);'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

class Sum_Where_Groupby(TestAggregatesBase):
    def test_sum_where_groupy0(self):
        pipeline_name = "test_sum_where_groupby0"
        # validated using postgres
        expected_data = [{"id": 0, "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5, "c6": 6, "c7": None, "c8": 8}]
        table_name = "sum_where_groupby0"
        view_query = f'''CREATE VIEW sum_view AS SELECT
                            id, SUM(c1) AS c1, SUM(c2) AS c2, SUM(c3) AS c3, SUM(c4) AS c4, SUM(c5) AS c5, SUM(c6) AS c6, SUM(c7) AS c7, SUM(c8) AS c8
                        FROM {table_name}
                        WHERE 
                            c1 is NOT NULl AND c3 is NOT NULL
                        GROUP BY id;'''
        self.execute_query(pipeline_name, expected_data, table_name, view_query)

    def test_sum_where_groupby1(self):
        pipeline_name = "test_sum_where_groupby1"
        # validated using postgres
        expected_data = [{"id": 1, "sum_c1": 11, "sum_c2": 22, "sum_c3": None, "sum_c4": 45, "sum_c5": 51, "sum_c6": 61,
                          "sum_c7": 72, "sum_c8": 88}]
        table_name = "sum_where_groupby1"
        view_query = (f'''CREATE VIEW sum_view AS
                         WITH sum_val AS (
                            SELECT
                                SUM(c1) AS sum_c1, SUM(c2) AS sum_c2, SUM(c3) AS sum_c3, SUM(c4) AS sum_c4, SUM(c5) AS sum_c5, SUM(c6) AS sum_c6, SUM(c7) AS sum_c7, SUM(c8) AS sum_c8
                            FROM {table_name})
                            SELECT
                                t.id, SUM(t.c1) AS sum_c1, SUM(t.c2) AS sum_c2, SUM(t.c3) AS sum_c3, SUM(t.c4) AS sum_c4,  SUM(t.c5) AS sum_c5, SUM(t.c6) AS sum_c6, SUM(t.c7) AS sum_c7, SUM(t.c8) AS sum_c8
                            FROM {table_name} t
                            WHERE (t.c4 + t.c5) > (SELECT sum_c4 FROM sum_val)
                            GROUP BY t.id;''')

        self.execute_query(pipeline_name, expected_data, table_name, view_query)

if __name__ == '__main__':
    unittest.main()