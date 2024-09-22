import unittest
from feldera import PipelineBuilder, Pipeline
from tests import TEST_CLIENT

class TestAggregatesBase(unittest.TestCase):
    def setUp(self) -> None:
        self.table_name = None
        self.view_name = None
        self.sql = None
        self.data = []
        return super().setUp()

    def execute_query(self, pipeline_name, expected_data, view_query):
        if self.table_name == None or self.view_name == None:
            raise RuntimeError("self.table_name or self.view_name not set.")

        sql = self.sql + view_query
        pipeline = PipelineBuilder(TEST_CLIENT, f'{pipeline_name}', sql=sql).create_or_replace()
        out = pipeline.listen(self.view_name)
        pipeline.start()
        pipeline.input_json(self.table_name, self.data, update_format="insert_delete")
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