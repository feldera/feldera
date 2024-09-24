import unittest

from feldera import PipelineBuilder
from feldera.enums import CompilationProfile
from tests import TEST_CLIENT


class Base(unittest.TestCase):
    def setUp(self):
        self.sql = ""
        self.pipeline_name = ""
        self.data = {}
        self.additional_data = {}
        self.expected_data = {}

    def set_tables(self, ddl):
        self.sql = ddl

    def set_data(self, table_name, data):
        self.data[table_name] = data

    def add_data(self, table_name, data, test_name):
        self.additional_data[test_name] = {table_name: data}

    def register_view(self, sql):
        self.sql += "\n"
        self.sql += sql

    def set_expected_output(self, test_name, view_name, expected_data):
        view_data = {view_name: expected_data}
        self.expected_data[test_name] = view_data

    def run_tests(self):
        # include all tests

        for name in dir(self):
            if name.startswith("test_"):
                method = getattr(self, name)
                method()

        pipeline = PipelineBuilder(TEST_CLIENT, self.pipeline_name, sql=self.sql, compilation_profile=CompilationProfile.DEV).create_or_replace()

        for test, expected_data in self.expected_data.items():
            print(f"Running pipeline for test: {test}")


            listeners = {}

            for view in expected_data.keys():
                listeners[view] = pipeline.listen(view)

            pipeline.start()

            input_data = self.data
            additional_data = self.additional_data.get(test, {})

            for tbl_name, add_data in additional_data.items():
                tbl_data = input_data.get(tbl_name, [])
                tbl_data.extend(add_data)
                input_data[tbl_name] = tbl_data

            for tbl_name, data in input_data.items():
                pipeline.input_json(tbl_name, data, update_format="insert_delete")

            pipeline.wait_for_completion(shutdown=False)

            for view, listener in listeners.items():
                got_data = listener.to_dict()
                # left to add insert_delete
                print(got_data)

                assert got_data == expected_data[view], f"ASSERTION ERROR: failed test:{test} for view:{view}"

            pipeline.shutdown()

            print(f"Passed test: {test}")