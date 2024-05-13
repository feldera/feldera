import unittest
from tests import TEST_CLIENT

from feldera.pipeline import Pipeline
from feldera.program import Program


class TestPipeline(unittest.TestCase):
    def test_create_pipeline(self, name: str = "blah", delete=False):
        sql = """
        CREATE TABLE tbl(id INT);
        CREATE VIEW V AS SELECT * FROM tbl;
        """
        program = Program(name, sql)
        TEST_CLIENT.compile_program(program)
        pipeline = Pipeline(name=name, program_name=name)
        TEST_CLIENT.create_pipeline(pipeline)

        if delete:
            TEST_CLIENT.delete_pipeline(pipeline.name)

    def test_validate_pipeline(self):
        name = "blah"
        self.test_create_pipeline(name, False)
        assert TEST_CLIENT.validate_pipeline(name)

        TEST_CLIENT.delete_pipeline(name)

    def test_delete_pipeline(self):
        name = "blah"
        self.test_create_pipeline(name, False)

        TEST_CLIENT.delete_pipeline(name)

    def __test_push_to_pipeline(self, data, format, array):
        name = "blah"
        self.test_create_pipeline(name, False)

        TEST_CLIENT.start_pipeline(name)

        TEST_CLIENT.push_to_pipeline(
            pipeline_name=name,
            table_name="tbl",
            format=format,
            array=array,
            data=data,
        )

        TEST_CLIENT.pause_pipeline(name)
        TEST_CLIENT.shutdown_pipeline(name)

        TEST_CLIENT.delete_pipeline(name)

    def test_push_to_pipeline_json(self):
        data = [
            {"id": 1},
            {"id": 2},
            {"id": 3},
        ]
        self.__test_push_to_pipeline(data, format="json", array=True)

    def test_push_to_pipeline_csv0(self):
        data = "1\n2\n"
        self.__test_push_to_pipeline(data, format="csv", array=False)

    def test_list_pipelines(self):
        name = "blah"
        self.test_create_pipeline(name, False)
        pipelines = TEST_CLIENT.pipelines()
        assert len(pipelines) > 0
        assert name in [p.name for p in pipelines]

        TEST_CLIENT.delete_pipeline(name)

    def test_get_pipeline(self):
        name = "blah"
        self.test_create_pipeline(name, False)
        p = TEST_CLIENT.get_pipeline(name)
        assert name == p.name

        TEST_CLIENT.delete_pipeline(name)

    # def test_get_pipeline_config(self):


if __name__ == '__main__':
    unittest.main()
