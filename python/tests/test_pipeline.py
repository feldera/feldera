import os
import pathlib
import unittest
import uuid
import threading

from tests import TEST_CLIENT

from feldera.rest.pipeline import Pipeline


class TestPipeline(unittest.TestCase):
    result = None

    def test_delete_all_pipelines(self):
        pipelines = TEST_CLIENT.pipelines()
        for pipeline in pipelines:
            TEST_CLIENT.shutdown_pipeline(pipeline.name)
            TEST_CLIENT.delete_pipeline(pipeline.name)

    def test_create_pipeline(
        self, name: str = "blah", delete=False, runtime_config: dict = {}
    ):
        self.test_delete_all_pipelines()
        sql = """
        CREATE TABLE tbl(id INT) WITH ('append_only' = 'true');
        CREATE VIEW V AS SELECT * FROM tbl;
        """
        pipeline = Pipeline(name, sql, "", "", {}, runtime_config)
        pipeline = TEST_CLIENT.create_pipeline(pipeline)

        if delete:
            TEST_CLIENT.delete_pipeline(pipeline.name)

    def test_delete_pipeline(self):
        name = str(uuid.uuid4())
        self.test_create_pipeline(name, False)

        TEST_CLIENT.delete_pipeline(name)

    def __test_push_to_pipeline(self, data, format, array):
        name = str(uuid.uuid4())
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
        name = str(uuid.uuid4())
        self.test_create_pipeline(name, False)
        pipelines = TEST_CLIENT.pipelines()
        assert len(pipelines) > 0
        assert name in [p.name for p in pipelines]

        TEST_CLIENT.delete_pipeline(name)

    def test_get_pipeline(self):
        name = str(uuid.uuid4())
        self.test_create_pipeline(name, False)
        p = TEST_CLIENT.get_pipeline(name)
        assert name == p.name

        TEST_CLIENT.delete_pipeline(name)

    def test_get_pipeline_config(self):
        name = str(uuid.uuid4())
        self.test_create_pipeline(name, False, {"workers": 2, "storage": False})
        config = TEST_CLIENT.get_runtime_config(name)

        assert config is not None
        assert config.get("workers") == 2
        assert not config.get("storage")

        TEST_CLIENT.delete_pipeline(name)

    def test_get_pipeline_stats(self):
        name = str(uuid.uuid4())
        self.test_create_pipeline(name, delete=False)

        TEST_CLIENT.start_pipeline(name)

        stats = TEST_CLIENT.get_pipeline_stats(name)

        assert stats is not None
        assert stats.get("global_metrics") is not None
        assert stats.get("inputs") is not None
        assert stats.get("outputs") is not None

        TEST_CLIENT.pause_pipeline(name)
        TEST_CLIENT.shutdown_pipeline(name)
        TEST_CLIENT.delete_pipeline(name)

    def __listener(self, name: str):
        gen_obj = TEST_CLIENT.listen_to_pipeline(
            pipeline_name=name,
            table_name="V",
            format="csv",
        )
        counter = 0
        for chunk in gen_obj:
            counter += 1
            text_data = chunk.get("text_data")
            if text_data:
                assert text_data == "1,1\n2,1\n"
                self.result = True
                break
            if counter > 10:
                self.result = False
                break

    def test_listen_to_pipeline(self):
        data = "1\n2\n"
        name = str(uuid.uuid4())
        self.test_create_pipeline(name, False)

        TEST_CLIENT.pause_pipeline(name)

        t1 = threading.Thread(target=self.__listener, args=(name,))
        t1.start()

        TEST_CLIENT.start_pipeline(name)
        TEST_CLIENT.push_to_pipeline(name, "tbl", "csv", data)

        t1.join()

        assert self.result

        TEST_CLIENT.shutdown_pipeline(name)
        TEST_CLIENT.delete_pipeline(name)

    def test_adhoc_query_text(self):
        data = "1\n2\n"
        name = str(uuid.uuid4())

        sql = """
        CREATE TABLE tbl(id INT) with ('materialized' = 'true');
        """

        pipeline = Pipeline(name, sql, "", "", {}, {})
        pipeline = TEST_CLIENT.create_pipeline(pipeline)

        TEST_CLIENT.start_pipeline(name)

        TEST_CLIENT.push_to_pipeline(name, "tbl", "csv", data)
        resp = TEST_CLIENT.query_as_text(pipeline.name, "SELECT * FROM tbl")
        expected = [
            """+----+
| id |
+----+
| 1  |
| 2  |
+----+""",
            """+----+
| id |
+----+
| 2  |
| 1  |
+----+""",
        ]

        got = "\n".join(resp)
        assert got in expected

        TEST_CLIENT.shutdown_pipeline(name)
        TEST_CLIENT.delete_pipeline(name)

    def test_adhoc_query_parquet(self):
        data = "1\n2\n"
        name = str(uuid.uuid4())

        sql = """
        CREATE TABLE tbl(id INT) with ('materialized' = 'true');
        """

        pipeline = Pipeline(name, sql, "", "", {}, {})
        pipeline = TEST_CLIENT.create_pipeline(pipeline)

        TEST_CLIENT.start_pipeline(name)

        TEST_CLIENT.push_to_pipeline(name, "tbl", "csv", data)

        file = name.split("-")[0]
        TEST_CLIENT.query_as_parquet(pipeline.name, "SELECT * FROM tbl", file)
        TEST_CLIENT.shutdown_pipeline(name)
        TEST_CLIENT.delete_pipeline(name)

        path = pathlib.Path(file + ".parquet")
        assert path.stat().st_size > 0

        os.remove(path)

    def test_adhoc_query_json(self):
        data = "1\n2\n"
        name = str(uuid.uuid4())

        sql = """
        CREATE TABLE tbl(id INT) with ('materialized' = 'true');
        """

        pipeline = Pipeline(name, sql, "", "", {}, {})
        pipeline = TEST_CLIENT.create_pipeline(pipeline)

        TEST_CLIENT.start_pipeline(name)

        TEST_CLIENT.push_to_pipeline(name, "tbl", "csv", data)
        resp = TEST_CLIENT.query_as_json(pipeline.name, "SELECT * FROM tbl")
        expected = [{"id": 2}, {"id": 1}]
        got = list(resp)

        self.assertCountEqual(got, expected)

        TEST_CLIENT.shutdown_pipeline(name)
        TEST_CLIENT.delete_pipeline(name)


if __name__ == "__main__":
    unittest.main()
