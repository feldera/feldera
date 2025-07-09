import os
import pathlib
import threading
import pandas as pd
import time
import unittest

from tests.shared_test_pipeline import SharedTestPipeline
from tests import TEST_CLIENT, enterprise_only
from feldera.enums import PipelineStatus


class TestPipeline(SharedTestPipeline):
    result = None

    def test_create_pipeline(self):
        """
        CREATE TABLE tbl(id INT) WITH ('materialized' = 'true');
        CREATE MATERIALIZED VIEW v0 AS SELECT * FROM tbl;
        """
        pass

    def __test_push_to_pipeline(self, data, format, array):
        self.pipeline.stop(force=True)
        self.pipeline.start()
        TEST_CLIENT.push_to_pipeline(
            pipeline_name=self.pipeline.name,
            table_name="tbl",
            format=format,
            array=array,
            data=data,
        )
        TEST_CLIENT.pause_pipeline(self.pipeline.name)
        TEST_CLIENT.stop_pipeline(self.pipeline.name, force=True)

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
        pipelines = TEST_CLIENT.pipelines()
        assert len(pipelines) > 0
        assert self.pipeline.name in [p.name for p in pipelines]

    def test_get_pipeline(self):
        p = TEST_CLIENT.get_pipeline(self.pipeline.name)
        assert self.pipeline.name == p.name

    def test_get_pipeline_config(self):
        config = TEST_CLIENT.get_runtime_config(self.pipeline.name)
        assert config is not None

    def test_get_pipeline_stats(self):
        self.pipeline.start()
        stats = TEST_CLIENT.get_pipeline_stats(self.pipeline.name)
        assert stats is not None
        assert stats.get("global_metrics") is not None
        assert stats.get("inputs") is not None
        assert stats.get("outputs") is not None
        TEST_CLIENT.pause_pipeline(self.pipeline.name)
        TEST_CLIENT.stop_pipeline(self.pipeline.name, force=True)

    def __listener(self):
        gen_obj = TEST_CLIENT.listen_to_pipeline(
            pipeline_name=self.pipeline.name,
            table_name="v0",
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
        TEST_CLIENT.pause_pipeline(self.pipeline.name)
        t1 = threading.Thread(target=self.__listener)
        t1.start()
        self.pipeline.resume()
        TEST_CLIENT.push_to_pipeline(self.pipeline.name, "tbl", "csv", data)
        t1.join()
        assert self.result
        TEST_CLIENT.stop_pipeline(self.pipeline.name, force=True)

    def test_adhoc_query_text(self):
        data = "1\n2\n"
        self.pipeline.start()
        TEST_CLIENT.push_to_pipeline(self.pipeline.name, "tbl", "csv", data)
        resp = TEST_CLIENT.query_as_text(
            self.pipeline.name, "SELECT * FROM tbl ORDER BY id"
        )
        expected = [
            """+----+
| id |
+----+
| 1  |
| 2  |
+----+"""
        ]

        got = "\n".join(resp)
        assert got in expected
        TEST_CLIENT.stop_pipeline(self.pipeline.name, force=True)

    def test_adhoc_query_parquet(self):
        data = "1\n2\n"
        self.pipeline.start()
        TEST_CLIENT.push_to_pipeline(self.pipeline.name, "tbl", "csv", data)
        file = self.pipeline.name.split("-")[0]
        TEST_CLIENT.query_as_parquet(self.pipeline.name, "SELECT * FROM tbl", file)
        TEST_CLIENT.stop_pipeline(self.pipeline.name, force=True)
        path = pathlib.Path(file + ".parquet")
        assert path.stat().st_size > 0
        os.remove(path)

    def test_adhoc_query_json(self):
        data = "1\n2\n"
        self.pipeline.start()
        TEST_CLIENT.push_to_pipeline(self.pipeline.name, "tbl", "csv", data)
        resp = TEST_CLIENT.query_as_json(self.pipeline.name, "SELECT * FROM tbl")
        expected = [{"id": 2}, {"id": 1}]
        got = list(resp)
        self.assertCountEqual(got, expected)
        TEST_CLIENT.stop_pipeline(self.pipeline.name, force=True)

    def test_local(self):
        """
        CREATE TABLE students (
            name STRING,
            id INT
        );
        CREATE TABLE grades (
            student_id INT,
            science INT,
            maths INT,
            art INT
        );
        CREATE MATERIALIZED VIEW average_scores AS
            SELECT name, ((science + maths + art) / 3) as average
            FROM students JOIN grades on id = student_id
            ORDER BY average DESC;
        """
        df_students = pd.read_csv("tests/students.csv")
        df_grades = pd.read_csv("tests/grades.csv")
        self.pipeline.start()
        out = self.pipeline.listen("average_scores")
        self.pipeline.input_pandas("students", df_students)
        self.pipeline.input_pandas("grades", df_grades)
        self.pipeline.wait_for_completion(True)
        df = out.to_pandas()
        assert df.shape[0] == 100

    def test_pipeline_get(self):
        df_students = pd.read_csv("tests/students.csv")
        df_grades = pd.read_csv("tests/grades.csv")
        self.pipeline.start()
        out = self.pipeline.listen("average_scores")
        self.pipeline.input_pandas("students", df_students)
        self.pipeline.input_pandas("grades", df_grades)
        self.pipeline.wait_for_completion(True)
        df = out.to_pandas()
        assert df.shape[0] == 100

    def test_local_listen_after_start(self):
        df_students = pd.read_csv("tests/students.csv")
        df_grades = pd.read_csv("tests/grades.csv")
        self.pipeline.start()
        out = self.pipeline.listen("average_scores")
        self.pipeline.input_pandas("students", df_students)
        self.pipeline.input_pandas("grades", df_grades)
        self.pipeline.wait_for_completion(True)
        df = out.to_pandas()
        assert df.shape[0] == 100

    def test_foreach_chunk(self):
        def callback(df: pd.DataFrame, seq_no: int):
            print(f"\nSeq No: {seq_no}, DF size: {df.shape[0]}\n")

        df_students = pd.read_csv("tests/students.csv")
        df_grades = pd.read_csv("tests/grades.csv")
        self.pipeline.foreach_chunk("average_scores", callback)
        self.pipeline.start()
        self.pipeline.input_pandas("students", df_students)
        self.pipeline.input_pandas("grades", df_grades)
        self.pipeline.wait_for_completion(True)

    def test_df_without_columns(self):
        df = pd.DataFrame([(1, "a"), (2, "b"), (3, "c")])
        self.pipeline.start()
        with self.assertRaises(ValueError):
            self.pipeline.input_pandas("students", df)
        self.pipeline.stop(force=True)

    def test_variant(self):
        """
        -- Ingest JSON as string; output it as VARIANT.
        CREATE TABLE json_table (json VARCHAR) with ('materialized' = 'true');
        CREATE MATERIALIZED VIEW json_view AS SELECT PARSE_JSON(json) AS json FROM json_table;
        CREATE MATERIALIZED VIEW json_string_view AS SELECT TO_JSON(json) AS json FROM json_view;

        CREATE MATERIALIZED VIEW average_view AS SELECT
        CAST(json['name'] AS VARCHAR) as name,
        ((CAST(json['scores'][1] AS DECIMAL(8, 2)) + CAST(json['scores'][2] AS DECIMAL(8, 2))) / 2) as average
        FROM json_view;

        -- Ingest JSON as variant; extract strongly typed columns from it.
        CREATE TABLE variant_table(val VARIANT) with ('materialized' = 'true');

        CREATE MATERIALIZED VIEW typed_view AS SELECT
            CAST(val['name'] AS VARCHAR) as name,
            CAST(val['scores'] AS DECIMAL ARRAY) as scores
        FROM variant_table;
        """
        from decimal import Decimal

        # Input as JSON strings
        input_strings = [
            {"json": '{"name":"Bob","scores":[8,10]}'},
            {"json": '{"name":"Dunce","scores":[3,4]}'},
            {"json": '{"name":"John","scores":[9,10]}'},
        ]

        # Input as VARIANT
        input_json = [
            {"val": {"name": "Bob", "scores": [8, 10]}},
            {"val": {"name": "Dunce", "scores": [3, 4]}},
            {"val": {"name": "John", "scores": [9, 10]}},
        ]

        # Expected outputs
        expected_strings = [{**j, "insert_delete": 1} for j in input_strings]
        expected_average = [
            {"name": "Bob", "average": Decimal(9)},
            {"name": "Dunce", "average": Decimal(3.5)},
            {"name": "John", "average": Decimal(9.5)},
        ]
        for datum in expected_average:
            datum["insert_delete"] = 1  # Add insert_delete marker
        expected_typed = [
            {"name": "Bob", "scores": [8, 10]},
            {"name": "Dunce", "scores": [3, 4]},
            {"name": "John", "scores": [9, 10]},
        ]
        for datum in expected_typed:
            datum["insert_delete"] = 1
        expected_variant = [
            {"json": {"name": "Bob", "scores": [8, 10]}, "insert_delete": 1},
            {"json": {"name": "Dunce", "scores": [3, 4]}, "insert_delete": 1},
            {"json": {"name": "John", "scores": [9, 10]}, "insert_delete": 1},
        ]
        # Set up listeners for all output views
        variant_out = self.pipeline.listen("json_view")
        json_out = self.pipeline.listen("json_string_view")
        average_out = self.pipeline.listen("average_view")
        typed_out = self.pipeline.listen("typed_view")
        self.pipeline.start()

        # Feed JSON as strings, receive output from `average_view` and `json_view`
        self.pipeline.input_json("json_table", input_strings)

        self.pipeline.wait_for_completion(False)
        assert expected_average == average_out.to_dict()
        assert expected_variant == variant_out.to_dict()
        assert expected_strings == json_out.to_dict()

        # Feed VARIANT, read strongly typed columns. Since output columns have the same
        # shape as inputs, output and input should be identical.
        self.pipeline.input_json("variant_table", input_json)
        self.pipeline.wait_for_completion(False)
        assert expected_typed == typed_out.to_dict()
        self.pipeline.wait_for_completion(True)

    def test_issue2142(self):
        self.pipeline.pause()
        data = [{"id": None}, {"id": 1}]
        out = self.pipeline.listen("v0")
        self.pipeline.resume()
        self.pipeline.input_json("tbl", data=data)
        self.pipeline.wait_for_completion(True)
        out_data = out.to_dict()
        expected = []
        for d in data:
            row = dict(d)
            row["insert_delete"] = 1
            expected.append(row)
        assert out_data == expected

    def test_failed_pipeline_stop(self):
        """
        CREATE VIEW id_plus_one AS SELECT id + 1 FROM tbl;
        """

        self.pipeline.start()
        data = [{"id": 2147483647}]
        self.pipeline.input_json("tbl", data)
        while True:
            status = self.pipeline.status()
            expected = PipelineStatus.STOPPED
            if status == expected and len(self.pipeline.deployment_error()) > 0:
                break
            time.sleep(1)
        self.pipeline.stop(force=True)

    def test_adhoc_execute(self):
        self.pipeline.start()
        self.pipeline.wait_for_completion()
        self.pipeline.execute("INSERT INTO tbl VALUES (1), (2);")
        resp = self.pipeline.query("SELECT * FROM tbl;")
        got = list(resp)
        expected = [{"id": 1}, {"id": 2}]
        self.pipeline.stop(force=True)
        self.assertCountEqual(got, expected)

    def test_input_json0(self):
        data = {"insert": {"id": 1}}
        self.pipeline.start()
        out = self.pipeline.listen("v0")
        self.pipeline.input_json("tbl", data, update_format="insert_delete")
        self.pipeline.wait_for_completion(True)
        out_data = out.to_dict()
        expected = [dict(data["insert"], insert_delete=1)]
        assert out_data == expected

    def test_input_json1(self):
        data = [{"id": 1}, {"id": 2}]
        self.pipeline.start()
        out = self.pipeline.listen("v0")
        self.pipeline.input_json("tbl", data)
        self.pipeline.wait_for_completion(True)
        out_data = out.to_dict()
        expected = [dict(row, insert_delete=1) for row in data]
        assert out_data == expected

    @enterprise_only
    def test_suspend(self):
        data = {"insert": {"id": 1}}
        self.pipeline.start()
        out = self.pipeline.listen("v0")
        self.pipeline.input_json("tbl", data, update_format="insert_delete")
        self.pipeline.wait_for_completion(False)
        self.pipeline.stop(force=False)
        out_data = out.to_dict()
        expected = [dict(data["insert"], insert_delete=1)]
        assert out_data == expected
        self.pipeline.stop(force=True)

    def test_timestamp_pandas(self):
        """
        CREATE TABLE tbl_timestamp (
            id INT,
            name STRING,
            birthdate TIMESTAMP
        );
        CREATE VIEW v_timestamp AS SELECT * FROM tbl_timestamp;
        """
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "birthdate": [
                    pd.Timestamp.now(),
                    pd.Timestamp.now(),
                    pd.Timestamp.now(),
                ],
            }
        )
        self.pipeline.start()
        out = self.pipeline.listen("v_timestamp")
        self.pipeline.input_pandas("tbl_timestamp", df)
        self.pipeline.wait_for_completion(True)
        df_out = out.to_pandas()
        assert df_out.shape[0] == 3

    def test_pandas_binary(self):
        """
        CREATE TABLE tbl_binary (c1 VARBINARY);
        CREATE VIEW v_binary AS SELECT SUBSTRING(c1 FROM 2) as c1 FROM tbl_binary;
        """
        data = [{"c1": [12, 34, 56]}]
        expected_data = [{"c1": [34, 56], "insert_delete": 1}]
        self.pipeline.start()
        out = self.pipeline.listen("v_binary")
        self.pipeline.input_json("tbl_binary", data=data)
        self.pipeline.wait_for_completion(True)
        got = out.to_dict()
        assert expected_data == got

    def test_pandas_decimal(self):
        """
        CREATE TABLE tbl_decimal (c1 DECIMAL(5, 2));
        CREATE VIEW v_decimal AS SELECT c1 + 2.75::DECIMAL(5, 2) as c1 FROM tbl_decimal;
        """
        from decimal import Decimal

        data = [{"c1": 2.25}]
        expected = [{"c1": Decimal("5.00"), "insert_delete": 1}]
        self.pipeline.start()
        out = self.pipeline.listen("v_decimal")
        self.pipeline.input_json("tbl_decimal", data=data)
        self.pipeline.wait_for_completion(True)
        got = out.to_dict()
        assert expected == got

    def test_pandas_array(self):
        """
        CREATE TABLE tbl_array (c1 INT ARRAY);
        CREATE VIEW v_array AS SELECT c1 FROM tbl_array;
        """
        data = [{"c1": [1, 2, 3]}]
        out = self.pipeline.listen("v_array")
        self.pipeline.start()
        self.pipeline.input_json("tbl_array", data=data)
        self.pipeline.wait_for_completion(True)
        got = out.to_dict()
        expected = [{"c1": [1, 2, 3], "insert_delete": 1}]
        assert got == expected

    def test_pandas_struct(self):
        """
        CREATE TYPE s_struct AS (
            f1 INT,
            f2 STRING
        );
        CREATE TABLE tbl_struct (c1 s_struct);
        CREATE VIEW v_struct AS SELECT c1 FROM tbl_struct;
        """
        data = [{"c1": {"f1": 1, "f2": "a"}}]
        out = self.pipeline.listen("v_struct")
        self.pipeline.start()
        self.pipeline.input_json("tbl_struct", data)
        self.pipeline.wait_for_completion(True)
        got = out.to_dict()
        expected = [{"c1": {"f1": 1, "f2": "a"}, "insert_delete": 1}]
        assert got == expected

    def test_pandas_date_time_timestamp(self):
        """
        CREATE TABLE tbl_datetime (c1 DATE, c2 TIME, c3 TIMESTAMP);
        CREATE VIEW v_datetime AS SELECT c1, c2, c3 FROM tbl_datetime;
        """
        from pandas import Timestamp, Timedelta

        data = [{"c1": "2022-01-01", "c2": "12:00:00", "c3": "2022-01-01 12:00:00"}]
        expected = [
            {
                "c1": Timestamp("2022-01-01 00:00:00"),
                "c2": Timedelta("0 days 12:00:00"),
                "c3": Timestamp("2022-01-01 12:00:00"),
                "insert_delete": 1,
            }
        ]
        self.pipeline.start()
        out = self.pipeline.listen("v_datetime")
        self.pipeline.input_json("tbl_datetime", data)
        self.pipeline.wait_for_completion(True)
        got = out.to_dict()
        assert expected == got

    def test_pandas_simple(self):
        """
        CREATE TABLE tbl_simple (
            c0 BOOLEAN, c1 TINYINT, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 VARCHAR, c8 CHAR
        );
        CREATE VIEW v_simple AS SELECT * FROM tbl_simple;
        """
        data = [
            {
                "c0": True,
                "c1": 1,
                "c2": 2,
                "c3": 3,
                "c4": 4,
                "c5": 5.0,
                "c6": 6.0,
                "c7": "seven",
                "c8": "c",
            }
        ]
        self.pipeline.start()
        out = self.pipeline.listen("v_simple")
        self.pipeline.input_json("tbl_simple", data)
        self.pipeline.wait_for_completion(True)
        got = out.to_dict()
        expected = []
        for d in data:
            row = dict(d)
            row["insert_delete"] = 1
            expected.append(row)
        assert got == expected

    def test_pandas_map(self):
        """
        CREATE TABLE tbl_map (c1 MAP<STRING, INT>);
        CREATE VIEW v_map AS SELECT c1 FROM tbl_map;
        """
        data = [{"c1": {"a": 1, "b": 2}}]
        expected = [{"c1": {"a": 1, "b": 2}, "insert_delete": 1}]
        out = self.pipeline.listen("v_map")
        self.pipeline.start()
        self.pipeline.input_json("tbl_map", data)
        self.pipeline.wait_for_completion(True)
        got = out.to_dict()
        assert expected == got
        # Second round: single dict
        self.pipeline.start()
        out = self.pipeline.listen("v_map")
        self.pipeline.input_json("tbl_map", {"c1": {"a": 1, "b": 2}})
        self.pipeline.wait_for_completion(True)
        got = out.to_dict()
        assert expected == got

    def test_uuid(self):
        """
        CREATE TABLE tbl_uuid(c0 UUID);
        CREATE MATERIALIZED VIEW v_uuid AS SELECT c0 FROM tbl_uuid;
        """
        import uuid

        data = [{"c0": uuid.uuid4()}]
        self.pipeline.start()
        out = self.pipeline.listen("v_uuid")
        self.pipeline.input_json("tbl_uuid", data)
        self.pipeline.wait_for_completion(True)
        got = out.to_dict()
        # Compare only the UUID values
        got_uuids = sorted([row["c0"] for row in got])
        expected_uuids = sorted([row["c0"] for row in data])
        assert got_uuids == expected_uuids

    def test_issue3754(self):
        """
        CREATE TABLE tbl_map_issue3754(m_var MAP<VARCHAR, VARCHAR>);
        CREATE MATERIALIZED VIEW v_map_issue3754 AS SELECT * FROM tbl_map_issue3754;
        """
        self.pipeline.start()
        with self.assertRaises(ValueError):
            data = {"insert": {"m_var": {None: 1}}}
            TEST_CLIENT.push_to_pipeline(
                self.pipeline.name, "tbl_map_issue3754", "insert_delete", [data]
            )
        with self.assertRaises(ValueError):
            data = {"delete": {"m_var": {None: 1}}}
            TEST_CLIENT.push_to_pipeline(
                self.pipeline.name, "tbl_map_issue3754", "insert_delete", [data]
            )
        with self.assertRaises(ValueError):
            data = {"m_var": {None: 1}}
            TEST_CLIENT.push_to_pipeline(
                self.pipeline.name, "tbl_map_issue3754", "raw", [data]
            )
        with self.assertRaises(ValueError):
            data = {"m_var": {None: 1}}
            self.pipeline.input_json("tbl_map_issue3754", [data])

    def test_pipeline_resource_config(self):
        from feldera.runtime_config import Resources, RuntimeConfig

        config = {
            "cpu_cores_max": 3,
            "cpu_cores_min": 2,
            "memory_mb_max": 500,
            "memory_mb_min": 300,
            "storage_mb_max": None,
            "storage_class": None,
        }

        resources = Resources(config)
        self.set_runtime_config(RuntimeConfig(resources=resources))
        self.pipeline.start()
        got = TEST_CLIENT.get_pipeline(self.pipeline.name).runtime_config["resources"]
        self.pipeline.stop(force=True)
        assert got == config
        self.reset_runtime_config()
        self.pipeline.clear_storage()


if __name__ == "__main__":
    unittest.main()
