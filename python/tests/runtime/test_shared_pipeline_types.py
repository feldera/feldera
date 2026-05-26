import os
import pathlib
import unittest

import pandas as pd

from feldera.enums import CompletionTokenStatus
from tests import TEST_CLIENT, enterprise_only
from tests.shared_test_pipeline import SharedTestPipeline, sql


BASE_SQL = """
CREATE TABLE tbl(id INT) WITH ('materialized' = 'true', 'connectors' = '[{
    "name": "d1",
    "paused": true,
    "transport": {
        "name": "datagen",
        "config": {
            "plan": [{
                "limit": 10,
                "rate": 1
            }]
        }
    }
}]');
CREATE MATERIALIZED VIEW v0 AS SELECT * FROM tbl;
CREATE MATERIALIZED VIEW "V0" AS SELECT * FROM tbl WHERE id % 2 <> 0;
CREATE MATERIALIZED VIEW "DATE" AS SELECT * FROM tbl WHERE id % 2 = 0;
"""


JOIN_SQL = """
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


class TestPipelineRuntime(SharedTestPipeline):
    def __test_push_to_pipeline(self, data, format, array):
        self.pipeline.start()
        TEST_CLIENT.push_to_pipeline(
            pipeline_name=self.pipeline.name,
            table_name="tbl",
            format=format,
            array=array,
            data=data,
        )

    @sql(BASE_SQL)
    def test_push_to_pipeline_json(self):
        data = [{"id": 1}, {"id": 2}, {"id": 3}]
        self.__test_push_to_pipeline(data, format="json", array=True)

    def test_push_to_pipeline_csv0(self):
        data = "1\n2\n"
        self.__test_push_to_pipeline(data, format="csv", array=False)

    def test_case_sensitive_views_listen(self):
        self.pipeline.start_paused()

        all_stream = self.pipeline.listen("v0")
        odd_stream = self.pipeline.listen("V0")
        even_stream = self.pipeline.listen("DATE")

        self.pipeline.resume()
        self.pipeline.input_json("tbl", [{"id": i} for i in range(10)])
        self.pipeline.wait_for_idle()

        all = all_stream.to_dict()
        odd = odd_stream.to_dict()
        even = even_stream.to_dict()

        expected_all = list(self.pipeline.query("select * from v0"))
        expected_odd = list(self.pipeline.query('select * from "V0"'))
        expected_even = list(self.pipeline.query('select * from "DATE"'))

        def extract_ids(x):
            return sorted(i["id"] for i in x)

        assert extract_ids(all) == extract_ids(expected_all)
        assert extract_ids(odd) == extract_ids(expected_odd)
        assert extract_ids(even) == extract_ids(expected_even)

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

    def test_adhoc_query_hash(self):
        data = "1\n2\n"
        self.pipeline.start()
        TEST_CLIENT.push_to_pipeline(self.pipeline.name, "tbl", "csv", data)
        resp = TEST_CLIENT.query_as_hash(
            self.pipeline.name, "SELECT * FROM tbl ORDER BY id"
        )
        assert (
            resp == "CCACBC763D343FB9855F285385B5A8A04FB5DAC4926DA3802F071B0C05BDF852"
        )

    def test_adhoc_query_parquet(self):
        data = "1\n2\n"
        self.pipeline.start()
        TEST_CLIENT.push_to_pipeline(self.pipeline.name, "tbl", "csv", data)
        file = self.pipeline.name.split("-")[0]
        TEST_CLIENT.query_as_parquet(self.pipeline.name, "SELECT * FROM tbl", file)
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

    def test_adhoc_query_arrow(self):
        import pyarrow as pa

        data = "1\n2\n"
        self.pipeline.start()
        TEST_CLIENT.push_to_pipeline(self.pipeline.name, "tbl", "csv", data)

        expected_rows = list(
            TEST_CLIENT.query_as_json(
                self.pipeline.name,
                "SELECT * FROM tbl ORDER BY id",
            )
        )
        expected_ids = [row["id"] for row in expected_rows]

        batches_client = list(
            TEST_CLIENT.query_as_arrow(
                self.pipeline.name,
                "SELECT * FROM tbl ORDER BY id",
            )
        )
        table_client = pa.Table.from_batches(batches_client)
        assert table_client.column("id").to_pylist() == expected_ids

        batches_pipeline = list(
            self.pipeline.query_arrow("SELECT * FROM tbl ORDER BY id")
        )
        table_pipeline = pa.Table.from_batches(batches_pipeline)
        assert table_pipeline.column("id").to_pylist() == expected_ids

    def test_adhoc_execute(self):
        self.pipeline.start()
        self.pipeline.execute("INSERT INTO tbl VALUES (1), (2);")
        self.pipeline.wait_for_idle()
        resp = self.pipeline.query("SELECT * FROM tbl;")
        got = list(resp)
        expected = [{"id": 1}, {"id": 2}]
        self.pipeline.stop(force=True)
        self.assertCountEqual(got, expected)

    def test_issue2142(self):
        self.pipeline.start_paused()
        data = [{"id": None}, {"id": 1}]
        out = self.pipeline.listen("v0")
        self.pipeline.resume()
        self.pipeline.input_json("tbl", data=data)
        self.pipeline.wait_for_idle()
        out_data = out.to_dict()
        expected = []
        for d in data:
            row = dict(d)
            row["insert_delete"] = 1
            expected.append(row)
        assert out_data == expected
        self.pipeline.stop(force=True)

    def test_input_json0(self):
        data = {"insert": {"id": 1}}
        self.pipeline.start_paused()
        out = self.pipeline.listen("v0")
        self.pipeline.resume()
        self.pipeline.input_json("tbl", data, update_format="insert_delete")
        self.pipeline.wait_for_idle()
        out_data = out.to_dict()
        expected = [dict(data["insert"], insert_delete=1)]
        assert out_data == expected
        self.pipeline.stop(force=True)

    def test_input_json1(self):
        data = [{"id": 1}, {"id": 2}]
        self.pipeline.start_paused()
        out = self.pipeline.listen("v0")
        self.pipeline.resume()
        self.pipeline.input_json("tbl", data)
        self.pipeline.wait_for_idle()
        out_data = out.to_dict()
        expected = [dict(row, insert_delete=1) for row in data]
        assert out_data == expected
        self.pipeline.stop(force=True)

    @enterprise_only
    def test_suspend(self):
        data = {"insert": {"id": 1}}
        self.pipeline.start_paused()
        out = self.pipeline.listen("v0")
        self.pipeline.resume()
        self.pipeline.input_json("tbl", data, update_format="insert_delete")
        self.pipeline.wait_for_idle()
        out_data = out.to_dict()
        expected = [dict(data["insert"], insert_delete=1)]
        assert out_data == expected
        self.pipeline.stop(force=False)

    def test_completion_tokens_sdk(self):
        self.pipeline.start()
        self.pipeline.resume_connector("tbl", "d1")
        token = self.pipeline.generate_completion_token("tbl", "d1")
        self.pipeline.wait_for_token(token)
        assert (
            self.pipeline.completion_token_status(token)
            == CompletionTokenStatus.COMPLETE
        )

    @sql(JOIN_SQL)
    def test_local(self):
        df_students = pd.read_csv("tests/assets/students.csv")
        df_grades = pd.read_csv("tests/assets/grades.csv")
        self.pipeline.start_paused()
        out = self.pipeline.listen("average_scores")
        self.pipeline.resume()
        self.pipeline.input_pandas("students", df_students)
        self.pipeline.input_pandas("grades", df_grades)
        self.pipeline.wait_for_idle()
        df = out.to_pandas()
        assert df.shape[0] == 100
        self.pipeline.stop(force=True)

    def test_local_listen_after_start(self):
        df_students = pd.read_csv("tests/assets/students.csv")
        df_grades = pd.read_csv("tests/assets/grades.csv")
        self.pipeline.start()
        out = self.pipeline.listen("average_scores")
        self.pipeline.input_pandas("students", df_students)
        self.pipeline.input_pandas("grades", df_grades)
        self.pipeline.wait_for_idle()
        df = out.to_pandas()
        assert df.shape[0] == 100
        self.pipeline.stop(force=True)

    def test_foreach_chunk(self):
        def callback(df: pd.DataFrame, seq_no: int):
            print(f"\nSeq No: {seq_no}, DF size: {df.shape[0]}\n")

        df_students = pd.read_csv("tests/assets/students.csv")
        df_grades = pd.read_csv("tests/assets/grades.csv")
        self.pipeline.start_paused()
        self.pipeline.foreach_chunk("average_scores", callback)
        self.pipeline.resume()
        self.pipeline.input_pandas("students", df_students)
        self.pipeline.input_pandas("grades", df_grades)
        self.pipeline.wait_for_idle()
        self.pipeline.stop(force=True)

    def test_df_without_columns(self):
        df = pd.DataFrame([(1, "a"), (2, "b"), (3, "c")])
        self.pipeline.start()
        with self.assertRaises(ValueError):
            self.pipeline.input_pandas("students", df)
        self.pipeline.stop(force=True)

    @sql("""
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
    """)
    def test_variant(self):
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

        expected_strings = [{**j, "insert_delete": 1} for j in input_strings]
        expected_average = [
            {"name": "Bob", "average": Decimal(9)},
            {"name": "Dunce", "average": Decimal(3.5)},
            {"name": "John", "average": Decimal(9.5)},
        ]
        for datum in expected_average:
            datum["insert_delete"] = 1
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
        self.pipeline.start_paused()
        variant_out = self.pipeline.listen("json_view")
        json_out = self.pipeline.listen("json_string_view")
        average_out = self.pipeline.listen("average_view")
        typed_out = self.pipeline.listen("typed_view")
        self.pipeline.resume()

        # Feed JSON as strings, receive output from `average_view` and `json_view`.
        self.pipeline.input_json("json_table", input_strings)

        self.pipeline.wait_for_idle()
        assert expected_average == average_out.to_dict()
        assert expected_variant == variant_out.to_dict()
        assert expected_strings == json_out.to_dict()

        # Feed VARIANT, read strongly typed columns. Since output columns have
        # the same shape as inputs, output and input should be identical.
        self.pipeline.input_json("variant_table", input_json)
        assert expected_typed == typed_out.to_dict()
        self.pipeline.stop(True)

    @sql("""
    CREATE TABLE tbl_timestamp (
        id INT,
        name STRING,
        birthdate TIMESTAMP
    );
    CREATE VIEW v_timestamp AS SELECT * FROM tbl_timestamp;
    """)
    def test_timestamp_pandas(self):
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
        self.pipeline.start_paused()
        out = self.pipeline.listen("v_timestamp")
        self.pipeline.resume()
        self.pipeline.input_pandas("tbl_timestamp", df)
        self.pipeline.wait_for_idle()
        df_out = out.to_pandas()
        assert df_out.shape[0] == 3
        self.pipeline.stop(force=True)

    @sql("""
    CREATE TABLE tbl_binary (c1 VARBINARY);
    CREATE VIEW v_binary AS SELECT SUBSTRING(c1 FROM 2) as c1 FROM tbl_binary;
    """)
    def test_pandas_binary(self):
        data = [{"c1": [12, 34, 56]}]
        expected_data = [{"c1": [34, 56], "insert_delete": 1}]
        self.pipeline.start_paused()
        out = self.pipeline.listen("v_binary")
        self.pipeline.resume()
        self.pipeline.input_json("tbl_binary", data=data)
        self.pipeline.wait_for_idle()
        got = out.to_dict()
        assert expected_data == got
        self.pipeline.stop(force=True)

    @sql("""
    CREATE TABLE tbl_decimal (c1 DECIMAL(5, 2));
    CREATE VIEW v_decimal AS SELECT c1 + 2.75::DECIMAL(5, 2) as c1 FROM tbl_decimal;
    """)
    def test_pandas_decimal(self):
        from decimal import Decimal

        data = [{"c1": 2.25}]
        expected = [{"c1": Decimal("5.00"), "insert_delete": 1}]
        self.pipeline.start_paused()
        out = self.pipeline.listen("v_decimal")
        self.pipeline.resume()
        self.pipeline.input_json("tbl_decimal", data=data)
        self.pipeline.wait_for_idle()
        got = out.to_dict()
        assert expected == got
        self.pipeline.stop(force=True)

    @sql("""
    CREATE TABLE tbl_array (c1 INT ARRAY);
    CREATE VIEW v_array AS SELECT c1 FROM tbl_array;
    """)
    def test_pandas_array(self):
        data = [{"c1": [1, 2, 3]}]
        self.pipeline.start_paused()
        out = self.pipeline.listen("v_array")
        self.pipeline.resume()
        self.pipeline.input_json("tbl_array", data=data)
        self.pipeline.wait_for_idle()
        got = out.to_dict()
        expected = [{"c1": [1, 2, 3], "insert_delete": 1}]
        assert got == expected
        self.pipeline.stop(force=True)

    @sql("""
    CREATE TYPE s_struct AS (
        f1 INT,
        f2 STRING
    );
    CREATE TABLE tbl_struct (c1 s_struct);
    CREATE VIEW v_struct AS SELECT c1 FROM tbl_struct;
    """)
    def test_pandas_struct(self):
        data = [{"c1": {"f1": 1, "f2": "a"}}]
        self.pipeline.start_paused()
        out = self.pipeline.listen("v_struct")
        self.pipeline.resume()
        self.pipeline.input_json("tbl_struct", data)
        self.pipeline.wait_for_idle()
        got = out.to_dict()
        expected = [{"c1": {"f1": 1, "f2": "a"}, "insert_delete": 1}]
        assert got == expected
        self.pipeline.stop(force=True)

    @sql("""
    CREATE TABLE tbl_datetime (c1 DATE, c2 TIME, c3 TIMESTAMP);
    CREATE VIEW v_datetime AS SELECT c1, c2, c3 FROM tbl_datetime;
    """)
    def test_pandas_date_time_timestamp(self):
        from pandas import Timedelta, Timestamp

        data = [{"c1": "2022-01-01", "c2": "12:00:00", "c3": "2022-01-01 12:00:00"}]
        expected = [
            {
                "c1": Timestamp("2022-01-01 00:00:00"),
                "c2": Timedelta("0 days 12:00:00"),
                "c3": Timestamp("2022-01-01 12:00:00"),
                "insert_delete": 1,
            }
        ]
        self.pipeline.start_paused()
        out = self.pipeline.listen("v_datetime")
        self.pipeline.resume()
        self.pipeline.input_json("tbl_datetime", data)
        self.pipeline.wait_for_idle()
        got = out.to_dict()
        assert expected == got
        self.pipeline.stop(force=True)

    @sql("""
    CREATE TABLE tbl_simple (
        c0 BOOLEAN, c1 TINYINT, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 VARCHAR, c8 CHAR
    );
    CREATE VIEW v_simple AS SELECT * FROM tbl_simple;
    """)
    def test_pandas_simple(self):
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
        self.pipeline.start_paused()
        out = self.pipeline.listen("v_simple")
        self.pipeline.resume()
        self.pipeline.input_json("tbl_simple", data)
        self.pipeline.wait_for_idle()
        got = out.to_dict()
        expected = []
        for d in data:
            row = dict(d)
            row["insert_delete"] = 1
            expected.append(row)
        assert got == expected
        self.pipeline.stop(force=True)

    @sql("""
    CREATE TABLE tbl_map (c1 MAP<STRING, INT>);
    CREATE VIEW v_map AS SELECT c1 FROM tbl_map;
    """)
    def test_pandas_map(self):
        data = [{"c1": {"a": 1, "b": 2}}]
        expected = [{"c1": {"a": 1, "b": 2}, "insert_delete": 1}]
        self.pipeline.start_paused()
        out = self.pipeline.listen("v_map")
        self.pipeline.resume()
        self.pipeline.input_json("tbl_map", data)
        self.pipeline.wait_for_idle()
        got = out.to_dict()
        assert expected == got
        self.pipeline.stop(force=True)

        # Second round: single dict
        self.pipeline.start_paused()
        out = self.pipeline.listen("v_map")
        self.pipeline.resume()
        self.pipeline.input_json("tbl_map", {"c1": {"a": 1, "b": 2}})
        self.pipeline.wait_for_idle()
        got = out.to_dict()
        assert expected == got
        self.pipeline.stop(force=True)

    @sql("""
    CREATE TABLE tbl_uuid(c0 UUID);
    CREATE MATERIALIZED VIEW v_uuid AS SELECT c0 FROM tbl_uuid;
    """)
    def test_uuid(self):
        import uuid

        data = [{"c0": uuid.uuid4()}]
        self.pipeline.start_paused()
        out = self.pipeline.listen("v_uuid")
        self.pipeline.resume()
        self.pipeline.input_json("tbl_uuid", data)
        self.pipeline.wait_for_idle()
        got = out.to_dict()
        got_uuids = sorted([row["c0"] for row in got])
        expected_uuids = sorted([row["c0"] for row in data])
        assert got_uuids == expected_uuids
        self.pipeline.stop(force=True)

    @sql("""
    CREATE TABLE tbl_map_issue3754(m_var MAP<VARCHAR, VARCHAR>);
    CREATE MATERIALIZED VIEW v_map_issue3754 AS SELECT * FROM tbl_map_issue3754;
    """)
    def test_issue3754(self):
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
        self.pipeline.stop(force=True)

    @sql("""
    CREATE TABLE "t1#a1" (
        c1 TEXT NOT NULL
    ) WITH ('materialized' = 'true');
    """)
    def test_url_encoding_ingress_egress_table_name(self):
        self.pipeline.start_paused()
        out = self.pipeline.listen("t1#a1")

        self.pipeline.resume()
        data = [{"c1": "test_value"}]

        TEST_CLIENT.push_to_pipeline(
            pipeline_name=self.pipeline.name,
            table_name="t1#a1",
            format="json",
            array=True,
            data=data,
        )

        result = list(self.pipeline.query('SELECT * FROM "t1#a1"'))
        expected = [{"c1": "test_value"}]
        self.assertCountEqual(result, expected)

        additional_data = [{"c1": "test_value_2"}]
        TEST_CLIENT.push_to_pipeline(
            pipeline_name=self.pipeline.name,
            table_name="t1#a1",
            format="json",
            array=True,
            data=additional_data,
        )

        self.pipeline.wait_for_idle()
        egress_result = out.to_dict()
        expected_egress = [
            {"c1": "test_value", "insert_delete": 1},
            {"c1": "test_value_2", "insert_delete": 1},
        ]
        self.assertCountEqual(egress_result, expected_egress)


if __name__ == "__main__":
    unittest.main()
