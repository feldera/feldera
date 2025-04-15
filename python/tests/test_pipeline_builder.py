import os
import time
import unittest
import uuid

import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

from feldera import PipelineBuilder, Pipeline
from feldera.enums import PipelineStatus
from tests import TEST_CLIENT, KAFKA_SERVER, PIPELINE_TO_KAFKA_SERVER


class TestPipelineBuilder(unittest.TestCase):
    def test_local(self):
        TBL_NAMES = ["students", "grades"]
        view_name = "average_scores"

        sql = f"""
        CREATE TABLE {TBL_NAMES[0]} (
            name STRING,
            id INT
        );

        CREATE TABLE {TBL_NAMES[1]} (
            student_id INT,
            science INT,
            maths INT,
            art INT
        );

        CREATE VIEW {view_name} AS SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="notebook", sql=sql
        ).create_or_replace()

        df_students = pd.read_csv("students.csv")
        df_grades = pd.read_csv("grades.csv")

        out = pipeline.listen("average_scores")

        pipeline.start()
        pipeline.input_pandas(TBL_NAMES[0], df_students)
        pipeline.input_pandas(TBL_NAMES[1], df_grades)
        pipeline.wait_for_completion(True)
        df = out.to_pandas()

        assert df.shape[0] == 100

        pipeline.delete()

    def test_pipeline_get(self):
        TBL_NAMES = ["students", "grades"]
        view_name = "average_scores"

        sql = f"""
        CREATE TABLE {TBL_NAMES[0]} (
            name STRING,
            id INT
        );

        CREATE TABLE {TBL_NAMES[1]} (
            student_id INT,
            science INT,
            maths INT,
            art INT
        );

        CREATE VIEW {view_name} AS SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="notebook", sql=sql
        ).create_or_replace()

        df_students = pd.read_csv("students.csv")
        df_grades = pd.read_csv("grades.csv")

        out = pipeline.listen("average_scores")

        pipeline.start()
        pipeline.input_pandas(TBL_NAMES[0], df_students)
        pipeline.input_pandas(TBL_NAMES[1], df_grades)
        pipeline.wait_for_completion(True)
        df = out.to_pandas()

        assert df.shape[0] == 100

        del pipeline
        del out

        pipeline = Pipeline.get("notebook", TEST_CLIENT)
        assert pipeline is not None
        pipeline.start()

        out = pipeline.listen("average_scores")
        pipeline.input_pandas(TBL_NAMES[0], df_students)
        pipeline.input_pandas(TBL_NAMES[1], df_grades)
        pipeline.wait_for_completion(True)
        df = out.to_pandas()

        assert df.shape[0] == 100
        pipeline.pause()
        pipeline.shutdown()
        pipeline.delete()

    def test_local_listen_after_start(self):
        TBL_NAMES = ["students", "grades"]
        view_name = "average_scores"

        sql = f"""
        CREATE TABLE {TBL_NAMES[0]} (
            name STRING,
            id INT
        );

        CREATE TABLE {TBL_NAMES[1]} (
            student_id INT,
            science INT,
            maths INT,
            art INT
        );

        CREATE VIEW {view_name} AS SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="notebook", sql=sql
        ).create_or_replace()

        df_students = pd.read_csv("students.csv")
        df_grades = pd.read_csv("grades.csv")

        pipeline.start()
        out = pipeline.listen(view_name)

        pipeline.input_pandas(TBL_NAMES[0], df_students)
        pipeline.input_pandas(TBL_NAMES[1], df_grades)

        pipeline.wait_for_completion(True)
        df = out.to_pandas()

        pipeline.delete()
        assert df.shape[0] == 100

    def test_two_pipelines(self):
        # https://github.com/feldera/feldera/issues/1770

        TBL_NAMES = ["students", "grades"]
        VIEW_NAMES = [n + "_view" for n in TBL_NAMES]

        sql1 = f"""
        CREATE TABLE {TBL_NAMES[0]} (
            name STRING,
            id INT
        );

        CREATE VIEW {VIEW_NAMES[0]} AS SELECT * FROM {TBL_NAMES[0]};
        """

        sql2 = f"""
        CREATE TABLE {TBL_NAMES[1]} (
            student_id INT,
            science INT,
            maths INT,
            art INT
        );

        CREATE VIEW {VIEW_NAMES[1]} AS SELECT * FROM {TBL_NAMES[1]};
        """

        pipeline1 = PipelineBuilder(
            TEST_CLIENT, name="p1", sql=sql1
        ).create_or_replace()
        pipeline2 = PipelineBuilder(
            TEST_CLIENT, name="p2", sql=sql2
        ).create_or_replace()

        df_students = pd.read_csv("students.csv")
        df_grades = pd.read_csv("grades.csv")

        out1 = pipeline1.listen(VIEW_NAMES[0])
        out2 = pipeline2.listen(VIEW_NAMES[1])

        pipeline1.start()
        pipeline2.start()

        pipeline1.input_pandas(TBL_NAMES[0], df_students)
        pipeline2.input_pandas(TBL_NAMES[1], df_grades)

        pipeline1.wait_for_completion(True)
        pipeline2.wait_for_completion(True)

        df1 = out1.to_pandas()
        df2 = out2.to_pandas()

        assert df1.columns.tolist() not in df2.columns.tolist()

        pipeline1.delete()
        pipeline2.delete()

    def test_foreach_chunk(self):
        def callback(df: pd.DataFrame, seq_no: int):
            print(f"\nSeq No: {seq_no}, DF size: {df.shape[0]}\n")

        TBL_NAMES = ["students", "grades"]
        view_name = "average_scores"

        sql = f"""
        CREATE TABLE {TBL_NAMES[0]} (
            name STRING,
            id INT
        );

        CREATE TABLE {TBL_NAMES[1]} (
            student_id INT,
            science INT,
            maths INT,
            art INT
        );

        CREATE VIEW {view_name} AS SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="foreach_chunk", sql=sql
        ).create_or_replace()

        df_students = pd.read_csv("students.csv")
        df_grades = pd.read_csv("grades.csv")

        pipeline.foreach_chunk(view_name, callback)
        pipeline.start()

        pipeline.input_pandas(TBL_NAMES[0], df_students)
        pipeline.input_pandas(TBL_NAMES[1], df_grades)

        pipeline.wait_for_completion(True)
        pipeline.delete()

    def test_df_without_columns(self):
        TBL_NAME = "student"

        sql = f"""
        CREATE TABLE {TBL_NAME} (
            id INT,
            name STRING
        );

        CREATE VIEW s AS SELECT * FROM {TBL_NAME};
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="df_without_columns", sql=sql
        ).create_or_replace()

        df = pd.DataFrame([(1, "a"), (2, "b"), (3, "c")])

        pipeline.start()

        with self.assertRaises(ValueError):
            pipeline.input_pandas(TBL_NAME, df)

        pipeline.shutdown()
        pipeline.delete()

    def test_sql_error(self):
        pipeline_name = "sql_error"

        sql = """
CREATE TABLE student(
    id INT,
    name STRING
);

CREATE VIEW s AS SELECT * FROM blah;
        """

        expected = f"""
Pipeline {pipeline_name} failed to compile:
Error in SQL statement
Object 'blah' not found
Code snippet:
    6|CREATE VIEW s AS SELECT * FROM blah;
                                     ^^^^""".strip()

        with self.assertRaises(Exception) as err:
            PipelineBuilder(
                TEST_CLIENT, name=pipeline_name, sql=sql
            ).create_or_replace()

        got_err: str = err.exception.args[0].strip()

        assert expected == got_err

        pipeline = Pipeline.get("sql_error", TEST_CLIENT)
        pipeline.delete()

    def test_kafka(self):
        import json

        in_ci = os.environ.get("IN_CI")

        if in_ci == "1":
            # if running in CI, skip the test
            return

        print("(Re-)creating topics...")
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_SERVER, client_id="test_client"
        )

        INPUT_TOPIC = "simple_count_input"
        OUTPUT_TOPIC = "simple_count_output"

        existing_topics = set(admin_client.list_topics())
        if INPUT_TOPIC in existing_topics:
            admin_client.delete_topics([INPUT_TOPIC])
        if OUTPUT_TOPIC in existing_topics:
            admin_client.delete_topics([OUTPUT_TOPIC])
        admin_client.create_topics(
            [
                NewTopic(INPUT_TOPIC, num_partitions=1, replication_factor=1),
                NewTopic(OUTPUT_TOPIC, num_partitions=1, replication_factor=1),
            ]
        )
        print("Topics ready")

        # Produce rows into the input topic
        print("Producing rows into input topic...")
        num_rows = 1000
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            client_id="test_client",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        for i in range(num_rows):
            producer.send("simple_count_input", value={"insert": {"id": i}})
        print("Input topic contains data")

        TABLE_NAME = "example"
        VIEW_NAME = "example_count"

        sql = f"""
        CREATE TABLE {TABLE_NAME} (id INT NOT NULL PRIMARY KEY)
        WITH (
            'connectors' = '[
                {{
                    "name": "kafka-2",
                    "transport": {{
                        "name": "kafka_input",
                        "config": {{
                            "bootstrap.servers": "{PIPELINE_TO_KAFKA_SERVER}",
                            "topics": ["{INPUT_TOPIC}"],
                            "auto.offset.reset": "earliest"
                        }}
                    }},
                    "format": {{
                        "name": "json",
                        "config": {{
                            "update_format": "insert_delete",
                            "array": false
                        }}
                    }}
                }}
            ]'
        );

        CREATE VIEW {VIEW_NAME}
        WITH (
            'connectors' = '[
                {{
                    "name": "kafka-3",
                    "transport": {{
                        "name": "kafka_output",
                        "config": {{
                            "bootstrap.servers": "{PIPELINE_TO_KAFKA_SERVER}",
                            "topic": "{OUTPUT_TOPIC}",
                            "auto.offset.reset": "earliest"
                        }}
                    }},
                    "format": {{
                        "name": "json",
                        "config": {{
                            "update_format": "insert_delete",
                            "array": false
                        }}
                    }}
                }}
            ]'
        )
        AS SELECT COUNT(*) as num_rows FROM {TABLE_NAME};
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="kafka_test", sql=sql
        ).create_or_replace()

        out = pipeline.listen(VIEW_NAME)
        pipeline.start()
        pipeline.wait_for_idle()
        pipeline.shutdown()
        df = out.to_pandas()
        assert df.shape[0] != 0

        pipeline.delete()

    def test_http_get(self):
        sql = """
        CREATE TABLE items (
            id INT,
            name STRING
        ) WITH (
            'connectors' = '[
                {
                    "name": "url_conn",
                    "transport": {
                        "name": "url_input",
                        "config": {
                            "path": "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"
                        }
                    },
                    "format": {
                        "name": "json",
                        "config": {
                            "update_format": "insert_delete",
                            "array": false
                        }
                    }
                }
            ]'
        );

        CREATE MATERIALIZED VIEW s AS SELECT * FROM items;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_http_get", sql=sql
        ).create_or_replace()

        out = pipeline.listen("s")

        pipeline.start()
        pipeline.wait_for_completion(True)

        df = out.to_pandas()

        assert df.shape[0] == 3

        pipeline.delete()

    def test_avro_format(self):
        import json

        TOPIC = "test_avro_format"

        in_ci = os.environ.get("IN_CI")

        if in_ci == "1":
            # if running in CI, skip the test
            return

        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_SERVER, client_id="test_client"
        )
        existing_topics = set(admin_client.list_topics())
        if TOPIC in existing_topics:
            admin_client.delete_topics([TOPIC])

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        sql = f"""
        CREATE TABLE items (
            id INT,
            name STRING
        );

        CREATE VIEW s WITH (
           'connectors'  = '[
                {{
                    "name": "kafka-1",
                    "transport": {{
                        "name": "kafka_output",
                        "config": {{
                            "bootstrap.servers": "{PIPELINE_TO_KAFKA_SERVER}",
                            "topic": "{TOPIC}"
                        }}
                    }},
                    "format": {{
                        "name": "avro",
                        "config": {{
                            "schema": {
            json.dumps(
                json.dumps(
                    {
                        "type": "record",
                        "name": "items",
                        "fields": [
                            {"name": "id", "type": ["null", "int"]},
                            {"name": "name", "type": ["null", "string"]},
                        ],
                    }
                )
            )
        }
                        }}
                    }}
                }}
           ]'
        )
        AS SELECT * FROM items;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_avro_format", sql=sql
        ).create_or_replace()

        pipeline.start()
        pipeline.input_pandas("items", df)
        pipeline.wait_for_completion(True)

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset="earliest",
        )

        msg = next(consumer)
        assert msg.value is not None

        pipeline.delete()

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
        name = "test_pipeline_resource_config"

        sql = """
        CREATE TABLE items (
            id INT,
            name STRING
        ) WITH (
            'connectors' = '[
                {
                    "name": "url_conn",
                    "transport": {
                        "name": "url_input",
                        "config": {
                            "path": "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"
                        }
                    },
                    "format": {
                        "name": "json",
                        "config": {
                            "update_format": "insert_delete",
                            "array": false
                        }
                    }
                }
            ]'
        );

        CREATE VIEW s AS SELECT * FROM items;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            name=name,
            sql=sql,
            runtime_config=RuntimeConfig(
                resources=resources, storage=False, workers=10
            ),
        ).create_or_replace()

        out = pipeline.listen("s")

        pipeline.start()
        pipeline.wait_for_completion(True)

        df = out.to_pandas()

        assert df.shape[0] == 3

        assert TEST_CLIENT.get_pipeline(name).runtime_config["resources"] == config

        pipeline.delete()

    def test_timestamp_pandas(self):
        TBL_NAME = "items"
        VIEW_NAME = "s"

        sql = f"""
        CREATE TABLE {TBL_NAME} (
            id INT,
            name STRING,
            birthdate TIMESTAMP
        );

        CREATE VIEW {VIEW_NAME} AS SELECT * FROM {TBL_NAME};
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_timestamp_pandas", sql=sql
        ).create_or_replace()

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

        out = pipeline.listen(VIEW_NAME)

        pipeline.start()
        pipeline.input_pandas(TBL_NAME, df)
        pipeline.wait_for_completion(True)

        df = out.to_pandas()

        assert df.shape[0] == 3

        pipeline.delete()

    def test_input_json0(self):
        TBL_NAME = "items"
        VIEW_NAME = "s"

        sql = f"""
        CREATE TABLE {TBL_NAME} (
            id INT,
            name STRING
        );

        CREATE VIEW {VIEW_NAME} AS SELECT * FROM {TBL_NAME};
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_input_json", sql=sql
        ).create_or_replace()

        data = {"insert": {"id": 1, "name": "a"}}

        out = pipeline.listen(VIEW_NAME)

        pipeline.start()
        pipeline.input_json(TBL_NAME, data, update_format="insert_delete")
        pipeline.wait_for_completion(True)

        out_data = out.to_dict()

        data["insert"].update({"insert_delete": 1})
        assert out_data == [data["insert"]]

        pipeline.delete()

    def test_input_json1(self):
        TBL_NAME = "items"
        VIEW_NAME = "s"

        sql = f"""
        CREATE TABLE {TBL_NAME} (
            id INT,
            name STRING
        );

        CREATE VIEW {VIEW_NAME} AS SELECT * FROM {TBL_NAME};
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_input_json", sql=sql
        ).create_or_replace()

        data = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

        out = pipeline.listen(VIEW_NAME)

        pipeline.start()
        pipeline.input_json(TBL_NAME, data)
        pipeline.wait_for_completion(True)

        out_data = out.to_dict()

        for datum in data:
            datum.update({"insert_delete": 1})

        assert out_data == data

        pipeline.delete()

    def test_issue2142(self):
        sql = """
        CREATE TABLE t0 (c1 INT);
        CREATE VIEW v0 AS SELECT * FROM t0;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_issue2142", sql=sql
        ).create_or_replace()

        data = [{"c1": None}, {"c1": 1}]

        out = pipeline.listen("v0")

        pipeline.start()
        pipeline.input_json("t0", data=data)
        pipeline.wait_for_completion(True)

        out_data = out.to_dict()

        for datum in data:
            datum.update({"insert_delete": 1})

        assert out_data == data

        pipeline.delete()

    def test_pandas_binary(self):
        sql = """
        CREATE TABLE t0 (c1 VARBINARY);
        CREATE VIEW v0 AS SELECT SUBSTRING(c1 FROM 2) as c1 FROM t0;
        """

        data = [{"c1": [12, 34, 56]}]
        expected_data = [{"c1": [34, 56], "insert_delete": 1}]

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_pandas_binary", sql=sql
        ).create_or_replace()
        out = pipeline.listen("v0")

        pipeline.start()
        pipeline.input_json("t0", data=data)
        pipeline.wait_for_completion(True)

        got = out.to_dict()

        assert expected_data == got
        pipeline.delete()

    def test_pandas_decimal(self):
        from decimal import Decimal

        sql = """
        CREATE TABLE t0 (c1 DECIMAL(5, 2));
        CREATE VIEW v0 AS SELECT c1 + 2.75::DECIMAL(5, 2) as c1 FROM t0;
        """

        data = [{"c1": 2.25}]
        expected = [{"c1": Decimal("5.00"), "insert_delete": 1}]

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_pandas_decimal", sql=sql
        ).create_or_replace()
        out = pipeline.listen("v0")

        pipeline.start()
        pipeline.input_json("t0", data=data)
        pipeline.wait_for_completion(True)

        got = out.to_dict()

        assert expected == got
        pipeline.delete()

    def test_pandas_array(self):
        sql = """
        CREATE TABLE t0 (c1 INT ARRAY);
        CREATE VIEW v0 AS SELECT c1 FROM t0;
        """

        data = [{"c1": [1, 2, 3]}]

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_pandas_array", sql=sql
        ).create_or_replace()
        out = pipeline.listen("v0")
        pipeline.start()
        pipeline.input_json("t0", data=data)
        pipeline.wait_for_completion(True)

        got = out.to_dict()

        for datum in data:
            datum.update({"insert_delete": 1})

        assert got == data
        pipeline.delete()

    def test_pandas_struct(self):
        sql = """
        CREATE TYPE s AS (
            f1 INT,
            f2 STRING
        );
        CREATE TABLE t0 (c1 s);
        CREATE VIEW v0 AS SELECT c1 FROM t0;
        """

        data = [{"c1": {"f1": 1, "f2": "a"}}]
        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_pandas_struct", sql=sql
        ).create_or_replace()
        out = pipeline.listen("v0")
        pipeline.start()
        pipeline.input_json("t0", data)
        pipeline.wait_for_completion(True)
        got = out.to_dict()

        for datum in data:
            datum.update({"insert_delete": 1})

        assert data == got
        pipeline.delete()

    def test_pandas_date_time_timestamp(self):
        from pandas import Timestamp, Timedelta

        sql = """
        CREATE TABLE t0 (c1 DATE, c2 TIME, c3 TIMESTAMP);
        CREATE VIEW v0 AS SELECT c1, c2, c3 FROM t0;
        """

        data = [{"c1": "2022-01-01", "c2": "12:00:00", "c3": "2022-01-01 12:00:00"}]
        expected = [
            {
                "c1": Timestamp("2022-01-01 00:00:00"),
                "c2": Timedelta("0 days 12:00:00"),
                "c3": Timestamp("2022-01-01 12:00:00"),
                "insert_delete": 1,
            }
        ]

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_pandas_date_time_timestamp", sql=sql
        ).create_or_replace()
        out = pipeline.listen("v0")
        pipeline.start()
        pipeline.input_json("t0", data)
        pipeline.wait_for_completion(True)
        got = out.to_dict()

        assert expected == got
        pipeline.delete()

    def test_pandas_simple(self):
        sql = """
        CREATE TABLE t0 (c0 BOOLEAN, c1 TINYINT, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 VARCHAR, c8 CHAR);
        CREATE VIEW v0 AS SELECT * FROM t0;
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

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_pandas_simple", sql=sql
        ).create_or_replace()
        out = pipeline.listen("v0")
        pipeline.start()
        pipeline.input_json("t0", data)
        pipeline.wait_for_completion(True)
        got = out.to_dict()

        for datum in data:
            datum.update({"insert_delete": 1})

        assert data == got
        pipeline.delete()

    def test_pandas_map(self):
        sql = """
        CREATE TABLE t0 (c1 MAP<STRING, INT>);
        CREATE VIEW v0 AS SELECT c1 FROM t0;
        """

        data = [{"c1": {"a": 1, "b": 2}}]
        expected = [{"c1": {"a": 1, "b": 2}, "insert_delete": 1}]

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_pandas_map", sql=sql
        ).create_or_replace()
        out = pipeline.listen("v0")
        pipeline.start()
        pipeline.input_json("t0", data)
        pipeline.wait_for_completion(True)
        pipeline.shutdown()

        got = out.to_dict()
        assert expected == got

        pipeline.start()
        out = pipeline.listen("v0")
        pipeline.input_json("t0", {"c1": {"a": 1, "b": 2}})
        pipeline.wait_for_completion(True)
        pipeline.shutdown()

        got = out.to_dict()
        assert expected == got

        pipeline.delete()

    def test_failed_pipeline_shutdown(self):
        sql = """
            CREATE TABLE t0 (c1 TINYINT);
            CREATE VIEW v0 AS SELECT c1 + 127::TINYINT FROM t0;"""

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_failed_pipeline_shutdown", sql=sql
        ).create_or_replace()
        pipeline.start()
        data = [{"c1": 127}]
        pipeline.input_json("t0", data)

        while True:
            status = pipeline.status()
            expected = PipelineStatus.FAILED
            if status == expected:
                break
            time.sleep(1)

        pipeline.shutdown()
        pipeline.delete()

    def test_adhoc_execute(self):
        sql = """
        CREATE TABLE t0 (c1 TINYINT) with ('materialized' = 'true');
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_adhoc_execute", sql=sql
        ).create_or_replace()
        pipeline.start()
        pipeline.wait_for_completion()
        pipeline.execute("INSERT INTO t0 VALUES (1), (2);")
        resp = pipeline.query("SELECT * FROM t0;")

        got = list(resp)
        expected = [{"c1": 1}, {"c1": 2}]

        pipeline.shutdown()

        self.assertCountEqual(got, expected)

    def test_issue2971(self):
        sql = """
        CREATE TABLE t0(c0 TINYINT) with ('materialized' = 'true');
        CREATE MATERIALIZED VIEW v0 AS SELECT (c0 + 127::TINYINT) as out FROM t0;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_issue2971", sql=sql
        ).create_or_replace()
        pipeline.restart()
        pipeline.input_json("t0", {"c0": 10})

        while pipeline.status() != PipelineStatus.FAILED:
            time.sleep(0.1)

        with self.assertRaises(RuntimeError) as err:
            pipeline.pause()

        got_err: str = err.exception.args[0].strip()
        assert "attempt to add with overflow" in got_err

        with self.assertRaises(RuntimeError) as err:
            pipeline.start()

        got_err: str = err.exception.args[0].strip()
        assert "attempt to add with overflow" in got_err

        pipeline.shutdown()
        pipeline.delete()

    def test_initialization_error(self):
        sql = """
        CREATE TABLE t0 (
            c0 INT NOT NULL
        ) with (
          'connectors' = '[{
            "transport": {
              "name": "datagen",
              "config": {
                "plan": [{
                    "fields": {
                        "c1": { "strategy": "uniform", "range": [100, 10000] }
                    }
                }]
              }
            }
          }]'
        );
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_initialization_error", sql=sql
        ).create_or_replace()
        with self.assertRaises(RuntimeError) as err:
            pipeline.start()

        pipeline.shutdown()
        pipeline.delete()

        got_err: str = err.exception.args[0].strip()
        assert "Unable to START the pipeline" in got_err

    def test_connector_orchestration(self):
        sql = """
        CREATE TABLE numbers (
          num INT
        ) WITH (
            'connectors' = '[
                {
                    "name": "c1",
                    "paused": true,
                    "transport": {
                        "name": "datagen",
                        "config": {"plan": [{ "rate": 1, "fields": { "num": { "range": [0, 10], "strategy": "uniform" } } }]}
                    }
                }
            ]'
        );
        """

        name = "test_connector_orchestration"

        pipeline = PipelineBuilder(TEST_CLIENT, name, sql=sql).create_or_replace()
        pipeline.start()

        pipeline.resume_connector("numbers", "c1")
        stats = TEST_CLIENT.get_pipeline_stats(name)
        c1_status = next(
            item["paused"]
            for item in stats["inputs"]
            if item["endpoint_name"] == "numbers.c1"
        )
        assert not c1_status

        pipeline.pause_connector("numbers", "c1")
        stats = TEST_CLIENT.get_pipeline_stats(name)
        c2_status = next(
            item["paused"]
            for item in stats["inputs"]
            if item["endpoint_name"] == "numbers.c1"
        )
        assert c2_status

        pipeline.shutdown()
        pipeline.delete()

    def test_uuid(self):
        name = "test_uuid"
        sql = """
        CREATE TABLE t0(c0 UUID);
        CREATE MATERIALIZED VIEW v0 AS SELECT c0 FROM t0;
        """

        pipeline = PipelineBuilder(TEST_CLIENT, name, sql=sql).create_or_replace()
        out = pipeline.listen("v0")
        pipeline.start()

        data = [{"c0": uuid.uuid4()}]
        pipeline.input_json("t0", data)

        for datum in data:
            datum.update({"insert_delete": 1})

        pipeline.wait_for_completion(True)

        got = out.to_dict()
        pipeline.shutdown()

        assert got == data

    def test_issue3754(self):
        name = "test_issue3754"
        sql = """
CREATE TABLE map_tbl(m_var MAP<VARCHAR, VARCHAR>);
CREATE MATERIALIZED VIEW v AS SELECT * FROM map_tbl;
        """

        pipeline = PipelineBuilder(TEST_CLIENT, name, sql=sql).create_or_replace()
        pipeline.start()

        with self.assertRaises(ValueError):
            data = [{"insert": {"m_var": {None: 1}}}]
            TEST_CLIENT.push_to_pipeline(name, "map_tbl", "insert_delete", data)

        with self.assertRaises(ValueError):
            data = [{"delete": {"m_var": {None: 1}}}]
            TEST_CLIENT.push_to_pipeline(name, "map_tbl", "insert_delete", data)

        with self.assertRaises(ValueError):
            data = {"insert": {"m_var": {None: 1}}}
            TEST_CLIENT.push_to_pipeline(name, "map_tbl", "insert_delete", data)

        with self.assertRaises(ValueError):
            data = {"delete": {"m_var": {None: 1}}}
            TEST_CLIENT.push_to_pipeline(name, "map_tbl", "insert_delete", data)

        with self.assertRaises(ValueError):
            data = [{"m_var": {None: 1}}]
            TEST_CLIENT.push_to_pipeline(name, "map_tbl", "raw", data)

        with self.assertRaises(ValueError):
            data = {"m_var": {None: 1}}
            TEST_CLIENT.push_to_pipeline(name, "map_tbl", "raw", data)

        with self.assertRaises(ValueError):
            data = {"m_var": {None: 1}}
            pipeline.input_json("map_tbl", data)

        pipeline.shutdown()
        pipeline.delete()

    def test_program_error0(self):
        sql = "create taabl;"
        name = "test_program_error0"

        try:
            _ = PipelineBuilder(TEST_CLIENT, name, sql).create_or_replace()
        except Exception:
            pass

        pipeline = Pipeline.get(name, TEST_CLIENT)
        err = pipeline.program_error()

        assert err["sql_compilation"] != 0

        pipeline.shutdown()
        pipeline.delete()

    def test_program_error1(self):
        sql = ""
        name = "test_program_error1"

        _ = PipelineBuilder(TEST_CLIENT, name, sql).create_or_replace()

        pipeline = Pipeline.get(name, TEST_CLIENT)
        err = pipeline.program_error()

        assert err["sql_compilation"]["exit_code"] == 0
        assert err["rust_compilation"]["exit_code"] == 0

        pipeline.shutdown()
        pipeline.delete()

    def test_errors0(self):
        sql = "SELECT invalid"
        name = "test_errors0"

        try:
            _ = PipelineBuilder(TEST_CLIENT, name, sql).create_or_replace()
        except Exception:
            pass

        pipeline = Pipeline.get(name, TEST_CLIENT)

        assert pipeline.errors()[0]["sql_compilation"]["exit_code"] != 0


if __name__ == "__main__":
    unittest.main()
