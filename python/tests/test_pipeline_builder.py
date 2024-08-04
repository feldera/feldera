import os
import unittest
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

from feldera import PipelineBuilder
from tests import TEST_CLIENT


class TestWireframes(unittest.TestCase):
    def test_local(self):
        TBL_NAMES = ['students', 'grades']
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

        pipeline = PipelineBuilder(TEST_CLIENT).with_name("notebook").with_sql(sql).create_or_replace()

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

        out = pipeline.listen("average_scores")

        pipeline.start()
        pipeline.input_pandas(TBL_NAMES[0], df_students)
        pipeline.input_pandas(TBL_NAMES[1], df_grades)
        pipeline.wait_for_completion(True)
        df = out.to_pandas()

        assert df.shape[0] == 100

        pipeline.delete()

    def test_local_listen_after_start(self):
        TBL_NAMES = ['students', 'grades']
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

        pipeline = PipelineBuilder(TEST_CLIENT).with_name("notebook").with_sql(sql).create_or_replace()

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

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

        TBL_NAMES = ['students', 'grades']
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

        pipeline1 = PipelineBuilder(TEST_CLIENT).with_name("p1").with_sql(sql1).create_or_replace()
        pipeline2 = PipelineBuilder(TEST_CLIENT).with_name("p2").with_sql(sql2).create_or_replace()

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

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

        TBL_NAMES = ['students', 'grades']
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

        pipeline = PipelineBuilder(TEST_CLIENT).with_name("foreach_chunk").with_sql(sql).create_or_replace()

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

        pipeline.start()
        pipeline.foreach_chunk(view_name, callback)

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

        pipeline = PipelineBuilder(TEST_CLIENT).with_name("df_without_columns").with_sql(sql).create_or_replace()

        df = pd.DataFrame([(1, "a"), (2, "b"), (3, "c")])

        pipeline.start()

        with self.assertRaises(ValueError):
            pipeline.input_pandas(TBL_NAME, df)

        pipeline.shutdown()
        pipeline.delete()

    def test_sql_error(self):
        TBL_NAME = "student"

        sql = f"""
        CREATE TABLE {TBL_NAME} (
            id INT,
            name STRING
        );

        CREATE VIEW s AS SELECT * FROM blah;
        """

        with self.assertRaises(Exception):
            PipelineBuilder(TEST_CLIENT).with_name("sql_error").with_sql(sql).create_or_replace()

        pipeline = PipelineBuilder(TEST_CLIENT).with_name("sql_error").get()
        pipeline.delete()

    def test_kafka(self):
        import json

        KAFKA_SERVER = "localhost:19092"
        PIPELINE_TO_KAFKA_SERVER = "redpanda:9092"

        in_ci = os.environ.get("IN_CI")

        if in_ci == "1":
            # if running in CI, skip the test
            return

        print("(Re-)creating topics...")
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_SERVER,
            client_id="test_client"
        )

        INPUT_TOPIC = "simple_count_input"
        OUTPUT_TOPIC = "simple_count_output"

        existing_topics = set(admin_client.list_topics())
        if INPUT_TOPIC in existing_topics:
            admin_client.delete_topics([INPUT_TOPIC])
        if OUTPUT_TOPIC in existing_topics:
            admin_client.delete_topics([OUTPUT_TOPIC])
        admin_client.create_topics([
            NewTopic(INPUT_TOPIC, num_partitions=1, replication_factor=1),
            NewTopic(OUTPUT_TOPIC, num_partitions=1, replication_factor=1),
        ])
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

        pipeline = PipelineBuilder(TEST_CLIENT).with_name("kafka_test").with_sql(sql).create_or_replace()

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

        CREATE VIEW s AS SELECT * FROM items;
        """

        pipeline = PipelineBuilder(TEST_CLIENT).with_name("test_http_get").with_sql(sql).create_or_replace()

        out = pipeline.listen("s")

        pipeline.start()
        pipeline.wait_for_completion(True)

        df = out.to_pandas()

        assert df.shape[0] == 3

        pipeline.delete()

    def test_avro_format(self):
        import json

        PIPELINE_TO_KAFKA_SERVER = "redpanda:9092"
        KAFKA_SERVER = "localhost:19092"

        TOPIC = "test_avro_format"

        in_ci = os.environ.get("IN_CI")

        if in_ci == "1":
            # if running in CI, skip the test
            return

        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_SERVER,
            client_id="test_client"
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
                            "schema": {json.dumps(json.dumps({
                                "type": "record",
                                "name": "items",
                                "fields": [
                                    {"name": "id", "type": ["null", "int"]},
                                    {"name": "name", "type": ["null", "string"]}
                                ]
                            }))}
                        }}
                    }}
                }}
           ]'
        )
        AS SELECT * FROM items;
        """

        pipeline = PipelineBuilder(TEST_CLIENT).with_name("test_avro_format").with_sql(sql).create_or_replace()

        pipeline.start()
        pipeline.input_pandas("items", df)
        pipeline.wait_for_completion(True)

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',
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

        pipeline = PipelineBuilder(TEST_CLIENT).with_name(name).with_sql(sql).with_runtime_config(RuntimeConfig(resources=resources)).create_or_replace()

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

        pipeline = PipelineBuilder(TEST_CLIENT).with_name("test_timestamp_pandas").with_sql(sql).create_or_replace()

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "birthdate": [
            pd.Timestamp.now(), pd.Timestamp.now(), pd.Timestamp.now()
        ]})

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

        pipeline = PipelineBuilder(TEST_CLIENT).with_name("test_input_json").with_sql(sql).create_or_replace()

        data = {"insert": {'id': 1, 'name': 'a'}}

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

        pipeline = PipelineBuilder(TEST_CLIENT).with_name("test_input_json").with_sql(sql).create_or_replace()

        data = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]

        out = pipeline.listen(VIEW_NAME)

        pipeline.start()
        pipeline.input_json(TBL_NAME, data)
        pipeline.wait_for_completion(True)

        out_data = out.to_dict()

        for datum in data:
            datum.update({"insert_delete": 1})

        assert out_data == data

        pipeline.delete()


if __name__ == '__main__':
    unittest.main()
