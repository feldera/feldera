import os
import unittest
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

from feldera import SQLContext
from tests import TEST_CLIENT


class TestWireframes(unittest.TestCase):
    def test_local(self):
        sql = SQLContext('notebook', TEST_CLIENT).get_or_create()

        TBL_NAMES = ['students', 'grades']
        view_name = "average_scores"

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

        sql.sql(f"""
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
        """)

        out = sql.listen(view_name)

        sql.start()

        sql.input_pandas(TBL_NAMES[0], df_students)
        sql.input_pandas(TBL_NAMES[1], df_grades)

        sql.wait_for_completion(True)

        df = out.to_pandas()

        assert df.shape[0] == 100

        sql.delete()

    def test_local_listen_after_start(self):
        sql = SQLContext('notebook', TEST_CLIENT).get_or_create()

        TBL_NAMES = ['students', 'grades']
        view_name = "average_scores"

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

        sql.sql(f"""
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
        """)

        query = f"SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC"

        sql.sql(f"CREATE VIEW {view_name} AS {query};")

        sql.start()
        out = sql.listen(view_name)

        sql.input_pandas(TBL_NAMES[0], df_students)
        sql.input_pandas(TBL_NAMES[1], df_grades)

        sql.wait_for_completion(True)

        df = out.to_pandas()

        sql.delete()
        assert df.shape[0] == 100

    def test_two_SQLContexts(self):
        # https://github.com/feldera/feldera/issues/1770

        sql = SQLContext('sql_context1', TEST_CLIENT).get_or_create()
        sql2 = SQLContext('sql_context2', TEST_CLIENT).get_or_create()

        TBL_NAMES = ['students', 'grades']
        VIEW_NAMES = [n + "_view" for n in TBL_NAMES]

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

        sql.sql(f"""
        CREATE TABLE {TBL_NAMES[0]} (
            name STRING,
            id INT
        );
        """)

        sql2.sql(f"""
        CREATE TABLE {TBL_NAMES[1]} (
            student_id INT,
            science INT,
            maths INT,
            art INT
        );
        """)

        sql.sql(f"CREATE VIEW {VIEW_NAMES[0]} AS SELECT * FROM {TBL_NAMES[0]};")
        sql2.sql(f"CREATE VIEW {VIEW_NAMES[1]} AS SELECT * FROM {TBL_NAMES[1]};")

        out = sql.listen(VIEW_NAMES[0])
        out2 = sql2.listen(VIEW_NAMES[1])

        sql.start()
        sql2.start()

        sql.input_pandas(TBL_NAMES[0], df_students)
        sql2.input_pandas(TBL_NAMES[1], df_grades)

        sql.wait_for_completion(True)
        sql2.wait_for_completion(True)

        df = out.to_pandas()
        df2 = out2.to_pandas()

        assert df.columns.tolist() not in df2.columns.tolist()

        sql.delete()
        sql2.delete()

    def test_foreach_chunk(self):
        def callback(df: pd.DataFrame, seq_no: int):
            print(f"\nSeq No: {seq_no}, DF size: {df.shape[0]}\n")

        sql = SQLContext('foreach_chunk', TEST_CLIENT).get_or_create()

        TBL_NAMES = ['students', 'grades']
        view_name = "average_scores"

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

        sql.sql(f"""
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
        """)

        query = f"SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC"
        sql.sql(f"CREATE VIEW {view_name} AS {query};")

        sql.start()
        sql.foreach_chunk(view_name, callback)

        sql.input_pandas(TBL_NAMES[0], df_students)
        sql.input_pandas(TBL_NAMES[1], df_grades)

        sql.wait_for_completion(True)
        sql.delete()

    def test_df_without_columns(self):
        sql = SQLContext('df_without_columns', TEST_CLIENT).get_or_create()
        TBL_NAME = "student"

        df = pd.DataFrame([(1, "a"), (2, "b"), (3, "c")])

        sql.sql(f"""
        CREATE TABLE {TBL_NAME} (
            id INT,
            name STRING
        );
        """)

        sql.sql(f"CREATE VIEW s AS SELECT * FROM {TBL_NAME} ;")

        sql.start()

        with self.assertRaises(ValueError):
            sql.input_pandas(TBL_NAME, df)

        sql.shutdown()
        sql.delete()

    def test_sql_error(self):
        sql = SQLContext('sql_error', TEST_CLIENT).get_or_create()
        TBL_NAME = "student"

        sql.sql(f"""
        CREATE TABLE {TBL_NAME} (
            id INT,
            name STRING
        );
        """)

        sql.sql(f"CREATE VIEW s AS SELECT * FROM blah;")
        _ = sql.listen("s")

        with self.assertRaises(Exception):
            sql.start()

        sql.delete()

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

        sql = SQLContext('kafka_test', TEST_CLIENT).get_or_create()

        sql.sql(f"""
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
                            "array": False
                        }}
                    }}
                }}
            ]'
        );
        """)
        sql.sql(f"""
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
                            "array": False
                        }}
                    }}
                }}
            ]'
        )
        AS SELECT COUNT(*) as num_rows FROM {TABLE_NAME};
        """)

        out = sql.listen(VIEW_NAME)
        sql.start()
        sql.wait_for_idle()
        sql.shutdown()
        df = out.to_pandas()
        assert df.shape[0] != 0

        sql.delete()

    def test_http_get(self):
        sql = SQLContext("test_http_get", TEST_CLIENT).get_or_create()

        sql.sql("""
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
        """)

        out = sql.listen("s")

        sql.start()
        sql.wait_for_completion(True)

        df = out.to_pandas()

        assert df.shape[0] == 3

        sql.delete()

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

        sql = SQLContext("test_avro_format", TEST_CLIENT).get_or_create()

        TBL_NAME = "items"
        VIEW_NAME = "s"

        sql.sql(f"""
        CREATE TABLE {TBL_NAME} (
            id INT,
            name STRING
        );
        """)

        avro_format = {
            "type": "record",
            "name": "items",
            "fields": [
                {"name": "id", "type": ["null", "int"]},
                {"name": "name", "type": ["null", "string"]}
            ]
        }

        # serialize to a string
        avro_schema = json.dumps(avro_format)
        # re-serialize the string
        avro_schema = json.dumps(avro_schema)

        sql.sql(f"""
        CREATE VIEW {VIEW_NAME} WITH (
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
                            "schema": {avro_schema}
                        }}
                    }}
                }}
           ]'
        )
        AS SELECT * FROM {TBL_NAME};
        """)

        sql.start()
        sql.input_pandas(TBL_NAME, df)
        sql.wait_for_completion(True)

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',
        )

        msg = next(consumer)
        assert msg.value is not None

        sql.delete()

    def test_pipeline_resource_config(self):
        from feldera.runtime_config import Resources

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

        sql = SQLContext(
            name,
            TEST_CLIENT,
            resources=resources
        ).get_or_create()

        sql.sql("""
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
        """)

        out = sql.listen("s")

        sql.start()
        sql.wait_for_completion(True)

        df = out.to_pandas()

        assert df.shape[0] == 3

        assert TEST_CLIENT.get_pipeline(name).runtime_config["resources"] == config

        sql.delete()

    def test_timestamp_pandas(self):
        sql = SQLContext("test_timestamp_pandas", TEST_CLIENT).get_or_create()

        TBL_NAME = "items"
        VIEW_NAME = "s"

        # backend doesn't support TIMESTAMP of format: "2024-06-06T18:06:28.443"
        sql.sql(f"""
        CREATE TABLE {TBL_NAME} (
            id INT,
            name STRING,
            birthdate TIMESTAMP
        );
        """)

        sql.sql( f"CREATE VIEW {VIEW_NAME} as SELECT * FROM {TBL_NAME}")

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "birthdate": [
            pd.Timestamp.now(), pd.Timestamp.now(), pd.Timestamp.now()
        ]})

        out = sql.listen(VIEW_NAME)

        sql.start()
        sql.input_pandas(TBL_NAME, df)
        sql.wait_for_completion(True)

        df = out.to_pandas()

        assert df.shape[0] == 3

        sql.delete()

    def test_input_json0(self):
        sql = SQLContext("test_input_json", TEST_CLIENT).get_or_create()

        TBL_NAME = "items"
        VIEW_NAME = "s"

        sql.sql(f"""
        CREATE TABLE {TBL_NAME} (
            id INT,
            name STRING
        );
        
        CREATE VIEW {VIEW_NAME} AS SELECT * FROM {TBL_NAME};
        """)

        data = {"insert": {'id': 1, 'name': 'a'}}

        out = sql.listen(VIEW_NAME)

        sql.start()
        sql.input_json(TBL_NAME, data, update_format="insert_delete")
        sql.wait_for_completion(True)

        out_data = out.to_dict()

        data["insert"].update({"insert_delete": 1})
        assert out_data == [data["insert"]]

    def test_input_json1(self):
        sql = SQLContext("test_input_json", TEST_CLIENT).get_or_create()

        TBL_NAME = "items"
        VIEW_NAME = "s"

        sql.sql(f"""
        CREATE TABLE {TBL_NAME} (
            id INT,
            name STRING
        );
        
        CREATE VIEW {VIEW_NAME} AS SELECT * FROM {TBL_NAME};
        """)

        data = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]

        out = sql.listen(VIEW_NAME)

        sql.start()
        sql.input_json(TBL_NAME, data)
        sql.wait_for_completion(True)

        out_data = out.to_dict()

        for datum in data:
            datum.update({"insert_delete": 1})

        assert out_data == data


if __name__ == '__main__':
    unittest.main()
