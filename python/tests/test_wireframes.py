import time
import unittest
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

from feldera import SQLContext, SQLSchema
from feldera.formats import JSONFormat, JSONUpdateFormat
from tests import TEST_CLIENT


class TestWireframes(unittest.TestCase):
    def test_local(self):
        sql = SQLContext('notebook', TEST_CLIENT).get_or_create()

        TBL_NAMES = ['students', 'grades']
        view_name = "average_scores"

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

        sql.register_table(TBL_NAMES[0], SQLSchema({"name": "STRING", "id": "INT"}))
        sql.register_table(TBL_NAMES[1], SQLSchema({
            "student_id": "INT",
            "science": "INT",
            "maths": "INT",
            "art": "INT"
        }))

        query = f"SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC"
        sql.register_view(view_name, query)

        sql.connect_source_pandas(TBL_NAMES[0], df_students)
        sql.connect_source_pandas(TBL_NAMES[1], df_grades)

        out = sql.listen(view_name)

        sql.run_to_completion()

        df = out.to_pandas()

        assert df.shape[0] == 100

    def test_two_SQLContexts(self):
        # https://github.com/feldera/feldera/issues/1770

        sql = SQLContext('sql_context1', TEST_CLIENT).get_or_create()
        sql2 = SQLContext('sql_context2', TEST_CLIENT).get_or_create()

        TBL_NAMES = ['students', 'grades']
        VIEW_NAMES = [n + "_view" for n in TBL_NAMES]

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

        sql.register_table(TBL_NAMES[0], SQLSchema({"name": "STRING", "id": "INT"}))
        sql2.register_table(TBL_NAMES[1], SQLSchema({
            "student_id": "INT",
            "science": "INT",
            "maths": "INT",
            "art": "INT"
        }))

        sql.register_view(VIEW_NAMES[0], f"SELECT * FROM {TBL_NAMES[0]}")
        sql2.register_view(VIEW_NAMES[1], f"SELECT * FROM {TBL_NAMES[1]}")

        sql.connect_source_pandas(TBL_NAMES[0], df_students)
        sql2.connect_source_pandas(TBL_NAMES[1], df_grades)

        out = sql.listen(VIEW_NAMES[0])
        out2 = sql2.listen(VIEW_NAMES[1])

        sql.run_to_completion()
        sql2.run_to_completion()

        df = out.to_pandas()
        df2 = out2.to_pandas()

        assert df.columns.tolist() not in df2.columns.tolist()

    def test_foreach_chunk(self):
        def callback(df: pd.DataFrame, seq_no: int):
            print(f"\nSeq No: {seq_no}, DF size: {df.shape[0]}\n")

        sql = SQLContext('foreach_chunk', TEST_CLIENT).get_or_create()

        TBL_NAMES = ['students', 'grades']
        view_name = "average_scores"

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

        sql.register_table(TBL_NAMES[0], SQLSchema({"name": "STRING", "id": "INT"}))
        sql.register_table(TBL_NAMES[1], SQLSchema({
            "student_id": "INT",
            "science": "INT",
            "maths": "INT",
            "art": "INT"
        }))

        query = f"SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC"
        sql.register_view(view_name, query)

        sql.connect_source_pandas(TBL_NAMES[0], df_students)
        sql.connect_source_pandas(TBL_NAMES[1], df_grades)

        sql.foreach_chunk(view_name, callback)

        sql.run_to_completion()

    def test_df_without_columns(self):

        sql = SQLContext('df_without_columns', TEST_CLIENT).get_or_create()
        TBL_NAME = "student"

        df = pd.DataFrame([(1, "a"), (2, "b"), (3, "c")])

        sql.register_table(TBL_NAME, SQLSchema({"id": "INT", "name": "STRING"}))
        sql.register_view("s", f"SELECT * FROM {TBL_NAME}")

        with self.assertRaises(ValueError):
            sql.connect_source_pandas(TBL_NAME, df)

    def test_sql_error(self):
        sql = SQLContext('sql_error', TEST_CLIENT).get_or_create()
        TBL_NAME = "student"
        df = pd.DataFrame([(1, "a"), (2, "b"), (3, "c")], columns=["id", "name"])
        sql.register_table(TBL_NAME, SQLSchema({"id": "INT", "name": "STRING"}))
        sql.register_view("s", f"SELECT FROM blah")
        sql.connect_source_pandas(TBL_NAME, df)
        _ = sql.listen("s")

        with self.assertRaises(Exception):
            sql.run_to_completion()

    def test_kafka(self):
        import json

        KAFKA_SERVER = "localhost:19092"

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
        sql.register_table(TABLE_NAME, SQLSchema({"id": "INT NOT NULL PRIMARY KEY"}))
        sql.register_view(VIEW_NAME, f"SELECT COUNT(*) as num_rows FROM {TABLE_NAME}")

        PIPELINE_TO_KAFKA_SERVER = "redpanda:9092"

        source_config = {
            "topics": [INPUT_TOPIC],
            "bootstrap.servers": PIPELINE_TO_KAFKA_SERVER,
            "auto.offset.reset": "earliest",
        }

        kafka_format = JSONFormat().with_update_format(JSONUpdateFormat.InsertDelete).with_array(False)

        sink_config = {
            "topic": OUTPUT_TOPIC,
            "bootstrap.servers": PIPELINE_TO_KAFKA_SERVER,
            "auto.offset.reset": "earliest",
        }

        sql.connect_source_kafka(TABLE_NAME, "kafka_conn_in", source_config, kafka_format)
        sql.connect_sink_kafka(VIEW_NAME, "kafka_conn_out", sink_config, kafka_format)

        out = sql.listen(VIEW_NAME)
        sql.start()
        sql.wait_for_idle()
        sql.shutdown()
        df = out.to_pandas()
        assert df.shape[0] != 0

    def test_http_get(self):
        sql = SQLContext("test_http_get", TEST_CLIENT).get_or_create()

        TBL_NAME = "items"
        VIEW_NAME = "s"

        sql.register_table(TBL_NAME, SQLSchema({"id": "INT", "name": "STRING"}))

        sql.register_view(VIEW_NAME, f"SELECT * FROM {TBL_NAME}")

        path = "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"

        fmt = JSONFormat().with_update_format(JSONUpdateFormat.InsertDelete).with_array(False)
        sql.connect_source_url(TBL_NAME, "part", path, fmt)

        out = sql.listen(VIEW_NAME)

        sql.run_to_completion()

        df = out.to_pandas()

        assert df.shape[0] == 3

    def test_avro_format(self):
        from feldera.formats import AvroFormat

        KAFKA_URL_FROM_PIPELINE = "redpanda:9092"
        KAFKA_SERVER = "localhost:19092"
        TOPIC = "test_avro_format"

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

        sql.register_table(TBL_NAME, SQLSchema({"id": "INT", "name": "STRING"}))
        sql.register_view(VIEW_NAME, f"SELECT * FROM {TBL_NAME}")

        sql.connect_source_pandas(TBL_NAME, df)

        sink_config = {
            "topic": TOPIC,
            "bootstrap.servers": KAFKA_URL_FROM_PIPELINE,
            "auto.offset.reset": "earliest",
        }

        avro_format = AvroFormat().with_schema({
            "type": "record",
            "name": "items",
            "fields": [
                {"name": "id", "type": ["null", "int"]},
                {"name": "name", "type": ["null", "string"]}
            ]
        })

        sql.connect_sink_kafka(VIEW_NAME, "out_avro_kafka", sink_config, avro_format)

        sql.run_to_completion()

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',
        )

        msg = next(consumer)
        assert msg.value is not None

    def test_pipeline_resource_config(self):
        from feldera.resources import Resources

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

        TBL_NAME = "items"
        VIEW_NAME = "s"

        sql.register_table(TBL_NAME, SQLSchema({"id": "INT", "name": "STRING"}))

        sql.register_view(VIEW_NAME, f"SELECT * FROM {TBL_NAME}")

        path = "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"

        fmt = JSONFormat().with_update_format(JSONUpdateFormat.InsertDelete).with_array(False)
        sql.connect_source_url(TBL_NAME, "part", path, fmt)

        out = sql.listen(VIEW_NAME)

        sql.run_to_completion()

        df = out.to_pandas()

        assert df.shape[0] == 3

        assert TEST_CLIENT.get_pipeline(name).config["resources"] == config

    def test_timestamp_pandas(self):
        sql = SQLContext("test_timestamp_pandas", TEST_CLIENT).get_or_create()

        TBL_NAME = "items"
        VIEW_NAME = "s"

        # backend doesn't support TIMESTAMP of format: "2024-06-06T18:06:28.443"
        sql.register_table(TBL_NAME, SQLSchema({"id": "INT", "name": "STRING", "birthdate": "TIMESTAMP"}))

        sql.register_view(VIEW_NAME, f"SELECT * FROM {TBL_NAME}")

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "birthdate": [
            pd.Timestamp.now(), pd.Timestamp.now(), pd.Timestamp.now()
        ]})

        sql.connect_source_pandas(TBL_NAME, df)

        out = sql.listen(VIEW_NAME)

        sql.run_to_completion()

        df = out.to_pandas()

        assert df.shape[0] == 3


if __name__ == '__main__':
    unittest.main()
