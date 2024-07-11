from feldera import FelderaClient, SQLContext, SQLSchema
from feldera.formats import JSONFormat, JSONUpdateFormat
import config
from sql_program import generate_program
import time

client = FelderaClient("http://localhost:8080")
sql = SQLContext("mil", client, workers=10, storage = False)

generate_program(sql)

# sql.foreach_chunk("user_agg", lambda df, chunk : print(df))

in_fmt = JSONFormat().with_array(False).with_update_format(JSONUpdateFormat.Raw)
sql.connect_source_kafka("interactions", "kafka_conn_in_interactions", {
   "topics": [config.KAFKA_TOPIC_NAME],
    "bootstrap.servers": config.KAFKA_SERVER_FROM_PIPELINE,
    "auto.offset.reset": "earliest",
    "poller_threads": 12,
}, in_fmt)

print("Starting Feldera Pipeline")
sql.start()
print("Pipeline started")

start_time = time.time()

sql.wait_for_idle(idle_interval_s = 1)

end_time = time.time()
elapsed = end_time - start_time

print(f"Pipeline finished in {elapsed}, shutting down...")

sql.shutdown()
