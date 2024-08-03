from feldera import FelderaClient, SQLContext
from feldera.formats import JSONFormat, JSONUpdateFormat
import config
from sql_program import generate_program
import time


client = FelderaClient("http://localhost:8080")
sql = SQLContext("mil", client, workers=10, storage=False)

code = generate_program(
    {
        "name": "kafka_input",
        "config": {
            "topics": [config.KAFKA_TOPIC_NAME],
            "bootstrap.servers": config.KAFKA_SERVER_FROM_PIPELINE,
            "auto.offset.reset": "earliest",
            "poller_threads": 12,
        },
    },
    JSONFormat().with_array(False).with_update_format(JSONUpdateFormat.Raw).to_dict(),
)

sql.sql(code)

print("Starting Feldera Pipeline")
sql.start()
print("Pipeline started")
# sql.foreach_chunk("user_agg", lambda df, chunk: print(df))

start_time = time.time()

sql.wait_for_idle(idle_interval_s=1)

end_time = time.time()
elapsed = end_time - start_time

print(f"Pipeline finished in {elapsed}, shutting down...")

sql.shutdown()
