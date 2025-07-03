from feldera import FelderaClient, PipelineBuilder
from feldera.runtime_config import RuntimeConfig
import config
from sql_program import generate_program
import time

client = FelderaClient("http://localhost:8080")

print("kafka_server:", config.KAFKA_SERVER)
print("pipeline to kafka: ", config.KAFKA_SERVER_FROM_PIPELINE)


json = {"name": "json", "config": {"update_format": "raw", "array": False}}

csv = {"name": "csv", "config": {}}


data_fmt = csv if config.DATA_FMT == "csv" else json


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
    data_fmt,
)

runtime_config = RuntimeConfig(storage=False, workers=10)
pipeline = PipelineBuilder(
    client, name="mil", sql=code, runtime_config=runtime_config
).create_or_replace()

print("Starting Feldera Pipeline")
pipeline.start()
print("Pipeline started")
# pipeline.foreach_chunk("user_agg", lambda df, chunk: print(df))

start_time = time.time()

pipeline.wait_for_idle(idle_interval_s=1)

end_time = time.time()
elapsed = end_time - start_time

print(f"Pipeline finished in {elapsed}, shutting down...")

pipeline.stop(force=True)
