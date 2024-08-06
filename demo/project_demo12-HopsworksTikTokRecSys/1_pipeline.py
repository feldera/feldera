from feldera import FelderaClient, PipelineBuilder
from feldera.runtime_config import RuntimeConfig
import config
from sql_program import generate_program
import time


client = FelderaClient("http://localhost:8080")

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
    {
        "name": "json",
        "config": {
            "update_format": "raw",
            "array": False
        }
    }
)

runtime_config = RuntimeConfig(storage=False, workers=10)
pipeline = PipelineBuilder(client, name="mil", sql=code, runtime_config=runtime_config).create_or_replace()

print("Starting Feldera Pipeline")
pipeline.start()
print("Pipeline started")
# pipeline.foreach_chunk("user_agg", lambda df, chunk: print(df))

start_time = time.time()

pipeline.wait_for_idle(idle_interval_s=1)

end_time = time.time()
elapsed = end_time - start_time

print(f"Pipeline finished in {elapsed}, shutting down...")

pipeline.shutdown()
