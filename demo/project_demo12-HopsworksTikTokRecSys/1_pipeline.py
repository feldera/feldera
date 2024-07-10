from feldera import FelderaClient, SQLContext, SQLSchema
from feldera.formats import JSONFormat, JSONUpdateFormat
import config
import time

client = FelderaClient("http://localhost:8080")
sql = SQLContext("mil", client, workers=16)

sql.register_table("interactions",
    SQLSchema({
        "interaction_id": "BIGINT",
        "user_id": "INT",
        "video_id": "INT",
        "category_id": "INT",
        "interaction_type": "STRING",
        "watch_time": "INT",
        "interaction_date": "TIMESTAMP LATENESS INTERVAL '10' SECONDS",
        "previous_interaction_date": "TIMESTAMP",
        "interaction_month": "TIMESTAMP",
    })
)

sql.register_view("video_agg", """
    SELECT
        video_id,
        interaction_type,
        count(*) OVER hour as interaction_len_h,
        count(*) OVER day as interaction_len_d,
        count(*) OVER week as interaction_len_w,
        avg(watch_time) OVER hour as average_watch_time_h,
        avg(watch_time) OVER day as average_watch_time_d,
        avg(watch_time) OVER week as average_watch_time_w,
        interaction_date as hour_start
    FROM interactions
    WINDOW
        hour AS (PARTITION BY video_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW),
        day AS (PARTITION BY video_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW),
        week AS (PARTITION BY video_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)
""")

sql.register_view("user_agg", """
    SELECT
        user_id,
        interaction_type,
        count(*) OVER hour as interaction_len_h,
        count(*) OVER day as interaction_len_d,
        count(*) OVER week as interaction_len_w,
        avg(watch_time) OVER hour as average_watch_time_h,
        avg(watch_time) OVER day as average_watch_time_d,
        avg(watch_time) OVER week as average_watch_time_w,
        interaction_date as hour_start
    FROM interactions
    WINDOW
        hour AS (PARTITION BY user_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW),
        day AS (PARTITION BY user_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW),
        week AS (PARTITION BY user_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)
""")


in_fmt = JSONFormat().with_array(False).with_update_format(JSONUpdateFormat.Raw)
sql.connect_source_kafka("interactions", "kafka_conn_in_interactions", {
   "topics": [config.KAFKA_TOPIC_NAME],
    "bootstrap.servers": config.KAFKA_SERVER_FROM_PIPELINE,
    "auto.offset.reset": "earliest",
    "poller_threads": 8,
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
