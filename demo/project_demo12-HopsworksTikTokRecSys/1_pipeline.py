from feldera import FelderaClient, SQLContext, SQLSchema
from feldera.formats import JSONFormat, JSONUpdateFormat
import config

client = FelderaClient("http://localhost:8080")
sql = SQLContext("mil", client)

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

sql.register_local_view("video_agg", """
    SELECT
        video_id,
        interaction_type,
        count(*) OVER week as interaction_len,
        avg(watch_time) OVER week as average_watch_time,
        interaction_date as hour_start
    FROM interactions
    WINDOW
        week AS (PARTITION BY video_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
""")

sql.register_local_view("user_agg", """
    SELECT
        user_id,
        interaction_type,
        count(*) OVER week as interaction_len,
        avg(watch_time) OVER week as average_watch_time,
        interaction_date as hour_start
    FROM interactions
    WINDOW
        week AS (PARTITION BY user_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
""")

sql.register_materialized_view("video_agg_1h", """
    SELECT * FROM video_agg
    PIVOT (
        SUM(interaction_len) FOR interaction_type IN (
            'like' as "like",
            'dislike' as "dislike",
            'view' as "view",
            'comment' as "comment",
            'share' as "share",
            'skip' as "skip"
        )
    )
""")

sql.register_materialized_view("user_agg_1h", """
    SELECT * FROM user_agg
    PIVOT (
        SUM(interaction_len) FOR interaction_type IN (
            'like' as "like",
            'dislike' as "dislike",
            'view' as "view",
            'comment' as "comment",
            'share' as "share",
            'skip' as "skip"
        )
    )
""")

in_fmt = JSONFormat().with_array(False).with_update_format(JSONUpdateFormat.Raw)
sql.connect_source_kafka("interactions", "kafka_conn_in_interactions", {
   "topics": [config.KAFKA_TOPIC_NAME],
    "bootstrap.servers": config.KAFKA_SERVER_FROM_PIPELINE,
    "auto.offset.reset": "earliest",
}, in_fmt)

print("Starting Feldera Pipeline")
sql.start()
print("Pipeline started")

sql.wait_for_idle()
print("Found pipeline to be idle, shutting down...")

sql.shutdown()
