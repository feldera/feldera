from feldera import FelderaClient, SQLContext, SQLSchema
from feldera.formats import JSONFormat, JSONUpdateFormat
import config

client = FelderaClient("http://localhost:8080")
sql = SQLContext("mil", client)

sql.register_table("interactions",
    SQLSchema({
        "interaction_id": "STRING",
        "user_id": "STRING",
        "video_id": "STRING",
        "category_id": "INT",
        "interaction_type": "STRING",
        "watch_time": "INT",
        "interaction_date": "TIMESTAMP LATENESS INTERVAL '10' SECONDS",
        "previous_interaction_date": "TIMESTAMP",
        "interaction_month": "TIMESTAMP",
    })
)

sql.register_local_view("interaction_type_groupby_video_id_count", """
    SELECT
        video_id,
        interaction_type,
        count(interaction_type) as interaction_type_count
    FROM
        interactions
    GROUP BY (interaction_type, video_id)
""")

sql.register_local_view("interaction_type_groupby_user_id_count", """
    SELECT
        user_id,
        interaction_type,
        count(interaction_type) as interaction_type_count
    FROM
        interactions
    GROUP BY (interaction_type, user_id)
""")

sql.register_local_view("video_id_with_interactions_type_pivot", """
    SELECT
        *
    FROM
        interaction_type_groupby_video_id_count
    PIVOT (
        SUM(interaction_type_count) FOR interaction_type IN (
            'like' as like_ct,
            'dislike' as dislike_ct,
            'view' as view_ct,
            'comment' as comment_ct,
            'share' as share_ct,
            'skip' as skip_ct
        )
    )
""")

sql.register_local_view("video_id_with_interactions_type_pivot", """
    SELECT
        *
    FROM
        interaction_type_groupby_video_id_count
    PIVOT (
        SUM(interaction_type_count) FOR interaction_type IN (
            'like' as like_ct,
            'dislike' as dislike_ct,
            'view' as view_ct,
            'comment' as comment_ct,
            'share' as share_ct,
            'skip' as skip_ct
        )
    )
""")

sql.register_local_view("user_id_with_interactions_type_pivot", """
    SELECT
        *
    FROM
        interaction_type_groupby_user_id_count
    PIVOT (
        SUM(interaction_type_count) FOR interaction_type IN (
            'like' as like_ct,
            'dislike' as dislike_ct,
            'view' as view_ct,
            'comment' as comment_ct,
            'share' as share_ct,
            'skip' as skip_ct
        )
    )
""")

sql.register_materialized_view("video_agg", """
    SELECT
        T1.video_id,
        count(*) OVER week as interaction_len,
        avg(watch_time) OVER week as average_watch_time,
        interaction_date as week_start,
        like_ct,
        dislike_ct,
        view_ct,
        comment_ct,
        share_ct,
        skip_ct
    FROM interactions T1 JOIN video_id_with_interactions_type_pivot T2
        ON T1.video_id = T2.video_id
    WINDOW
        week AS (PARTITION BY T1.video_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)
""")

sql.register_materialized_view("user_agg", """
    SELECT
        T1.user_id,
        count(*) OVER week as interaction_len,
        avg(watch_time) OVER week as average_watch_time,
        interaction_date as week_start,
        like_ct,
        dislike_ct,
        view_ct,
        comment_ct,
        share_ct,
        skip_ct
    FROM interactions T1 JOIN user_id_with_interactions_type_pivot T2
        ON T1.user_id = T2.user_id
    WINDOW
        week AS (PARTITION BY T1.user_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)
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
