from datetime import datetime
from feldera.formats import JSONFormat, JSONUpdateFormat
from sql_program import generate_program
from feldera import SQLContext, FelderaClient
import pandas as pd
from typing import Dict, Any, List


def process_input(sql: SQLContext, data: List[Dict[str, Any]]):
    sql.input_pandas("interactions", pd.DataFrame(data))
    sql.wait_for_completion(shutdown=False)


client = FelderaClient("http://localhost:8080")
sql = SQLContext("tiktok_test", client, workers=10, storage=False)

code = generate_program(None, None)
sql.sql(code)

# sql.foreach_chunk("user_agg", lambda df, chunk : print(df))

print("Starting Feldera Pipeline")
sql.start()
print("Pipeline started")

hvideo_agg = sql.listen("video_agg")
huser_agg = sql.listen("user_agg")

process_input(
    sql,
    [
        {
            "interaction_id": 1,
            "user_id": 1,
            "video_id": 1,
            "category_id": 100,
            "interaction_type": "like",
            "watch_time": 10,
            "interaction_date": datetime.strptime(
                "2024-07-11 01:00:00", "%Y-%m-%d %H:%M:%S"
            ),
            "previous_interaction_date": datetime.strptime(
                "2024-07-11 00:00:00", "%Y-%m-%d %H:%M:%S"
            ),
            "interaction_month": datetime.strptime("2024-07-01", "%Y-%m-%d"),
        },
        {
            "interaction_id": 2,
            "user_id": 1,
            "video_id": 2,
            "category_id": 100,
            "interaction_type": "like",
            "watch_time": 20,
            "interaction_date": datetime.strptime(
                "2024-07-11 01:00:00", "%Y-%m-%d %H:%M:%S"
            ),
            "previous_interaction_date": datetime.strptime(
                "2024-07-11 00:00:00", "%Y-%m-%d %H:%M:%S"
            ),
            "interaction_month": datetime.strptime("2024-07-01", "%Y-%m-%d"),
        },
    ],
)

process_input(
    sql,
    [
        {
            "interaction_id": 3,
            "user_id": 1,
            "video_id": 1,
            "category_id": 100,
            "interaction_type": "like",
            "watch_time": 10,
            "interaction_date": datetime.strptime(
                "2024-07-11 01:00:10", "%Y-%m-%d %H:%M:%S"
            ),
            "previous_interaction_date": datetime.strptime(
                "2024-07-11 00:00:00", "%Y-%m-%d %H:%M:%S"
            ),
            "interaction_month": datetime.strptime("2024-07-01", "%Y-%m-%d"),
        },
        {
            "interaction_id": 4,
            "user_id": 1,
            "video_id": 2,
            "category_id": 100,
            "interaction_type": "like",
            "watch_time": 20,
            "interaction_date": datetime.strptime(
                "2024-07-11 01:00:20", "%Y-%m-%d %H:%M:%S"
            ),
            "previous_interaction_date": datetime.strptime(
                "2024-07-11 00:00:00", "%Y-%m-%d %H:%M:%S"
            ),
            "interaction_month": datetime.strptime("2024-07-01", "%Y-%m-%d"),
        },
    ],
)

process_input(
    sql,
    [
        {
            "interaction_id": 5,
            "user_id": 1,
            "video_id": 1,
            "category_id": 100,
            "interaction_type": "like",
            "watch_time": 70,
            "interaction_date": datetime.strptime(
                "2024-07-11 03:00:10", "%Y-%m-%d %H:%M:%S"
            ),
            "previous_interaction_date": datetime.strptime(
                "2024-07-11 00:00:00", "%Y-%m-%d %H:%M:%S"
            ),
            "interaction_month": datetime.strptime("2024-07-01", "%Y-%m-%d"),
        },
        {
            "interaction_id": 6,
            "user_id": 1,
            "video_id": 2,
            "category_id": 100,
            "interaction_type": "like",
            "watch_time": 70,
            "interaction_date": datetime.strptime(
                "2024-07-11 03:00:20", "%Y-%m-%d %H:%M:%S"
            ),
            "previous_interaction_date": datetime.strptime(
                "2024-07-11 00:00:00", "%Y-%m-%d %H:%M:%S"
            ),
            "interaction_month": datetime.strptime("2024-07-01", "%Y-%m-%d"),
        },
    ],
)

print("video_agg")
print(hvideo_agg.to_pandas())

print("user_agg")
print(huser_agg.to_pandas())

print("Success")
sql.shutdown()
