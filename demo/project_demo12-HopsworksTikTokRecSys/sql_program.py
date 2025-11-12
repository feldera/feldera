import json


def generate_program(transport_cfg, format_cfg):
    code = """
        CREATE TABLE interactions (
            interaction_id BIGINT,
            user_id INT,
            video_id INT,
            category_id INT,
            interaction_type STRING,
            watch_time INT,
            interaction_date TIMESTAMP LATENESS INTERVAL 15 MINUTES,
            interaction_month TIMESTAMP
        )"""

    if transport_cfg is not None and format_cfg is not None:
        code += """ with (
            'connectors' = '[{{
                "name": "connector",
                "transport": {0},
                "format": {1}
            }}]'
        );""".format(json.dumps(transport_cfg), json.dumps(format_cfg))
    else:
        code += ";"

    code += """
    CREATE VIEW video_agg AS (SELECT
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
        week AS (PARTITION BY video_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW));

    CREATE VIEW user_agg AS (SELECT
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
        week AS (PARTITION BY user_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW));
    """
    return code


if __name__ == "__main__":
    print(
        generate_program(
            {
                "name": "kafka_input",
                "config": {
                    "topics": ["blah"],
                    "bootstrap.servers": "blarg",
                    "auto.offset.reset": "earliest",
                    "poller_threads": 12,
                },
            },
            {
                "name": "json",
                "config": {
                    "update_format": "raw",
                    "array": False,
                },
            },
        )
    )
