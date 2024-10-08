CREATE TABLE interactions (
    interaction_id BIGINT,
    user_id INT,
    video_id INT,
    category_id INT,
    interaction_type STRING,
    watch_time INT,
    interaction_date TIMESTAMP LATENESS INTERVAL 15 MINUTES,
    interaction_month TIMESTAMP
) WITH ('connectors' = '{interactions}');
