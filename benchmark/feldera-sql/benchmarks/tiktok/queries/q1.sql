CREATE VIEW q0 AS
SELECT
    interaction_id,
    count(*),
    avg(watch_time)
FROM TABLE(
    HOP(
        TABLE interactions,
        DESCRIPTOR(interaction_date),
        INTERVAL '90' MINUTES,
        INTERVAL '24' HOURS
    )
)
GROUP BY
interaction_id;

CREATE VIEW q1 AS
SELECT
    interaction_id,
    count(*),
    avg(watch_time)
FROM TABLE(
    TUMBLE(
        TABLE interactions,
        DESCRIPTOR(interaction_date),
        INTERVAL '90' MINUTES
    )
)
GROUP BY
interaction_id;

CREATE VIEW q2 AS
SELECT
    user_id,
    interaction_type,
    count(*) OVER day as interaction_len_d,
    avg(watch_time) OVER day as average_watch_time_d,
    interaction_date
FROM interactions
WINDOW
    day AS (PARTITION BY user_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW);
