CREATE VIEW window_011 AS
SELECT
    content,
    id,
    v,
    lead(v, 0) IGNORE NULLS OVER w lead_0,
    lead(v, 1) IGNORE NULLS OVER w lead_1,
    lead(v, 2) IGNORE NULLS OVER w lead_2,
    lead(v, 3) IGNORE NULLS OVER w lead_3,
    lag(v, 0) IGNORE NULLS OVER w lag_0,
    lag(v, 1) IGNORE NULLS OVER w lag_1,
    lag(v, 2) IGNORE NULLS OVER w lag_2,
    lag(v, 3) IGNORE NULLS OVER w lag_3,
    lag(v, +3) IGNORE NULLS OVER w lag_plus_3,
    nth_value(v, 1) IGNORE NULLS OVER w nth_value_1,
    nth_value(v, 2) IGNORE NULLS OVER w nth_value_2,
    nth_value(v, 3) IGNORE NULLS OVER w nth_value_3,
    first_value(v) IGNORE NULLS OVER w first_value,
    any_value(v) IGNORE NULLS OVER w any_value,
    last_value(v) IGNORE NULLS OVER w last_value
FROM
    test_ignore_null
WINDOW w AS (ORDER BY id)
ORDER BY id;
