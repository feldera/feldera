CREATE VIEW window_016 AS
SELECT
    content,
    id,
    v,
    nth_value(v, 1) IGNORE NULLS OVER w nth_value_1,
    nth_value(v, 2) IGNORE NULLS OVER w nth_value_2,
    nth_value(v, 3) IGNORE NULLS OVER w nth_value_3,
    first_value(v) IGNORE NULLS OVER w first_value,
    any_value(v) IGNORE NULLS OVER w any_value,
    last_value(v) IGNORE NULLS OVER w last_value
FROM
    test_ignore_null
WINDOW w AS (ORDER BY id RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
ORDER BY id;
