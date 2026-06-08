CREATE VIEW outer-join_002 AS
SELECT *
FROM (
SELECT
    COALESCE(t2.int_col1, t1.int_col1) AS int_col
    FROM t1
    LEFT JOIN t2 ON false
) t where (t.int_col) is not null;
