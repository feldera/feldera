CREATE FUNCTION COUNT_CHAR(S VARCHAR, C CHAR) RETURNS INT
AS LENGTH(S) - LENGTH(REPLACE(S, C, ''));

CREATE VIEW Q14 AS
SELECT
    auction,
    bidder,
    0.908 * price as price,
    CASE
        WHEN HOUR(date_time) >= 8 AND HOUR(date_time) <= 18 THEN 'dayTime'
        WHEN HOUR(date_time) <= 6 OR HOUR(date_time) >= 20 THEN 'nightTime'
        ELSE 'otherTime'
    END AS bidTimeType,
    date_time,
    extra,
    count_char(extra, 'c') AS c_counts
FROM bid
WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000;
