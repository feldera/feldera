CREATE VIEW Q12 AS
SELECT
    B.bidder,
    count(*) as bid_count,
    TUMBLE_START(B.date_time, INTERVAL '10' SECOND) as starttime,
    TUMBLE_END(B.date_time, INTERVAL '10' SECOND) as endtime
FROM bid B
GROUP BY B.bidder, TUMBLE(B.date_time, INTERVAL '10' SECOND);
