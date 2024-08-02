CREATE VIEW Q12 AS
SELECT
    B.bidder,
    count(*) as bid_count,
    TUMBLE_START(B.p_time, INTERVAL '10' SECOND) as starttime,
    TUMBLE_END(B.p_time, INTERVAL '10' SECOND) as endtime
FROM (SELECT *, NOW() as p_time FROM bid) B
GROUP BY B.bidder, TUMBLE(B.p_time, INTERVAL '10' SECOND);
