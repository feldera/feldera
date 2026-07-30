CREATE VIEW q10 AS -- PARTITIONED BY (dt, hm) AS
SELECT auction, bidder, price, date_time, extra, FORMAT_TIMESTAMP('%Y-%m-%d', date_time), FORMAT_TIMESTAMP('%H:%M', date_time)
FROM bid;