CREATE VIEW q10 AS -- PARTITIONED BY (dt, hm) AS
SELECT auction, bidder, price, date_time, extra, FORMAT_DATE('yyyy-MM-dd', date_time), FORMAT_DATE('HH:mm', date_time)
FROM bid;