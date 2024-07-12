CREATE VIEW q7 AS
SELECT B.auction, B.price, B.bidder, B.date_time, B.extra
from bid B
JOIN (
  SELECT MAX(B1.price) AS maxprice, TUMBLE_START(B1.date_time, INTERVAL '10' SECOND) as date_time
  FROM bid B1
  GROUP BY TUMBLE(B1.date_time, INTERVAL '10' SECOND)
) B1
ON B.price = B1.maxprice
WHERE B.date_time BETWEEN B1.date_time  - INTERVAL '10' SECOND AND B1.date_time;