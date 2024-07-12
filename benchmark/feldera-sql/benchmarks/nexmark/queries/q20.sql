CREATE VIEW q20 AS
SELECT
    auction, bidder, price, channel, url, B.date_time, B.extra,
    itemName, description, initialBid, reserve, A.date_time as AdateTime, expires, seller, category, A.extra as Aextra
FROM
    bid AS B INNER JOIN auction AS A on B.auction = A.id
WHERE A.category = 10;