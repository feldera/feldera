-- -------------------------------------------------------------------------------------------------
-- Query 5: Hot Items
-- -------------------------------------------------------------------------------------------------
-- Which auctions have seen the most bids in the last period?
-- Illustrates sliding windows and combiners.
--
-- The original Nexmark Query5 calculate the hot items in the last hour (updated every minute).
-- To make things a bit more dynamic and easier to test we use much shorter windows,
-- i.e. in the last 10 seconds and update every 2 seconds.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW q5 AS
SELECT AuctionBids.auction, AuctionBids.num
 FROM (
   SELECT
     B1.auction,
     count(*) AS num,
     window_start AS starttime,
     window_end AS endtime
   FROM TABLE(HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL 2 SECOND, INTERVAL 10 SECOND)) AS B1
   GROUP BY
     B1.auction,
     window_start,
     window_end
 ) AS AuctionBids
 JOIN (
   SELECT
     max(CountBids.num) AS maxn,
     CountBids.starttime,
     CountBids.endtime
   FROM (
     SELECT
       count(*) AS num,
       window_start AS starttime,
       window_end AS endtime
     FROM TABLE(HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL 2 SECOND, INTERVAL 10 SECOND)) AS B2
     GROUP BY
       B2.auction,
       window_start,
       window_end
     ) AS CountBids
   GROUP BY CountBids.starttime, CountBids.endtime
 ) AS MaxBids
 ON AuctionBids.starttime = MaxBids.starttime AND
    AuctionBids.endtime = MaxBids.endtime AND
    AuctionBids.num >= MaxBids.maxn;

