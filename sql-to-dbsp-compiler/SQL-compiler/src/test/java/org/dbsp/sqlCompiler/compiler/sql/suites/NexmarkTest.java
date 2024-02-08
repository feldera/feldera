/*
 * Copyright 2023 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.sql.suites;

import org.apache.calcite.config.Lex;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.BaseSQLTests;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Test SQL queries from the Nexmark suite.
 * https://github.com/nexmark/nexmark/tree/master/nexmark-flink/src/main/resources/queries
 */
@SuppressWarnings("JavadocLinkAsPlainText")
public class NexmarkTest extends BaseSQLTests {
    static final String table = """
            CREATE TABLE NEXMARK_TABLE (
            event_type int,
            "person.id"  BIGINT,
            "person.name"  VARCHAR,
            "person.emailAddress"  VARCHAR,
            "person.creditCard"  VARCHAR,
            "person.city"  VARCHAR,
            "person.state"  VARCHAR,
            "person.dateTime" TIMESTAMP(3),
            "person.extra"  VARCHAR,
            "auction.id"  BIGINT,
            "auction.itemName"  VARCHAR,
            "auction.description"  VARCHAR,
            "auction.initialBid"  BIGINT,
            "auction.reserve"  BIGINT,
            "auction.dateTime"  TIMESTAMP(3),
            "auction.expires"  TIMESTAMP(3),
            "auction.seller"  BIGINT,
            "auction.category"  BIGINT,
            "auction.extra"  VARCHAR,
            "bid.auction"  BIGINT,
            "bid.bidder"  BIGINT,
            "bid.price"  BIGINT,
            "bid.channel"  VARCHAR,
            "bid.url"  VARCHAR,
            "bid.dateTime"  TIMESTAMP(3),
            "bid.extra"  VARCHAR)
            """;

    static final String[] views = {
            """
CREATE VIEW person AS
SELECT
    "person.id" as id,
    "person.name" as name,
    "person.emailAddress" as emailAddress,
    "person.creditCard" as creditCard,
    "person.city" as city,
    "person.state" as state,
    "person.dateTime" AS "dateTime",
    "person.extra" as extra
FROM NEXMARK_TABLE WHERE event_type = 0""",
            """
CREATE VIEW auction AS
SELECT
    "auction.id" as id,
    "auction.itemName" as itemName,
    "auction.description" as description,
    "auction.initialBid" as initialBid,
    "auction.reserve" as reserve,
    "auction.dateTime" AS "dateTime",
    "auction.expires" AS expires,
    "auction.seller" as seller,
    "auction.category" as category,
    "auction.extra" as extra
FROM NEXMARK_TABLE WHERE event_type = 1""",
            """
CREATE VIEW bid AS
SELECT
    "bid.auction" AS auction,
    "bid.bidder" AS bidder,
    "bid.price" AS price,
    "bid.channel" AS channel,
    "bid.url" AS url,
    "bid.dateTime" AS "dateTime",
    "bid.extra" as extra
FROM NEXMARK_TABLE WHERE event_type = 2"""
    };

    static final String[] queries = {
            """
-- -------------------------------------------------------------------------------------------------
-- Query 0: Pass through (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- This measures the monitoring overhead of the Flink SQL implementation including the source generator.
-- Using `bid` events here, as they are most numerous with default configuration.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW q0 AS SELECT auction, bidder, price, "dateTime", extra FROM bid""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query1: Currency conversion
-- -------------------------------------------------------------------------------------------------
-- Convert each bid value from dollars to euros. Illustrates a simple transformation.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW q1 AS
SELECT
    auction,
    bidder,
    0.908 * price as price, -- convert dollar to euro
    "dateTime",
    extra
FROM bid""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query2: Selection
-- -------------------------------------------------------------------------------------------------
-- Find bids with specific auction ids and show their bid price.
--
-- In original Nexmark queries, Query2 is as following (in CQL syntax):
--
--   SELECT Rstream(auction, price)
--   FROM Bid [NOW]
--   WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
--
-- However, that query will only yield a few hundred results over event streams of arbitrary size.
-- To make it more interesting we instead choose bids for every 123'th auction.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW q2 AS SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0
""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 3: Local Item Suggestion
-- -------------------------------------------------------------------------------------------------
-- Who is selling in OR, ID or CA in category 10, and for what auction ids?
-- Illustrates an incremental join (using per-key state and timer) and filter.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW q3 AS SELECT
    P.name, P.city, P.state, A.id
FROM
    auction AS A INNER JOIN person AS P on A.seller = P.id
WHERE
    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA')""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 4: Average Price for a Category
-- -------------------------------------------------------------------------------------------------
-- Select the average of the wining bid prices for all auctions in each category.
-- Illustrates complex join and aggregation.
-- -------------------------------------------------------------------------------------------------
CREATE VIEW q4 AS
SELECT
    Q.category,
    AVG(Q.final)
FROM (
    SELECT MAX(B.price) AS final, A.category
    FROM auction A, bid B
    WHERE A.id = B.auction AND B."dateTime" BETWEEN A."dateTime" AND A.expires
    GROUP BY A.id, A.category
) Q
GROUP BY Q.category""",

            """
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
     HOP_START(B1."dateTime", INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
     HOP_END(B1."dateTime", INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
   FROM bid B1
   GROUP BY
     B1.auction,
     HOP(B1."dateTime", INTERVAL '2' SECOND, INTERVAL '10' SECOND)
 ) AS AuctionBids
 JOIN (
   SELECT
     max(CountBids.num) AS maxn,
     CountBids.starttime,
     CountBids.endtime
   FROM (
     SELECT
       count(*) AS num,
       HOP_START(B2."dateTime", INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
       HOP_END(B2."dateTime", INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
     FROM bid B2
     GROUP BY
       B2.auction,
       HOP(B2."dateTime", INTERVAL '2' SECOND, INTERVAL '10' SECOND)
     ) AS CountBids
   GROUP BY CountBids.starttime, CountBids.endtime
 ) AS MaxBids
 ON AuctionBids.starttime = MaxBids.starttime AND
    AuctionBids.endtime = MaxBids.endtime AND
    AuctionBids.num >= MaxBids.maxn""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 6: Average Selling Price by Seller
-- -------------------------------------------------------------------------------------------------
-- What is the average selling price per seller for their last 10 closed auctions.
-- Shares the same ‘winning bids’ core as for Query4, and illustrates a specialized combiner.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q6 AS
SELECT
    Q.seller,
    AVG(Q.final) OVER
        (PARTITION BY Q.seller ORDER BY Q."dateTime" ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
FROM (
    SELECT MAX(B.price) AS final, A.seller, B."dateTime"
    FROM auction AS A, bid AS B
    WHERE A.id = B.auction and B."dateTime" between A."dateTime" and A.expires
    GROUP BY A.id, A.seller
) AS Q""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 7: Highest Bid
-- -------------------------------------------------------------------------------------------------
-- What are the highest bids per period?
-- Deliberately implemented using a side input to illustrate fanout.
--
-- The original Nexmark Query7 calculate the highest bids in the last minute.
-- We will use a shorter window (10 seconds) to help make testing easier.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q7 AS
SELECT B.auction, B.price, B.bidder, B."dateTime", B.extra
from bid B
JOIN (
  SELECT MAX(B1.price) AS maxprice, TUMBLE_ROWTIME(B1."dateTime", INTERVAL '10' SECOND) as "dateTime"
  FROM bid B1
  GROUP BY TUMBLE(B1."dateTime", INTERVAL '10' SECOND)
) B1
ON B.price = B1.maxprice
WHERE B."dateTime" BETWEEN B1."dateTime"  - INTERVAL '10' SECOND AND B1."dateTime"
""",
            """
-- -------------------------------------------------------------------------------------------------
-- Query 8: Monitor New Users
-- -------------------------------------------------------------------------------------------------
-- Select people who have entered the system and created auctions in the last period.
-- Illustrates a simple join.
--
-- The original Nexmark Query8 monitors the new users the last 12 hours, updated every 12 hours.
-- To make things a bit more dynamic and easier to test we use much shorter windows (10 seconds).
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q8 AS
SELECT P.id, P.name, P.starttime
FROM (
  SELECT P.id, P.name,
         TUMBLE_START(P."dateTime", INTERVAL '10' SECOND) AS starttime,
         TUMBLE_END(P."dateTime", INTERVAL '10' SECOND) AS endtime
  FROM person P
  GROUP BY P.id, P.name, TUMBLE(P."dateTime", INTERVAL '10' SECOND)
) P
JOIN (
  SELECT A.seller,
         TUMBLE_START(A."dateTime", INTERVAL '10' SECOND) AS starttime,
         TUMBLE_END(A."dateTime", INTERVAL '10' SECOND) AS endtime
  FROM auction A
  GROUP BY A.seller, TUMBLE(A."dateTime", INTERVAL '10' SECOND)
) A
ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 9: Winning Bids (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Find the winning bid for each auction.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q9 AS
SELECT
    id, itemName, description, initialBid, reserve, "dateTime", expires, seller, category, extra,
    auction, bidder, price, bid_dateTime, bid_extra
FROM (
   SELECT A.*, B.auction, B.bidder, B.price, B."dateTime" AS bid_dateTime, B.extra AS bid_extra,
     ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B."dateTime" ASC) AS rownum
   FROM auction A, bid B
   WHERE A.id = B.auction AND B."dateTime" BETWEEN A."dateTime" AND A.expires
)
WHERE rownum <= 1""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 10: Log to File System (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Log all events to file system. Illustrates windows streaming data into partitioned file system.
--
-- Every minute, save all events from the last period into partitioned log files.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q10 AS -- PARTITIONED BY (dt, hm) AS
SELECT auction, bidder, price, "dateTime", extra, FORMAT_DATE('yyyy-MM-dd', "dateTime"), FORMAT_DATE('HH:mm', "dateTime")
FROM bid""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 11: User Sessions (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids did a user make in each session they were active? Illustrates session windows.
--
-- Group bids by the same user into sessions with max session gap.
-- Emit the number of bids per session.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q11 AS
SELECT
    B.bidder,
    count(*) as bid_count,
    SESSION_START(B."dateTime", INTERVAL '10' SECOND) as starttime,
    SESSION_END(B."dateTime", INTERVAL '10' SECOND) as endtime
FROM bid B
GROUP BY B.bidder, SESSION(B."dateTime", INTERVAL '10' SECOND)""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 12: Processing Time Windows (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids does a user make within a fixed processing time limit?
-- Illustrates working in processing time window.
--
-- Group bids by the same user into processing time windows of 10 seconds.
-- Emit the count of bids per window.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q12 AS
SELECT
    B.bidder,
    count(*) as bid_count,
    TUMBLE_START(B.p_time, INTERVAL '10' SECOND) as starttime,
    TUMBLE_END(B.p_time, INTERVAL '10' SECOND) as endtime
FROM (SELECT *, PROCTIME() as p_time FROM bid) B
GROUP BY B.bidder, TUMBLE(B.p_time, INTERVAL '10' SECOND)""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 13: Bounded Side Input Join (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Joins a stream to a bounded side input, modeling basic stream enrichment.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q13 AS
SELECT
    B.auction,
    B.bidder,
    B.price,
    B."dateTime",
    S.value
FROM (SELECT *, PROCTIME() as p_time FROM bid) B
JOIN side_input FOR SYSTEM_TIME AS OF B.p_time AS S
ON mod(B.auction, 10000) = S.key""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 14: Calculation (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Convert bid timestamp into types and find bids with specific price.
-- Illustrates duplicate expressions and usage of user-defined-functions.
-- -------------------------------------------------------------------------------------------------

-- CREATE FUNCTION count_char AS 'com.github.nexmark.flink.udf.CountChar';

CREATE VIEW Q14 AS
SELECT
    auction,
    bidder,
    0.908 * price as price,
    CASE
        WHEN HOUR("dateTime") >= 8 AND HOUR("dateTime") <= 18 THEN 'dayTime'
        WHEN HOUR("dateTime") <= 6 OR HOUR("dateTime") >= 20 THEN 'nightTime'
        ELSE 'otherTime'
    END AS bidTimeType,
    "dateTime",
    extra,
    count_char(extra, 'c') AS c_counts
FROM bid
WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 15: Bidding Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many distinct users join the bidding for different level of price?
-- Illustrates multiple distinct aggregations with filters.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q15 AS
SELECT
     FORMAT_DATE('yyyy-MM-dd', "dateTime") as 'day',
     count(*) AS total_bids,
     count(*) filter (where price < 10000) AS rank1_bids,
     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
     count(*) filter (where price >= 1000000) AS rank3_bids,
     count(distinct bidder) AS total_bidders,
     count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
     count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
     count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
     count(distinct auction) AS total_auctions,
     count(distinct auction) filter (where price < 10000) AS rank1_auctions,
     count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
     count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
FROM bid
GROUP BY FORMAT_DATE('yyyy-MM-dd', "dateTime")""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 16: Channel Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many distinct users join the bidding for different level of price for a channel?
-- Illustrates multiple distinct aggregations with filters for multiple keys.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q16 AS
SELECT
    channel,
    format_date('yyyy-MM-dd', "dateTime") as 'day',
    max(format_date('HH:mm', "dateTime")) as 'minute',
    count(*) AS total_bids,
    count(*) filter (where price < 10000) AS rank1_bids,
    count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
    count(*) filter (where price >= 1000000) AS rank3_bids,
    count(distinct bidder) AS total_bidders,
    count(distinct bidder) filter (where price < 10000) AS rank1_bidders,
    count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,
    count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,
    count(distinct auction) AS total_auctions,
    count(distinct auction) filter (where price < 10000) AS rank1_auctions,
    count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
    count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
FROM bid
GROUP BY channel, format_date('yyyy-MM-dd', "dateTime")""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 17: Auction Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids on an auction made a day and what is the price?
-- Illustrates an unbounded group aggregation.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q17 AS
SELECT
     auction,
     format_date('yyyy-MM-dd', "dateTime") as 'day',
     count(*) AS total_bids,
     count(*) filter (where price < 10000) AS rank1_bids,
     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
     count(*) filter (where price >= 1000000) AS rank3_bids,
     min(price) AS min_price,
     max(price) AS max_price,
     avg(price) AS avg_price,
     sum(price) AS sum_price
FROM bid
GROUP BY auction, format_date('yyyy-MM-dd', "dateTime")""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 18: Find last bid (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What's a's last bid for bidder to auction?
-- Illustrates a Deduplicate query.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q18 AS
SELECT auction, bidder, price, channel, url, "dateTime", extra
 FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY "dateTime" DESC) AS rank_number
       FROM bid)
 WHERE rank_number <= 1""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 19: Auction TOP-10 Price (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What's the top price 10 bids of an auction?
-- Illustrates a TOP-N query.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q19 AS
SELECT * FROM
(SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid)
WHERE rank_number <= 10""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 20: Expand bid with auction (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Get bids with the corresponding auction information where category is 10.
-- Illustrates a filter join.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q20 AS
SELECT
    auction, bidder, price, channel, url, B."dateTime", B.extra,
    itemName, description, initialBid, reserve, A."dateTime" as AdateTime, expires, seller, category, A.extra as Aextra
FROM
    bid AS B INNER JOIN auction AS A on B.auction = A.id
WHERE A.category = 10""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 21: Add channel id (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Add a channel_id column to the bid table.
-- Illustrates a 'CASE WHEN' + 'REGEXP_EXTRACT' SQL.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q21 AS
SELECT
    auction, bidder, price, channel,
    CASE
        WHEN lower(channel) = 'apple' THEN '0'
        WHEN lower(channel) = 'google' THEN '1'
        WHEN lower(channel) = 'facebook' THEN '2'
        WHEN lower(channel) = 'baidu' THEN '3'
        ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2)
        END
    AS channel_id FROM bid
    where REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) is not null or
          lower(channel) in ('apple', 'google', 'facebook', 'baidu')""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 22: Get URL Directories (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What is the directory structure of the URL?
-- Illustrates a SPLIT_INDEX SQL.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q22 AS
SELECT
    auction, bidder, price, channel,
    SPLIT_INDEX(url, '/', 3) as dir1,
    SPLIT_INDEX(url, '/', 4) as dir2,
    SPLIT_INDEX(url, '/', 5) as dir3 FROM bid"""
    };

    @Test
    public void testCompile() {
        CompilerOptions options = new CompilerOptions();
        options.languageOptions.lexicalRules = Lex.ORACLE;
        options.languageOptions.throwOnError = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.compileStatement(table);
        for (String view: views)
            compiler.compileStatement(view);

        Set<Integer> unsupported = new HashSet<>() {{
            add(5); // hop
            add(6); // group-by
            add(7); // tumble
            add(8); // tumble
            add(11); // session
            add(12); // proctime
            add(13); // proctime
            add(14); // count_char
            add(15); // error in Hep planner
            add(16); // error in Hep planner
            add(21); // regexp_extract
            add(22); // split_index
        }};

        int index = 0;
        for (String query: queries) {
            if (!unsupported.contains(index)) {
                System.out.println("Q" + index);
                compiler.compileStatement(query);
            }
            index++;
        }

        Assert.assertFalse(compiler.hasErrors());
        this.addRustTestCase("NexmarkTest", compiler, getCircuit(compiler));
    }
}
