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

package org.dbsp.sqlCompiler.compiler.sql.suites.nexmark;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/* Test SQL queries from the Nexmark suite.
 * https://github.com/nexmark/nexmark/tree/master/nexmark-flink/src/main/resources/queries */
public class NexmarkTest extends StreamingTestBase {
    static final String tables = """
CREATE TABLE person (
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR,
    emailAddress VARCHAR,
    creditCard VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time TIMESTAMP(3) NOT NULL LATENESS INTERVAL 4 SECONDS,
    extra  VARCHAR
);
CREATE TABLE auction (
    id  BIGINT NOT NULL PRIMARY KEY,
    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    date_time  TIMESTAMP(3) NOT NULL LATENESS INTERVAL 4 SECONDS,
    expires  TIMESTAMP(3),
    seller  BIGINT FOREIGN KEY REFERENCES person(id),
    category  BIGINT,
    extra  VARCHAR
);
CREATE TABLE bid (
    auction  BIGINT FOREIGN KEY REFERENCES auction(id),
    bidder  BIGINT NOT NULL PRIMARY KEY,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    date_time TIMESTAMP(3) NOT NULL LATENESS INTERVAL 4 SECONDS,
    extra  VARCHAR
);
CREATE TABLE side_input (
  date_time TIMESTAMP,
  key BIGINT,
  value VARCHAR
);""";

    static final String[] queries = {
            """
-- -------------------------------------------------------------------------------------------------
-- Query 0: Pass through (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- This measures the monitoring overhead of the Flink SQL implementation including the source generator.
-- Using `bid` events here, as they are most numerous with default configuration.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW q0 AS SELECT auction, bidder, price, date_time, extra FROM bid""",

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
    date_time,
    extra
FROM bid;""",

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

CREATE VIEW q2 AS SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0;
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
    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA');""",

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
    WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires
    GROUP BY A.id, A.category
) Q
GROUP BY Q.category;""",

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
    AuctionBids.num >= MaxBids.maxn;""",

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
        (PARTITION BY Q.seller ORDER BY Q.date_time ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
FROM (
    SELECT MAX(B.price) AS final, A.seller, ARG_MAX(B.price, B.date_time) as date_time
    FROM auction AS A, bid AS B
    WHERE A.id = B.auction and B.date_time between A.date_time and A.expires
    GROUP BY A.id, A.seller
) AS Q;""",

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
SELECT B.auction, B.price, B.bidder, B.date_time, B.extra
from bid B
JOIN (
  SELECT MAX(B1.price) AS maxprice, TUMBLE_START(B1.date_time, INTERVAL '10' SECOND) as date_time
  FROM bid B1
  GROUP BY TUMBLE(B1.date_time, INTERVAL '10' SECOND)
) B1
ON B.price = B1.maxprice
WHERE B.date_time BETWEEN B1.date_time  - INTERVAL '10' SECOND AND B1.date_time;
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
         TUMBLE_START(P.date_time, INTERVAL '10' SECOND) AS starttime,
         TUMBLE_END(P.date_time, INTERVAL '10' SECOND) AS endtime
  FROM person P
  GROUP BY P.id, P.name, TUMBLE(P.date_time, INTERVAL '10' SECOND)
) P
JOIN (
  SELECT A.seller,
         TUMBLE_START(A.date_time, INTERVAL '10' SECOND) AS starttime,
         TUMBLE_END(A.date_time, INTERVAL '10' SECOND) AS endtime
  FROM auction A
  GROUP BY A.seller, TUMBLE(A.date_time, INTERVAL '10' SECOND)
) A
ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 9: Winning Bids (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Find the winning bid for each auction.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q9 AS
SELECT
    id, itemName, description, initialBid, reserve, date_time, expires, seller, category, extra,
    auction, bidder, price, bid_dateTime, bid_extra
FROM (
   SELECT A.*, B.auction, B.bidder, B.price, B.date_time AS bid_dateTime, B.extra AS bid_extra,
     ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.date_time ASC) AS rownum
   FROM auction A, bid B
   WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires
)
WHERE rownum <= 1;""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 10: Log to File System (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Log all events to file system. Illustrates windows streaming data into partitioned file system.
--
-- Every minute, save all events from the last period into partitioned log files.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q10 AS -- PARTITIONED BY (dt, hm) AS
SELECT auction, bidder, price, date_time, extra, FORMAT_DATE('yyyy-MM-dd', date_time), FORMAT_DATE('HH:mm', date_time)
FROM bid;""",

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
    SESSION_START(B.date_time, INTERVAL '10' SECOND) as starttime,
    SESSION_END(B.date_time, INTERVAL '10' SECOND) as endtime
FROM bid B
GROUP BY B.bidder, SESSION(B.date_time, INTERVAL '10' SECOND);""",

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
    -- original query used B.proctime, but it's not clear why
    TUMBLE_START(B.date_time, INTERVAL '10' SECOND) as starttime,
    TUMBLE_END(B.date_time, INTERVAL '10' SECOND) as endtime
FROM bid B
GROUP BY B.bidder, TUMBLE(B.date_time, INTERVAL '10' SECOND);""",
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
    B.date_time,
    S.value
FROM (SELECT *, date_time as p_time, mod(auction, 10000) as mod FROM bid) B
LEFT ASOF JOIN side_input AS S
MATCH_CONDITION B.p_time >= S.date_time
ON B.mod = S.key;""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 14: Calculation (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Convert bid timestamp into types and find bids with specific price.
-- Illustrates duplicate expressions and usage of user-defined-functions.
-- -------------------------------------------------------------------------------------------------

-- CREATE FUNCTION count_char AS 'com.github.nexmark.flink.udf.CountChar';

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
WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000;""",
            """
-- -------------------------------------------------------------------------------------------------
-- Query 15: Bidding Statistics Report (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many distinct users join the bidding for different level of price?
-- Illustrates multiple distinct aggregations with filters.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q15 AS
SELECT
     CAST(date_time AS DATE) as 'day',
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
GROUP BY CAST(date_time AS DATE);""",

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
    CAST(date_time AS DATE) as 'day',
    format_date('HH:mm', max(date_time)) as 'minute',
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
GROUP BY channel, CAST(date_time AS date);""",

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
     CAST(date_time AS DATE) as 'day',
     count(*) AS total_bids,
     count(*) filter (where price < 10000) AS rank1_bids,
     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
     count(*) filter (where price >= 1000000) AS rank3_bids,
     min(price) AS min_price,
     max(price) AS max_price,
     avg(price) AS avg_price,
     sum(price) AS sum_price
FROM bid
GROUP BY auction, CAST(date_time AS DATE);""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 18: Find last bid (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What's a's last bid for bidder to auction?
-- Illustrates a Deduplicate query.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q18 AS
SELECT auction, bidder, price, channel, url, date_time, extra
 FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY date_time DESC) AS rank_number
       FROM bid)
 WHERE rank_number <= 1;""",

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
WHERE rank_number <= 10;""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 20: Expand bid with auction (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Get bids with the corresponding auction information where category is 10.
-- Illustrates a filter join.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q20 AS
SELECT
    auction, bidder, price, channel, url, B.date_time, B.extra,
    itemName, description, initialBid, reserve, A.date_time as AdateTime, expires, seller, category, A.extra as Aextra
FROM
    bid AS B INNER JOIN auction AS A on B.auction = A.id
WHERE A.category = 10;""",

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
          lower(channel) in ('apple', 'google', 'facebook', 'baidu');""",

            """
-- -------------------------------------------------------------------------------------------------
-- Query 22: Get URL Directories (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What is the directory structure of the URL?
-- Illustrates a SPLIT_INDEX SQL.
-- -------------------------------------------------------------------------------------------------

CREATE FUNCTION SPLIT_INDEX(s VARCHAR, sep CHAR, ix INT) RETURNS VARCHAR
AS SPLIT(s, CAST(sep AS VARCHAR))[ix + 1];

CREATE VIEW Q22 AS
SELECT
    auction, bidder, price, channel,
    SPLIT_INDEX(url, '/', 3) as dir1,
    SPLIT_INDEX(url, '/', 4) as dir2,
    SPLIT_INDEX(url, '/', 5) as dir3 FROM bid;"""
    };

    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation(tables);
    }

    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = new CompilerOptions();
        options.languageOptions.streaming = true;
        options.languageOptions.throwOnError = true;
        options.languageOptions.incrementalize = true;
        options.languageOptions.generateInputForEveryTable = true;
        options.ioOptions.emitHandles = true;
        options.ioOptions.quiet = true;
        return options;
    }

    CompilerCircuitStream createTest(int query, String... scriptsAndTables) {
        Assert.assertEquals(0, scriptsAndTables.length % 2);
        DBSPCompiler compiler = this.testCompiler();
        this.prepareInputs(compiler);
        compiler.submitStatementsForCompilation(queries[query]);
        final boolean debug = false;
        Class<?> module = DBSPCompiler.class;
        int previous;
        //noinspection ConstantValue
        if (debug)
            previous = Logger.INSTANCE.setLoggingLevel(module, 1);
        CompilerCircuitStream ccs = this.getCCS(compiler);
        //noinspection ConstantValue
        if (debug)
            Logger.INSTANCE.setLoggingLevel(module, previous);
        for (int i = 0; i < scriptsAndTables.length; i += 2)
            ccs.step(scriptsAndTables[i], scriptsAndTables[i + 1]);
        return ccs;
    }

    @Test
    public void q0Test() {
        this.createTest(0,
                """
                INSERT INTO Auction VALUES(1, 'item-name', 'description', 5, 10, '2020-01-01 00:00:01', '2020-01-02 00:00:00', 99, 1, '');
                INSERT INTO Bid VALUES(1, 1, 80, 'my-channel', 'https://example.com', '2020-01-01 00:00:01', '');
                INSERT INTO Bid VALUES(1, 1, 100, 'my-channel', 'https://example.com', '2020-01-01 00:00:02', '');""",
                """
                auction | bidder | price | date_time           | extra | weight
                ----------------------------------------------------------------
                 1      | 1      | 80    | 2020-01-01 00:00:01 | | 1
                 1      | 1      | 100   | 2020-01-01 00:00:02 | | 1""",
                """
INSERT INTO Auction VALUES(2, 'item-name', 'description', 5, 10, '2020-01-01 01:00:00', '2020-01-02 00:00:01', 99, 1, '');
INSERT INTO Bid VALUES(2, 1, 80, 'my-channel', 'https://example.com', '2020-01-01 00:00:01', '');
INSERT INTO Bid VALUES(2, 1, 100, 'my-channel', 'https://example.com', '2020-01-01 00:00:02', '');""",
                """
                auction | bidder | price | date_time           | extra | weight
                ----------------------------------------------------------------
                 2      | 1      | 80    | 2020-01-01 00:00:01 | | 1
                 2      | 1      | 100   | 2020-01-01 00:00:02 | | 1""");
    }

    @Test
    public void q1Test() {
        this.createTest(1,
"""
INSERT INTO Auction VALUES(1, 'item-name', 'description', 5, 10, '2020-01-01 00:00:01', '2020-01-01 00:10:00', 99, 1, '');
INSERT INTO Bid VALUES(1, 1, 80, 'my-channel', 'https://example.com', '2020-01-01 00:00:01', '');
INSERT INTO Bid VALUES(1, 1, 100, 'my-channel', 'https://example.com', '2020-01-01 00:00:02', '');""",
                """
                auction | bidder | price | date_time           | extra | weight
                ----------------------------------------------------------------
                 1      | 1      | 72.64  | 2020-01-01 00:00:01 | | 1
                 1      | 1      | 90.8   | 2020-01-01 00:00:02 | | 1""",
                """
INSERT INTO Auction VALUES(2, 'item-name', 'description', 5, 10, '2020-01-01 00:00:01', '2020-01-01 00:10:00', 99, 1, '');
INSERT INTO Bid VALUES(2, 1, 80, 'my-channel', 'https://example.com', '2020-01-01 00:00:01', '');
INSERT INTO Bid VALUES(2, 1, 100, 'my-channel', 'https://example.com', '2020-01-01 00:00:02', '');""",
                """
                auction | bidder | price | date_time           | extra | weight
                ----------------------------------------------------------------
                 2      | 1      | 72.64  | 2020-01-01 00:00:01 | | 1
                 2      | 1      | 90.8   | 2020-01-01 00:00:02 | | 1""");
    }

    @Test
    public void q2Test() {
        this.createTest(2,
                """
                INSERT INTO Bid VALUES(1, 1, 80, 'my-channel', 'https://example.com', '2020-01-01 00:00:01', '');
                INSERT INTO Bid VALUES(123, 1, 111, 'my-channel', 'https://example.com', '2020-01-01 00:00:02', '');
                INSERT INTO Bid VALUES(124, 1, 100, 'my-channel', 'https://example.com', '2020-01-01 00:00:02', '');""",
                """
                auction | price | weight
                -------------------------------
                 123    | 111   | 1""",
                """
                INSERT INTO Bid VALUES(271, 1, 80, 'my-channel', 'https://example.com', '2020-01-01 00:00:01', '');
                INSERT INTO Bid VALUES(492, 1, 222, 'my-channel', 'https://example.com', '2020-01-01 00:00:02', '');""",
                """
                auction | price | weight
                -----------------------------
                 492    | 222   | 1""");
    }

    @Test
    public void q3Test() {
        this.createTest(3,
                """
INSERT INTO Person VALUES(1, 'NL Seller', 'AAABBB@example.com', '1111 2222 3333 4444', 'Phoenix', 'NL', '2020-01-01 00:00:00', '');
INSERT INTO Person VALUES(2, 'CA Seller', 'AAABBB@example.com', '1111 2222 3333 4444', 'Phoenix', 'CA', '2020-01-01 00:00:00', '');
INSERT INTO Person VALUES(3, 'ID Seller', 'AAABBB@example.com', '1111 2222 3333 4444', 'Phoenix', 'ID', '2020-01-01 00:00:00', '');
INSERT INTO Auction VALUES(999, 'item-name', 'description', 5, 10, '2020-01-01 01:00:00', '2020-01-02 00:00:00', 2, 10, '');
INSERT INTO Auction VALUES(452, 'item-name', 'description', 5, 10, '2020-01-01 01:00:00', '2020-01-02 00:00:00', 3, 10, '');
""",
                """
                 name     | city   | state | id | weight
                -----------------------------------------
                 CA Seller| Phoenix| CA| 999 | 1
                 ID Seller| Phoenix| ID| 452 | 1""",
                """
INSERT INTO Person VALUES(4, 'OR Seller', 'AAABBB@example.com', '1111 2222 3333 4444', 'Phoenix', 'PR', '2020-01-01 00:00:00', '');
INSERT INTO Auction VALUES(999, 'item-name', 'description', 5, 10, '2020-01-01 01:00:00', '2020-01-02 00:00:00', 4, 11, '');
INSERT INTO Person VALUES(5, 'OR Seller', 'AAABBB@example.com', '1111 2222 3333 4444', 'Phoenix', 'OR', '2020-01-01 00:00:00', '');
INSERT INTO Auction VALUES(333, 'item-name', 'description', 5, 10, '2020-01-01 01:00:00', '2020-01-02 00:00:00', 5, 10, '');""",
                """
                 name     | city   | state | id | weight
                ------------------------------------------
                 OR Seller| Phoenix| OR| 333 | 1"""
                );
    }

    @Test
    public void q4Test() {
        this.createTest(4,
                """
INSERT INTO Auction VALUES(1, 'item-name', 'description', 5, 10, '2020-01-01 00:00:00', '2020-01-01 02:00:00', 1, 1, '');
INSERT INTO Auction VALUES(2, 'item-name', 'description', 5, 10, '2020-01-01 00:00:00', '2020-01-02 00:00:00', 1, 1, '');
INSERT INTO Auction VALUES(3, 'item-name', 'description', 5, 10, '2020-01-01 00:00:00', '2020-01-02 00:00:00', 1, 2, '');
-- Winning bid for auction 1 (category 1).
INSERT INTO Bid VALUES(1, 1, 80, 'my-channel', 'https://example.com', '2020-01-01 00:00:01.1', '');
-- This bid would have one but isn't included as it came in too late.
INSERT INTO Bid VALUES(1, 1, 100, 'my-channel', 'https://example.com', '2020-01-01 00:00:01.5', '');
-- Max bid for auction 2 (category 1).
INSERT INTO Bid VALUES(2, 1, 300, 'my-channel', 'https://example.com', '2020-01-01 00:00:00', '');
INSERT INTO Bid VALUES(2, 1, 200, 'my-channel', 'https://example.com', '2020-01-01 00:00:00', '');
-- Only bid for auction 3 (category 2)
INSERT INTO Bid VALUES(3, 1, 20, 'my-channel', 'https://example.com', '2020-01-01 00:00:00', '');
""",
                """
                 category | final | weight
                ----------------------------
                 1        | 200   | 1
                 2        | 20    | 1""",
                """
--  Another bid for auction 3 that should update the winning bid for category 2.
INSERT INTO Bid VALUES(3, 1, 30, 'my-channel', 'https://example.com', '2020-01-01 00:00:00', '');
                        """,
                """
                 category | final | weight
                ----------------------------
                 2        | 20    | -1
                 2        | 30    | 1""",
                """
-- Another auction with a single winning bid in category 2.
INSERT INTO Auction VALUES(4, 'item-name', 'description', 5, 10, '2020-01-01 00:00:00', '2020-01-01 00:00:02', 1, 2, '');
INSERT INTO Bid VALUES(4, 1, 60, 'my-channel', 'https://example.com', '2020-01-01 00:00:00', '');
                        """,
                """
                 category | final | weight
                ----------------------------
                 2        | 30    | -1
                 2        | 45    | 1"""
        );
    }

    @Test
    public void q5Test() {
        this.createTest(5,
                """
                """,
                """
                 auction | num
                ---------------""");
    }

    @Test @Ignore("OVER with ROWS")
    public void q6test() {
        this.createTest(6,
                """
                """,
                """
                 auction | price | bidder | date_time           | extra | weight
                -----------------------------------------------------------------""");
    }

    @Test @Ignore("The results are wrong, must investigate")
    public void q7test() {
        this.createTest(7,
        // The rust code has transposed columns 'price' and 'bidder' in the output
        """
-- The latest bid is at t=32_000, so the watermark as at t=28_000
-- and the tumbled window is from 10_000 - 20_000.
INSERT INTO bid VALUES(1, 1, 1000000, 'my-channel', 'https://example.com', '2020-01-01 00:00:09', '');
INSERT INTO bid VALUES(1, 1, 50, 'my-channel', 'https://example.com', '2020-01-01 00:00:11', '');
INSERT INTO bid VALUES(1, 1, 90, 'my-channel', 'https://example.com', '2020-01-01 00:00:14', '');
INSERT INTO bid VALUES(1, 1, 70, 'my-channel', 'https://example.com', '2020-01-01 00:00:16', '');
INSERT INTO bid VALUES(1, 1, 1000000, 'my-channel', 'https://example.com', '2020-01-01 00:00:21', '');
INSERT INTO bid VALUES(1, 1, 1000000, 'my-channel', 'https://example.com', '2020-01-01 00:00:32', '');""",
                """
                 auction | price | bidder | date_time           | extra | weight
                -----------------------------------------------------------------
                 1       | 90     | 1     | 2020-01-01 00:00:14 | | 1""");
    }

    @Test
    public void q8test() {
        // Persons 2 and 3 were both added during the 10-20 interval and created auctions in
        // that same interval. Person 1 was added in the previous interval (0-10) though their
        // auction is in the correct interval. Person 4 was added in the interval, but their auction is
        // in the next.
        this.createTest(8, """
INSERT INTO person VALUES(1, 'James Potter', '', '', '', '', '2020-01-01 00:00:09', '');
INSERT INTO person VALUES(2, 'Lili Potter', '', '', '', '', '2020-01-01 00:00:12', '');
INSERT INTO person VALUES(3, 'Harry Potter', '', '', '', '', '2020-01-01 00:00:15', '');
INSERT INTO person VALUES(4, 'Aldus D', '', '', '', '', '2020-01-01 00:00:18', '');
INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '2020-01-01 00:00:11', '2020-01-01 00:00:02', 1, 1, '');
INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '2020-01-01 00:00:15', '2020-01-01 00:00:02', 2, 1, '');
INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '2020-01-01 00:00:18', '2020-01-01 00:00:02', 3, 1, '');
INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '2020-01-01 00:00:21', '2020-01-01 00:00:02', 4, 1, '');
INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '2020-01-01 00:00:32', '2020-01-01 00:00:02', 99, 1, '');
                """, """
                 id | name | starttime | weight
                --------------------------------
                  2 | Lili Potter| 2020-01-01 00:00:10 | 1
                  3 | Harry Potter| 2020-01-01 00:00:10 | 1""");

        /*
        This part of the test requires WATERMARKS.
        this.createTest(8,
                """
INSERT INTO person VALUES(1, 'James Potter', '', '', '', '', '2020-01-01 00:00:10', '');
INSERT INTO person VALUES(2, 'Lili Potter', '', '', '', '', '2020-01-01 00:00:12', '');
INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '2020-01-01 00:00:14', '2020-01-01 00:00:02', 1, 1, '');
INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '2020-01-01 00:00:15', '2020-01-01 00:00:02', 2, 1, '');
""",
                """
                 id | name | starttime | weight
                ---------------------------""",
                """
INSERT INTO person VALUES(3, 'Harry Potter', '', '', '', '', '2020-01-01 00:00:22', '');
INSERT INTO auction VALUES(3, 'item-name', 'description', 5, 10, '2020-01-01 00:00:25', '2020-01-01 00:00:02', 1, 1, '');
INSERT INTO auction VALUES(99, 'item-name', 'description', 5, 10, '2020-01-01 00:00:32', '2020-01-01 00:00:02', 2, 1, '');
""",
                """
                 id | name | starttime | weight
                ---------------------------
                 1 | James Potter| 2020-01-01 00:00:10 | 1
                 2 | Lili Potter|  2020-01-01 00:00:10 | 1""",
                """
INSERT INTO auction VALUES(101, 'item-name', 'description', 5, 10, '2020-01-01 00:00:42', '2020-01-01 00:00:02', 1, 1, '');
""",
                """
                 id | name | starttime | weight
                --------------------------------
                 1 | James Potter| 2020-01-01 00:00:10 | 1
                 2 | Lili Potter| 2020-01-01 00:00:10 | 1
                 3 | Harry Potter| 2020-01-01 00:00:20 | 1""");
         */
    }

    @Test
    public void q9test() {
        // The first batch has a single auction for seller 99 with a highest bid of 100
        // (currently).
        this.createTest(9, """
INSERT INTO AUCTION VALUES(1, 'item-name', 'description', 5, 10, '2020-01-01 00:00:00', '2020-01-01 00:00:10', 99, 1, '');
INSERT INTO BID VALUES(1, 1, 80, 'my-channel', 'https://example.com', '2020-01-01 00:00:01', '');
INSERT INTO BID VALUES(1, 1, 100, 'my-channel', 'https://example.com', '2020-01-01 00:00:02', '');
""", """
 id | item | description | initialBid | reserve | date_time           | expires             | seller | category | extra | auction | bidder | price | bid_datetime         | bid_extra | weight
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  1 | item-name | description | 5     |      10 | 2020-01-01 00:00:00 | 2020-01-01 00:00:10 |     99 |        1 |       |       1 |      1 |    100 | 2020-01-01 00:00:02 |           | 1""",
        // The second batch has a new highest bid for the (currently) only auction.
        // And adds a new auction without any bids (empty join).
        """
INSERT INTO BID VALUES(1, 1, 200, 'my-channel', 'https://example.com', '2020-01-01 00:00:09', '');
INSERT INTO AUCTION VALUES(2, 'item-name', 'description', 5, 10, '2020-01-01 00:00:00', '2020-01-01 00:00:20', 101, 1, '');
                        """, """
 id | item | description | initialBid | reserve | date_time           | expires             | seller | category | extra | auction | bidder | price | bid_datetime         | bid_extra | weight
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  1 | item-name | description | 5     |      10 | 2020-01-01 00:00:00 | 2020-01-01 00:00:10 |     99 |        1 |       |       1 |      1 |    100 | 2020-01-01 00:00:02 |           | -1
  1 | item-name | description | 5     |      10 | 2020-01-01 00:00:00 | 2020-01-01 00:00:10 |     99 |        1 |       |       1 |      1 |    200 | 2020-01-01 00:00:09 |           | 1""",
        // The third batch has a new bid, but it's not higher, so no effect to the first
        // auction. A bid added for the second auction, so it is added.
                """
INSERT INTO BID VALUES(1, 1, 150, 'my-channel', 'https://example.com', '2020-01-01 00:00:09.5', '');
INSERT INTO BID VALUES(2, 1, 400, 'my-channel', 'https://example.com', '2020-01-01 00:00:19', '');""", """
                
id | item | description | initialBid | reserve | date_time           | expires             | seller | category | extra | auction | bidder | price | bid_datetime         | bid_extra | weight
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 2 | item-name | description | 5     |      10 | 2020-01-01 00:00:00 | 2020-01-01 00:00:20 |    101 |        1 |       |       2 |      1 |    400 | 2020-01-01 00:00:19 |           | 1""",
        // The fourth and final batch has a new bid for auction 2, but it's
        // come in too late to be valid, so no change.
                """
INSERT INTO BID VALUES(2, 1, 999, 'my-channel', 'https://example.com', '2020-01-01 00:00:20.1', '');""", """
id | item | description | initialBid | reserve | date_time | expires | seller | category | extra | auction | bidder | price | bid_datetime | bid_extra | weight
-----------------------------------------------------------------------------------------------------------------------------------------------------------------"""
        );
    }

    @Test
    public void q10test() {
        // No test data in Rust
        this.createTest(10, "",
                """
 auction | bidder | price | date_time | extra | date | time | weight
---------------------------------------------------------------------""");
    }

    @Test
    public void q12test() {
        this.createTest(12, "",
                """
 bidder | bid_count | starttime | endtime
------------------------------------------""");
    }

    @Test
    public void q13test() {
        // The original Rust test has a bigger side_input table - all pairs with equal values 0-9999
        this.createTest(13, """
INSERT INTO SIDE_INPUT VALUES('2020-01-01 00:00:00', 5, 5);
INSERT INTO SIDE_INPUT VALUES('2020-01-01 00:00:00', 1005, 1005);
INSERT INTO BID VALUES(1005, 1, 99, 'my-channel', 'https://example.com', '2020-01-01 00:00:00', '');
INSERT INTO BID VALUES(10005, 1, 99, 'my-channel', 'https://example.com', '2020-01-01 00:00:00', '');""",
                """
 auction | bidder | price | date_time           | value | weight
------------------------------------------------------------------
    1005 |      1 |    99 | 2020-01-01 00:00:00 | 1005| 1
   10005 |      1 |    99 | 2020-01-01 00:00:00 | 5| 1""");
    }

    @Test
    public void q15test() {
        var ccs = this.createTest(15, "INSERT INTO BID VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');",
                """
day | total_bids | rank1_bids | rank2_bids | rank3_bids | total_bidders | rank1_bidders | rank2_bidders | rank3_bidders | total_auctions | rank1_auctions | rank2_auctions | rank3_auctions | weight
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1970-01-01 | 1   |          1 |          0 |          0 |             1 |             1 |             0 |             0 |              1 |              1 |              0 |              0 | 1""", """
INSERT INTO BID VALUES(2, 1, 10001, 'my-channel', 'https://example.com', '1970-01-01 00:00:06', '');
INSERT INTO BID VALUES(3, 2, 1000001, 'my-channel', 'https://example.com', '1970-01-01 23:59:59.999', '');
INSERT INTO BID VALUES(3, 3, 99, 'my-channel', 'https://example.com', '1970-01-02 00:00:00.001', '');
INSERT INTO BID VALUES(3, 4, 99, 'my-channel', 'https://example.com', '1970-01-03 00:00:00.001', '');
INSERT INTO BID VALUES(3, 5, 99, 'my-channel', 'https://example.com', '1970-01-04 00:00:00.001', '');
INSERT INTO BID VALUES(3, 2, 99, 'my-channel', 'https://example.com', '1970-01-05 00:00:00.001', '');""",
                """
 day | total_bids | rank1_bids | rank2_bids | rank3_bids | total_bidders | rank1_bidders | rank2_bidders | rank3_bidders | total_auctions | rank1_auctions | rank2_auctions | rank3_auctions | weight
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1970-01-01 | 1    |          1 |          0 |          0 |             1 |             1 |             0 |             0 |              1 |              1 |              0 |              0 | -1
1970-01-01 | 3    |          1 |          1 |          1 |             2 |             1 |             1 |             1 |              3 |              1 |              1 |              1 | 1
1970-01-02 | 1    |          1 |          0 |          0 |             1 |             1 |             0 |             0 |              1 |              1 |              0 |              0 | 1
1970-01-03 | 1    |          1 |          0 |          0 |             1 |             1 |             0 |             0 |              1 |              1 |              0 |              0 | 1
1970-01-04 | 1    |          1 |          0 |          0 |             1 |             1 |             0 |             0 |              1 |              1 |              0 |              0 | 1
1970-01-05 | 1    |          1 |          0 |          0 |             1 |             1 |             0 |             0 |              1 |              1 |              0 |              0 | 1""",
                "INSERT INTO BID VALUES(4, 1, 99, 'my-channel', 'https://example.com', '2022-01-01 00:00:00', '');", """
day | total_bids | rank1_bids | rank2_bids | rank3_bids | total_bidders | rank1_bidders | rank2_bidders | rank3_bidders | total_auctions | rank1_auctions | rank2_auctions | rank3_auctions | weight
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
2022-01-01 | 1   |          1 |          0 |          0 |             1 |             1 |             0 |             0 |              1 |              1 |              0 |              0 | 1""");
        // Test for https://github.com/feldera/feldera/issues/2250
        CircuitVisitor v = new CircuitVisitor(ccs.compiler) {
            @Override
            public VisitDecision preorder(DBSPSimpleOperator node) {
                Assert.assertTrue( !node.operation.contains("aggregate") ||
                       node.operation.equals("aggregate_linear_postprocess_retain_keys"));
                return super.preorder(node);
            }
        };
        ccs.visit(v);
    }

    @Test
    public void q16test() {
        this.createTest(16, "",
                """
 channel | day | minute | total_bids | rank1_bids | rank2_bids | rank3_bids | total_bidders | rank1_bidders | rank2_bidders | rank3_bidders | total_auctions | rank1_auctions | rank2_auctions | rank3_auctions
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------""");
    }

    @Test
    public void q17test() {
        this.createTest(17, "",
                """
 auction | date | total_bids | rank1_bids | rank2_bids | rank3_bids | min_price | max_price | avg_price | sum_price | weight
-----------------------------------------------------------------------------------------------------------------------------""");
    }

    @Test
    public void q18test() {
        this.createTest(18, "",
                """
 auction | bidder | price | channel | url | date_time | extra | weight
-----------------------------------------------------------------------""");
    }

    @Test
    public void q19test() {
        this.createTest(19, "",
                """
 auction | bidder | price | channel | url | date_time | extra | row_number | weight
------------------------------------------------------------------------------------""");
    }

    @Test
    public void q20test() {
        this.createTest(19, "",
                """
 auction | bidder | price | channel | url | date_time | extra | itemName | description | initialBid | reserve | ADateTime | expires | seller | category | Aextra | weight
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------""");
    }

    @Test
    public void q22test() {
        this.createTest(22, "",
                """
 auction | bidder | price | channel | dir1 | dir2 | dir3
---------------------------------------------------------""");
    }

    @Test
    public void testCompile() {
        DBSPCompiler compiler = this.testCompiler();
        this.prepareInputs(compiler);

        Set<Integer> unsupported = new HashSet<>() {{
            add(6);  // over with rows
            add(11); // session
            add(21); // regexp_extract, needs to be done as a UDF
        }};

        int index = 0;
        for (String query: queries) {
            if (!unsupported.contains(index)) {
                compiler.submitStatementsForCompilation(query);
            }
            index++;
        }

        Assert.assertFalse(compiler.hasErrors());
        Assert.assertFalse(compiler.hasWarnings());
        this.getCCS(compiler);
    }
}
