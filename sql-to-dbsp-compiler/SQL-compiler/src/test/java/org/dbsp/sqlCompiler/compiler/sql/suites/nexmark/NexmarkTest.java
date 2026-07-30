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
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/* Test SQL queries from the Nexmark suite.
 * https://github.com/nexmark/nexmark/tree/master/nexmark-flink/src/main/resources/queries */
public class NexmarkTest extends StreamingTestBase {
    static final String tables = """
CREATE TABLE person (
    id BIGINT NOT NULL,
    name VARCHAR,
    emailAddress VARCHAR,
    creditCard VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time TIMESTAMP(3) NOT NULL LATENESS INTERVAL 4 SECONDS,
    extra  VARCHAR
);
CREATE TABLE auction (
    id BIGINT NOT NULL,
    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    date_time  TIMESTAMP(3) NOT NULL LATENESS INTERVAL 4 SECONDS,
    expires  TIMESTAMP(3),
    seller  BIGINT,
    category  BIGINT,
    extra  VARCHAR
);
CREATE TABLE bid (
    auction  BIGINT,
    bidder  BIGINT NOT NULL,
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
    SELECT MAX(B.price) AS final, A.seller, ARG_MAX(B.date_time, B.price) as date_time
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
--
-- The original query uses TUMBLE_ROWTIME, which is the window end; TUMBLE_END is the
-- closest translation.
-- -------------------------------------------------------------------------------------------------

CREATE VIEW Q7 AS
SELECT B.auction, B.price, B.bidder, B.date_time, B.extra
from bid B
JOIN (
  SELECT MAX(B1.price) AS maxprice, TUMBLE_END(B1.date_time, INTERVAL '10' SECOND) as date_time
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
SELECT auction, bidder, price, date_time, extra, FORMAT_TIMESTAMP('%Y-%m-%d', date_time), FORMAT_TIMESTAMP('%H:%M', date_time)
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
    format_timestamp('%H:%M', max(date_time)) as 'minute',
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
        options.ioOptions.testing = true;
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
        CompilerCircuitStream ccs = this.getCCS(compiler).withStringTrim();
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
                 name      | city    | state | id  | weight
                --------------------------------------------
                 CA Seller | Phoenix | CA    | 999 | 1
                 ID Seller | Phoenix | ID    | 452 | 1""",
                """
INSERT INTO Person VALUES(4, 'OR Seller', 'AAABBB@example.com', '1111 2222 3333 4444', 'Phoenix', 'PR', '2020-01-01 00:00:00', '');
INSERT INTO Auction VALUES(999, 'item-name', 'description', 5, 10, '2020-01-01 01:00:00', '2020-01-02 00:00:00', 4, 11, '');
INSERT INTO Person VALUES(5, 'OR Seller', 'AAABBB@example.com', '1111 2222 3333 4444', 'Phoenix', 'OR', '2020-01-01 00:00:00', '');
INSERT INTO Auction VALUES(333, 'item-name', 'description', 5, 10, '2020-01-01 01:00:00', '2020-01-02 00:00:00', 5, 10, '');""",
                """
                 name      | city    | state | id  | weight
                --------------------------------------------
                 OR Seller | Phoenix | OR    | 333 | 1"""
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
        // Test data from crates/nexmark/src/queries/q5.rs.
        // The Rust query emits hot auctions only for the last 10 second window before the
        // watermark, rounded to 2 seconds; the SQL query emits one row per hop window,
        // so a single (auction, num) pair can be produced by several windows (larger weights).

        // latest_bid_determines_window: auction 1 bids at 2.001, 4 and 11; auction 2 bid at 20.
        this.createTest(5,
                """
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:02.001', '');
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:04', '');
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:11', '');
                INSERT INTO bid VALUES(2, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:20', '');""",
                """
                 auction | num | weight
                ------------------------
                 1       | 1   | 4
                 1       | 2   | 4
                 1       | 3   | 1
                 2       | 1   | 5""");

        // windows_rounded_to_2s_boundary: auction 1 bids at 2.001, 4, 11 and 15; auction 2 bid at 19.
        this.createTest(5,
                """
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:02.001', '');
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:04', '');
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:11', '');
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:15', '');
                INSERT INTO bid VALUES(2, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:19', '');""",
                """
                 auction | num | weight
                ------------------------
                 1       | 1   | 3
                 1       | 2   | 7
                 1       | 3   | 1
                 2       | 1   | 4""");

        // multiple_auctions_have_same_hotness: both auctions have two bids in the earliest windows.
        this.createTest(5,
                """
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:02', '');
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:03.999', '');
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:08', '');
                INSERT INTO bid VALUES(2, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:02', '');
                INSERT INTO bid VALUES(2, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:03.999', '');""",
                """
                 auction | num | weight
                ------------------------
                 1       | 1   | 3
                 1       | 2   | 3
                 1       | 3   | 2
                 2       | 2   | 3""");

        // batch_2_updates_hotness_to_new_window: the second batch only creates new windows
        // that contain the single new bid of auction 1.
        this.createTest(5,
                """
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:02', '');
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:04', '');
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:06', '');
                INSERT INTO bid VALUES(2, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:02', '');
                INSERT INTO bid VALUES(2, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:04', '');
                INSERT INTO bid VALUES(2, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:08', '');
                INSERT INTO bid VALUES(2, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:12', '');""",
                """
                 auction | num | weight
                ------------------------
                 1       | 1   | 1
                 1       | 2   | 1
                 1       | 3   | 3
                 2       | 1   | 3
                 2       | 2   | 3
                 2       | 3   | 3""",
                "INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:20', '');",
                """
                 auction | num | weight
                ------------------------
                 1       | 1   | 5""");
    }

    @Test
    public void q6test() {
        // Test data from crates/nexmark/src/queries/q6.rs.
        // One running average per auction, over a window of 11 rows (10 PRECEDING +
        // CURRENT ROW) ordered by the date of the winning bid.

        // single_seller_single_auction
        this.createTest(6,
                """
                INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:10', 99, 1, '');
                INSERT INTO bid VALUES(1, 1, 80, 'my-channel', 'https://example.com', '1970-01-01 00:00:01', '');
                INSERT INTO bid VALUES(1, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:02', '');""",
                """
                 seller | avg | weight
                -----------------------
                 99     | 100 | 1""",
                // A new highest bid updates the average.
                "INSERT INTO bid VALUES(1, 1, 200, 'my-channel', 'https://example.com', '1970-01-01 00:00:09', '');",
                """
                 seller | avg | weight
                -----------------------
                 99     | 100 | -1
                 99     | 200 | 1""",
                // A later bid that is not higher does not change the average.
                "INSERT INTO bid VALUES(1, 1, 150, 'my-channel', 'https://example.com', '1970-01-01 00:00:09.5', '');",
                """
                 seller | avg | weight
                -----------------------""");

        // single_seller_multiple_auctions: the second auction adds a running average row
        // of 150; the row of the first auction stays.
        this.createTest(6,
                """
                INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:10', 99, 1, '');
                INSERT INTO bid VALUES(1, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:02', '');""",
                """
                 seller | avg | weight
                -----------------------
                 99     | 100 | 1""",
                """
                INSERT INTO auction VALUES(2, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(2, 1, 200, 'my-channel', 'https://example.com', '1970-01-01 00:00:15', '');""",
                """
                 seller | avg | weight
                -----------------------
                 99     | 150 | 1""");

        // single_seller_more_than_10_auctions: 11 auctions, the first with a winning bid
        // of 200, the others with 100.  The winning bids get distinct timestamps
        // (auction i's bid is at second i), so the ORDER BY has no ties.
        this.createTest(6,
                """
                INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(1, 1, 200, 'my-channel', 'https://example.com', '1970-01-01 00:00:01', '');
                INSERT INTO auction VALUES(2, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(2, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:02', '');
                INSERT INTO auction VALUES(3, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(3, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:03', '');
                INSERT INTO auction VALUES(4, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(4, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:04', '');
                INSERT INTO auction VALUES(5, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(5, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:05', '');""",
                // Running averages of 200, 100, 100, 100, 100.
                """
                 seller | avg | weight
                -----------------------
                 99     | 200 | 1
                 99     | 150 | 1
                 99     | 133 | 1
                 99     | 125 | 1
                 99     | 120 | 1""",
                """
                INSERT INTO auction VALUES(6, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(6, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:06', '');
                INSERT INTO auction VALUES(7, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(7, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:07', '');
                INSERT INTO auction VALUES(8, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(8, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:08', '');
                INSERT INTO auction VALUES(9, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(9, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:09', '');
                INSERT INTO auction VALUES(10, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(10, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:10', '');""",
                // Five new rows extend the running averages; the previous rows are unchanged.
                """
                 seller | avg | weight
                -----------------------
                 99     | 116 | 1
                 99     | 114 | 1
                 99     | 112 | 1
                 99     | 111 | 1
                 99     | 110 | 1""",
                """
                INSERT INTO auction VALUES(11, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(11, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:11', '');""",
                // The new row averages 11 rows: (200 + 10 * 100) / 11 = 109; the bid of 200
                // is still inside the 11 row window.
                """
                 seller | avg | weight
                -----------------------
                 99     | 109 | 1""");

        // multiple_sellers_multiple_auctions
        this.createTest(6,
                """
                INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:10', 99, 1, '');
                INSERT INTO bid VALUES(1, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:02', '');""",
                """
                 seller | avg | weight
                -----------------------
                 99     | 100 | 1""",
                """
                INSERT INTO auction VALUES(2, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 33, 1, '');
                INSERT INTO bid VALUES(2, 1, 200, 'my-channel', 'https://example.com', '1970-01-01 00:00:15', '');""",
                """
                 seller | avg | weight
                -----------------------
                 33     | 200 | 1""",
                """
                INSERT INTO auction VALUES(3, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 99, 1, '');
                INSERT INTO bid VALUES(3, 1, 200, 'my-channel', 'https://example.com', '1970-01-01 00:00:15', '');
                INSERT INTO auction VALUES(4, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:20', 33, 1, '');
                INSERT INTO bid VALUES(4, 1, 200, 'my-channel', 'https://example.com', '1970-01-01 00:00:15', '');""",
                """
                 seller | avg | weight
                -----------------------
                 33     | 200 | 1
                 99     | 150 | 1""");
    }

    @Test
    public void q7test() {
        // Test data from crates/nexmark/src/queries/q7.rs.
        // The Rust query emits max bids only for the last complete 10 second window before
        // the watermark; the SQL query emits the max bids of every tumbled window.
        // The Rust output column order is (auction, bidder, price); the SQL view
        // uses (auction, price, bidder).

        // latest_bid_determines_window: in the Rust test only the row at 14 survives,
        // because the watermark selects the window 10-20.
        this.createTest(7,
                """
                INSERT INTO bid VALUES(1, 1, 1000000, 'my-channel', 'https://example.com', '1970-01-01 00:00:09', '');
                INSERT INTO bid VALUES(1, 1, 50, 'my-channel', 'https://example.com', '1970-01-01 00:00:11', '');
                INSERT INTO bid VALUES(1, 1, 90, 'my-channel', 'https://example.com', '1970-01-01 00:00:14', '');
                INSERT INTO bid VALUES(1, 1, 70, 'my-channel', 'https://example.com', '1970-01-01 00:00:16', '');
                INSERT INTO bid VALUES(1, 1, 1000000, 'my-channel', 'https://example.com', '1970-01-01 00:00:21', '');
                INSERT INTO bid VALUES(1, 1, 1000000, 'my-channel', 'https://example.com', '1970-01-01 00:00:32', '');""",
                """
                 auction | price   | bidder | date_time           | extra | weight
                -------------------------------------------------------------------
                 1       | 1000000 | 1      | 1970-01-01 00:00:09 | | 1
                 1       | 90      | 1      | 1970-01-01 00:00:14 | | 1
                 1       | 1000000 | 1      | 1970-01-01 00:00:21 | | 1
                 1       | 1000000 | 1      | 1970-01-01 00:00:32 | | 1""");

        // tumble_into_new_window: each batch adds a bid in a new window.
        this.createTest(7,
                """
                INSERT INTO bid VALUES(1, 1, 1000000, 'my-channel', 'https://example.com', '1970-01-01 00:00:09', '');
                INSERT INTO bid VALUES(1, 1, 50, 'my-channel', 'https://example.com', '1970-01-01 00:00:11', '');
                INSERT INTO bid VALUES(1, 1, 90, 'my-channel', 'https://example.com', '1970-01-01 00:00:14', '');
                INSERT INTO bid VALUES(1, 1, 70, 'my-channel', 'https://example.com', '1970-01-01 00:00:16', '');
                INSERT INTO bid VALUES(1, 1, 1000000, 'my-channel', 'https://example.com', '1970-01-01 00:00:21', '');""",
                """
                 auction | price   | bidder | date_time           | extra | weight
                -------------------------------------------------------------------
                 1       | 1000000 | 1      | 1970-01-01 00:00:09 | | 1
                 1       | 90      | 1      | 1970-01-01 00:00:14 | | 1
                 1       | 1000000 | 1      | 1970-01-01 00:00:21 | | 1""",
                "INSERT INTO bid VALUES(1, 1, 10, 'my-channel', 'https://example.com', '1970-01-01 00:00:32', '');",
                """
                 auction | price   | bidder | date_time           | extra | weight
                -------------------------------------------------------------------
                 1       | 10      | 1      | 1970-01-01 00:00:32 | | 1""",
                "INSERT INTO bid VALUES(1, 1, 10, 'my-channel', 'https://example.com', '1970-01-01 00:00:42', '');",
                """
                 auction | price   | bidder | date_time           | extra | weight
                -------------------------------------------------------------------
                 1       | 10      | 1      | 1970-01-01 00:00:42 | | 1""");

        // multiple_max_bids: all bids that tie for the window maximum are output.
        this.createTest(7,
                """
                INSERT INTO bid VALUES(1, 1, 90, 'my-channel', 'https://example.com', '1970-01-01 00:00:11', '');
                INSERT INTO bid VALUES(1, 1, 90, 'my-channel', 'https://example.com', '1970-01-01 00:00:14', '');
                INSERT INTO bid VALUES(1, 1, 90, 'my-channel', 'https://example.com', '1970-01-01 00:00:16', '');
                INSERT INTO bid VALUES(1, 1, 1000000, 'my-channel', 'https://example.com', '1970-01-01 00:00:21', '');
                INSERT INTO bid VALUES(1, 1, 1000000, 'my-channel', 'https://example.com', '1970-01-01 00:00:32', '');""",
                """
                 auction | price   | bidder | date_time           | extra | weight
                -------------------------------------------------------------------
                 1       | 90      | 1      | 1970-01-01 00:00:11 | | 1
                 1       | 90      | 1      | 1970-01-01 00:00:14 | | 1
                 1       | 90      | 1      | 1970-01-01 00:00:16 | | 1
                 1       | 1000000 | 1      | 1970-01-01 00:00:21 | | 1
                 1       | 1000000 | 1      | 1970-01-01 00:00:32 | | 1""");
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
  1 | item-name| description| 5     |      10 | 2020-01-01 00:00:00 | 2020-01-01 00:00:10 |     99 |        1 | |       1 |      1 |    100 | 2020-01-01 00:00:02 | | 1""",
        // The second batch has a new highest bid for the (currently) only auction.
        // And adds a new auction without any bids (empty join).
        """
INSERT INTO BID VALUES(1, 1, 200, 'my-channel', 'https://example.com', '2020-01-01 00:00:09', '');
INSERT INTO AUCTION VALUES(2, 'item-name', 'description', 5, 10, '2020-01-01 00:00:00', '2020-01-01 00:00:20', 101, 1, '');
                        """, """
 id | item | description | initialBid | reserve | date_time           | expires             | seller | category | extra | auction | bidder | price | bid_datetime         | bid_extra | weight
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  1 | item-name| description| 5     |      10 | 2020-01-01 00:00:00 | 2020-01-01 00:00:10 |     99 |        1 | |       1 |      1 |    100 | 2020-01-01 00:00:02 | | -1
  1 | item-name| description| 5     |      10 | 2020-01-01 00:00:00 | 2020-01-01 00:00:10 |     99 |        1 | |       1 |      1 |    200 | 2020-01-01 00:00:09 | | 1""",
        // The third batch has a new bid, but it's not higher, so no effect to the first
        // auction. A bid added for the second auction, so it is added.
                """
INSERT INTO BID VALUES(1, 1, 150, 'my-channel', 'https://example.com', '2020-01-01 00:00:09.5', '');
INSERT INTO BID VALUES(2, 1, 400, 'my-channel', 'https://example.com', '2020-01-01 00:00:19', '');""", """
                
id | item | description | initialBid | reserve | date_time           | expires             | seller | category | extra | auction | bidder | price | bid_datetime         | bid_extra | weight
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 2 | item-name| description| 5     |      10 | 2020-01-01 00:00:00 | 2020-01-01 00:00:20 |    101 |        1 | |       2 |      1 |    400 | 2020-01-01 00:00:19 | | 1""",
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
        // Test data from crates/nexmark/src/queries/q12.rs.
        // The Rust test drives a mock processing-time clock; the SQL query uses the event
        // time date_time instead, so the mock clock values become the bid timestamps.

        // one_bidder_single_window
        this.createTest(12,
                """
                INSERT INTO bid VALUES(1, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:03', '');
                INSERT INTO bid VALUES(2, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:04', '');
                INSERT INTO bid VALUES(99, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:05', '');
                INSERT INTO bid VALUES(25, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:06', '');""",
                """
                 bidder | bid_count | starttime           | endtime             | weight
                -------------------------------------------------------------------------
                 1      | 4         | 1970-01-01 00:00:00 | 1970-01-01 00:00:10 | 1""",
                """
                INSERT INTO bid VALUES(16, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:07', '');
                INSERT INTO bid VALUES(2, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:08', '');""",
                """
                 bidder | bid_count | starttime           | endtime             | weight
                -------------------------------------------------------------------------
                 1      | 4         | 1970-01-01 00:00:00 | 1970-01-01 00:00:10 | -1
                 1      | 6         | 1970-01-01 00:00:00 | 1970-01-01 00:00:10 | 1""");

        // one_bidder_multiple_windows
        this.createTest(12,
                """
                INSERT INTO bid VALUES(99, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:03', '');
                INSERT INTO bid VALUES(63, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:04', '');
                INSERT INTO bid VALUES(2, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:05', '');
                INSERT INTO bid VALUES(45, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:06', '');""",
                """
                 bidder | bid_count | starttime           | endtime             | weight
                -------------------------------------------------------------------------
                 1      | 4         | 1970-01-01 00:00:00 | 1970-01-01 00:00:10 | 1""",
                """
                INSERT INTO bid VALUES(29, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:11', '');
                INSERT INTO bid VALUES(21, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:12', '');""",
                """
                 bidder | bid_count | starttime           | endtime             | weight
                -------------------------------------------------------------------------
                 1      | 2         | 1970-01-01 00:00:10 | 1970-01-01 00:00:20 | 1""");

        // multiple_bidders_multiple_windows
        this.createTest(12,
                """
                INSERT INTO bid VALUES(12, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:03', '');
                INSERT INTO bid VALUES(102, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:04', '');
                INSERT INTO bid VALUES(22, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:05', '');
                INSERT INTO bid VALUES(79, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:06', '');
                INSERT INTO bid VALUES(16, 2, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:07', '');
                INSERT INTO bid VALUES(81, 2, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:08', '');""",
                """
                 bidder | bid_count | starttime           | endtime             | weight
                -------------------------------------------------------------------------
                 1      | 4         | 1970-01-01 00:00:00 | 1970-01-01 00:00:10 | 1
                 2      | 2         | 1970-01-01 00:00:00 | 1970-01-01 00:00:10 | 1""",
                """
                INSERT INTO bid VALUES(49, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:11', '');
                INSERT INTO bid VALUES(77, 1, 99, 'my-channel', 'https://example.com', '1970-01-01 00:00:12', '');""",
                """
                 bidder | bid_count | starttime           | endtime             | weight
                -------------------------------------------------------------------------
                 1      | 2         | 1970-01-01 00:00:10 | 1970-01-01 00:00:20 | 1""");
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
    public void q14test() {
        // Test data from crates/nexmark/src/queries/q14.rs.
        // The Rust date_time_is_daytime_2022 case uses an approximate 2022 timestamp
        // computed with 366-day years; here a plain 2022 date is used instead.
        this.createTest(14,
                """
                -- 0.908 * 2000000 = 1816000 is inside the price range; date_time 0 is night time.
                INSERT INTO bid VALUES(1, 1, 2000000, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                -- 0.908 * 1000000 = 908000 is not larger than 1000000, so this bid is dropped.
                INSERT INTO bid VALUES(1, 1, 1000000, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                -- extra contains four 'c' characters.
                INSERT INTO bid VALUES(1, 1, 2000000, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', 'cause I can''t calculate has four of them.');""",
                """
                 auction | bidder | price   | bidTimeType | date_time           | extra | c_counts | weight
                --------------------------------------------------------------------------------------------
                 1       | 1      | 1816000 | nightTime| 1970-01-01 00:00:00 | | 0 | 1
                 1       | 1      | 1816000 | nightTime| 1970-01-01 00:00:00 | cause I can't calculate has four of them.| 4 | 1""",
                "INSERT INTO bid VALUES(1, 1, 2000000, 'my-channel', 'https://example.com', '1970-01-01 07:59:59.999', '');",
                """
                 auction | bidder | price   | bidTimeType | date_time           | extra | c_counts | weight
                --------------------------------------------------------------------------------------------
                 1       | 1      | 1816000 | otherTime| 1970-01-01 07:59:59.999 | | 0 | 1""",
                "INSERT INTO bid VALUES(1, 1, 2000000, 'my-channel', 'https://example.com', '1970-01-01 08:00:00.001', '');",
                """
                 auction | bidder | price   | bidTimeType | date_time           | extra | c_counts | weight
                --------------------------------------------------------------------------------------------
                 1       | 1      | 1816000 | dayTime| 1970-01-01 08:00:00.001 | | 0 | 1""",
                "INSERT INTO bid VALUES(1, 1, 2000000, 'my-channel', 'https://example.com', '1970-01-01 20:00:00.001', '');",
                """
                 auction | bidder | price   | bidTimeType | date_time           | extra | c_counts | weight
                --------------------------------------------------------------------------------------------
                 1       | 1      | 1816000 | nightTime| 1970-01-01 20:00:00.001 | | 0 | 1""",
                "INSERT INTO bid VALUES(1, 1, 2000000, 'my-channel', 'https://example.com', '2022-01-01 08:00:00.001', '');",
                """
                 auction | bidder | price   | bidTimeType | date_time           | extra | c_counts | weight
                --------------------------------------------------------------------------------------------
                 1       | 1      | 1816000 | dayTime| 2022-01-01 08:00:00.001 | | 0 | 1""");
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
                        node.operation.equals("aggregate_linear_postprocess_retain_keys") ||
                        node.operation.equals("chain_aggregate"));
                return super.preorder(node);
            }
        };
        ccs.visit(v);
    }

    @Test
    public void q16test() {
        // Test data from crates/nexmark/src/queries/q16.rs.
        this.createTest(16,
                "INSERT INTO bid VALUES(1, 1, 99, 'channel-1', 'https://example.com', '1970-01-01 00:00:00', '');",
                """
 channel | day | minute | total_bids | rank1_bids | rank2_bids | rank3_bids | total_bidders | rank1_bidders | rank2_bidders | rank3_bidders | total_auctions | rank1_auctions | rank2_auctions | rank3_auctions | weight
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 channel-1| 1970-01-01 | 00:00| 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1""",
                """
                -- A rank 2 bid six minutes after the epoch, and rank 3 and rank 1 bids
                -- close to the midnight boundaries of the following days.
                INSERT INTO bid VALUES(2, 1, 10001, 'channel-1', 'https://example.com', '1970-01-01 00:06:00', '');
                INSERT INTO bid VALUES(3, 2, 1000001, 'channel-1', 'https://example.com', '1970-01-01 23:59:59.999', '');
                INSERT INTO bid VALUES(3, 3, 99, 'channel-1', 'https://example.com', '1970-01-02 00:00:00.001', '');
                INSERT INTO bid VALUES(3, 4, 99, 'channel-1', 'https://example.com', '1970-01-03 00:00:00.001', '');
                INSERT INTO bid VALUES(3, 5, 99, 'channel-1', 'https://example.com', '1970-01-04 00:00:00.001', '');
                INSERT INTO bid VALUES(3, 2, 99, 'channel-1', 'https://example.com', '1970-01-05 00:00:00.001', '');""",
                """
 channel | day | minute | total_bids | rank1_bids | rank2_bids | rank3_bids | total_bidders | rank1_bidders | rank2_bidders | rank3_bidders | total_auctions | rank1_auctions | rank2_auctions | rank3_auctions | weight
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 channel-1| 1970-01-01 | 00:00| 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | -1
 channel-1| 1970-01-01 | 23:59| 3 | 1 | 1 | 1 | 2 | 1 | 1 | 1 | 3 | 1 | 1 | 1 | 1
 channel-1| 1970-01-02 | 00:00| 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1
 channel-1| 1970-01-03 | 00:00| 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1
 channel-1| 1970-01-04 | 00:00| 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1
 channel-1| 1970-01-05 | 00:00| 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1""",
                "INSERT INTO bid VALUES(4, 1, 99, 'channel-1', 'https://example.com', '2022-01-01 00:00:00', '');",
                """
 channel | day | minute | total_bids | rank1_bids | rank2_bids | rank3_bids | total_bidders | rank1_bidders | rank2_bidders | rank3_bidders | total_auctions | rank1_auctions | rank2_auctions | rank3_auctions | weight
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 channel-1| 2022-01-01 | 00:00| 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 1""");
    }

    @Test
    public void q17test() {
        // Test data from crates/nexmark/src/queries/q17.rs.

        // multiple_auctions_single_batch: the average of auction 2 truncates 5000 / 3 to 1666.
        this.createTest(17,
                """
                INSERT INTO bid VALUES(1, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(2, 1, 500, 'my-channel', 'https://example.com', '1970-01-01 00:00:05', '');
                INSERT INTO bid VALUES(1, 1, 700, 'my-channel', 'https://example.com', '1970-01-01 00:00:10', '');
                INSERT INTO bid VALUES(2, 1, 1500, 'my-channel', 'https://example.com', '1970-01-01 00:00:15', '');
                INSERT INTO bid VALUES(1, 1, 400, 'my-channel', 'https://example.com', '1970-01-01 00:00:20', '');
                INSERT INTO bid VALUES(2, 1, 3000, 'my-channel', 'https://example.com', '1970-01-01 00:00:25', '');""",
                """
 auction | day | total_bids | rank1_bids | rank2_bids | rank3_bids | min_price | max_price | avg_price | sum_price | weight
-----------------------------------------------------------------------------------------------------------------------------
 1       | 1970-01-01 | 3 | 3 | 0 | 0 | 100 | 700  | 400  | 1200 | 1
 2       | 1970-01-01 | 3 | 3 | 0 | 0 | 500 | 3000 | 1666 | 5000 | 1""");

        // multiple_auctions_multiple_batches: the second batch updates the aggregate of the
        // first day and adds an aggregate for a new day.
        this.createTest(17,
                "INSERT INTO bid VALUES(1, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');",
                """
 auction | day | total_bids | rank1_bids | rank2_bids | rank3_bids | min_price | max_price | avg_price | sum_price | weight
-----------------------------------------------------------------------------------------------------------------------------
 1       | 1970-01-01 | 1 | 1 | 0 | 0 | 100 | 100 | 100 | 100 | 1""",
                """
                INSERT INTO bid VALUES(1, 1, 10100, 'my-channel', 'https://example.com', '1970-01-01 23:59:59.999', '');
                INSERT INTO bid VALUES(2, 1, 1000000, 'my-channel', 'https://example.com', '1970-01-03 00:00:00', '');
                INSERT INTO bid VALUES(2, 1, 2000000, 'my-channel', 'https://example.com', '1970-01-03 00:00:01', '');""",
                """
 auction | day | total_bids | rank1_bids | rank2_bids | rank3_bids | min_price | max_price | avg_price | sum_price | weight
-----------------------------------------------------------------------------------------------------------------------------
 1       | 1970-01-01 | 1 | 1 | 0 | 0 | 100     | 100     | 100     | 100     | -1
 1       | 1970-01-01 | 2 | 1 | 1 | 0 | 100     | 10100   | 5100    | 10200   | 1
 2       | 1970-01-03 | 2 | 0 | 0 | 2 | 1000000 | 2000000 | 1500000 | 3000000 | 1""");
    }

    @Test
    public void q18test() {
        // Test data from crates/nexmark/src/queries/q18.rs.

        // last_bid_for_single_bidder_single_auction
        this.createTest(18,
                """
                INSERT INTO bid VALUES(1, 1, 10, 'my-channel', 'https://example.com', '1970-01-01 00:00:01', '');
                INSERT INTO bid VALUES(1, 1, 20, 'my-channel', 'https://example.com', '1970-01-01 00:00:03', '');
                INSERT INTO bid VALUES(1, 1, 30, 'my-channel', 'https://example.com', '1970-01-01 00:00:02', '');""",
                """
                 auction | bidder | price | channel | url | date_time | extra | weight
                -----------------------------------------------------------------------
                 1       | 1      | 20    | my-channel| https://example.com| 1970-01-01 00:00:03 | | 1""",
                "INSERT INTO bid VALUES(1, 1, 50, 'my-channel', 'https://example.com', '1970-01-01 00:00:04', '');",
                """
                 auction | bidder | price | channel | url | date_time | extra | weight
                -----------------------------------------------------------------------
                 1       | 1      | 20    | my-channel| https://example.com| 1970-01-01 00:00:03 | | -1
                 1       | 1      | 50    | my-channel| https://example.com| 1970-01-01 00:00:04 | | 1""");

        // last_bid_for_multi_bidders_single_auction
        this.createTest(18,
                """
                INSERT INTO bid VALUES(1, 1, 10, 'my-channel', 'https://example.com', '1970-01-01 00:00:01', '');
                INSERT INTO bid VALUES(1, 2, 20, 'my-channel', 'https://example.com', '1970-01-01 00:00:03', '');
                INSERT INTO bid VALUES(1, 1, 30, 'my-channel', 'https://example.com', '1970-01-01 00:00:02', '');
                INSERT INTO bid VALUES(1, 2, 40, 'my-channel', 'https://example.com', '1970-01-01 00:00:04', '');""",
                """
                 auction | bidder | price | channel | url | date_time | extra | weight
                -----------------------------------------------------------------------
                 1       | 1      | 30    | my-channel| https://example.com| 1970-01-01 00:00:02 | | 1
                 1       | 2      | 40    | my-channel| https://example.com| 1970-01-01 00:00:04 | | 1""",
                """
                INSERT INTO bid VALUES(1, 1, 40, 'my-channel', 'https://example.com', '1970-01-01 00:00:05', '');
                INSERT INTO bid VALUES(1, 2, 50, 'my-channel', 'https://example.com', '1970-01-01 00:00:06', '');
                INSERT INTO bid VALUES(1, 1, 70, 'my-channel', 'https://example.com', '1970-01-01 00:00:07', '');
                INSERT INTO bid VALUES(1, 2, 80, 'my-channel', 'https://example.com', '1970-01-01 00:00:08', '');""",
                """
                 auction | bidder | price | channel | url | date_time | extra | weight
                -----------------------------------------------------------------------
                 1       | 1      | 30    | my-channel| https://example.com| 1970-01-01 00:00:02 | | -1
                 1       | 2      | 40    | my-channel| https://example.com| 1970-01-01 00:00:04 | | -1
                 1       | 1      | 70    | my-channel| https://example.com| 1970-01-01 00:00:07 | | 1
                 1       | 2      | 80    | my-channel| https://example.com| 1970-01-01 00:00:08 | | 1""");

        // last_bid_for_multi_bidders_multi_auctions
        this.createTest(18,
                """
                INSERT INTO bid VALUES(1, 1, 10, 'my-channel', 'https://example.com', '1970-01-01 00:00:01', '');
                INSERT INTO bid VALUES(1, 2, 20, 'my-channel', 'https://example.com', '1970-01-01 00:00:02', '');
                INSERT INTO bid VALUES(2, 1, 30, 'my-channel', 'https://example.com', '1970-01-01 00:00:03', '');
                INSERT INTO bid VALUES(2, 2, 40, 'my-channel', 'https://example.com', '1970-01-01 00:00:04', '');""",
                """
                 auction | bidder | price | channel | url | date_time | extra | weight
                -----------------------------------------------------------------------
                 1       | 1      | 10    | my-channel| https://example.com| 1970-01-01 00:00:01 | | 1
                 1       | 2      | 20    | my-channel| https://example.com| 1970-01-01 00:00:02 | | 1
                 2       | 1      | 30    | my-channel| https://example.com| 1970-01-01 00:00:03 | | 1
                 2       | 2      | 40    | my-channel| https://example.com| 1970-01-01 00:00:04 | | 1""",
                """
                INSERT INTO bid VALUES(1, 1, 50, 'my-channel', 'https://example.com', '1970-01-01 00:00:05', '');
                INSERT INTO bid VALUES(1, 2, 60, 'my-channel', 'https://example.com', '1970-01-01 00:00:06', '');
                INSERT INTO bid VALUES(2, 1, 70, 'my-channel', 'https://example.com', '1970-01-01 00:00:07', '');
                INSERT INTO bid VALUES(2, 2, 80, 'my-channel', 'https://example.com', '1970-01-01 00:00:08', '');""",
                """
                 auction | bidder | price | channel | url | date_time | extra | weight
                -----------------------------------------------------------------------
                 1       | 1      | 10    | my-channel| https://example.com| 1970-01-01 00:00:01 | | -1
                 1       | 2      | 20    | my-channel| https://example.com| 1970-01-01 00:00:02 | | -1
                 2       | 1      | 30    | my-channel| https://example.com| 1970-01-01 00:00:03 | | -1
                 2       | 2      | 40    | my-channel| https://example.com| 1970-01-01 00:00:04 | | -1
                 1       | 1      | 50    | my-channel| https://example.com| 1970-01-01 00:00:05 | | 1
                 1       | 2      | 60    | my-channel| https://example.com| 1970-01-01 00:00:06 | | 1
                 2       | 1      | 70    | my-channel| https://example.com| 1970-01-01 00:00:07 | | 1
                 2       | 2      | 80    | my-channel| https://example.com| 1970-01-01 00:00:08 | | 1""");
    }

    @Test
    public void q19test() {
        // Test data from crates/nexmark/src/queries/q19.rs.
        // The view outputs rank_number, so a new top bid shifts the rank of every lower
        // bid and retracts all shifted rows.

        // top_bids_for_single_auction
        this.createTest(19,
                """
                INSERT INTO bid VALUES(1, 12, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 1, 1200, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 3, 1100, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 4, 1000, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 5, 200, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 6, 300, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 7, 400, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 8, 500, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 9, 600, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 10, 700, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 11, 800, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 12, 900, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');""",
                """
                 auction | bidder | price | channel | url | date_time | extra | rank_number | weight
                -------------------------------------------------------------------------------------
                 1       | 1      | 1200  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 1  | 1
                 1       | 3      | 1100  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 2  | 1
                 1       | 4      | 1000  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 3  | 1
                 1       | 12     | 900   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 4  | 1
                 1       | 11     | 800   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 5  | 1
                 1       | 10     | 700   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 6  | 1
                 1       | 9      | 600   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 7  | 1
                 1       | 8      | 500   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 8  | 1
                 1       | 7      | 400   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 9  | 1
                 1       | 6      | 300   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 10 | 1""",
                // A new top bid of 1300 shifts every rank down by one; the bid of 50 ranks last.
                """
                INSERT INTO bid VALUES(1, 1, 1300, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 1, 50, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');""",
                """
                 auction | bidder | price | channel | url | date_time | extra | rank_number | weight
                -------------------------------------------------------------------------------------
                 1       | 1      | 1200  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 1  | -1
                 1       | 3      | 1100  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 2  | -1
                 1       | 4      | 1000  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 3  | -1
                 1       | 12     | 900   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 4  | -1
                 1       | 11     | 800   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 5  | -1
                 1       | 10     | 700   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 6  | -1
                 1       | 9      | 600   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 7  | -1
                 1       | 8      | 500   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 8  | -1
                 1       | 7      | 400   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 9  | -1
                 1       | 6      | 300   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 10 | -1
                 1       | 1      | 1300  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 1  | 1
                 1       | 1      | 1200  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 2  | 1
                 1       | 3      | 1100  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 3  | 1
                 1       | 4      | 1000  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 4  | 1
                 1       | 12     | 900   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 5  | 1
                 1       | 11     | 800   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 6  | 1
                 1       | 10     | 700   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 7  | 1
                 1       | 9      | 600   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 8  | 1
                 1       | 8      | 500   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 9  | 1
                 1       | 7      | 400   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 10 | 1""");

        // top_bids_for_multiple_auctions
        this.createTest(19,
                """
                INSERT INTO bid VALUES(1, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 1, 200, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(7, 1, 100, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(7, 1, 1200, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(7, 1, 1100, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(7, 1, 1000, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(7, 1, 200, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(7, 1, 300, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(7, 1, 400, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(7, 1, 500, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(7, 1, 600, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(7, 1, 700, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(7, 1, 800, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(7, 1, 900, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');""",
                """
                 auction | bidder | price | channel | url | date_time | extra | rank_number | weight
                -------------------------------------------------------------------------------------
                 1       | 1      | 200   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 1  | 1
                 1       | 1      | 100   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 2  | 1
                 7       | 1      | 1200  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 1  | 1
                 7       | 1      | 1100  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 2  | 1
                 7       | 1      | 1000  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 3  | 1
                 7       | 1      | 900   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 4  | 1
                 7       | 1      | 800   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 5  | 1
                 7       | 1      | 700   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 6  | 1
                 7       | 1      | 600   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 7  | 1
                 7       | 1      | 500   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 8  | 1
                 7       | 1      | 400   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 9  | 1
                 7       | 1      | 300   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 10 | 1""",
                """
                INSERT INTO bid VALUES(1, 1, 1300, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 1, 50, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');""",
                """
                 auction | bidder | price | channel | url | date_time | extra | rank_number | weight
                -------------------------------------------------------------------------------------
                 1       | 1      | 200   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 1 | -1
                 1       | 1      | 100   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 2 | -1
                 1       | 1      | 1300  | my-channel| https://example.com| 1970-01-01 00:00:00 | | 1 | 1
                 1       | 1      | 200   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 2 | 1
                 1       | 1      | 100   | my-channel| https://example.com| 1970-01-01 00:00:00 | | 3 | 1
                 1       | 1      | 50    | my-channel| https://example.com| 1970-01-01 00:00:00 | | 4 | 1""");
    }

    @Test
    public void q20test() {
        // Test data from crates/nexmark/src/queries/q20.rs.

        // auction_bids_single_auction: the bid on auction 2 has no matching auction.
        this.createTest(20,
                """
                INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:02', 1, 10, '');
                INSERT INTO bid VALUES(1, 10, 10, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 20, 20, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(2, 50, 50, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 30, 30, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');""",
                """
 auction | bidder | price | channel | url | date_time | extra | itemName | description | initialBid | reserve | ADateTime | expires | seller | category | Aextra | weight
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 1 | 10 | 10 | my-channel| https://example.com| 1970-01-01 00:00:00 | | item-name| description| 5 | 10 | 1970-01-01 00:00:00 | 1970-01-01 00:00:02 | 1 | 10 | | 1
 1 | 20 | 20 | my-channel| https://example.com| 1970-01-01 00:00:00 | | item-name| description| 5 | 10 | 1970-01-01 00:00:00 | 1970-01-01 00:00:02 | 1 | 10 | | 1
 1 | 30 | 30 | my-channel| https://example.com| 1970-01-01 00:00:00 | | item-name| description| 5 | 10 | 1970-01-01 00:00:00 | 1970-01-01 00:00:02 | 1 | 10 | | 1""",
                "INSERT INTO bid VALUES(1, 40, 40, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');",
                """
 auction | bidder | price | channel | url | date_time | extra | itemName | description | initialBid | reserve | ADateTime | expires | seller | category | Aextra | weight
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 1 | 40 | 40 | my-channel| https://example.com| 1970-01-01 00:00:00 | | item-name| description| 5 | 10 | 1970-01-01 00:00:00 | 1970-01-01 00:00:02 | 1 | 10 | | 1""");

        // auction_bids_wrong_category
        this.createTest(20,
                """
                INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:02', 1, 9, '');
                INSERT INTO bid VALUES(1, 10, 10, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 20, 20, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 30, 30, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');""",
                """
 auction | bidder | price | channel | url | date_time | extra | itemName | description | initialBid | reserve | ADateTime | expires | seller | category | Aextra | weight
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------""",
                "INSERT INTO bid VALUES(1, 40, 40, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');",
                """
 auction | bidder | price | channel | url | date_time | extra | itemName | description | initialBid | reserve | ADateTime | expires | seller | category | Aextra | weight
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------""");

        // auction_bids_multiple_auctions
        this.createTest(20,
                """
                INSERT INTO auction VALUES(1, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:02', 1, 10, '');
                INSERT INTO bid VALUES(1, 10, 10, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 20, 20, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO auction VALUES(2, 'item-name', 'description', 5, 10, '1970-01-01 00:00:00', '1970-01-01 00:00:02', 1, 10, '');
                INSERT INTO bid VALUES(2, 50, 50, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 30, 30, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');""",
                """
 auction | bidder | price | channel | url | date_time | extra | itemName | description | initialBid | reserve | ADateTime | expires | seller | category | Aextra | weight
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 1 | 10 | 10 | my-channel| https://example.com| 1970-01-01 00:00:00 | | item-name| description| 5 | 10 | 1970-01-01 00:00:00 | 1970-01-01 00:00:02 | 1 | 10 | | 1
 1 | 20 | 20 | my-channel| https://example.com| 1970-01-01 00:00:00 | | item-name| description| 5 | 10 | 1970-01-01 00:00:00 | 1970-01-01 00:00:02 | 1 | 10 | | 1
 1 | 30 | 30 | my-channel| https://example.com| 1970-01-01 00:00:00 | | item-name| description| 5 | 10 | 1970-01-01 00:00:00 | 1970-01-01 00:00:02 | 1 | 10 | | 1
 2 | 50 | 50 | my-channel| https://example.com| 1970-01-01 00:00:00 | | item-name| description| 5 | 10 | 1970-01-01 00:00:00 | 1970-01-01 00:00:02 | 1 | 10 | | 1""",
                """
                INSERT INTO bid VALUES(1, 40, 40, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(2, 60, 60, 'my-channel', 'https://example.com', '1970-01-01 00:00:00', '');""",
                """
 auction | bidder | price | channel | url | date_time | extra | itemName | description | initialBid | reserve | ADateTime | expires | seller | category | Aextra | weight
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 1 | 40 | 40 | my-channel| https://example.com| 1970-01-01 00:00:00 | | item-name| description| 5 | 10 | 1970-01-01 00:00:00 | 1970-01-01 00:00:02 | 1 | 10 | | 1
 2 | 60 | 60 | my-channel| https://example.com| 1970-01-01 00:00:00 | | item-name| description| 5 | 10 | 1970-01-01 00:00:00 | 1970-01-01 00:00:02 | 1 | 10 | | 1""");
    }

    @Test
    public void q22test() {
        // Test data from crates/nexmark/src/queries/q22.rs.
        // channel and url carry the same value, so the channel column echoes the split
        // URL.  SPLIT_INDEX produces NULL for a missing directory.

        // bids_with_well_formed_urls
        this.createTest(22,
                """
                INSERT INTO bid VALUES(1, 1, 99, 'https://example.com/foo/bar/zed', 'https://example.com/foo/bar/zed', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 1, 99, 'https://example.com/dir1/dir2/dir3/dir4/dir5', 'https://example.com/dir1/dir2/dir3/dir4/dir5', '1970-01-01 00:00:00', '');""",
                """
                 auction | bidder | price | channel | dir1 | dir2 | dir3 | weight
                ------------------------------------------------------------------
                 1       | 1      | 99    | https://example.com/foo/bar/zed| foo| bar| zed| 1
                 1       | 1      | 99    | https://example.com/dir1/dir2/dir3/dir4/dir5| dir1| dir2| dir3| 1""");

        // bids_mixed_with_non_urls
        this.createTest(22,
                """
                INSERT INTO bid VALUES(1, 1, 99, 'https://example.com/foo/bar/zed', 'https://example.com/foo/bar/zed', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 1, 99, 'Google', 'Google', '1970-01-01 00:00:00', '');
                INSERT INTO bid VALUES(1, 1, 99, 'https:badly.formed/dir1/dir2/dir3', 'https:badly.formed/dir1/dir2/dir3', '1970-01-01 00:00:00', '');""",
                """
                 auction | bidder | price | channel | dir1 | dir2 | dir3 | weight
                ------------------------------------------------------------------
                 1       | 1      | 99    | https://example.com/foo/bar/zed| foo| bar| zed| 1
                 1       | 1      | 99    | Google|NULL|NULL|NULL| 1
                 1       | 1      | 99    | https:badly.formed/dir1/dir2/dir3| dir3|NULL|NULL| 1""");
    }

    @Test
    public void testCompile() {
        DBSPCompiler compiler = this.testCompiler();
        this.prepareInputs(compiler);

        Set<Integer> unsupported = new HashSet<>() {{
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
