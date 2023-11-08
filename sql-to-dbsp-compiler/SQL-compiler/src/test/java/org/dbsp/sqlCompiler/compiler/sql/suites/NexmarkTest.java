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
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.sql.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.junit.Assert;

/**
 * Test SQL queries from the Nexmark suite.
 * https://github.com/nexmark/nexmark/tree/master/nexmark-flink/src/main/resources/queries
 */
@SuppressWarnings("JavadocLinkAsPlainText")
public class NexmarkTest extends BaseSQLTests {
    static final String table = "CREATE TABLE NEXMARK_TABLE (\n" +
            "event_type int,\n" +
            "\"person.id\"  BIGINT,\n" +
            "\"person.name\"  VARCHAR,\n" +
            "\"person.emailAddress\"  VARCHAR,\n" +
            "\"person.creditCard\"  VARCHAR,\n" +
            "\"person.city\"  VARCHAR,\n" +
            "\"person.state\"  VARCHAR,\n" +
            "\"person.dateTime\" TIMESTAMP(3),\n" +
            "\"person.extra\"  VARCHAR,\n" +
            "\"auction.id\"  BIGINT,\n" +
            "\"auction.itemName\"  VARCHAR,\n" +
            "\"auction.description\"  VARCHAR,\n" +
            "\"auction.initialBid\"  BIGINT,\n" +
            "\"auction.reserve\"  BIGINT,\n" +
            "\"auction.dateTime\"  TIMESTAMP(3),\n" +
            "\"auction.expires\"  TIMESTAMP(3),\n" +
            "\"auction.seller\"  BIGINT,\n" +
            "\"auction.category\"  BIGINT,\n" +
            "\"auction.extra\"  VARCHAR,\n" +
            "\"bid.auction\"  BIGINT,\n" +
            "\"bid.bidder\"  BIGINT,\n" +
            "\"bid.price\"  BIGINT,\n" +
            "\"bid.channel\"  VARCHAR,\n" +
            "\"bid.url\"  VARCHAR,\n" +
            "\"bid.dateTime\"  TIMESTAMP(3),\n" +
            "\"bid.extra\"  VARCHAR)\n";

    static final String[] views = {
        "CREATE VIEW person AS\n" +
        "SELECT\n" +
        "    \"person.id\" as id,\n" +
        "    \"person.name\" as name,\n" +
        "    \"person.emailAddress\" as emailAddress,\n" +
        "    \"person.creditCard\" as creditCard,\n" +
        "    \"person.city\" as city,\n" +
        "    \"person.state\" as state,\n" +
        "    \"person.dateTime\" AS dateTime,\n" +
        "    \"person.extra\" as extra\n" +
        "FROM NEXMARK_TABLE WHERE event_type = 0",
        "CREATE VIEW auction AS\n" +
        "SELECT\n" +
        "    \"auction.id\" as id,\n" +
        "    \"auction.itemName\" as itemName,\n" +
        "    \"auction.description\" as description,\n" +
        "    \"auction.initialBid\" as initialBid,\n" +
        "    \"auction.reserve\" as reserve,\n" +
        "    \"auction.dateTime\" AS dateTime,\n" +
        "    \"auction.expires\" AS expires,\n" +
        "    \"auction.seller\" as seller,\n" +
        "    \"auction.category\" as category,\n" +
        "    \"auction.extra\" as extra\n" +
        "FROM NEXMARK_TABLE WHERE event_type = 1",
        "CREATE VIEW bid AS\n" +
        "SELECT\n" +
        "    \"bid.auction\" AS auction,\n" +
        "    \"bid.bidder\" AS bidder,\n" +
        "    \"bid.price\" AS price,\n" +
        "    \"bid.channel\" AS channel,\n" +
        "    \"bid.url\" AS url,\n" +
        "    \"bid.dateTime\" AS dateTime,\n" +
        "    \"bid.extra\" as extra\n" +
        "FROM NEXMARK_TABLE WHERE event_type = 2"
    };

    static final String[] queries = {
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 0: Pass through (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- This measures the monitoring overhead of the Flink SQL implementation including the source generator.\n" +
            "-- Using `bid` events here, as they are most numerous with default configuration.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW q0 AS " +
            "SELECT auction, bidder, price, dateTime, extra FROM bid",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query1: Currency conversion\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Convert each bid value from dollars to euros. Illustrates a simple transformation.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW q1 AS\n" +
            "SELECT\n" +
            "    auction,\n" +
            "    bidder,\n" +
            "    0.908 * price as price, -- convert dollar to euro\n" +
            "    dateTime,\n" +
            "    extra\n" +
            "FROM bid",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query2: Selection\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Find bids with specific auction ids and show their bid price.\n" +
            "--\n" +
            "-- In original Nexmark queries, Query2 is as following (in CQL syntax):\n" +
            "--\n" +
            "--   SELECT Rstream(auction, price)\n" +
            "--   FROM Bid [NOW]\n" +
            "--   WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;\n" +
            "--\n" +
            "-- However, that query will only yield a few hundred results over event streams of arbitrary size.\n" +
            "-- To make it more interesting we instead choose bids for every 123'th auction.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW q0 AS " +
            "SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0\n",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 3: Local Item Suggestion\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Who is selling in OR, ID or CA in category 10, and for what auction ids?\n" +
            "-- Illustrates an incremental join (using per-key state and timer) and filter.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW q2 AS " +
            "SELECT\n" +
            "    P.name, P.city, P.state, A.id\n" +
            "FROM\n" +
            "    auction AS A INNER JOIN person AS P on A.seller = P.id\n" +
            "WHERE\n" +
            "    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA')",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 4: Average Price for a Category\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Select the average of the wining bid prices for all auctions in each category.\n" +
            "-- Illustrates complex join and aggregation.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "CREATE VIEW q4 AS\n" +
            "SELECT\n" +
            "    Q.category,\n" +
            "    AVG(Q.final)\n" +
            "FROM (\n" +
            "    SELECT MAX(B.price) AS final, A.category\n" +
            "    FROM auction A, bid B\n" +
            "    WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires\n" +
            "    GROUP BY A.id, A.category\n" +
            ") Q\n" +
            "GROUP BY Q.category",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 5: Hot Items\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Which auctions have seen the most bids in the last period?\n" +
            "-- Illustrates sliding windows and combiners.\n" +
            "--\n" +
            "-- The original Nexmark Query5 calculate the hot items in the last hour (updated every minute).\n" +
            "-- To make things a bit more dynamic and easier to test we use much shorter windows,\n" +
            "-- i.e. in the last 10 seconds and update every 2 seconds.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW q5 AS\n" +
            "SELECT AuctionBids.auction, AuctionBids.num\n" +
            " FROM (\n" +
            "   SELECT\n" +
            "     B1.auction,\n" +
            "     count(*) AS num,\n" +
            "     HOP_START(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,\n" +
            "     HOP_END(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime\n" +
            "   FROM bid B1\n" +
            "   GROUP BY\n" +
            "     B1.auction,\n" +
            "     HOP(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)\n" +
            " ) AS AuctionBids\n" +
            " JOIN (\n" +
            "   SELECT\n" +
            "     max(CountBids.num) AS maxn,\n" +
            "     CountBids.starttime,\n" +
            "     CountBids.endtime\n" +
            "   FROM (\n" +
            "     SELECT\n" +
            "       count(*) AS num,\n" +
            "       HOP_START(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,\n" +
            "       HOP_END(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime\n" +
            "     FROM bid B2\n" +
            "     GROUP BY\n" +
            "       B2.auction,\n" +
            "       HOP(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)\n" +
            "     ) AS CountBids\n" +
            "   GROUP BY CountBids.starttime, CountBids.endtime\n" +
            " ) AS MaxBids\n" +
            " ON AuctionBids.starttime = MaxBids.starttime AND\n" +
            "    AuctionBids.endtime = MaxBids.endtime AND\n" +
            "    AuctionBids.num >= MaxBids.maxn",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 6: Average Selling Price by Seller\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- What is the average selling price per seller for their last 10 closed auctions.\n" +
            "-- Shares the same ‘winning bids’ core as for Query4, and illustrates a specialized combiner.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q6 AS\n" +
            "SELECT\n" +
            "    Q.seller,\n" +
            "    AVG(Q.final) OVER\n" +
            "        (PARTITION BY Q.seller ORDER BY Q.dateTime ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)\n" +
            "FROM (\n" +
            "    SELECT MAX(B.price) AS final, A.seller, B.dateTime\n" +
            "    FROM auction AS A, bid AS B\n" +
            "    WHERE A.id = B.auction and B.dateTime between A.dateTime and A.expires\n" +
            "    GROUP BY A.id, A.seller\n" +
            ") AS Q",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 7: Highest Bid\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- What are the highest bids per period?\n" +
            "-- Deliberately implemented using a side input to illustrate fanout.\n" +
            "--\n" +
            "-- The original Nexmark Query7 calculate the highest bids in the last minute.\n" +
            "-- We will use a shorter window (10 seconds) to help make testing easier.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q7 AS\n" +
            "SELECT B.auction, B.price, B.bidder, B.dateTime, B.extra\n" +
            "from bid B\n" +
            "JOIN (\n" +
            "  SELECT MAX(B1.price) AS maxprice, TUMBLE_ROWTIME(B1.dateTime, INTERVAL '10' SECOND) as dateTime\n" +
            "  FROM bid B1\n" +
            "  GROUP BY TUMBLE(B1.dateTime, INTERVAL '10' SECOND)\n" +
            ") B1\n" +
            "ON B.price = B1.maxprice\n" +
            "WHERE B.dateTime BETWEEN B1.dateTime  - INTERVAL '10' SECOND AND B1.dateTime",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 8: Monitor New Users\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Select people who have entered the system and created auctions in the last period.\n" +
            "-- Illustrates a simple join.\n" +
            "--\n" +
            "-- The original Nexmark Query8 monitors the new users the last 12 hours, updated every 12 hours.\n" +
            "-- To make things a bit more dynamic and easier to test we use much shorter windows (10 seconds).\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q8 AS\n" +
            "SELECT P.id, P.name, P.starttime\n" +
            "FROM (\n" +
            "  SELECT P.id, P.name,\n" +
            "         TUMBLE_START(P.dateTime, INTERVAL '10' SECOND) AS starttime,\n" +
            "         TUMBLE_END(P.dateTime, INTERVAL '10' SECOND) AS endtime\n" +
            "  FROM person P\n" +
            "  GROUP BY P.id, P.name, TUMBLE(P.dateTime, INTERVAL '10' SECOND)\n" +
            ") P\n" +
            "JOIN (\n" +
            "  SELECT A.seller,\n" +
            "         TUMBLE_START(A.dateTime, INTERVAL '10' SECOND) AS starttime,\n" +
            "         TUMBLE_END(A.dateTime, INTERVAL '10' SECOND) AS endtime\n" +
            "  FROM auction A\n" +
            "  GROUP BY A.seller, TUMBLE(A.dateTime, INTERVAL '10' SECOND)\n" +
            ") A\n" +
            "ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 9: Winning Bids (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Find the winning bid for each auction.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q9 AS\n" +
            "SELECT\n" +
            "    id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra,\n" +
            "    auction, bidder, price, bid_dateTime, bid_extra\n" +
            "FROM (\n" +
            "   SELECT A.*, B.auction, B.bidder, B.price, B.dateTime AS bid_dateTime, B.extra AS bid_extra,\n" +
            "     ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.dateTime ASC) AS rownum\n" +
            "   FROM auction A, bid B\n" +
            "   WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires\n" +
            ")\n" +
            "WHERE rownum <= 1",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 10: Log to File System (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Log all events to file system. Illustrates windows streaming data into partitioned file system.\n" +
            "--\n" +
            "-- Every minute, save all events from the last period into partitioned log files.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q10 AS -- PARTITIONED BY (dt, hm) AS\n" +
            "SELECT auction, bidder, price, dateTime, extra, DATE_FORMAT(dateTime, 'yyyy-MM-dd'), DATE_FORMAT(dateTime, 'HH:mm')\n" +
            "FROM bid",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 11: User Sessions (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- How many bids did a user make in each session they were active? Illustrates session windows.\n" +
            "--\n" +
            "-- Group bids by the same user into sessions with max session gap.\n" +
            "-- Emit the number of bids per session.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q11 AS\n" +
            "SELECT\n" +
            "    B.bidder,\n" +
            "    count(*) as bid_count,\n" +
            "    SESSION_START(B.dateTime, INTERVAL '10' SECOND) as starttime,\n" +
            "    SESSION_END(B.dateTime, INTERVAL '10' SECOND) as endtime\n" +
            "FROM bid B\n" +
            "GROUP BY B.bidder, SESSION(B.dateTime, INTERVAL '10' SECOND)",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 12: Processing Time Windows (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- How many bids does a user make within a fixed processing time limit?\n" +
            "-- Illustrates working in processing time window.\n" +
            "--\n" +
            "-- Group bids by the same user into processing time windows of 10 seconds.\n" +
            "-- Emit the count of bids per window.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q12 AS\n" +
            "SELECT\n" +
            "    B.bidder,\n" +
            "    count(*) as bid_count,\n" +
            "    TUMBLE_START(B.p_time, INTERVAL '10' SECOND) as starttime,\n" +
            "    TUMBLE_END(B.p_time, INTERVAL '10' SECOND) as endtime\n" +
            "FROM (SELECT *, PROCTIME() as p_time FROM bid) B\n" +
            "GROUP BY B.bidder, TUMBLE(B.p_time, INTERVAL '10' SECOND)",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 13: Bounded Side Input Join (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Joins a stream to a bounded side input, modeling basic stream enrichment.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q13 AS\n" +
            "SELECT\n" +
            "    B.auction,\n" +
            "    B.bidder,\n" +
            "    B.price,\n" +
            "    B.dateTime,\n" +
            "    S.value\n" +
            "FROM (SELECT *, PROCTIME() as p_time FROM bid) B\n" +
            "JOIN side_input FOR SYSTEM_TIME AS OF B.p_time AS S\n" +
            "ON mod(B.auction, 10000) = S.key",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 14: Calculation (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Convert bid timestamp into types and find bids with specific price.\n" +
            "-- Illustrates duplicate expressions and usage of user-defined-functions.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "-- CREATE FUNCTION count_char AS 'com.github.nexmark.flink.udf.CountChar';\n" +
            "\n" +
            "CREATE VIEW Q14 AS\n" +
            "SELECT \n" +
            "    auction,\n" +
            "    bidder,\n" +
            "    0.908 * price as price,\n" +
            "    CASE\n" +
            "        WHEN HOUR(dateTime) >= 8 AND HOUR(dateTime) <= 18 THEN 'dayTime'\n" +
            "        WHEN HOUR(dateTime) <= 6 OR HOUR(dateTime) >= 20 THEN 'nightTime'\n" +
            "        ELSE 'otherTime'\n" +
            "    END AS bidTimeType,\n" +
            "    dateTime,\n" +
            "    extra,\n" +
            "    count_char(extra, 'c') AS c_counts\n" +
            "FROM bid\n" +
            "WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 15: Bidding Statistics Report (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- How many distinct users join the bidding for different level of price?\n" +
            "-- Illustrates multiple distinct aggregations with filters.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q15 AS\n" +
            "SELECT\n" +
            "     DATE_FORMAT(dateTime, 'yyyy-MM-dd') as 'day',\n" +
            "     count(*) AS total_bids,\n" +
            "     count(*) filter (where price < 10000) AS rank1_bids,\n" +
            "     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,\n" +
            "     count(*) filter (where price >= 1000000) AS rank3_bids,\n" +
            "     count(distinct bidder) AS total_bidders,\n" +
            "     count(distinct bidder) filter (where price < 10000) AS rank1_bidders,\n" +
            "     count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,\n" +
            "     count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,\n" +
            "     count(distinct auction) AS total_auctions,\n" +
            "     count(distinct auction) filter (where price < 10000) AS rank1_auctions,\n" +
            "     count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,\n" +
            "     count(distinct auction) filter (where price >= 1000000) AS rank3_auctions\n" +
            "FROM bid\n" +
            "GROUP BY DATE_FORMAT(dateTime, 'yyyy-MM-dd')",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 16: Channel Statistics Report (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- How many distinct users join the bidding for different level of price for a channel?\n" +
            "-- Illustrates multiple distinct aggregations with filters for multiple keys.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q16 AS\n" +
            "SELECT\n" +
            "    channel,\n" +
            "    DATE_FORMAT(dateTime, 'yyyy-MM-dd') as 'day',\n" +
            "    max(DATE_FORMAT(dateTime, 'HH:mm')) as 'minute',\n" +
            "    count(*) AS total_bids,\n" +
            "    count(*) filter (where price < 10000) AS rank1_bids,\n" +
            "    count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,\n" +
            "    count(*) filter (where price >= 1000000) AS rank3_bids,\n" +
            "    count(distinct bidder) AS total_bidders,\n" +
            "    count(distinct bidder) filter (where price < 10000) AS rank1_bidders,\n" +
            "    count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,\n" +
            "    count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,\n" +
            "    count(distinct auction) AS total_auctions,\n" +
            "    count(distinct auction) filter (where price < 10000) AS rank1_auctions,\n" +
            "    count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,\n" +
            "    count(distinct auction) filter (where price >= 1000000) AS rank3_auctions\n" +
            "FROM bid\n" +
            "GROUP BY channel, DATE_FORMAT(dateTime, 'yyyy-MM-dd')",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 17: Auction Statistics Report (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- How many bids on an auction made a day and what is the price?\n" +
            "-- Illustrates an unbounded group aggregation.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q17 AS\n" +
            "SELECT\n" +
            "     auction,\n" +
            "     DATE_FORMAT(dateTime, 'yyyy-MM-dd') as 'day',\n" +
            "     count(*) AS total_bids,\n" +
            "     count(*) filter (where price < 10000) AS rank1_bids,\n" +
            "     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,\n" +
            "     count(*) filter (where price >= 1000000) AS rank3_bids,\n" +
            "     min(price) AS min_price,\n" +
            "     max(price) AS max_price,\n" +
            "     avg(price) AS avg_price,\n" +
            "     sum(price) AS sum_price\n" +
            "FROM bid\n" +
            "GROUP BY auction, DATE_FORMAT(dateTime, 'yyyy-MM-dd')",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 18: Find last bid (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- What's a's last bid for bidder to auction?\n" +
            "-- Illustrates a Deduplicate query.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q18 AS\n" +
            "SELECT auction, bidder, price, channel, url, dateTime, extra\n" +
            " FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY dateTime DESC) AS rank_number\n" +
            "       FROM bid)\n" +
            " WHERE rank_number <= 1",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 19: Auction TOP-10 Price (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- What's the top price 10 bids of an auction?\n" +
            "-- Illustrates a TOP-N query.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q19 AS\n" +
            "SELECT * FROM\n" +
            "(SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid)\n" +
            "WHERE rank_number <= 10",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 20: Expand bid with auction (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Get bids with the corresponding auction information where category is 10.\n" +
            "-- Illustrates a filter join.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q20 AS\n" +
            "SELECT\n" +
            "    auction, bidder, price, channel, url, B.dateTime, B.extra,\n" +
            "    itemName, description, initialBid, reserve, A.dateTime, expires, seller, category, A.extra\n" +
            "FROM\n" +
            "    bid AS B INNER JOIN auction AS A on B.auction = A.id\n" +
            "WHERE A.category = 10",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 21: Add channel id (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Add a channel_id column to the bid table.\n" +
            "-- Illustrates a 'CASE WHEN' + 'REGEXP_EXTRACT' SQL.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q21 AS\n" +
            "SELECT\n" +
            "    auction, bidder, price, channel,\n" +
            "    CASE\n" +
            "        WHEN lower(channel) = 'apple' THEN '0'\n" +
            "        WHEN lower(channel) = 'google' THEN '1'\n" +
            "        WHEN lower(channel) = 'facebook' THEN '2'\n" +
            "        WHEN lower(channel) = 'baidu' THEN '3'\n" +
            "        ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2)\n" +
            "        END\n" +
            "    AS channel_id FROM bid\n" +
            "    where REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) is not null or\n" +
            "          lower(channel) in ('apple', 'google', 'facebook', 'baidu')",

            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- Query 22: Get URL Directories (Not in original suite)\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "-- What is the directory structure of the URL?\n" +
            "-- Illustrates a SPLIT_INDEX SQL.\n" +
            "-- -------------------------------------------------------------------------------------------------\n" +
            "\n" +
            "CREATE VIEW Q22 AS\n" +
            "SELECT\n" +
            "    auction, bidder, price, channel,\n" +
            "    SPLIT_INDEX(url, '/', 3) as dir1,\n" +
            "    SPLIT_INDEX(url, '/', 4) as dir2,\n" +
            "    SPLIT_INDEX(url, '/', 5) as dir3 FROM bid"
    };

    //@Test
    public void testCompile() {
        CompilerOptions options = new CompilerOptions();
        options.languageOptions.lexicalRules = Lex.ORACLE;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.compileStatement(table);
        for (String view: views)
            compiler.compileStatement(view);
        for (String query: queries)
            compiler.compileStatement(query);
        compiler.throwIfErrorsOccurred();
        DBSPCircuit circuit = getCircuit(compiler);
        Assert.assertNotNull(circuit);
    }
}
