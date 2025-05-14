package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.util.Linq;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Regression1Tests extends SqlIoTest {
    @Test
    public void issue3952() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(t0 TIMESTAMP, t1 TIMESTAMP NOT NULL);
                
                CREATE LOCAL VIEW intervalv AS SELECT
                (t0 - TIMESTAMP '2025-06-21 14:23:44') YEAR AS yr0,
                (TIMESTAMP '2025-06-21 14:23:44' - t1) YEAR AS yr1,
                (t0 - TIMESTAMP '2025-06-21 14:23:44') DAYS AS d0,
                (TIMESTAMP '2025-06-21 14:23:44' - t1) DAYS AS d1
                FROM tbl;
                
                CREATE MATERIALIZED VIEW v AS SELECT
                CAST(ABS(yr0) AS BIGINT), CAST(ABS(yr1) AS BIGINT),
                CAST(ABS(d0) AS BIGINT), CAST(ABS(d1) AS BIGINT)
                FROM intervalv;""");
        ccs.step("INSERT INTO tbl VALUES(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2019-01-01 00:00:00')",
                """
                  v0 | v1 |  v2  |  v3  | weight
                 ----------------------------
                   5 |  6 | 1998 | 2363 | 1""");
    }

    @Test
    public void temporal() {
        var ccs = this.getCCS("""
                CREATE TABLE a (
                    c BIGINT,
                    j TIMESTAMP,
                    ad DOUBLE,
                    av VARCHAR,
                    bg DOUBLE,
                    bk SMALLINT
                );
                
                CREATE VIEW bl AS
                SELECT
                    bm.c AS bn,
                    bm.av AS bo,
                    SUM(CASE WHEN bm.bk = 0 THEN 1 ELSE 0 END) AS bp,
                    SUM(CASE WHEN bm.bk = 1 THEN 1 ELSE 0 END) AS bq,
                    COUNT(*) AS br,
                    SUM(CASE WHEN bm.bk = 0 THEN bm.bg ELSE 0.0 END) AS bs,
                    SUM(CASE WHEN bm.bk = 1 THEN bm.bg ELSE 0.0 END) AS bt,
                    SUM(bm.bg) AS bu
                FROM
                    a bm
                WHERE
                    bm.j >= NOW() - INTERVAL '1' year
                    AND bm.av IS NOT NULL
                    AND bm.bk IS NOT NULL
                    AND bm.bg IS NOT NULL
                    AND bm.ad IS NOT NULL
                    AND bm.bg > 0
                GROUP BY
                    bm.c,
                    bm.av;""");

        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String j = now.format(formatter);
        ccs.step("INSERT INTO A VALUES(NULL, '" + j + "', 0, 'a', NULL, 0)", """
                                     bn | bo | bp | bq | br | bs | bt | weight
                                    -------------------------------------------""");
        for (var c : Linq.list("NULL", "0")) {
            for (var ad : Linq.list("NULL", "0")) {
                for (var av: Linq.list("NULL", "'a'")) {
                    for (var bg: Linq.list("NULL", "0", "1")) {
                        for (var bk : Linq.list("NULL", "0")) {
                            String result = "";
                            if (nn(ad) && nn(av) && nn(bg) && nn(bk) && !bg.equals("0")) {
                                result = "\n " + c + " | a | " + (bk.equals("0") ? "1" : "0") + " | 0 | 1 | 1 | 0 | 1 | 1";
                            }
                            ccs.step("INSERT INTO A VALUES(" + c + ", '" + j + "', " + ad + ", " + av + ", " + bg + ", " + bk + ")", """
                                     bn | bo | bp | bq | br | bs | bt | weight
                                    -------------------------------------------""" + result);
                        }
                    }
                }
            }
        }
    }

    static boolean nn(String v) {
        return !v.equals("NULL");
    }

    @Test
    public void issue4010() {
        this.getCCS("""
                -- Customers.
                CREATE TABLE customer (
                    id BIGINT NOT NULL,
                    name varchar,
                    state VARCHAR,
                    -- Lateness annotation: customer records cannot arrive more than 7 days out of order.
                    ts TIMESTAMP LATENESS INTERVAL 7 DAYS
                );
                
                -- Credit card transactions.
                CREATE TABLE transaction (
                    -- Lateness annotation: transactions cannot arrive more than 1 day out of order.
                    ts TIMESTAMP LATENESS INTERVAL 1 DAYS,
                    amt DOUBLE,
                    customer_id BIGINT NOT NULL,
                    state VARCHAR
                );
                
                -- Data enrichment:
                -- * Use ASOF JOIN to find the most recent customer record for each transaction.
                -- * Compute 'out_of_state' flag, which indicates that the transaction was performed outside
                --   of the customer's home state.
                CREATE VIEW enriched_transaction AS
                SELECT
                    transaction.*,
                    (transaction.state != customer.state) AS out_of_state
                FROM
                    transaction LEFT ASOF JOIN customer
                    MATCH_CONDITION ( transaction.ts >= customer.ts )
                    ON transaction.customer_id = customer.id;
                
                -- Rolling aggregation: Compute the number of out-of-state transactions in the last 30 days for each transaction.
                CREATE VIEW transaction_with_history AS
                SELECT
                    *,
                    SUM(1) OVER window_30_day as out_of_state_count
                FROM
                    transaction
                WINDOW window_30_day AS (PARTITION BY customer_id ORDER BY ts RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW);""");
    }
}
