package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/master/src/test/regress/expected/interval.out
public class PostgresIntervalTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        // was a table, but we don't support intervals in tables
        compiler.compileStatements("""
                CREATE LOCAL VIEW SHORT_INTERVAL_TBL(i) AS SELECT * FROM (VALUES
                (INTERVAL 1 MINUTES),
                (INTERVAL 5 HOUR),
                (INTERVAL 10 DAY),
                (INTERVAL -14 SECONDS),
                (INTERVAL '1 2:03:04' DAY to SECONDS));
                
                CREATE LOCAL VIEW LONG_INTERVAL_TBL(i) AS SELECT * FROM (VALUES
                (INTERVAL 34 YEAR),
                (INTERVAL 3 MONTHS),
                (INTERVAL 6 YEARS),
                (INTERVAL -5 MONTHS),
                (INTERVAL -'10-5' YEARS TO MONTHS));""");
    }

    @Test
    public void testCalciteExamples() {
        this.q("""
                SELECT INTERVAL '1-5' YEAR TO MONTH, INTERVAL '45' DAY, INTERVAL '1 2:34:56.789' DAY TO SECOND;
                i0 | i1 | i2
                ------------
                1 year 5 months | 45 days | 1 day 02:34:56.789""");
    }

    @Test
    public void testInterval() {
        // Had to add the symbolic intervals by hand in many places
        // Postgres allows individual signs on fragments, the
        // standard doesn't
        this.qs("""
                SELECT INTERVAL '01:00' HOUR TO MINUTE AS "One hour";
                 One hour
                ----------
                 01:00:00
                (1 row)

                SELECT INTERVAL '+02:00' HOUR TO MINUTE AS "Two hours";
                 Two hours
                -----------
                 02:00:00
                (1 row)

                SELECT INTERVAL '-08:00' HOUR TO MINUTE AS "Eight hours";
                 Eight hours
                -------------
                 -08:00:00
                (1 row)

                SELECT INTERVAL '-1 02:03' DAYS TO MINUTE AS "26 hours ago...";
                  26 hours ago...
                -------------------
                 -1 days 02:03:00
                (1 row)

                SELECT INTERVAL '-1 02:03' DAYS TO MINUTES AS "26 hours ago...";
                  26 hours ago...
                -------------------
                 -1 days 02:03:00
                (1 row)
                """);
                // Not supported by Calcite
                //"SELECT INTERVAL '1.5' WEEKS AS \"Ten days twelve hours\";\n" +
                //" Ten days twelve hours \n" +
                //"-----------------------\n" +
                //" 10 days 12:00:00\n" +
                //"(1 row)\n" +
                //"\n" +
                //"SELECT INTERVAL '1.5' MONTHS AS \"One month 15 days\";\n" +
                //" One month 15 days \n" +
                //"-------------------\n" +
                //" 1 mon 15 days\n" +
                //"(1 row)\n" +
                //"\n" +
                // Not supported by Calcite
                //"SELECT INTERVAL '10 11 12 +13:14' years to minutes AS \"9 years...\";\n" +
                //"            9 years...            \n" +
                //"----------------------------------\n" +
                //" 9 years 1 mon -12 days +13:14:00\n" +
                //"(1 row)");
    }

    @Test
    public void testIntervalOperators() {
        this.qs("""
                SELECT * FROM SHORT_INTERVAL_TBL;
                 i
                ---
                 1 mins
                 5 hours
                 10 days
                 -14 secs
                 1 day 2 hours 3 mins 4 secs
                (5 rows)
                
                SELECT * FROM LONG_INTERVAL_TBL;
                 i
                ---
                 34 years
                 3 months
                 6 years
                 5 months ago
                 10 years 5 months ago
                (5 rows)
                
                SELECT * FROM SHORT_INTERVAL_TBL WHERE i <> INTERVAL 10 DAYS;
                 i
                ---
                 1 mins
                 5 hours
                 -14 secs
                 1 day 2 hours 3 mins 4 secs
                (4 rows)
                
                SELECT * FROM SHORT_INTERVAL_TBL WHERE i <= INTERVAL 5 HOURS;
                 i
                ---
                 1 mins
                 5 hours
                 -14 secs
                (3 rows)
                
                SELECT * FROM SHORT_INTERVAL_TBL WHERE i < INTERVAL 1 DAY;
                 i
                ---
                 1 mins
                 5 hours
                 -14 secs
                (3 rows)
                
                SELECT * FROM LONG_INTERVAL_TBL WHERE i = INTERVAL 34 YEARS;
                 i
                ---
                 34 years
                (1 row)
                
                SELECT * FROM LONG_INTERVAL_TBL WHERE i >= INTERVAL 1 MONTH;
                 i
                ---
                 34 years
                 3 months
                 6 years
                (3 rows)
                
                SELECT * FROM SHORT_INTERVAL_TBL WHERE i > INTERVAL -3 SECONDS;
                 i
                ---
                 1 mins
                 5 hours
                 10 days
                 1 day 2 hours 3 mins 4 secs
                (4 rows)
                
                SELECT * FROM LONG_INTERVAL_TBL WHERE i < INTERVAL 1 YEAR;
                 i
                ---
                 3 months
                 5 months ago
                 10 years 5 months ago
                (3 rows)""");
    }

    @Test
    public void testIntervalOperations() {
        this.qs("""
                SELECT r1.*, r2.*
                FROM SHORT_INTERVAL_TBL r1, SHORT_INTERVAL_TBL r2
                WHERE r1.i > r2.i;
                       f1        |       f1
                -----------------+-----------------
                 00:01:00        | -00:00:14
                 05:00:00        | -00:00:14
                 05:00:00        | 00:01:00
                 1 day 02:03:04  | -00:00:14
                 1 day 02:03:04  | 00:01:00
                 1 day 02:03:04  | 05:00:00
                 10 days         | -00:00:14
                 10 days         | 00:01:00
                 10 days         | 05:00:00
                 10 days         | 1 day 02:03:04
                (10 rows)
                
                SELECT i, -i FROM SHORT_INTERVAL_TBL;
                       i         |       -i
                -----------------+-----------------
                 00:01:00        | -00:01:00
                 05:00:00        | -05:00:00
                 10 days         | -10 days
                 -00:00:14       | 00:00:14
                 1 day 02:03:04  | -1 days 02:03:04
                (5 rows)
                
                SELECT i, -i FROM LONG_INTERVAL_TBL;
                       i         |       -i
                -----------------+-----------------
                 34 years        | 34 years ago
                 6 years         | 6 years ago
                 3 months        | 3 months ago
                 5 months ago    | 5 months
                 10 years 5 months ago | 10 years 5 months
                (5 rows)""");
    }

    @Test @Ignore("Calcite simplification bugs")
    public void testLimits() {
        this.qf("SELECT -(INTERVAL -2147483648 months)",
                "Overflow during LongInterval negation");
        this.qf("SELECT -(INTERVAL -9223372036854775.808 SECONDS)", "");
        this.qf("SELECT -(INTERVAL -2147483648 days)", "");
        this.qf("SELECT INTERVAL 2147483647 years", "Out of range");
    }

    @Test
    public void testLimitsPositive() {
        this.qs("""
                SELECT -(INTERVAL -2147483647 months);
                        ?column?
                ------------------------
                 178956970 years 7 months
                (1 row)

                SELECT -(INTERVAL -2147483647 days);
                    ?column?
                -----------------
                 2147483647 days
                (1 row)

                SELECT -(INTERVAL -9223372036854.775807 SECONDS);
                        ?column?
                -------------------------
                 2562047788 hours 00 mins 54.775 secs
                (1 row)""");
    }

    @Test
    public void testIntervalSub() {
        this.qs("""
                SELECT i - i FROM SHORT_INTERVAL_TBL;
                 ?column?
                ----------
                 00:00:00
                 00:00:00
                 00:00:00
                 00:00:00
                 00:00:00
                (5 rows)""");
    }

    @Test @Ignore("Calcite simplification bugs")
    public void testOverflows() {
        this.qf("SELECT INTERVAL 3000000 months * 1000",
                "Overflow in LongInterval multiplication");
        this.qf("SELECT INTERVAL 3000000 months / .0001",
                "Overflow in LongInterval division");
    }
    
    @Test
    public void literals() {
        this.qs("""
                SELECT interval '999' second;  -- oversize leading field is ok
                 interval
                ----------
                 00:16:39
                (1 row)
                
                SELECT interval '999' minute;
                 interval
                ----------
                 16:39:00
                (1 row)
                
                SELECT interval '999' hour;
                 interval
                -----------
                 999 hours
                (1 row)
                
                SELECT interval '999' day;
                 interval
                ----------
                 999 days
                (1 row)
                
                SELECT interval '999' month;
                    interval
                -----------------
                 83 years 3 months
                (1 row)
                
                -- test SQL-spec syntaxes for restricted field sets
                SELECT interval '1' year;
                 interval
                ----------
                 1 year
                (1 row)
                
                SELECT interval '2' month;
                 interval
                ----------
                 2 months
                (1 row)
                
                SELECT interval '3' day;
                 interval
                ----------
                 3 days
                (1 row)
                
                SELECT interval '4' hour;
                 interval
                ----------
                 04:00:00
                (1 row)
                
                SELECT interval '5' minute;
                 interval
                ----------
                 00:05:00
                (1 row)
                
                SELECT interval '6' second;
                 interval
                ----------
                 00:00:06
                (1 row)
                
                SELECT interval '1-2' year to month;
                   interval
                ---------------
                 1 year 2 months
                (1 row)
                
                SELECT interval '1 2' day to hour;
                    interval
                ----------------
                 1 day 02:00:00
                (1 row)
                
                SELECT interval '1 2:03' day to minute;
                    interval
                ----------------
                 1 day 02:03:00
                (1 row)
                
                SELECT interval '1 2:03:04' day to second;
                    interval
                ----------------
                 1 day 02:03:04
                (1 row)
                
                SELECT interval '123 11' day to hour;
                     interval
                -------------------
                 123 days 11:00:00
                (1 row)
                """);
        // Following ones are legal in Postgres but not in Calcite
        String ill = "Illegal interval literal";
        this.queryFailingInCompilation("SELECT interval '1' year to month", ill);
        this.queryFailingInCompilation("SELECT interval '1 2:03' day to hour", ill);
        this.queryFailingInCompilation("SELECT interval '1 2:03:04' day to hour", ill);
        this.queryFailingInCompilation("SELECT interval '1 2:03:04' day to minute", ill);
        this.queryFailingInCompilation("SELECT interval '1 2:03' day to second", ill);
        this.queryFailingInCompilation("SELECT interval '1 2:03:04' hour to minute", ill);
        this.queryFailingInCompilation("SELECT interval '1 2:03' hour to second", ill);
        this.queryFailingInCompilation("SELECT interval '1 2:03:04' hour to second", ill);
        this.queryFailingInCompilation("SELECT interval '1 2:03' minute to second", ill);
        this.queryFailingInCompilation("SELECT interval '1 2:03:04' minute to second", ill);
        this.queryFailingInCompilation("SELECT interval '1 2:03' hour to minute", ill);
        this.queryFailingInCompilation("SELECT interval '1 +2:03' minute to second", ill);
        this.queryFailingInCompilation("SELECT interval '1 +2:03:04' minute to second", ill);
        this.queryFailingInCompilation("SELECT interval '1 -2:03' minute to second", ill);
        this.queryFailingInCompilation("SELECT interval '1 -2:03:04' minute to second", ill);
        this.queryFailingInCompilation("SELECT interval '1 2' hour to second", ill);
        this.queryFailingInCompilation("SELECT interval '1 2' minute to second", ill);
        this.queryFailingInCompilation("SELECT interval '1 2' day to second", ill);
        this.queryFailingInCompilation("SELECT interval '1 2' day to minute", ill);
        this.queryFailingInCompilation("SELECT interval '1 2' hour to minute", ill);
    }

    // skipped tests that require precision in intervals

    @Test
    public void testCast() {
        // Have to check the spec for the expected result
        this.qs("""
                SELECT CAST(i AS INTERVAL YEAR) FROM LONG_INTERVAL_TBL;
                 i
                ---
                 5 months ago
                 3 months
                 72 months
                 408 months
                 125 months ago
                (5 rows)""");
    }

    @Test
    public void checkExtract() {
        // I think our definition of EXTRACT(EPOCH) is saner than Postgres
        this.qs("""
                SELECT i,
                    EXTRACT(MICROSECOND FROM i) AS MICROSECOND,
                    EXTRACT(MILLISECOND FROM i) AS MILLISECOND,
                    EXTRACT(SECOND FROM i) AS SECOND,
                    EXTRACT(MINUTE FROM i) AS MINUTE,
                    EXTRACT(HOUR FROM i) AS HOUR,
                    EXTRACT(DAY FROM i) AS DAY,
                    EXTRACT(EPOCH FROM i) AS EPOCH
                    FROM SHORT_INTERVAL_TBL;
                            i                | microsecond | millisecond |   second   | minute |   hour    |    day    |       epoch
                -----------------------------+-------------+-------------+------------+--------+-----------+-----------+-------------------
                 1 min                       |           0 |       0     |   0        |      1 |         0 |         0 |         60
                 5 hours                     |           0 |       0     |   0        |      0 |         5 |         0 |      18000
                 10 days                     |           0 |       0     |   0        |      0 |         0 |        10 |     864000
                 14 secs ago                 |   -14000000 |  -14000     | -14        |      0 |         0 |         0 |        -14
                 1 day 2 hours 3 mins 4 secs |     4000000 |    4000     |   4        |      3 |         2 |         1 |      93784
                (5 rows)
                
                SELECT i,
                    EXTRACT(MICROSECOND FROM i) AS MICROSECOND,
                    EXTRACT(MILLISECOND FROM i) AS MILLISECOND,
                    EXTRACT(SECOND FROM i) AS SECOND,
                    EXTRACT(MINUTE FROM i) AS MINUTE,
                    EXTRACT(HOUR FROM i) AS HOUR,
                    EXTRACT(DAY FROM i) AS DAY,
                    EXTRACT(MONTH FROM i) AS MONTH,
                    EXTRACT(QUARTER FROM i) AS QUARTER,
                    EXTRACT(YEAR FROM i) AS YEAR,
                    EXTRACT(DECADE FROM i) AS DECADE,
                    EXTRACT(CENTURY FROM i) AS CENTURY,
                    EXTRACT(MILLENNIUM FROM i) AS MILLENNIUM,
                    EXTRACT(EPOCH FROM i) AS EPOCH
                    FROM LONG_INTERVAL_TBL;
                            i                | microsecond | millisecond |   second   | minute |   hour    |    day    | month | quarter |   year    |  decade   |  century  | millennium |       epoch
                -----------------------------+-------------+-------------+------------+--------+-----------+-----------+-------+---------+-----------+-----------+-----------+------------+-------------------
                 34 years                    |           0 |       0     |   0        |      0 |         0 |         0 |     0 |       1 |        34 |         3 |         0 |          0 | 1072915200
                 3 months                    |           0 |       0     |   0        |      0 |         0 |         0 |     3 |       2 |         0 |         0 |         0 |          0 |    7776000
                 6 years                     |           0 |       0     |   0        |      0 |         0 |         0 |     0 |       1 |         6 |         0 |         0 |          0 |  189302400
                 5 months ago                |           0 |       0     |   0        |      0 |         0 |         0 |    -5 |       3 |         0 |         0 |         0 |          0 |  -13219200
                 10 years 5 months ago       |           0 |       0     |   0        |      0 |         0 |         0 |    -5 |       3 |       -10 |        -1 |         0 |          0 | -328838400
                (5 rows)""");
    }

    @Test
    public void testLongExtract() {
        this.qs("""
                SELECT EXTRACT(DECADE FROM INTERVAL '100' years);
                 extract
                ---------
                      10
                (1 row)
                
                SELECT EXTRACT(DECADE FROM INTERVAL '99' years);
                 extract
                ---------
                       9
                (1 row)
                
                SELECT EXTRACT(DECADE FROM INTERVAL '-99' years);
                 extract
                ---------
                      -9
                (1 row)
                
                SELECT EXTRACT(DECADE FROM INTERVAL '-100' years);
                 extract
                ---------
                     -10
                (1 row)
                
                SELECT EXTRACT(CENTURY FROM INTERVAL '100' years);
                 extract
                ---------
                       1
                (1 row)
                
                SELECT EXTRACT(CENTURY FROM INTERVAL '99' years);
                 extract
                ---------
                       0
                (1 row)
                
                SELECT EXTRACT(CENTURY FROM INTERVAL '-99' years);
                 extract
                ---------
                       0
                (1 row)
                
                SELECT EXTRACT(CENTURY FROM INTERVAL '-100' years);
                 extract
                ---------
                      -1
                (1 row)""");
    }

    @Test
    public void testCastString() {
        // These are not from postgres
        this.qs("""
                SELECT CAST(INTERVAL 1 YEARS AS VARCHAR);
                 x
                ---
                 +1
                (1 row)
                
                SELECT CAST(INTERVAL 22 MONTHS AS VARCHAR);
                 x
                ---
                 +22
                (1 row)
                
                SELECT CAST(INTERVAL '1-10' YEARS TO MONTHS AS VARCHAR);
                 x
                ---
                 +1-10
                (1 row)
                
                SELECT CAST(INTERVAL -22 MONTHS AS VARCHAR);
                 x
                ---
                 -22
                (1 row)
                
                SELECT CAST(INTERVAL -1 YEARS AS VARCHAR);
                 x
                ---
                 -1
                (1 row)
                
                SELECT CAST(INTERVAL '-1-10' YEARS TO MONTHS AS VARCHAR);
                 x
                ---
                 -1-10
                (1 row)""");
    }

    @Test
    public void doubleCastTest() {
        this.qs("""
                SELECT CAST(CAST(INTERVAL '1 02:03:04.5' DAYS TO SECONDS AS INTERVAL DAYS) AS VARCHAR);
                 x
                ---
                 +1
                (1 row)
                
                SELECT CAST(CAST(INTERVAL '1 02:03:04.5' DAYS TO SECONDS AS INTERVAL DAYS TO HOURS) AS VARCHAR);
                 x
                ---
                 +1 02
                (1 row)
                
                SELECT CAST(CAST(INTERVAL '1 02:03:04.5' DAYS TO SECONDS AS INTERVAL DAYS TO MINUTES) AS VARCHAR);
                 x
                ---
                 +1 02:03
                (1 row)
                
                SELECT CAST(CAST(INTERVAL '1 02:03:04.5' DAYS TO SECONDS AS INTERVAL DAYS TO SECONDS) AS VARCHAR);
                 x
                ---
                 +1 02:03:04.000000
                (1 row)

                SELECT CAST(CAST(INTERVAL '1 02:03:04.5' DAYS TO SECONDS AS INTERVAL HOURS) AS VARCHAR);
                 x
                ---
                 +26
                (1 row)
                
                SELECT CAST(CAST(INTERVAL '1 02:03:04.5' DAYS TO SECONDS AS INTERVAL HOURS TO MINUTES) AS VARCHAR);
                 x
                ---
                 +26:03
                (1 row)
                
                SELECT CAST(CAST(INTERVAL '1 02:03:04.5' DAYS TO SECONDS AS INTERVAL HOURS TO SECONDS) AS VARCHAR);
                 x
                ---
                 +26:03:04.000000
                (1 row)
                
                SELECT CAST(CAST(INTERVAL '1 02:03:04.5' DAYS TO SECONDS AS INTERVAL MINUTES) AS VARCHAR);
                 x
                ---
                 +1563
                (1 row)
                
                SELECT CAST(CAST(INTERVAL '1 02:03:04.5' DAYS TO SECONDS AS INTERVAL MINUTES TO SECONDS) AS VARCHAR);
                 x
                ---
                 +1563:04.000000
                (1 row)
                
                SELECT CAST(CAST(INTERVAL '1 02:03:04.5' DAYS TO SECONDS AS INTERVAL SECONDS) AS VARCHAR);
                 x
                ---
                 +93784.000000
                (1 row)""");
    }

    @Test
    public void testCastShortIntervalToString() {
        this.qs("""                
                SELECT CAST(INTERVAL '1 02:03:04.5' DAYS TO SECONDS AS VARCHAR);
                 x
                ---
                 +1 02:03:04.000000
                (1 row)
                
                SELECT CAST(INTERVAL '1' DAYS AS VARCHAR);
                 x
                ---
                 +1
                (1 row)
                
                SELECT CAST(INTERVAL '1 02' DAYS TO HOURS AS VARCHAR);
                 x
                ---
                 +1 02
                (1 row)
                
                SELECT CAST(INTERVAL '1 02:03' DAYS TO MINUTES AS VARCHAR);
                 x
                ---
                 +1 02:03
                (1 row)
                
                SELECT CAST(INTERVAL '02:03:04.5' HOURS TO SECONDS AS VARCHAR);
                 x
                ---
                 +2:03:04.000000
                (1 row)
                
                SELECT CAST(INTERVAL '02:03' HOURS TO MINUTES AS VARCHAR);
                 x
                ---
                 +2:03
                (1 row)

                SELECT CAST(INTERVAL '2' HOURS AS VARCHAR);
                 x
                ---
                 +2
                (1 row)
                
                SELECT CAST(INTERVAL '3' MINUTES AS VARCHAR);
                 x
                ---
                 +3
                (1 row)
                
                SELECT CAST(INTERVAL '4.000' SECONDS AS VARCHAR);
                 x
                ---
                 +4.000000
                (1 row)

                SELECT CAST(INTERVAL '-1 02:03:04.5' DAYS TO SECONDS AS VARCHAR);
                 x
                ---
                 -1 02:03:04.000000
                (1 row)
                
                SELECT CAST(INTERVAL '-1' DAYS AS VARCHAR);
                 x
                ---
                 -1
                (1 row)
                
                SELECT CAST(INTERVAL '-1 02' DAYS TO HOURS AS VARCHAR);
                 x
                ---
                 -1 02
                (1 row)
                
                SELECT CAST(INTERVAL '-1 02:03' DAYS TO MINUTES AS VARCHAR);
                 x
                ---
                 -1 02:03
                (1 row)
                
                SELECT CAST(INTERVAL '-2:03' HOURS TO MINUTES AS VARCHAR);
                 x
                ---
                 -2:03
                (1 row)
                
                SELECT CAST(INTERVAL '-2' HOURS AS VARCHAR);
                 x
                ---
                 -2
                (1 row)
                
                SELECT CAST(INTERVAL '-3' MINUTES AS VARCHAR);
                 x
                ---
                 -3
                (1 row)
                
                SELECT CAST(INTERVAL '-02:03:04.5' HOURS TO SECONDS AS VARCHAR);
                 x
                ---
                 -2:03:04.000000
                (1 row)
                
                SELECT CAST(INTERVAL '-4.000' SECONDS AS VARCHAR);
                 x
                ---
                 -4.000000
                (1 row)""");
    }

    @Test
    public void testCastToInterval() {
        // These are not from postgres
        this.qs("""
                SELECT CAST(1 AS INTERVAL YEARS);
                 x
                ---
                 1 years
                (1 row)
                
                SELECT CAST(INTERVAL 22 MONTHS AS INTERVAL YEARS);
                 x
                ---
                 22 months
                (1 row)
                
                SELECT CAST(INTERVAL '1-10' YEAR TO MONTH AS INTERVAL YEARS);
                 x
                ---
                 22 months
                (1 row)""");
    }
}
