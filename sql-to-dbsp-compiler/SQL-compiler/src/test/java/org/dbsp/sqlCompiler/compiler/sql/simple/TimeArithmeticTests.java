package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

public class TimeArithmeticTests extends SqlIoTest {
    @Test
    public void testTimestampAddLongInterval() {
        this.qst("""
                SELECT TIMESTAMP '2024-01-01 10:23:45' + INTERVAL 10 MONTHS;
                        ts
                ---------------------
                 2024-11-01 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-01 10:23:45' + INTERVAL -10 MONTHS;
                        ts
                ---------------------
                 2023-03-01 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-01 10:23:45' - INTERVAL 10 MONTHS;
                        ts
                ---------------------
                 2023-03-01 10:23:45
                (1 row)

                SELECT TIMESTAMP '2024-01-01 10:23:45' - INTERVAL -10 MONTHS;
                        ts
                ---------------------
                 2024-11-01 10:23:45
                (1 row)

                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL 10 MONTHS;
                        ts
                ---------------------
                 2024-11-30 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL -10 MONTHS;
                        ts
                ---------------------
                 2023-03-31 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL -10 MONTHS;
                        ts
                ---------------------
                 2024-11-30 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL 1 MONTHS;
                        ts
                ---------------------
                 2024-02-29 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL 100 MONTHS;
                        ts
                ---------------------
                 2032-05-31 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL '8-4' YEARS TO MONTHS;
                        ts
                ---------------------
                 2032-05-31 10:23:45
                (1 row)

                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL -100 MONTHS;
                        ts
                ---------------------
                 2032-05-31 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL '-8-4' YEARS TO MONTHS;
                        ts
                ---------------------
                 2032-05-31 10:23:45
                (1 row)

                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL 100 MONTHS;
                        ts
                ---------------------
                 2015-09-30 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL '8-4' YEARS TO MONTHS;
                        ts
                ---------------------
                 2015-09-30 10:23:45
                (1 row)

                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL 10 YEARS;
                        ts
                ---------------------
                 2034-01-31 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL 10 YEARS;
                        ts
                ---------------------
                 2014-01-31 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' + INTERVAL -10 YEARS;
                        ts
                ---------------------
                 2014-01-31 10:23:45
                (1 row)
                
                SELECT TIMESTAMP '2024-01-31 10:23:45' - INTERVAL -10 YEARS;
                        ts
                ---------------------
                 2034-01-31 10:23:45
                (1 row)""");
    }

    @Test
    public void testTimeAddInterval() {
        this.qst("""
                SELECT '23:00:00'::time + INTERVAL '10' MINUTES;
                   time
                ----------
                 23:10:00
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '10' MINUTES;
                   time
                --------
                 00:10:00
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '-10' MINUTES;
                   time
                --------
                 23:50:00
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '2' DAYS;
                 time
                --------
                 00:00:00
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '-2' DAYS;
                 time
                --------
                 00:00:00
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '2' HOURS;
                 time
                --------
                 02:00:00
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '-2' HOURS;
                 time
                --------
                 22:00:00
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '2' SECONDS;
                 time
                --------
                 00:00:02
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '-2' SECONDS;
                 time
                --------
                 23:59:58
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '2' HOURS + INTERVAL '10' MINUTES + INTERVAL '10' SECONDS;
                 time
                --------
                 02:10:10
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '-2' HOURS + INTERVAL '-10' MINUTES + INTERVAL '-10' SECONDS;
                 time
                --------
                 21:49:50
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '2' HOURS + INTERVAL '10' MINUTES + INTERVAL '-10' SECONDS;
                 time
                --------
                 02:09:50
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '-2' HOURS + INTERVAL '-10' MINUTES + INTERVAL '10' SECONDS;
                 time
                --------
                 21:50:10
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '2' HOURS + INTERVAL '-10' MINUTES + INTERVAL '10' SECONDS;
                 time
                --------
                 01:50:10
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '-2' HOURS + INTERVAL '10' MINUTES + INTERVAL '-10' SECONDS;
                 time
                --------
                 22:09:50
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '2' HOURS + INTERVAL '-10' MINUTES + INTERVAL '-10' SECONDS;
                 time
                --------
                 01:49:50
                (1 row)

                SELECT '00:00:00'::time + INTERVAL '-2' HOURS + INTERVAL '10' MINUTES + INTERVAL '10' SECONDS;
                 time
                --------
                 22:10:10
                (1 row)

                SELECT '23:59:59'::time + INTERVAL '1' SECONDS;
                   time
                ----------
                 00:00:00
                (1 row)

                SELECT '23:59:59'::time + INTERVAL '1' HOURS;
                   time
                --------
                 00:59:59
                (1 row)

                SELECT '23:59:59'::time + INTERVAL '1' MINUTES;
                   time
                --------
                 00:00:59
                (1 row)

                SELECT '23:59:59'::time + INTERVAL '1' DAYS;
                   time
                --------
                 23:59:59
                (1 row)

                SELECT '23:59:59'::time + INTERVAL '1' HOURS + INTERVAL '1' MINUTES + INTERVAL '1' SECONDS;
                   time
                --------
                 01:01:00
                (1 row)

                SELECT '23:59:59'::time + INTERVAL '1' HOURS + INTERVAL '1' MINUTES + INTERVAL '1' SECONDS + INTERVAL '1' DAYS;
                   time
                --------
                 01:01:00
                (1 row)

                SELECT '23:59:59'::time + INTERVAL '1' HOURS + INTERVAL '1' MINUTES + INTERVAL '1' SECONDS + INTERVAL '1' DAYS + INTERVAL '1' HOURS;
                   time
                --------
                 02:01:00
                (1 row)

                SELECT INTERVAL '2' HOURS + '15:12:24'::time;
                 time
                ------
                 17:12:24
                (1 row)

                SELECT '12:34:56'::time + INTERVAL '1' YEAR;
                 time
                ------
                 12:34:56
                (1 row)

                SELECT '12:34:56'::time + INTERVAL '2' YEARS;
                 time
                ------
                 12:34:56
                (1 row)

                SELECT '12:34:56'::time + INTERVAL '2' MONTHS;
                 time
                ------
                 12:34:56
                (1 row)

                SELECT '12:34:56'::time + INTERVAL '2' MONTH;
                 time
                ------
                 12:34:56
                (1 row)

                SELECT '12:34:56'::time + INTERVAL '10 10:30' DAY TO MINUTE;
                 time
                ------
                 23:04:56
                (1 row)

                SELECT '00:00:01'::time + INTERVAL '-10' MINUTES;
                     time
                --------------
                 23:50:01
                (1 row)

                SELECT '00:00:01'::time + INTERVAL '-48:10' HOURS TO MINUTE;
                     time
                --------------
                 23:50:01
                (1 row)
                """
        );
    }

    @Test
    public void testTimeSubInterval() {
        this.qst("""
                SELECT '00:00:00'::time - INTERVAL '10' MINUTES;
                   time
                --------
                 23:50:00
                (1 row)

                SELECT '00:00:00'::time - INTERVAL '-10' MINUTES;
                   time
                --------
                 00:10:00
                (1 row)

                SELECT '00:00:00'::time - INTERVAL '2' DAYS;
                 time
                --------
                 00:00:00
                (1 row)

                SELECT '00:00:00'::time - INTERVAL '-2' DAYS;
                 time
                --------
                 00:00:00
                (1 row)

                SELECT '00:00:00'::time - INTERVAL '2' HOURS;
                 time
                --------
                 22:00:00
                (1 row)

                SELECT '00:00:00'::time - INTERVAL '-2' HOURS;
                 time
                --------
                 02:00:00
                (1 row)

                SELECT '00:00:00'::time - INTERVAL '2' SECONDS;
                 time
                --------
                 23:59:58
                (1 row)

                SELECT '00:00:00'::time - INTERVAL '-2' SECONDS;
                 time
                --------
                 00:00:02
                (1 row)

                SELECT '00:00:00'::time - INTERVAL '2' HOURS - INTERVAL '10' MINUTES - INTERVAL '10' SECONDS;
                 time
                --------
                 21:49:50
                (1 row)

                SELECT '00:00:00'::time - INTERVAL '-2' HOURS - INTERVAL '-10' MINUTES - INTERVAL '-10' SECONDS;
                 time
                --------
                 02:10:10
                (1 row)

                SELECT '00:00:00'::time - INTERVAL '2' HOURS - INTERVAL '10' MINUTES - INTERVAL '-10' SECONDS;
                 time
                --------
                 21:50:10
                (1 row)

                SELECT '12:34:56'::time - INTERVAL '1' YEAR;
                 time
                ------
                 12:34:56
                (1 row)

                SELECT '12:34:56'::time - INTERVAL '2' YEARS;
                 time
                ------
                 12:34:56
                (1 row)

                SELECT '12:34:56'::time - INTERVAL '2' MONTHS;
                 time
                ------
                 12:34:56
                (1 row)

                SELECT '12:34:56'::time - INTERVAL '2' MONTH;
                 time
                ------
                 12:34:56
                (1 row)

                SELECT '12:34:56'::time - INTERVAL '10 10:30' DAY TO MINUTE;
                 time
                ------
                 02:04:56
                (1 row)
                """);
    }

    @Test
    public void testTimeAddInterval1() {
        this.qst("""
                SELECT '12:34:56'::time + INTERVAL '25' DAYS;
                   time
                ---------
                 12:34:56
                (1 row)
                """
        );
    }

    @Test
    public void intervalArithmeticTruncatesToTheType() {
        // A product, a quotient and a cast of a number have an interval type; the value is truncated
        // toward zero to the last field of the type.  Each pair of columns compares a literal, which
        // the compiler folds, with a column, which the runtime computes.  Validated on HSQLDB.
        var ccs = this.getCCS("""
                CREATE TABLE A(x DECIMAL(5, 2), d DOUBLE, n INT, d2 DATE, r REAL);
                CREATE VIEW V AS SELECT
                    CAST(CAST(INTERVAL '1' YEAR * 1.5 AS INTERVAL MONTH) AS INT) AS yl,
                    CAST(CAST(INTERVAL '1' YEAR * x AS INTERVAL MONTH) AS INT) AS yc,
                    CAST(CAST(INTERVAL '1' YEAR * -1.5e0 AS INTERVAL MONTH) AS INT) AS ydl,
                    CAST(CAST(INTERVAL '1' YEAR * -d AS INTERVAL MONTH) AS INT) AS ydc,
                    CAST(CAST(INTERVAL '1' DAY / 2 AS INTERVAL HOUR) AS INT) AS dl,
                    CAST(CAST(INTERVAL '1' DAY / n AS INTERVAL HOUR) AS INT) AS dc,
                    CAST(CAST(INTERVAL '-1 01' DAY TO HOUR * 1.5 AS INTERVAL HOUR) AS INT) AS dhl,
                    CAST(CAST(INTERVAL '-1 01' DAY TO HOUR * x AS INTERVAL HOUR) AS INT) AS dhc,
                    CAST(CAST(INTERVAL '1' DAY * CAST(1.5 AS REAL) AS INTERVAL HOUR) AS INT) AS rl,
                    CAST(CAST(INTERVAL '1' DAY * r AS INTERVAL HOUR) AS INT) AS rc,
                    CAST(CAST(INTERVAL '-4:31' HOUR TO MINUTE / 2 AS INTERVAL SECOND) AS INT) AS hml,
                    CAST(CAST(INTERVAL '-4:31' HOUR TO MINUTE / n AS INTERVAL SECOND) AS INT) AS hmc,
                    INTERVAL '1' SECOND * 1.5 = INTERVAL '1.5' SECOND AS sl,
                    INTERVAL '1' SECOND * x = INTERVAL '1.5' SECOND AS sc,
                    CAST(CAST(CAST(-1.5 AS INTERVAL DAY) AS INTERVAL HOUR) AS INT) AS castl,
                    CAST(CAST(CAST(-x AS INTERVAL DAY) AS INTERVAL HOUR) AS INT) AS castc,
                    DATE '2020-01-01' + INTERVAL '1' YEAR * 1.5 AS datel,
                    d2 + INTERVAL '1' YEAR * x AS datec
                FROM A;""");
        ccs.stepWeightOne("INSERT INTO A VALUES (1.5, 1.5, 2, DATE '2020-01-01', 1.5);", """
                 yl | yc | ydl | ydc | dl | dc | dhl | dhc | rl | rc | hml   | hmc   | sl   | sc   | castl | castc | datel      | datec
                -------------------------------------------------------------------------------------------------------------------------
                 12 | 12 | -12 | -12 | 0  | 0  | -37 | -37 | 24 | 24 | -8100 | -8100 | true | true | -24   | -24   | 2021-01-01 | 2021-01-01""");
    }

    @Test
    public void datetimeDifferenceCountsFieldUnits() {
        // (a - b) unit converts a and b to whole units of the last field of the qualifier and
        // subtracts them: from 2020-12-31 to 2021-01-01 is a year (validated on HSQLDB).
        // TIMESTAMPDIFF counts elapsed units: from 2020-12-31 to 2021-01-01 is 0 years.
        var ccs = this.getCCS("""
                CREATE TABLE D(d1 DATE, d2 DATE, d3 DATE, d4 DATE, t1 TIMESTAMP, t2 TIMESTAMP, tm1 TIME, tm2 TIME,
                               t3 TIMESTAMP, t4 TIMESTAMP, z1 TIMESTAMP WITH TIME ZONE, z2 TIMESTAMP WITH TIME ZONE);
                CREATE VIEW V AS SELECT
                    CAST((DATE '2021-01-01' - DATE '2020-12-31') YEAR AS INT) AS yl,
                    CAST((d1 - d2) YEAR AS INT) AS yc,
                    CAST((DATE '2021-01-01' - DATE '2020-12-31') MONTH AS INT) AS ml,
                    CAST((d1 - d2) MONTH AS INT) AS mc,
                    CAST((DATE '2020-06-21' - DATE '2023-02-26') YEAR AS INT) AS nl,
                    CAST((d3 - d4) YEAR AS INT) AS nc,
                    CAST((TIMESTAMP '2020-01-02 01:00:00' - TIMESTAMP '2020-01-01 23:00:00') DAY AS INT) AS dl,
                    CAST((t1 - t2) DAY AS INT) AS dc,
                    CAST((TIME '11:00:01' - TIME '10:59:59') HOUR AS INT) AS hl,
                    CAST((tm1 - tm2) HOUR AS INT) AS hc,
                    CAST(CAST((DATE '2021-01-01' - DATE '2020-12-31') YEAR TO MONTH AS INTERVAL MONTH) AS INT) AS yml,
                    CAST(CAST((d1 - d2) YEAR TO MONTH AS INTERVAL MONTH) AS INT) AS ymc,
                    CAST(CAST((TIMESTAMP '2019-12-05 08:27:00' - TIMESTAMP '2014-11-05 12:45:00') DAY TO HOUR
                        AS INTERVAL HOUR) AS INT) AS dhl,
                    CAST(CAST((t3 - t4) DAY TO HOUR AS INTERVAL HOUR) AS INT) AS dhc,
                    CAST((TIMESTAMP WITH TIME ZONE '2020-01-02 01:00:00 UTC' -
                          TIMESTAMP WITH TIME ZONE '2020-01-01 23:00:00 UTC') DAY AS INT) AS zl,
                    CAST((z1 - z2) DAY AS INT) AS zc,
                    TIMESTAMPDIFF(YEAR, d2, d1) AS ty,
                    TIMESTAMPDIFF(DAY, t2, t1) AS td
                FROM D;""");
        ccs.stepWeightOne("""
                INSERT INTO D VALUES (DATE '2021-01-01', DATE '2020-12-31', DATE '2020-06-21', DATE '2023-02-26',
                    TIMESTAMP '2020-01-02 01:00:00', TIMESTAMP '2020-01-01 23:00:00', TIME '11:00:01', TIME '10:59:59',
                    TIMESTAMP '2019-12-05 08:27:00', TIMESTAMP '2014-11-05 12:45:00',
                    TIMESTAMP WITH TIME ZONE '2020-01-02 01:00:00 UTC', TIMESTAMP WITH TIME ZONE '2020-01-01 23:00:00 UTC');""", """
                 yl | yc | ml | mc | nl | nc | dl | dc | hl | hc | yml | ymc | dhl   | dhc   | zl | zc | ty | td
                ---------------------------------------------------------------------------------------------------
                 1  | 1  | 1  | 1  | -3 | -3 | 1  | 1  | 1  | 1  | 1   | 1   | 44540 | 44540 | 1  | 1  | 0  | 0""");
    }

    @Test
    public void intervalArithmeticErrors() {
        // The compiler does not fold an interval product or quotient that fails: the runtime
        // reports the error, for literals and for columns
        this.qf("SELECT INTERVAL '1' YEAR * 200000000", "overflow in INTERVAL multiplication");
        this.qf("SELECT INTERVAL '1' DAY / 0", "division by zero in INTERVAL division");

        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation("""
                CREATE TABLE N(n INT);
                CREATE VIEW V AS SELECT INTERVAL '1' YEAR * n AS y FROM N;""");
        this.runtimeFail(compiler, "INSERT INTO N VALUES (200000000);", "overflow in INTERVAL multiplication");

        compiler = this.testCompiler();
        compiler.submitStatementsForCompilation("""
                CREATE TABLE N(n INT);
                CREATE VIEW V AS SELECT INTERVAL '1' DAY / n AS d FROM N;""");
        this.runtimeFail(compiler, "INSERT INTO N VALUES (0);", "division by zero in INTERVAL division");
    }
}
