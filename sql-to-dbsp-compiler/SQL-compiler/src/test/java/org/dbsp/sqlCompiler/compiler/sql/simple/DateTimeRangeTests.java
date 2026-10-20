package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests for DATE and TIMESTAMP values at the edges of the legal range. */
public class DateTimeRangeTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                CREATE TABLE T(d DATE, ts TIMESTAMP, ms BIGINT, sts VARCHAR, sd VARCHAR);
                INSERT INTO T VALUES('9999-12-31', '9999-12-31 23:59:59', 9223372036854775807,
                    '0000-06-01 00:00:00', '0000-06-01');""");
    }

    @Test
    public void safeCastOutOfRange() {
        // An out-of-range result is a cast error, so SAFE_CAST returns NULL
        this.qst("""
                SELECT SAFE_CAST(ms AS TIMESTAMP) FROM T;
                 r
                ---
                NULL
                (1 row)

                SELECT SAFE_CAST(sts AS TIMESTAMP) FROM T;
                 r
                ---
                NULL
                (1 row)

                SELECT SAFE_CAST(sd AS DATE) FROM T;
                 r
                ---
                NULL
                (1 row)""");
    }

    @Test
    public void castOutOfRange() {
        this.qf("SELECT CAST(ms AS TIMESTAMP) FROM T", "produces a value outside the range");
        this.qf("SELECT CAST(sd AS DATE) FROM T", "produces a value outside the range");
    }

    @Test
    public void arithmeticOutOfRange() {
        // The computations use a table column, so the compiler cannot fold them
        this.qf("SELECT d + INTERVAL 1 DAY FROM T", "Cannot add interval");
        this.qf("SELECT ts + INTERVAL 1 SECOND FROM T", "Cannot add interval");
        this.qf("SELECT ts + INTERVAL 1 MONTH FROM T", "Cannot add interval");
        this.qf("SELECT CEIL(ts TO DAY) FROM T", "Rounding timestamp");
    }

    @Test
    public void temporalFilterBeforeNow() {
        // Before the first NOW value, the bounds of a temporal filter are not computed
        // from the minimum timestamp, whose differences are outside the legal range
        var ccs = this.getCCS("""
                CREATE TABLE w(d DATE, ts TIMESTAMP);
                CREATE VIEW v AS SELECT d, ts FROM w
                WHERE d >= CAST(NOW() AS DATE) - INTERVAL 1 DAY
                AND ts >= NOW() - INTERVAL 1 YEAR;""");
        ccs.step("INSERT INTO w VALUES ('9999-12-20', '9999-12-20 00:00:00');", """
                 d          | ts                  | weight
                ---------------------------------------------
                 9999-12-20 | 9999-12-20 00:00:00 | 1""");
    }

    @Test
    public void latenessAtTheMinimum() {
        // The waterline of a value within the lateness of the minimum is the minimum
        var ccs = this.getCCS("""
                CREATE TABLE l(
                    d DATE NOT NULL LATENESS INTERVAL 1 DAY,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
                CREATE VIEW v AS SELECT d, ts, COUNT(*) AS c FROM l GROUP BY d, ts;""");
        ccs.step("INSERT INTO l VALUES ('0001-01-01', '0001-01-01 00:00:00');", """
                 d          | ts                  | c | weight
                -------------------------------------------------
                 0001-01-01 | 0001-01-01 00:00:00 | 1 | 1""");
    }
}
