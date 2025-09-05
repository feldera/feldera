package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/master/src/test/regress/expected/time.out
// This test seems complete (covering all test cases from Postgres).
public class PostgresTimeTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        // Calcite format is much stricter.  Converted to the right format
        compiler.submitStatementsForCompilation("""
                CREATE TABLE TIME_TBL (f1 time);
                INSERT INTO TIME_TBL VALUES ('00:00:00');
                INSERT INTO TIME_TBL VALUES ('01:00:00');
                INSERT INTO TIME_TBL VALUES ('02:03:00');
                INSERT INTO TIME_TBL VALUES ('11:59:00');
                INSERT INTO TIME_TBL VALUES ('12:00:00');
                INSERT INTO TIME_TBL VALUES ('12:01:00');
                INSERT INTO TIME_TBL VALUES ('23:59:00');
                INSERT INTO TIME_TBL VALUES ('23:59:59.99');
                INSERT INTO TIME_TBL VALUES ('15:36:39');
                INSERT INTO TIME_TBL VALUES ('15:36:39');""");
    }

    @Test
    public void testTime() {
        this.qs("""
                SELECT f1 AS "Time" FROM TIME_TBL;
                    Time
                -------------
                 00:00:00
                 01:00:00
                 02:03:00
                 11:59:00
                 12:00:00
                 12:01:00
                 23:59:00
                 23:59:59.99
                 15:36:39
                 15:36:39
                (10 rows)

                SELECT f1 AS "Three" FROM TIME_TBL WHERE f1 < '05:06:07';
                  Three
                ----------
                 00:00:00
                 01:00:00
                 02:03:00
                (3 rows)

                SELECT f1 AS "Five" FROM TIME_TBL WHERE f1 > '05:06:07';
                    Five
                -------------
                 11:59:00
                 12:00:00
                 12:01:00
                 23:59:00
                 23:59:59.99
                 15:36:39
                 15:36:39
                (7 rows)

                SELECT f1 AS "None" FROM TIME_TBL WHERE f1 < '00:00:00';
                 None
                ------
                (0 rows)

                SELECT f1 AS "Eight" FROM TIME_TBL WHERE f1 >= '00:00:00';
                    Eight
                -------------
                 00:00:00
                 01:00:00
                 02:03:00
                 11:59:00
                 12:00:00
                 12:01:00
                 23:59:00
                 23:59:59.99
                 15:36:39
                 15:36:39
                (10 rows)""");
    }

    @Test
    public void testConstants() {
        this.qs("""
                -- Check edge cases
                SELECT '23:59:59.999999'::time;
                      time
                -----------------
                 23:59:59.999999
                (1 row)

                SELECT '23:59:59.9999999'::time;  -- rounds up
                   time
                ----------
                 23:59:59.9999999
                (1 row)""");
    }

    @Test
    public void testFailConstants() {
        // The following tests pass in Postgres.
        // Changed to TIME literals from strings cast to ::time
        this.queryFailingInCompilation("SELECT TIME '23:59:60'", "Illegal TIME literal");
        this.queryFailingInCompilation("SELECT TIME '24:00:00'::time", "Illegal TIME literal");
        this.queryFailingInCompilation("SELECT TIME '24:00:00.01'", "Illegal TIME literal");
        this.queryFailingInCompilation("SELECT TIME '23:59:60.01'", "Illegal TIME literal");
        this.queryFailingInCompilation("SELECT TIME '24:01:00'", "Illegal TIME literal");
        this.queryFailingInCompilation("SELECT TIME '25:00:00'", "Illegal TIME literal");
    }

    @Test
    public void testFailPlus() {
        // Changed TIME literal to conform
        this.queryFailingInCompilation("SELECT f1 + time '00:01:00' AS \"Illegal\" FROM TIME_TBL",
                "Cannot apply '+' to arguments");
    }

    @Test
    public void testUnits() {
        // Removed dates
        // Extract second and millisecond return integers in Calcite instead of DECIMAL
        this.qs("""
                SELECT EXTRACT(MICROSECOND FROM TIME '13:30:25.575401');
                 extract
                ----------
                 25575401
                (1 row)
                
                SELECT EXTRACT(NANOSECOND FROM TIME '13:30:25.575401');
                 extract
                ----------
                 25575401000
                (1 row)
                
                SELECT EXTRACT(NANOSECOND FROM TIME '13:30:25.575401234');
                 extract
                ----------
                 25575401234
                (1 row)
                
                SELECT EXTRACT(MILLISECOND FROM TIME '13:30:25.575401');
                  extract
                -----------
                 25575
                (1 row)

                SELECT EXTRACT(SECOND      FROM TIME '13:30:25.575401');
                  extract
                -----------
                 25
                (1 row)

                SELECT EXTRACT(MINUTE      FROM TIME '13:30:25.575401');
                 extract
                ---------
                      30
                (1 row)

                SELECT EXTRACT(HOUR        FROM TIME '13:30:25.575401');
                 extract
                ---------
                      13
                (1 row)""");
    }

    @Test
    public void illegalUnits() {
        this.queryFailingInCompilation("SELECT EXTRACT(DAY FROM TIME '13:30:25.575401')",
                "Cannot apply 'EXTRACT' to");
        // Following don't work in Calcite
        // SELECT EXTRACT(FORTNIGHT FROM TIME '13:30:25.575401')
        // SELECT EXTRACT(TIMEZONE FROM TIME '13:30:25.575401')
        this.queryFailingInCompilation("SELECT EXTRACT(EPOCH FROM TIME '13:30:25.575401')",
                "Cannot apply 'EXTRACT' to");
    }

    @Test
    public void testDatePart() {
        this.qs("""
                SELECT date_part(microsecond, TIME '13:30:25.575401');
                 date_part
                -----------
                  25575401
                (1 row)

                SELECT date_part(millisecond, TIME '13:30:25.575401');
                 date_part
                -----------
                 25575
                (1 row)

                SELECT date_part(second,      TIME '13:30:25.575401');
                 date_part
                -----------
                 25
                (1 row)""");
                /*
                Calcite does not yet handle date_part(epoch, ...);
                """SELECT date_part(epoch,       TIME '13:30:25.575401');
                  date_part
                --------------
                 48625
                (1 row)"""
                 */
    }
}
