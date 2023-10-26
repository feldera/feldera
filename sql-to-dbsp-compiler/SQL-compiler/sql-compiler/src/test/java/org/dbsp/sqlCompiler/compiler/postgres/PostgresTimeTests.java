package org.dbsp.sqlCompiler.compiler.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.junit.Ignore;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/03734a7fed7d924679770adb78a7db8a37d14188/src/test/regress/expected/time.out
// This test seems complete (convering all test cases from Postgres).
public class PostgresTimeTests extends PostgresBaseTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        // Calcite format is much stricter.  Converted to the right format
        compiler.compileStatements("CREATE TABLE TIME_TBL (f1 time(2));\n" +
                "INSERT INTO TIME_TBL VALUES ('00:00:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('01:00:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('02:03:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('11:59:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('12:00:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('12:01:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('23:59:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('23:59:59.99');\n" +
                "INSERT INTO TIME_TBL VALUES ('15:36:39');\n" +
                "INSERT INTO TIME_TBL VALUES ('15:36:39');");
    }

    @Test
    public void testTime() {
        this.qs("SELECT f1 AS \"Time\" FROM TIME_TBL;\n" +
                "    Time     \n" +
                "-------------\n" +
                " 00:00:00\n" +
                " 01:00:00\n" +
                " 02:03:00\n" +
                " 11:59:00\n" +
                " 12:00:00\n" +
                " 12:01:00\n" +
                " 23:59:00\n" +
                " 23:59:59.99\n" +
                " 15:36:39\n" +
                " 15:36:39\n" +
                "(10 rows)\n" +
                "\n" +
                "SELECT f1 AS \"Three\" FROM TIME_TBL WHERE f1 < '05:06:07';\n" +
                "  Three   \n" +
                "----------\n" +
                " 00:00:00\n" +
                " 01:00:00\n" +
                " 02:03:00\n" +
                "(3 rows)\n" +
                "\n" +
                "SELECT f1 AS \"Five\" FROM TIME_TBL WHERE f1 > '05:06:07';\n" +
                "    Five     \n" +
                "-------------\n" +
                " 11:59:00\n" +
                " 12:00:00\n" +
                " 12:01:00\n" +
                " 23:59:00\n" +
                " 23:59:59.99\n" +
                " 15:36:39\n" +
                " 15:36:39\n" +
                "(7 rows)\n" +
                "\n" +
                "SELECT f1 AS \"None\" FROM TIME_TBL WHERE f1 < '00:00:00';\n" +
                " None \n" +
                "------\n" +
                "(0 rows)\n" +
                "\n" +
                "SELECT f1 AS \"Eight\" FROM TIME_TBL WHERE f1 >= '00:00:00';\n" +
                "    Eight    \n" +
                "-------------\n" +
                " 00:00:00\n" +
                " 01:00:00\n" +
                " 02:03:00\n" +
                " 11:59:00\n" +
                " 12:00:00\n" +
                " 12:01:00\n" +
                " 23:59:00\n" +
                " 23:59:59.99\n" +
                " 15:36:39\n" +
                " 15:36:39\n" +
                "(10 rows)");
    }

    @Test
    public void testConstants() {
        this.qs("-- Check edge cases\n" +
                "SELECT '23:59:59.999999'::time;\n" +
                "      time       \n" +
                "-----------------\n" +
                " 23:59:59.999999\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT '23:59:59.9999999'::time;  -- rounds up\n" +
                "   time   \n" +
                "----------\n" +
                " 23:59:59.9999999\n" +
                "(1 row)");
    }

    @Test
    public void testFailConstants() {
        // The following tests pass in Postgres.
        // Changed to TIME literals from strings cast to ::time
        this.shouldFail("SELECT TIME '23:59:60'", "Illegal TIME literal");
        this.shouldFail("SELECT TIME '24:00:00'::time", "Illegal TIME literal");
        this.shouldFail("SELECT TIME '24:00:00.01'", "Illegal TIME literal");
        this.shouldFail("SELECT TIME '23:59:60.01'", "Illegal TIME literal");
        this.shouldFail("SELECT TIME '24:01:00'", "Illegal TIME literal");
        this.shouldFail("SELECT TIME '25:00:00'", "Illegal TIME literal");
    }

    @Test
    public void testFailPlus() {
        // Changed TIME literal to conform
        this.shouldFail("SELECT f1 + time '00:01:00' AS \"Illegal\" FROM TIME_TBL",
                "Cannot apply '+' to arguments");
    }

    @Test @Ignore("Bug in Calcite https://issues.apache.org/jira/browse/CALCITE-5919")
    public void testMicrosecond() {
        this.q("SELECT EXTRACT(MICROSECOND FROM TIME '13:30:25.575401');\n" +
                " extract  \n" +
                "----------\n" +
                " 25575401");
    }

    @Test
    public void testUnits() {
        // Removed dates
        // Extract second and millisecond return integers in Calcite instead of DECIMAL
        this.qs("SELECT EXTRACT(MILLISECOND FROM TIME '13:30:25.575401');\n" +
                "  extract  \n" +
                "-----------\n" +
                " 25575\n" + // -- 25575.401
                "(1 row)\n" +
                "\n" +
                "SELECT EXTRACT(SECOND      FROM TIME '13:30:25.575401');\n" +
                "  extract  \n" +
                "-----------\n" +
                " 25\n" + // -- 25.575401
                "(1 row)\n" +
                "\n" +
                "SELECT EXTRACT(MINUTE      FROM TIME '13:30:25.575401');\n" +
                " extract \n" +
                "---------\n" +
                "      30\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT EXTRACT(HOUR        FROM TIME '13:30:25.575401');\n" +
                " extract \n" +
                "---------\n" +
                "      13\n" +
                "(1 row)");
    }

    @Test @Ignore("https://issues.apache.org/jira/browse/CALCITE-6015")
    public void illegalUnits() {
        this.shouldFail("SELECT EXTRACT(DAY FROM TIME '13:30:25.575401')", "??");
        // Following don't work in Calcite
        // SELECT EXTRACT(FORTNIGHT FROM TIME '13:30:25.575401')
        // SELECT EXTRACT(TIMEZONE FROM TIME '13:30:25.575401')
        this.shouldFail("SELECT EXTRACT(EPOCH FROM TIME '2020-05-26 13:30:25.575401')", "");
    }

    @Test @Ignore("https://issues.apache.org/jira/browse/CALCITE-6030")
    public void testDatePart() {
        this.qs("SELECT date_part(microsecond, TIME '13:30:25.575401');\n" +
                " date_part \n" +
                "-----------\n" +
                "  25575401\n" +
                "(1 row)" +
                "\n" +
                "SELECT date_part('millisecond', TIME '13:30:25.575401');\n" +
                " date_part \n" +
                "-----------\n" +
                " 25575.401\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT date_part('second',      TIME '13:30:25.575401');\n" +
                " date_part \n" +
                "-----------\n" +
                " 25.575401\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT date_part('epoch',       TIME '13:30:25.575401');\n" +
                "  date_part   \n" +
                "--------------\n" +
                " 48625.575401");
    }
}
