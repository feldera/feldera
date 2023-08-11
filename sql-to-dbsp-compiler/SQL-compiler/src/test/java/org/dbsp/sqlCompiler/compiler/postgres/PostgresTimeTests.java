package org.dbsp.sqlCompiler.compiler.postgres;

import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/03734a7fed7d924679770adb78a7db8a37d14188/src/test/regress/expected/time.out
public class PostgresTimeTests extends PostgresBaseTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        if (false)
        // Calcite format is much stricter.  Converted to the right format
        compiler.compileStatements("CREATE TABLE TIME_TBL (f1 time(2));\n" +
                "INSERT INTO TIME_TBL VALUES ('00:00:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('01:00:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('02:03:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('11:59:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('12:00:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('12:01:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('23:59:00');\n" +
                "INSERT INTO TIME_TBL VALUES ('23:59:59.99');" +
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
                "SELECT f1 AS \"None\" FROM TIME_TBL WHERE f1 < '00:00';\n" +
                " None \n" +
                "------\n" +
                "(0 rows)\n" +
                "\n" +
                "SELECT f1 AS \"Eight\" FROM TIME_TBL WHERE f1 >= '00:00';\n" +
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
                "(1 row)\n" +
                "\n" +
                "SELECT '23:59:60'::time;  -- rounds up\n" +
                "   time   \n" +
                "----------\n" +
                " 23:59:60\n" +
                "(1 row)");
        //      not allowed in Calcite, but legal in Postgres.
        //        "SELECT '24:00:00'::time;  -- allowed\n" +
        //        "   time   \n" +
        //        "----------\n" +
        //        " 24:00:00");
    }
}
