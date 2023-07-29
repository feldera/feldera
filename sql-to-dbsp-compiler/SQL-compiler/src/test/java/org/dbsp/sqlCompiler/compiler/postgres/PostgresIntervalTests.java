package org.dbsp.sqlCompiler.compiler.postgres;

import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/03734a7fed7d924679770adb78a7db8a37d14188/src/test/regress/expected/interval.out
public class PostgresIntervalTests extends PostgresBaseTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
    }

    @Test
    public void testCalciteExamples() {
        this.q("SELECT INTERVAL '1-5' YEAR TO MONTH, " +
                        "INTERVAL '45' DAY, INTERVAL '1 2:34:56.789' DAY TO SECOND;\n" +
                "i0 | i1 | i2\n" +
                "------------\n" +
                "1 year 5 months | 45 days | 1 day 02:34:56.789");
    }

    @Test
    public void testInterval() {
        // Had to add the symbolic intervals by hand in many places
        // Postgres allows individual signs on fragments, the
        // standard doesn't
        this.qs("SELECT INTERVAL '01:00' HOUR TO MINUTE AS \"One hour\";\n" +
                " One hour \n" +
                "----------\n" +
                " 01:00:00\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT INTERVAL '+02:00' HOUR TO MINUTE AS \"Two hours\";\n" +
                " Two hours \n" +
                "-----------\n" +
                " 02:00:00\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT INTERVAL '-08:00' HOUR TO MINUTE AS \"Eight hours\";\n" +
                " Eight hours \n" +
                "-------------\n" +
                " -08:00:00\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT INTERVAL '-1 02:03' DAYS TO MINUTE AS \"26 hours ago...\";\n" +
                "  26 hours ago...  \n" +
                "-------------------\n" +
                " -1 days 02:03:00\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT INTERVAL '-1 02:03' DAYS TO MINUTES AS \"26 hours ago...\";\n" +
                "  26 hours ago...  \n" +
                "-------------------\n" +
                " -1 days 02:03:00\n" +
                "(1 row)\n");
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
    public void testSelect() {
        this.qs("-- test interval operators\n" +
                "SELECT * FROM INTERVAL_TBL;\n" +
                "       f1        \n" +
                "-----------------\n" +
                " 00:01:00\n" +
                " 05:00:00\n" +
                " 10 days\n" +
                " 34 years\n" +
                " 3 mons\n" +
                " -00:00:14\n" +
                " 1 day 02:03:04\n" +
                " 6 years\n" +
                " 5 mons\n" +
                " 5 mons 12:00:00\n" +
                "(10 rows)\n" +
                "\n" +
                "SELECT * FROM INTERVAL_TBL\n" +
                "   WHERE INTERVAL_TBL.f1 <> interval '@ 10 days';\n" +
                "       f1        \n" +
                "-----------------\n" +
                " 00:01:00\n" +
                " 05:00:00\n" +
                " 34 years\n" +
                " 3 mons\n" +
                " -00:00:14\n" +
                " 1 day 02:03:04\n" +
                " 6 years\n" +
                " 5 mons\n" +
                " 5 mons 12:00:00\n" +
                "(9 rows)\n" +
                "\n" +
                "SELECT * FROM INTERVAL_TBL\n" +
                "   WHERE INTERVAL_TBL.f1 <= interval '@ 5 hours';\n" +
                "    f1     \n" +
                "-----------\n" +
                " 00:01:00\n" +
                " 05:00:00\n" +
                " -00:00:14\n" +
                "(3 rows)\n" +
                "\n" +
                "SELECT * FROM INTERVAL_TBL\n" +
                "   WHERE INTERVAL_TBL.f1 < interval '@ 1 day';\n" +
                "    f1     \n" +
                "-----------\n" +
                " 00:01:00\n" +
                " 05:00:00\n" +
                " -00:00:14\n" +
                "(3 rows)\n" +
                "\n" +
                "SELECT * FROM INTERVAL_TBL\n" +
                "   WHERE INTERVAL_TBL.f1 = interval '@ 34 years';\n" +
                "    f1    \n" +
                "----------\n" +
                " 34 years\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT * FROM INTERVAL_TBL\n" +
                "   WHERE INTERVAL_TBL.f1 >= interval '@ 1 month';\n" +
                "       f1        \n" +
                "-----------------\n" +
                " 34 years\n" +
                " 3 mons\n" +
                " 6 years\n" +
                " 5 mons\n" +
                " 5 mons 12:00:00\n" +
                "(5 rows)\n" +
                "\n" +
                "SELECT * FROM INTERVAL_TBL\n" +
                "   WHERE INTERVAL_TBL.f1 > interval '@ 3 seconds ago';\n" +
                "       f1        \n" +
                "-----------------\n" +
                " 00:01:00\n" +
                " 05:00:00\n" +
                " 10 days\n" +
                " 34 years\n" +
                " 3 mons\n" +
                " 1 day 02:03:04\n" +
                " 6 years\n" +
                " 5 mons\n" +
                " 5 mons 12:00:00\n" +
                "(9 rows)\n" +
                "\n" +
                "SELECT r1.*, r2.*\n" +
                "   FROM INTERVAL_TBL r1, INTERVAL_TBL r2\n" +
                "   WHERE r1.f1 > r2.f1\n" +
                "   ORDER BY r1.f1, r2.f1;\n" +
                "       f1        |       f1        \n" +
                "-----------------+-----------------\n" +
                " 00:01:00        | -00:00:14\n" +
                " 05:00:00        | -00:00:14\n" +
                " 05:00:00        | 00:01:00\n" +
                " 1 day 02:03:04  | -00:00:14\n" +
                " 1 day 02:03:04  | 00:01:00\n" +
                " 1 day 02:03:04  | 05:00:00\n" +
                " 10 days         | -00:00:14\n" +
                " 10 days         | 00:01:00\n" +
                " 10 days         | 05:00:00\n" +
                " 10 days         | 1 day 02:03:04\n" +
                " 3 mons          | -00:00:14\n" +
                " 3 mons          | 00:01:00\n" +
                " 3 mons          | 05:00:00\n" +
                " 3 mons          | 1 day 02:03:04\n" +
                " 3 mons          | 10 days\n" +
                " 5 mons          | -00:00:14\n" +
                " 5 mons          | 00:01:00\n" +
                " 5 mons          | 05:00:00\n" +
                " 5 mons          | 1 day 02:03:04\n" +
                " 5 mons          | 10 days\n" +
                " 5 mons          | 3 mons\n" +
                " 5 mons 12:00:00 | -00:00:14\n" +
                " 5 mons 12:00:00 | 00:01:00\n" +
                " 5 mons 12:00:00 | 05:00:00\n" +
                " 5 mons 12:00:00 | 1 day 02:03:04\n" +
                " 5 mons 12:00:00 | 10 days\n" +
                " 5 mons 12:00:00 | 3 mons\n" +
                " 5 mons 12:00:00 | 5 mons\n" +
                " 6 years         | -00:00:14\n" +
                " 6 years         | 00:01:00\n" +
                " 6 years         | 05:00:00\n" +
                " 6 years         | 1 day 02:03:04\n" +
                " 6 years         | 10 days\n" +
                " 6 years         | 3 mons\n" +
                " 6 years         | 5 mons\n" +
                " 6 years         | 5 mons 12:00:00\n" +
                " 34 years        | -00:00:14\n" +
                " 34 years        | 00:01:00\n" +
                " 34 years        | 05:00:00\n" +
                " 34 years        | 1 day 02:03:04\n" +
                " 34 years        | 10 days\n" +
                " 34 years        | 3 mons\n" +
                " 34 years        | 5 mons\n" +
                " 34 years        | 5 mons 12:00:00\n" +
                " 34 years        | 6 years\n" +
                "(45 rows)");
    }
}
