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
}
