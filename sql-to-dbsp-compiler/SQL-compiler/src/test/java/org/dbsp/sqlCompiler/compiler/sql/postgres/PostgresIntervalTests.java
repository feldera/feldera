package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/master/src/test/regress/expected/interval.out
public class PostgresIntervalTests extends SqlIoTest {
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
}
