package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

// https://github.com/postgres/postgres/blob/master/src/test/regress/expected/groupingsets.out
public class PostgresGroupingSetsTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        String sql = "create table gstest1(a integer, b integer, v integer);\n" +
                "insert into gstest1 values (1,1,10),(1,1,11),(1,2,12),(1,2,13),(1,3,14)," +
                "            (2,3,15),(3,3,16),(3,4,17),(4,1,18),(4,1,19);";
        compiler.submitStatementsForCompilation(sql);
    }

    @Test
    public void testRollup() {
        // commented out order by
        this.qs("""
                select a, b, grouping(a,b), sum(v), count(*), max(v)
                  from gstest1 group by rollup (a,b);
                 a | b | grouping | sum | count | max
                ---+---+----------+-----+-------+-----
                 1 | 1 |        0 |  21 |     2 |  11
                 1 | 2 |        0 |  25 |     2 |  13
                 1 | 3 |        0 |  14 |     1 |  14
                 1 |   |        1 |  60 |     5 |  14
                 2 | 3 |        0 |  15 |     1 |  15
                 2 |   |        1 |  15 |     1 |  15
                 3 | 3 |        0 |  16 |     1 |  16
                 3 | 4 |        0 |  17 |     1 |  17
                 3 |   |        1 |  33 |     2 |  17
                 4 | 1 |        0 |  37 |     2 |  19
                 4 |   |        1 |  37 |     2 |  19
                   |   |        3 | 145 |    10 |  19
                (12 rows)

                select a, b, grouping(a,b), sum(v), count(*), max(v)
                  from gstest1 group by rollup (a,b); -- order by a,b
                 a | b | grouping | sum | count | max
                ---+---+----------+-----+-------+-----
                 1 | 1 |        0 |  21 |     2 |  11
                 1 | 2 |        0 |  25 |     2 |  13
                 1 | 3 |        0 |  14 |     1 |  14
                 1 |   |        1 |  60 |     5 |  14
                 2 | 3 |        0 |  15 |     1 |  15
                 2 |   |        1 |  15 |     1 |  15
                 3 | 3 |        0 |  16 |     1 |  16
                 3 | 4 |        0 |  17 |     1 |  17
                 3 |   |        1 |  33 |     2 |  17
                 4 | 1 |        0 |  37 |     2 |  19
                 4 |   |        1 |  37 |     2 |  19
                   |   |        3 | 145 |    10 |  19
                (12 rows)

                select a, b, grouping(a,b), sum(v), count(*), max(v)
                  from gstest1 group by rollup (a,b); -- order by b desc, a;
                 a | b | grouping | sum | count | max
                ---+---+----------+-----+-------+-----
                 1 |   |        1 |  60 |     5 |  14
                 2 |   |        1 |  15 |     1 |  15
                 3 |   |        1 |  33 |     2 |  17
                 4 |   |        1 |  37 |     2 |  19
                   |   |        3 | 145 |    10 |  19
                 3 | 4 |        0 |  17 |     1 |  17
                 1 | 3 |        0 |  14 |     1 |  14
                 2 | 3 |        0 |  15 |     1 |  15
                 3 | 3 |        0 |  16 |     1 |  16
                 1 | 2 |        0 |  25 |     2 |  13
                 1 | 1 |        0 |  21 |     2 |  11
                 4 | 1 |        0 |  37 |     2 |  19
                (12 rows)

                select a, b, grouping(a,b), sum(v), count(*), max(v)
                  from gstest1 group by rollup (a,b); -- order by coalesce(a,0)+coalesce(b,0);
                 a | b | grouping | sum | count | max
                ---+---+----------+-----+-------+-----
                   |   |        3 | 145 |    10 |  19
                 1 |   |        1 |  60 |     5 |  14
                 1 | 1 |        0 |  21 |     2 |  11
                 2 |   |        1 |  15 |     1 |  15
                 3 |   |        1 |  33 |     2 |  17
                 1 | 2 |        0 |  25 |     2 |  13
                 1 | 3 |        0 |  14 |     1 |  14
                 4 |   |        1 |  37 |     2 |  19
                 4 | 1 |        0 |  37 |     2 |  19
                 2 | 3 |        0 |  15 |     1 |  15
                 3 | 3 |        0 |  16 |     1 |  16
                 3 | 4 |        0 |  17 |     1 |  17
                (12 rows)""");
    }
}
