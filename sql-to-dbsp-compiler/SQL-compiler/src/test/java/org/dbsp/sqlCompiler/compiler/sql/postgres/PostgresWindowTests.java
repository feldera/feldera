package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

public class PostgresWindowTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.compileStatements("""
                CREATE TABLE series(x int NOT NULL);
                INSERT INTO series VALUES
                (1),
                (2),
                (3),
                (4),
                (5),
                (6),
                (7),
                (8),
                (9),
                (10);
                
                --CREATE TABLE tenk1 (
                --	unique1		int4,
                --	unique2		int4,
                --	two			int4,
                --	four		int4,
                --	ten			int4,
                --	twenty		int4,
                --	hundred		int4,
                --	thousand	int4,
                --	twothousand	int4,
                --	fivethous	int4,
                --	tenthous	int4,
                --	odd			int4,
                --	even		int4,
                --	stringu1	varchar,
                --	stringu2	varchar,
                --	string4		varchar
                --);
                --
                -- smaller version, only contains values where unique2 < 10
                CREATE TABLE tenk1_small (
                	unique1		int4,
                	unique2		int4,
                	two			int4,
                	four		int4,
                	ten			int4,
                	twenty		int4,
                	hundred		int4,
                	thousand	int4,
                	twothousand	int4,
                	fivethous	int4,
                	tenthous	int4,
                	odd			int4,
                	even		int4,
                	stringu1	varchar,
                	stringu2	varchar,
                	string4		varchar
                );""");
        //this.insertFromResource("tenk1", compiler);
        this.insertFromResource("tenk1_small", compiler);
    }

    @Test
    public void testLeadLag() {
        this.qs("""
                SELECT lag(ten) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_small WHERE unique2 < 10;
                 lag | ten | four
                -----+-----+------
                     |   0 |    0
                   0 |   0 |    0
                   0 |   4 |    0
                     |   1 |    1
                   1 |   1 |    1
                   1 |   7 |    1
                   7 |   9 |    1
                     |   0 |    2
                     |   1 |    3
                   1 |   3 |    3
                (10 rows)
                
                SELECT lead(ten) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_small
                WHERE unique2 < 10;
                 lead | ten | four
                ------+-----+------
                    0 |   0 |    0
                    4 |   0 |    0
                      |   4 |    0
                    1 |   1 |    1
                    7 |   1 |    1
                    9 |   7 |    1
                      |   9 |    1
                      |   0 |    2
                    3 |   1 |    3
                      |   3 |    3
                (10 rows)
                
                SELECT lead(ten * 2, 1) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_small
                WHERE unique2 < 10;
                 lead | ten | four
                ------+-----+------
                    0 |   0 |    0
                    8 |   0 |    0
                      |   4 |    0
                    2 |   1 |    1
                   14 |   1 |    1
                   18 |   7 |    1
                      |   9 |    1
                      |   0 |    2
                    6 |   1 |    3
                      |   3 |    3
                (10 rows)
                
                SELECT lead(ten * 2, 1, -1) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_small
                WHERE unique2 < 10;
                 lead | ten | four
                ------+-----+------
                    0 |   0 |    0
                    8 |   0 |    0
                   -1 |   4 |    0
                    2 |   1 |    1
                   14 |   1 |    1
                   18 |   7 |    1
                   -1 |   9 |    1
                   -1 |   0 |    2
                    6 |   1 |    3
                   -1 |   3 |    3
                (10 rows)""", false);
    }

    @Test @Ignore("https://issues.apache.org/jira/browse/CALCITE-6382")
    public void testFails() {
        this.qs("""
                SELECT lead(ten * 2, 1, -1.4) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_small
                WHERE unique2 < 10
                -- ORDER BY four, ten
                ;
                 lead | ten | four
                ------+-----+------
                    0 |   0 |    0
                    8 |   0 |    0
                 -1.4 |   4 |    0
                    2 |   1 |    1
                   14 |   1 |    1
                   18 |   7 |    1
                 -1.4 |   9 |    1
                 -1.4 |   0 |    2
                    6 |   1 |    3
                 -1.4 |   3 |    3
                (10 rows)""", false);
    }

    @Test @Ignore("Lead with variable amounts not supported")
    public void testLeadLagVariable() {
        this.qs("""
                select x, lag(x, 1) over (order by x), lead(x, 3) over (order by x)
                from series;
                 x  | lag | lead\s
                ----+-----+------
                  1 |     |    4
                  2 |   1 |    5
                  3 |   2 |    6
                  4 |   3 |    7
                  5 |   4 |    8
                  6 |   5 |    9
                  7 |   6 |   10
                  8 |   7 |
                  9 |   8 |
                 10 |   9 |
                (10 rows)
                
                SELECT lag(ten, four) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_small
                WHERE unique2 < 10;
                 lag | ten | four
                -----+-----+------
                   0 |   0 |    0
                   0 |   0 |    0
                   4 |   4 |    0
                     |   1 |    1
                   1 |   1 |    1
                   1 |   7 |    1
                   7 |   9 |    1
                     |   0 |    2
                     |   1 |    3
                     |   3 |    3
                (10 rows)
                
                SELECT lag(ten, four, 0) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_small
                WHERE unique2 < 10;
                 lag | ten | four
                -----+-----+------
                   0 |   0 |    0
                   0 |   0 |    0
                   4 |   4 |    0
                   0 |   1 |    1
                   1 |   1 |    1
                   1 |   7 |    1
                   7 |   9 |    1
                   0 |   0 |    2
                   0 |   1 |    3
                   0 |   3 |    3
                (10 rows)
                
                SELECT lag(ten, four, 0.7) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_small
                WHERE unique2 < 10 ORDER BY four, ten;
                 lag | ten | four
                -----+-----+------
                   0 |   0 |    0
                   0 |   0 |    0
                   4 |   4 |    0
                 0.7 |   1 |    1
                   1 |   1 |    1
                   1 |   7 |    1
                   7 |   9 |    1
                 0.7 |   0 |    2
                 0.7 |   1 |    3
                 0.7 |   3 |    3
                (10 rows)""", false);
    }
}
