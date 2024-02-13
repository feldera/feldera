package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

/*
 * Tests manually adapted from
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/float8.out
 */
public class PostgresFloat8Part2Tests extends SqlIoTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        String prepareQuery = """
                CREATE TABLE FLOAT8_TBL(f1 float8);
                INSERT INTO FLOAT8_TBL(f1) VALUES ('    0.0   ');
                INSERT INTO FLOAT8_TBL(f1) VALUES ('-1004.30  ');
                INSERT INTO FLOAT8_TBL(f1) VALUES ('   -34.84');
                INSERT INTO FLOAT8_TBL(f1) VALUES ('-1.2345678901234e+200');
                INSERT INTO FLOAT8_TBL(f1) VALUES ('-1.2345678901234e-200');
                """;

        compiler.compileStatements(prepareQuery);
    }

    @Test
    public void testSelect() {
        this.qs("""
                SELECT * FROM FLOAT8_TBL;
                          f1
                -----------------------
                                     0
                                -34.84
                               -1004.3
                 -1.2345678901234e+200
                 -1.2345678901234e-200
                (5 rows)
                
                SELECT power(0, 0) + power(0, 1) + power(0, 0.0) + power(0, 0.5);
                 ?column?
                ----------
                        2
                (1 row)
                """
        );
    }

    @Test
    public void testsThatFailInPostgres() {
        this.qs("""
                -- this fails in Postgres with overflow but we return -inf
                SELECT f.f1 * '1e200' from FLOAT8_TBL f;
                            f1
                -------------------------
                                       0
                              -3.484e201
                        -1.2345678901234
                 -1.0042999999999999e203
                               -Infinity
                (5 rows)
                
                -- postgres overflows
                SELECT power(f.f1, 1e200) from FLOAT8_TBL f;
                            f1
                -------------------------
                                       0
                                       0
                                Infinity
                                Infinity
                                Infinity
                (5 rows)
                
                SELECT f.f1 / '0.0' from FLOAT8_TBL f;
                            f1
                -------------------------
                               -Infinity
                               -Infinity
                               -Infinity
                               -Infinity
                                     NaN
                (5 rows)
                
                SELECT exp(f.f1) from FLOAT8_TBL f;
                            f1
                -------------------------
                                       0
                                       0
                                       1
                                       1
                 0.0000000000000007399123060905129
                (5 rows)
                """
        );
    }

    // https://github.com/feldera/feldera/issues/1363
    @Test
    public void testLn() {
        this.qs("""
                -- postgres doesn't allow ln(0)
                SELECT log10(f.f1) from FLOAT8_TBL f where f.f1 = '0.0';
                            f1
                -------------------------
                               -Infinity
                (1 row)
                """
        );
    }

    @Test
    public void testSelectFails() {
        this.qf("SELECT ln(f.f1) from FLOAT8_TBL f where f.f1 < '0.0'", "Unable to calculate");
    }
}
